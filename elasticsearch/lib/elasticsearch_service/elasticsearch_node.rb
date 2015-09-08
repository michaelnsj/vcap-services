# Copyright (c) 2009-2011 VMware, Inc.
require "erb"
require "fileutils"
require "logger"
require "set"
require "timeout"
require "uuidtools"
require "pp"
require "data_mapper"

require 'vcap/common'
require 'vcap/component'
require "elasticsearch_service/common"
require 'rest-client'
require 'net/http'

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), '..', '..', '..', 'base', 'lib')
require 'base/node'

module VCAP
  module Services
    module ElasticSearch
      class Node < VCAP::Services::Base::Node
      end
    end
  end
end

class VCAP::Services::ElasticSearch::Node

  include VCAP::Services::ElasticSearch::Common

  # timeout for es request like index status, process status etc.
  ES_TIMEOUT = 30

  class ProvisionedService
    include DataMapper::Resource
    property :name,       String,       :key => true
    property :cluster_name, String,     :required => true
    property :http_port,  Integer,      :required => true
    property :tcp_port,   Integer,      :required => true
    property :password,   String,       :required => true
    property :plan,       Enum[:free],  :required => true
    property :pid,        Integer
    property :username,   String,       :required => true

    def listening?
      begin
        TCPSocket.open('localhost', http_port).close
        return true
      rescue => e
        return false
      end
    end

    def running?
      VCAP.process_running? pid
    end

    def kill(sig=9)
      Process.detach(pid)
      Process.kill(sig, pid) if running?
    end

    def wait_killed(timeout=5, interval=0.2)
      begin
        Timeout::timeout(timeout) do
          while running? do
            sleep interval
          end
        end
      rescue Timeout::Error
        return false
      end
      true
    end
  end

  def initialize(options)
    super(options)

    @base_dir = options[:base_dir]
    FileUtils.mkdir_p(@base_dir)
    @elasticsearch_path = get_es_path(options[:exec_path])
    @pid_file = options[:pid]
    @max_memory = options[:max_memory]
    @capacity = options[:capacity]
    @logs_dir = options[:logs_dir]
    @master_data_dir = options[:master_data_dir]
    @host_name = options[:db_hostname]

    @config_template = ERB.new(File.read(options[:es_conf_template]))
    @logging_template = ERB.new(File.read(options[:logging_conf_template]))

    DataMapper.setup(:default, options[:local_db])
    DataMapper::auto_upgrade!

    @free_tcp_ports = Set.new
    @free_http_ports = Set.new
    options[:tcp_port_range].each {|port| @free_tcp_ports << port}
    options[:http_port_range].each {|port| @free_http_ports << port}
    @capacity_lock = Mutex.new
    @mutex = Mutex.new
    
    @managed_services = []
    
    %w(INT TERM).each{|signal|
      Signal.trap(signal) {
        @logger.warn("shutdown with signal #{signal}")
        shutdown
      }
    }
  end

  def pre_send_announcement
    @capacity_lock.synchronize do
      ProvisionedService.all.each do |provisioned_service|
        delete_ports({:tcp => provisioned_service.tcp_port, :http => provisioned_service.http_port})

        begin
          port_accupied, pid = false, nil

          if provisioned_service.listening?
            port_accupied = true
            pid = pid_by_keyword(provisioned_service.name)
          else
            pid = start_instance(provisioned_service, false)
          end

          if (pid.nil?)
            @logger.error("can\'t start the provision_service: #{provisioned_service.name}")
            @logger.error("The provisioned_service can't start, the port #{provisioned_service.http_port} is occupied, service: #{provisioned_service.name} ") if port_accupied
            next
          else
            provisioned_service.pid = pid.to_i
            raise "Cannot save provision_service #{provisioned_service.name} with pid: #{provisioned_service.pid}" unless provisioned_service.save
            @logger.info("service #{provisioned_service.name} is already ran with pid: #{pid}") if port_accupied
          end

          @capacity -= 1
        rescue => e
          provisioned_service.kill
          @logger.error("Error starting service #{provisioned_service.name}: #{e}")
        end
      end
    end
  end

  def shutdown
    super
    ProvisionedService.all.each do |service|
      stop_service(service)
    end

  end

  def announcement
    {:available_capacity => @capacity}
  end

  def all_instances_list
    ProvisionedService.all.map{ |ps| ps["name"] }
  end

  def all_bindings_list
    list = []
    ProvisionedService.all.each do |ps|
      if @managed_services.include?(ps.name)
        begin
          url = "http://#{ps.username}:#{ps.password}@#{@host_name}:#{ps.http_port}/_nodes/#{ps.name}"
          response = ''
          Timeout::timeout(ES_TIMEOUT) do
            response = RestClient.get(url)
          end
          credential = {
            'name' => ps.name,
            'port' => ps.tcp_port,
            'http_port' => ps.http_port,
            'username' => ps.username
          }
          list << credential if response =~ /"#{ps.name}"/
        rescue => e
          @logger.warn("Failed to fetch status for #{ps.name}: #{e.message}")
        end
      end
    end
    @logger.info("[all_bindings_list]: #{list}")
    list
  end

  def varz_details
    # Do disk summary
    du_hash = {}
    du_all_out = `cd #{@base_dir}; du -sk * 2> /dev/null`
    du_entries = du_all_out.split("\n")
    du_entries.each do |du_entry|
      size, dir = du_entry.split("\t")
      size = size.to_i * 1024 # Convert to bytes
      du_hash[dir] = size
    end

    # Get elasticsearch health, index & process status
    stats = []
    ProvisionedService.all.each do |provisioned_service|
      stat = {}
      stat['health'] = elasticsearch_health_stats(provisioned_service)
      stat['index'] = elasticsearch_index_stats(provisioned_service)
      stat['process'] = elasticsearch_process_stats(provisioned_service)
      stat['name'] = provisioned_service.name
      stats << stat
    end

    # Get service instance status
    provisioned_instances = {}
    begin
      ProvisionedService.all.each do |instance|
        provisioned_instances[instance.name.to_sym] = elasticsearch_status(instance)
      end
    rescue => e
      @logger.error("Error get instance list: #{e}")
    end

    {
      :running_services     => stats,
      :disk                 => du_hash,
      :available_capacity   => @capacity,
      :instances            => provisioned_instances
    }
  end

  def provision(plan, credentials = nil, version=nil)
    raise "Exceed the max capacity: #{@capacity}" if (@capacity <= 0)
    provisioned_service = ProvisionedService.new
    if credentials
      provisioned_service.name = credentials["name"]
      provisioned_service.username = credentials["username"]
      provisioned_service.password = credentials["password"]
    else
      provisioned_service.name = "elasticsearch-#{UUIDTools::UUID.random_create.to_s}"
      provisioned_service.username = UUIDTools::UUID.random_create.to_s
      provisioned_service.password = UUIDTools::UUID.random_create.to_s
    end

    provisioned_service.plan = plan
    pid = start_instance(provisioned_service, true)
    if pid.nil?
      @logger.error("Failed to provision service with plan: #{plan}, credentials: #{credentials}")
      return
    end
    
    raise "Could not save entry: #{provisioned_service.errors.pretty_inspect}" unless provisioned_service.save

    @managed_services << provisioned_service.name
    @capacity -= 1
    response = get_credentials(provisioned_service)
    @logger.debug("response: #{response}")

    return response
  rescue => e
    @logger.error("Error provision instance: #{e}")
    cleanup_service(provisioned_service)
    raise e
  end

  def unprovision(name, credentials = nil)
    provisioned_service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if provisioned_service.nil?

    cleanup_service(provisioned_service)
    @managed_services.delete(provisioned_service.name)
    @capacity += 1

    @logger.debug("Successfully fulfilled unprovision request: #{name}.")
  end

  # fake user name/password, es has no user level security
  def bind(name, bind_opts = 'rw', credentials = nil)
    @logger.debug("Bind request: name=#{name}, bind_opts=#{bind_opts}")

    provisioned_service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if provisioned_service.nil?

    response = get_credentials(provisioned_service)
    @logger.debug("response: #{response}")
    response
  end

  # fake user name/password, es has no user level security
  def unbind(credentials)
    @logger.debug("Unbind request: credentials=#{credentials}")

    name = credentials['name']
    provisioned_service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if provisioned_service.nil?

    @logger.debug("Successfully unbound #{credentials}")
    true
  end

  def start_instance(provisioned_service, new_service)
    if !new_service && (provisioned_service.http_port.nil? || provisioned_service.tcp_port.nil? || provisioned_service.cluster_name.nil?)
      raise "Either tcp port or http port is empty, skip the service #{provisioned_service.name}."
    end
    
    if (new_service)
      configs = setup_server(provisioned_service)
      
      provisioned_service.http_port = configs['http.port']
      provisioned_service.tcp_port = configs['transport.tcp.port']
      provisioned_service.cluster_name = configs['cluster.name']
    else
      configs = setup_server(provisioned_service)
    end

    pid_file = pid_file(provisioned_service.name)

    `export ES_HEAP_SIZE="#{@max_memory}m" && #{@elasticsearch_path} -p #{pid_file} -Des.config=#{configs['config.file']} -d`     
    status = $?
    @logger.info("Service start up finished, status = #{status}")

    pid = `[ -f #{pid_file} ] && cat #{pid_file}`
    status = $?
    @logger.info("Service #{provisioned_service.name} running with pid #{pid}, status = #{status}")

    # info per instance from runtime
    if pid
      provisioned_service.pid = pid.to_i
      return provisioned_service.pid
    end
    nil
  end

  def elasticsearch_health_stats(instance)
    url = "http://#{instance.username}:#{instance.password}@#{@host_name}:#{instance.http_port}/_cluster/health"
    response = nil
    Timeout::timeout(ES_TIMEOUT) do
      response = RestClient.get(url)
    end
    JSON.parse(response) if response
  rescue => e
    warning = "Failed elasticsearch_health_stats: #{e.message}, instance: #{instance.name}"
    @logger.warn(warning)
    warning
  end

  def elasticsearch_index_stats(instance)
    url = "http://#{instance.username}:#{instance.password}@#{@host_name}:#{instance.http_port}/_nodes/#{instance.name}/stats"
    response = nil
    Timeout::timeout(ES_TIMEOUT) do
      response = RestClient.get(url)
    end
    nodes = JSON.parse(response)['nodes']
    
    return nodes.flatten[1]['indices'] if nodes && !nodes.empty?
    return 'no nodes avaiable.'
  rescue => e
    warning = "Failed elasticsearch_index_stats: #{e.message}, instance: #{instance.name}"
    @logger.warn(warning)
    warning
  end

  def elasticsearch_process_stats(instance)
    url = "http://#{instance.username}:#{instance.password}@#{@host_name}:#{instance.http_port}/_nodes/#{instance.name}/process"
    response = nil
    Timeout::timeout(ES_TIMEOUT) do
      response = RestClient.get(url)
    end

    nodes = JSON.parse(response)['nodes']

    return nodes.flatten[1]['process'] if nodes && !nodes.empty?
    return 'no nodes avaiable.'
  rescue => e
    warning = "Failed elasticsearch_process_stats: #{e.message}, instance: #{instance.name}"
    @logger.warn(warning)
    warning
  end

  def elasticsearch_status(instance)
    url = "http://#{instance.username}:#{instance.password}@#{@host_name}:#{instance.http_port}/_nodes/#{instance.name}"
    Timeout::timeout(ES_TIMEOUT) do
      RestClient.get(url)
    end
    "ok"
  rescue => e
    "fail"
  end

  def get_credentials(provisioned_service)
    raise "Could not access provisioned service" unless provisioned_service
    credentials = {
      "hostname" => @host_name,
      "host"     => @host_name,
      "port"     => provisioned_service.tcp_port,
      "http_port"=> provisioned_service.http_port,
      "username" => provisioned_service.username,
      "password" => provisioned_service.password,
      "name"     => provisioned_service.name,
      "cluster_name" => provisioned_service.cluster_name
    }
    credentials["url"] = "http://#{credentials['username']}:#{credentials['password']}@#{credentials['host']}:#{credentials['http_port']}"
    credentials
  end

  def cleanup_service(provisioned_service)
    @logger.debug("Killing #{provisioned_service.name} started with pid #{provisioned_service.pid}")

    stop_service(provisioned_service, :SIGKILL)
    raise "Could not cleanup service: #{provisioned_service.errors.pretty_inspect}" unless provisioned_service.destroy

    EM.defer do
      FileUtils.rm_rf(service_dir(provisioned_service.name))
      FileUtils.rm_rf(log_dir(provisioned_service.name))
      FileUtils.rm_rf(pid_file(provisioned_service.name))
    end
    return_ports({:tcp => provisioned_service.tcp_port, :http => provisioned_service.http_port})

    true
  rescue => e
    @logger.warn(e)
  end

  def stop_service(service, signal=:SIGTERM)
    begin
      @logger.info("Stopping #{service.name} HTTP PORT #{service.http_port} TCP PORT #{service.tcp_port} PID #{service.pid}")
      service.kill(signal)
#      service.wait_killed ?
#        @logger.debug("elasticsearch pid:#{service.pid} terminated") :
#        @logger.error("Timeout to terminate elasticsearch pid:#{service.pid}")
    rescue => e
      @logger.error("Failed to stop service #{service.name} HTTP PORT #{service.http_port} TCP PORT #{service.tcp_port} PID #{service.pid}: #{e}")
    end
  end

  def fetch_ports()
    @mutex.synchronize do
      tcp_port = @free_tcp_ports.first
      @free_tcp_ports.delete(tcp_port)
      
      http_port = @free_http_ports.first
      @free_http_ports.delete(http_port)
      {:tcp => tcp_port, :http => http_port}
    end
  end

  def return_ports(ports)
    @mutex.synchronize do
      @free_tcp_ports << ports[:tcp]
      @free_http_ports << ports[:http]
    end
  end

  def delete_ports(ports)
    @mutex.synchronize do
      @free_tcp_ports.delete(ports[:tcp])
      @free_http_ports.delete(ports[:http])
    end
  end

  def setup_server(instance)
    instance_id = instance.name

    conf_dir = config_dir(instance_id)
    data_dir = data_dir(instance_id)
    work_dir = work_dir(instance_id)
    logs_dir = log_dir(instance_id)

    ports = if instance.http_port.nil? || instance.tcp_port.nil?
      fetch_ports()
    end
    
    # node.name, path.data, path.conf, path.logs and ports are specified to the instance
    other_conf = {
      'path.conf' => conf_dir,
      'path.data' => data_dir,
      'path.logs' => logs_dir,
      'http.enabled' => true,
      'http.port' => instance.http_port || ports[:http],
      'transport.tcp.port' => instance.tcp_port || ports[:tcp],
      'node.name' => instance_id,
      'cluster.name' => instance.cluster_name || UUIDTools::UUID.random_create.to_s
    }
    
    es_default_conf = @options[:elasticsearch]
    final_conf = es_default_conf.merge(other_conf)

    FileUtils.mkdir_p(final_conf['path.conf'])
    FileUtils.mkdir_p(final_conf['path.data'])
    FileUtils.mkdir_p(final_conf['path.logs'])

    config_file = gen_es_config(final_conf['path.conf'], final_conf)
    final_conf['config.file'] = config_file

    final_conf
  end
  
  # es is one java process
  def pid_by_keyword(keyword)
    val = `ps -f -C java | grep #{keyword} | awk '{ print $2 }'`
    return val.strip if val
  end

  def is_process_running?(pid_file)
    return false unless File.file?(pid_file)
    # get the file content
    pid = File.read(pid_file)
    system "ps -p #{pid} > /dev/null"
  end
  
  def get_es_path(es_path)
    real_path = File.readlink(es_path)
    File.join(real_path, 'bin', 'elasticsearch')
  rescue
    File.join(es_path, 'bin', 'elasticsearch')
  end

  def gen_es_config(config_dir, configurations)
    config_file = File.join(config_dir, 'elasticsearch.yml')
    File.open(config_file, "w") { |f| f.write(@config_template.result(binding)) }

    logging_file = File.join(config_dir, 'logging.yml')
    File.open(logging_file, "w") { |f| f.write(@logging_template.result(binding)) }

    config_file
  end
  
  def config_dir(instance_id)
    File.join(service_dir(instance_id), 'conf')
  end
  
  def data_dir(instance_id)
    File.join(service_dir(instance_id), 'data')
  end
  
  def work_dir(instance_id)
    File.join(service_dir(instance_id), 'work')
  end

  def log_dir(instance_id)
    File.join(@logs_dir, instance_id)
  end
  
  def pid_file(instance_id)
    File.join(@base_dir, "elasticsearch_#{instance_id}.pid")
  end
  
  def service_dir(instance_id)
    File.join(@base_dir, instance_id)
  end
end