$:.unshift(File.dirname(__FILE__))
require "spec_helper"

describe "mongodb_node provision" do
  it "should be able to login" do
    begin
      conn = Mongo::MongoClient.new('localhost', 27017)
      user = conn.db('admin').add_user('test', 'test', false, :roles => [ 'root'])
      #conn.db('test').remove_user('test')
      conn.db('admin').authenticate('test', 'test')
      conn.db('test').command({:serverStatus => 1})
      
    ensure
      conn.close if conn
    end
  end
end
