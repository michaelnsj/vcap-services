# Copyright (c) 2009-2011 VMware, Inc.
require "base/provisioner"
require "elasticsearch_service/common"

class VCAP::Services::ElasticSearch::Provisioner < VCAP::Services::Base::Provisioner

  include VCAP::Services::ElasticSearch::Common

end
