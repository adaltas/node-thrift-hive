
thrift = require 'thrift'
transport = require 'thrift/lib/thrift/transport'

#module.exports = class Client
    #

module.exports.createClient = (options = {}) ->
    options.version ?= '0.7.1-cdh3u2'
    options.server ?= '127.0.0.1'
    options.port ?= 10000
    options.timeout ?= 1000
    options.transport ?= transport.TBufferedTransport
    connection = thrift.createConnection options.server, options.port, options
    client = thrift.createClient require("./#{options.version}/ThriftHive"), connection
    client.connection = connection
    client.end = connection.end
    client
    
