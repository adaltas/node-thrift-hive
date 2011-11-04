
thrift = require 'thrift'
transport = require 'thrift/lib/thrift/transport'

module.exports.createClient = (options = {}) ->
    options.version ?= '0.7.1-cdh3u2'
    options.server ?= '127.0.0.1'
    options.port ?= 10000
    options.timeout ?= 1000
    options.transport ?= transport.TBufferedTransport
    connection = thrift.createConnection options.server, options.port, options
    client = thrift.createClient require("./#{options.version}/ThriftHive"), connection
    # Returned object
    connection: connection
    client: client
    end: connection.end.bind connection
    execute: (query, callback) ->
        client.execute query, (err) ->
            callback err if callback
    query: (query, size, callback) ->
        if arguments.length is 2 and typeof callback is 'function'
            callback = size
            size = -1
        client.execute query, (err) ->
            callback err if err and callback
            if size is -1
                client.fetchAll (err, data) ->
                    data = data.map (row) -> row.split '\t'
                    callback err, data if callback
            else
                client.fetchN size, (err, data) ->
                    data = data.map (row) -> row.split '\t'
                    callback err, data if callback
    
