
thrift = require 'thrift'
transport = require 'thrift/lib/thrift/transport'
EventEmitter = require('events').EventEmitter

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
    query: (query, size) ->
        if arguments.length is 2 and typeof size is 'function'
            callback = size
            size = -1
        client.execute query, (err) ->
            if err
                emitter.emit 'error', err if emitter.listeners('error').length
                emitter.emit 'end', err
                return
            fetch()
        closed = false
        emitter = new EventEmitter
        emitter.paused = 0
        emitter.pause = ->
            @paused++
        emitter.resume = ->
            @paused--
            fetch() if @paused is 0
        handle = (err, rows) =>
            if err
                closed = true
                emitter.emit 'error', err
                emitter.emit 'end', err
                return
            rows = rows.map (row) -> row.split '\t'
            for row in rows
                emitter.emit 'row', row
            if rows.length is size
                fetch() 
            else
                closed = true
                emitter.emit 'success', err
                emitter.emit 'end'
        fetch = ->
            return if emitter.paused or closed
            if size
            then client.fetchN size, handle
            else client.fetchAll handle
        emitter
    
