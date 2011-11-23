
Stream = require 'stream'
each = require 'each'

thrift = require 'thrift'
transport = require 'thrift/lib/thrift/transport'
EventEmitter = require('events').EventEmitter

split = module.exports.split = (queries) ->
    return queries if Array.isArray queries
    queries = queries.split('\n').filter( (line) -> line.trim().indexOf('--') isnt 0 ).join('\n')
    queries = queries.split ';'
    queries = queries.map (query) -> query.trim()
    queries = queries.filter (query) -> query.indexOf('--') isnt 0 and query isnt ''

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
        exec = ->
            client.execute query, (err) ->
                if err
                    emitter.readable = false
                    emitter.emit 'error', err if emitter.listeners('error').length
                    return
                fetch()
        exec() if query
        buffer = []
        #emitter = new EventEmitter
        count = 0
        emitter = new Stream
        emitter.readable = true
        emitter.paused = 0
        emitter.query = (q) ->
            throw new Error 'Query already defined' if query
            query = q
            exec()
        emitter.pause = ->
            @paused = 1
        emitter.resume = ->
            @was = @paused
            @paused = 0
            fetch() if @was
        handle = (err, rows) =>
            if err
                emitter.readable = false
                emitter.emit 'error', err
                return
            rows = rows.map (row) -> row.split '\t'
            for row in rows
                emitter.emit 'row-first', row, 0 if count is 0
                emitter.emit 'row', row, count++
            if rows.length is size
                fetch() unless emitter.paused
            else
                emitter.emit 'row-last', row, count - 1
                emitter.readable = false
                emitter.emit 'end'
        fetch = ->
            return if emitter.paused or not emitter.readable
            if size
            then client.fetchN size, handle
            else client.fetchAll handle
        emitter
    multi_execute: (queries, callback) ->
        queries = split(queries)
        each(queries)
        .on 'item', (next, query) =>
            @execute query, next
        .on 'both', (err) -> callback err
    multi_query: (hqls, callback) ->
        hqls = split(hqls)
        each(hqls)
        .on 'item', (next, hql, i) =>
            unless hqls.length is i + 1
                @execute hql, next
            else 
                query.query(hql)
        .on 'both', (err) -> callback err
        query = @query()
        
    
