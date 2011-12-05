
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
        emitter = new EventEmitter
        process.nextTick ->
            emitter.emit 'before', query
            client.execute query, (err) ->
                #if err
                #then emitter.emit 'error', err
                #else emitter.emit 'end', query
                callback err, callback
        emitter
    query: (query, size) ->
        if arguments.length is 2 and typeof size is 'function'
            callback = size
            size = -1
        exec = ->
            emitter.emit 'before', query
            client.execute query, (err) ->
                if err
                    emitter.readable = false
                    # emit error only if
                    # - there is an error callback
                    # - there is no error callback and no both callback
                    lerror = emitter.listeners('error').length
                    lboth = emitter.listeners('both').length
                    emitError = lerror or (not lerror and not lboth)
                    emitter.emit 'error', err if emitError
                    emitter.emit 'both', err, query
                    return
                fetch()
        process.nextTick exec if query
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
                # emit error only if
                # - there is an error callback
                # - there is no error callback and no both callback
                lerror = emitter.listeners('error').length
                lboth = emitter.listeners('both').length
                emitError = lerror or (not lerror and not lboth)
                emitter.emit 'error', err if emitError
                emitter.emit 'both', err, query
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
                emitter.emit 'end', query
                emitter.emit 'both', null, query
        fetch = ->
            return if emitter.paused or not emitter.readable
            if size
            then client.fetchN size, handle
            else client.fetchAll handle
        emitter
    multi_execute: (queries, callback) ->
        emitter = new EventEmitter
        queries = split(queries)
        each(queries)
        .on 'item', (next, query) =>
            exec = @execute query, next
            exec.on 'before', -> emitter.emit.call emitter, 'before', arguments...
            exec.on 'error', -> emitter.emit.call emitter, 'error', arguments...
        .on 'both', (err) ->
            if err
            then emitter.emit.call emitter, 'error', arguments...
            else emitter.emit.call emitter, 'end', arguments...
            emitter.emit.call emitter, 'both', arguments...
            callback err if callback
        emitter
    multi_query: (hqls, callback) ->
        hqls = split(hqls)
        query = @query()
        each(hqls)
        .on 'item', (next, hql, i) =>
            unless hqls.length is i + 1
                exec = @execute hql, next
                exec.on 'before', -> query.emit.call query, 'before', arguments...
                exec.on 'error', -> query.emit.call query, 'error', arguments...
            else 
                query.query(hql)
        .on 'both', (err) -> callback err
        query
    
