
Stream = require 'stream'
each = require 'each'

thrift = require 'thrift'
transport = require 'thrift/lib/thrift/transport'
EventEmitter = require('events').EventEmitter

split = module.exports.split = (hqls) ->
  return hqls if Array.isArray hqls
  commented = false # Are we in a commented mode
  hqls = hqls.split('\n').filter( (line) -> 
    line = line.trim()
    skip = false # Should we skip the current line
    if not commented and line.indexOf('/*') isnt -1
      commented = '/*' 
      skip = true
    else if not commented and line is '--'
      commented = '--' 
      skip = true
    else if commented is '/*' and line.lastIndexOf('*/') isnt -1 and line.lastIndexOf('*/') is (line.length - 2)
      commented = false 
      skip = true
    else if commented is '--' and line is '--'
      commented = false 
      skip = true
    skip = true if line.indexOf('--') is 0
    not commented and not skip
  ).join('\n')
  hqls = hqls.split ';'
  hqls = hqls.map (query) -> query.trim()
  hqls = hqls.filter (query) -> query.indexOf('--') isnt 0 and query isnt ''

module.exports.createClient = (options = {}) ->
  options.version ?= '0.7.1-cdh3u3'
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
        if err
          emitter.readable = false
          # emit error only if
          # - an error callback or
          # - no error callback, no both callback, no user callback
          lerror = emitter.listeners('error').length
          lboth = emitter.listeners('both').length
          emitError = lerror or (not lerror and not lboth and not callback )
          emitter.emit 'error', err if emitError
        else
          emitter.emit 'end', null, query
        emitter.emit 'both', err, query
        callback err, callback if callback
    emitter
  query: (query, size) ->
    if arguments.length is 2 and typeof size is 'function'
      callback = size
      size = null
    exec = ->
      emitter.emit 'before', query
      client.execute query, (err) ->
        if err
          emitter.readable = false
          # emit error only if
          # - an error callback or
          # - no error callback and no both callback
          lerror = emitter.listeners('error').length
          lboth = emitter.listeners('both').length
          emitError = lerror or (not lerror and not lboth) # and not callback if we add callback support
          emitter.emit 'error', err if emitError
          emitter.emit 'both', err, query
          return
        fetch()
    process.nextTick exec if query
    buffer = []
    #emitter = new EventEmitter
    count = 0
    emitter = new Stream
    emitter.size = size
    emitter.readable = true
    emitter.paused = 0
    emitter.query = (q) ->
      throw new Error 'Query already defined' if query
      query = q
      exec()
      @
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
        # - an error callback or
        # - no error callback and no both callback
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
      if rows.length is emitter.size
        fetch() unless emitter.paused
      else
        emitter.emit 'row-last', row, count - 1
        emitter.readable = false
        emitter.emit 'end', query
        emitter.emit 'both', null, query
    fetch = ->
      return if emitter.paused or not emitter.readable
      if emitter.size
      then client.fetchN emitter.size, handle
      else client.fetchAll handle
    emitter
  multi_execute: (hqls, callback) ->
    emitter = new EventEmitter
    hqls = split(hqls)
    each(hqls)
    .on 'item', (next, query) =>
      exec = @execute query, next
      exec.on 'before', -> emitter.emit.call emitter, 'before', arguments...
    .on 'both', (err) ->
      if err
      then emitter.emit.call emitter, 'error', arguments...
      else emitter.emit.call emitter, 'end', arguments...
      emitter.emit.call emitter, 'both', arguments...
      callback err if callback
    emitter
  multi_query: (hqls, size) ->
    hqls = split(hqls)
    query = @query()
    each(hqls)
    .on 'item', (next, hql, i) =>
      unless hqls.length is i + 1
        exec = @execute hql#, next
        exec.on 'before', -> query.emit.call query, 'before', arguments...
        exec.on 'error', (err) ->
          query.readable = false
          # emit error only if
          # - an error callback or
          # - no error callback and no both callback
          lerror = query.listeners('error').length
          lboth = query.listeners('both').length
          emitError = lerror or (not lerror and not lboth)
          query.emit 'error', err if emitError
          query.emit 'both', err, query
        exec.on 'end', -> next()
      else 
        query.query(hql, size)
    query
  
