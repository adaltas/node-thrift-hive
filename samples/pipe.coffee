#!/usr/bin/env coffee

fs = require 'fs'
hive = require 'thrift-hive'
# Client connection
client = hive.createClient
  version: '0.7.1-cdh3u2'
  server: '127.0.0.1'
  port: 10000
  timeout: 1000
# Execute query
client.query('show tables')
.on 'row', (database) ->
  this.emit 'data', 'Found ' + database + '\n'
.on 'error', (err) ->
  client.end()
.on 'end', () ->
  client.end()
.pipe( fs.createWriteStream "#{__dirname}/pipe.out" )
