#!/usr/bin/env node

hive = require 'thrift-hive'
# Client connection
client = hive.createClient
  version: '0.7.1-cdh3u2'
  server: '127.0.0.1'
  port: 10000
  timeout: 1000
# Execute
client.execute 'USE default', (err) ->
  console.log err.message if err
  client.end()
