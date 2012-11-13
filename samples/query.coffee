#!/usr/bin/env coffee

assert = require 'assert'
hive = require "#{__dirname}/.."

client = hive.createClient()

client.execute 'use test_database', (err) ->
  assert.ifError err
  query = client.query('select * from test_table limit 10', 10)
  .on 'row', (row) ->
    query.pause()
    setTimeout ->
      console.log row
      query.resume()
    , 100
  .on 'error', (err) ->
  .on 'end', () ->
    console.log err.message if err
    client.end()
      