#!/usr/bin/env coffee

    assert = require 'assert'
    hive = require '../../'
    
    client = hive.createClient
        version: '0.7.1-cdh3u2'
        server: '127.0.0.1'
        port: 10000
        timeout: 1000
    
    client.execute 'use my_db', (err) ->
        client.query 'select * from my_table limit 10', (err, results) ->
            assert.ifError(err)
            console.log(results)
            client.end()