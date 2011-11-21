#!/usr/bin/env node

    var assert = require('assert');
    var hive = require('thrift-hive');
    // Client connection
    var client = hive.createClient({
        version: '0.7.1-cdh3u2',
        server: '127.0.0.1',
        port: 10000,
        timeout: 1000
    });
    // Execute query
    client.query('show databases')
    .on('row', function(database){
        console.log(database);
    })
    .on('end', function(err){
        assert.ifError(err);
        client.end();
    });
