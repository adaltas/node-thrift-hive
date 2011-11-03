#!/usr/bin/env node

    var assert = require('assert');
    var hive = require('../../');
    // Client connection
    var client = hive.createClient({
        version: '0.7.1-cdh3u2',
        server: '127.0.0.1',
        port: 10000,
        timeout: 1000
    });
    // Execute with fetchAll
    client.execute('show databases', function(err){
        assert.ifError(err);
        client.fetchAll(function(err, databases){
            assert.ifError(err);
            console.log(databases);
            client.end();
        });
    });