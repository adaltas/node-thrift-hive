#!/usr/bin/env node

var assert   = require('assert');
var thrift   = require('thrift');
var transport  = require('thrift/lib/thrift/transport');
var ThriftHive = require('../lib/0.7.1-cdh3u2/ThriftHive');
// Client connection
var options = {transport: transport.TBufferedTransport, timeout: 1000};
var connection = thrift.createConnection('127.0.0.1', 10000, options);
var client = thrift.createClient(ThriftHive, connection);
// Execute query
client.execute('use default', function(err){
  client.execute('show tables', function(err){
    assert.ifError(err);
    client.fetchAll(function(err, databases){
      if(err){
        console.log(err.message);
      }else{
        console.log(databases);
      }
      connection.end();
    });
  });
});