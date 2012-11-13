#!/usr/bin/env node

var hive = require('thrift-hive');
// Client connection
var client = hive.createClient({
  version: '0.7.1-cdh3u2',
  server: '127.0.0.1',
  port: 10000,
  timeout: 1000
});
// Execute query
client.execute('use default', function(err){
  client.query('show tables')
  .on('row', function(database){
    console.log(database);
  })
  .on('error', function(err){
    console.log(err.message);
    client.end();
  });
  .on('end', function(){
    client.end();
  });
});
