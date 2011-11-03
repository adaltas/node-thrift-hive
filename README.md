# Thrift Hive - Hive client using the Apache Thrift RPC system.

The project export the Hive API throught Thrift. Multiple versions of hive are
supported. 

The only function added to the generated Thrift code is `hive.createClient`. It 
take an `options` object as its argument and return the result of 
`thrift.createClient`. This client object is enriched with a `connection` 
property to expose the object returned by `thrift.createConnection` as well as
with the `end` function as a shortcut to `connection.end`.

## Hive connection: suggar example

```javascript
    var hive = require('thrift-hive');
	// Client connection
    var client = hive.createClient({
    	version: '0.7.1-cdh3u2'
    	server: '127.0.0.1'
    	port: 10000
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
```
## Hive connection: raw example

```javascript
	var thrift     = require('thrift');
	var transport = require('thrift/transport');
	var ThriftHive = require('thrift-hive/0.7.1-cdh3u2/ThriftHive');
    // Client connection
	var options = {transport: transport.TBufferedTransport, timeout: 1000};
	var connection = thrift.createConnection('127.0.0.1', 10000, options);
	var client = thrift.createClient(ThriftHive, connection);
    // Execute with fetchAll
    client.execute('show databases', function(err){
        assert.ifError(err);
        client.fetchAll(function(err, databases){
            assert.ifError(err);
            console.log(databases);
            connection.end();
        });
    });
```

