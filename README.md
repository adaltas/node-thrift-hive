# Thrift Hive - Hive client with multi versions support and a Readable Stream API.

The project export the [Hive API][1] using [Apache Thrift RPC system][2]. It 
support multiple versions and a readable stream API.

## Installation

```
    npm install thrift-hive
```

## Hive Client

We've added a function `hive.createClient` to simplify coding. However, you 
are free to use the raw Thrift API. The client take an `options` object as its 
argument andexpose an `execute` and a `query` methods.

Available options
-   `version`   
    default to '0.7.1-cdh3u2'
-   `server`   
    default to '127.0.0.1'
-   `port`   
    default to 10000
-   `timeout`   
    default to 1000 milliseconds

Available API

-   `client`   
    A reference to the thrift client returned by `thrift.createClient`
-   `connection`   
    A reference to the thrift connection returned by `thrift.createConnection`
-   `end([callback])`   
    Close the Thrift connection
-   `execute(query, [callback])`   
    Execute a query
-   `query(query, [size])`   
    Execute a query and return its results as an array of arrays (rows and 
    columns). The size argument is optional and indicate the number of row to 
    return on each fetch.

```coffeescript
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
```

## Hive Query

The `client.query` function implement the [EventEmitter API][3].

The following events are emitted:

-   `row`
-   `row-first`
-   `row-last`
-   `error`
-   `end`

The `client.query` functionreturn a Node [Readable Stream][4]. It is possible to 
pipe the data into a [Writable Stream][5] but it is your responsibility to emit
the `data` event, usually inside the `row` event.

## Raw versus sugar API

Here's an exemple using the raw API

```javascript
    var assert     = require('assert');
    var thrift     = require('thrift');
    var transport  = require('thrift/lib/thrift/transport');
	var ThriftHive = require('../lib/0.7.1-cdh3u2/ThriftHive');
	// Client connection
	var options = {transport: transport.TBufferedTransport, timeout: 1000};
	var connection = thrift.createConnection('127.0.0.1', 10000, options);
	var client = thrift.createClient(ThriftHive, connection);
    // Execute query
    client.execute('show databases', function(err){
        assert.ifError(err);
        client.fetchAll(function(err, databases){
            assert.ifError(err);
            console.log(databases);
            connection.end();
        });
    });
```

Here's an exemple using our sugar API

```javascript
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
```


[1]: http://hive.apache.org  "Apache Hive"
[2]: http://thrift.apache.org  "Apache Thrift"
[3]: http://nodejs.org/docs/v0.6.2/api/events.html#events.EventEmitter  "EventEmitter API"
[4]: http://nodejs.org/docs/v0.6.2/api/streams.html#readable_Stream  "Readable Stream API"
[5]: http://nodejs.org/docs/v0.6.2/api/streams.html#writable_Stream  "Writable Stream API"
