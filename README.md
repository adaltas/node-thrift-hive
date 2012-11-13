# Thrift Hive - Hive client using the Apache Thrift RPC system

Hive client with the following main features:
- fetch rows with optional batch size
- implement Node Readable Stream API (including `pipe`)
- hive multiple version support
- multiple query support through the `multi_execute` and `multi_query` functions
- advanced comments parsing

The project export the [Hive API][1] using [Apache Thrift RPC system][2]. It 
support multiple versions and a readable stream API.

## Installation

```
npm install thrift-hive
```

## Quick example

```javascript
var hive = require('thrift-hive');
// Client connection
var client = hive.createClient({
  version: '0.7.1-cdh3u2',
  server: '127.0.0.1',
  port: 10000,
  timeout: 1000
});
// Execute call
client.execute('use default', function(err){
  // Query call
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
    Execute a query and, when done, call the provided callback with an optional 
    error.
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
    Emitted for each row returned by Hive. Contains a two arguments, the row
    as an array and the row index.
-   `row-first`
    Emitted after the first row returned by Hive. Contains a two arguments, 
    the row as an array and the row index (always 0).
-   `row-last`
    Emitted after the last row returned by Hive. Contains a two arguments, 
    the row as an array and the row index.
-   `error`
    Emitted when the connection failed or when Hive return an error.
-   `end`
    Emitted when there are no more rows to retrieve, not called if there was
    an error before.
-   `both`
    Convenient event combining the `error` and `end` events. Emitted when an
    error occured or when there are no more rows to retrieve. Return the same 
    arguments than the `error` or `end` event depending on the operation 
    outturn.

The `client.query` function return a Node [readable stream][4]. It is possible to 
pipe the data into a [writable stream][5] but it is your responsibility to emit
the `data` event, usually inside the `row` event.

The following code written in CoffeeScript is an example of piping data returned by the query into a [writable stream][5].

```coffeescript
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
```

## Navite Thrift API

Here's the same example as the one in the "Quick example" section but using the 
native thrift API.

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
```

## Multi queries

For conveniency, we've added two functions, `multi_execute` and `multi_query` which
may run multiple requests in sequential mode inside a same client connection. They 
are both the same except how the last query is handled:

-   `multi_execute` will end with an `execute` call, thus it's API is the same 
    as the `execute` function.
-   `multi_query` will end with a `query` call, thus it's API is the same 
    as the `query` function.

They accept the same arguments as their counterpart but the query may be an 
array or a string of queries. If it is a string, it will be split into multiple 
queries. Note, the parser is pretty light, removing ';' and comments but it 
seems to do the job.

## Testing

Run the samples:

```bash
node samples/execute.js
node samples/query.js
node samples/style_native.js
node samples/style_sugar.js
```

Run the tests with `expresso`:

Hive must be started with Thrift support. By default, the tests will connect to
Hive Thrift server on the host `localhost` and the port `10000`. Edit the file
"./test/config.json" if you wish to change the connection settings used accross
the tests. A database `test_database` will be created if it does not yet exist
and all the tests will run on it.

```bash
npm install -g expresso
expresso -s
```

[1]: http://hive.apache.org  "Apache Hive"
[2]: http://thrift.apache.org  "Apache Thrift"
[3]: http://nodejs.org/docs/v0.6.2/api/events.html#events.EventEmitter  "EventEmitter API"
[4]: http://nodejs.org/docs/v0.6.2/api/streams.html#readable_Stream  "Readable Stream API"
[5]: http://nodejs.org/docs/v0.6.2/api/streams.html#writable_Stream  "Writable Stream API"
