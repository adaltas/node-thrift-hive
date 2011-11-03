# Thrift Hive - Hive client using the Apache Thrift RPC system.

The project export the Hive API throught Thrift. 

## Quick Example

```javascript
    var hive = require('thrift-hive');
    // Initialization
    var client = hive.createClient('0.7.1-cdh3u2',{
    	server: '127.0.0.1'
    	port: 10000
    	timeout: 1000
    });
```

