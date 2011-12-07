
assert = require 'assert'
hive = require "#{__dirname}/.."

db = 'test_database'
table = 'test_table'

client = hive.createClient require './config.json'

module.exports =
    'Multi # Execute # String': (next) ->
        count_before = 0
        count_end = 0
        count_both = 0
        execute = client.multi_execute """
        -- create db
        CREATE DATABASE IF NOT EXISTS #{db};
        -- create table
        CREATE TABLE IF NOT EXISTS #{table} ( 
            a_bigint BIGINT,
            an_int INT,
            a_date STRING
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ',';
        -- load data
        LOAD DATA LOCAL INPATH '#{__dirname}/data.csv' OVERWRITE INTO TABLE #{table};
        """, (err) ->
            assert.ifError err
            assert.eql count_before, 3
            assert.eql count_end, 1
            assert.eql count_both, 1
            next()
        execute.on 'before', (query) ->
            count_before++
        execute.on 'end', (query) ->
            count_end++
        execute.on 'both', (query) ->
            count_both++
    'Multi # Execute # Error': (next) ->
        count_before = 0
        count_error = 0
        count_both = 0
        execute = client.multi_execute """
        -- Throw err
        Whow, that should throw an exception!;
        -- create db
        CREATE DATABASE IF NOT EXISTS #{db};
        """, (err) ->
            assert.ok err instanceof Error
            assert.eql err.name, 'HiveServerException'
            assert.eql count_before, 1
            assert.eql count_error, 1
            assert.eql count_both, 1
            next()
        execute.on 'before', (query) ->
            count_before++
        execute.on 'error', (err) ->
            assert.ok err instanceof Error
            assert.eql err.name, 'HiveServerException'
            count_error++
        execute.on 'both', (query) ->
            count_both++
    'Close': (next) ->
        client.end()
        next()
