
assert = require 'assert'
hive = require "#{__dirname}/.."

db = 'test_database'
table = 'test_table'

client = hive.createClient require './config.json'

module.exports =
    'Multi # Query # String': (next) ->
        count_before = 0
        count_row = 0
        client.multi_query("""
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
        -- return data
        SELECT * FROM #{table};
        """)
        .on 'before', (query) ->
            count_before++
        .on 'row', (row) ->
            count_row++
        .on 'error', (err) ->
            assert.ifError err
        .on 'end', (query) ->
            assert.eql count_before, 4
            assert.eql count_row, 54
            assert.eql query, "SELECT * FROM #{table}"
            next()
    'Multi # Query # Error in execute # No callback': (next) ->
        count_before = 0
        count_error = 0
        client.multi_query("""
        -- Throw err
        Whow, that should throw an exception!;
        -- create db
        CREATE DATABASE IF NOT EXISTS #{db};
        """)
        .on 'before', (query) ->
            count_before++
        .on 'error', (err) ->
            count_error++
        .on 'end', (query) ->
            assert.ok false
        .on 'both', (err) ->
            assert.ok err instanceof Error
            assert.eql err.name, 'HiveServerException'
            assert.eql count_before, 1
            assert.eql count_error, 1
            next()
    'Close': (next) ->
        client.end()
        next()
