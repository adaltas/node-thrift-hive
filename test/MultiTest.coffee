
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
    'Close': (next) ->
        client.end()
        next()
