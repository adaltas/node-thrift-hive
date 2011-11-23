
assert = require 'assert'
hive = require "#{__dirname}/.."

db = 'test_database'
table = 'test_table'

client = hive.createClient require './config.json'

module.exports =
    'Multi # Execute # String': (next) ->
        client.multi_execute """
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
            next()
    'Multi # Query # String': (next) ->
        count = 0
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
        .on 'row', (row) ->
            count++
        .on 'error', (err) ->
            assert.ifError err
        .on 'end', (row) ->
            assert.eql count, 54
            next()
    'Close': (next) ->
        client.end()
        next()
