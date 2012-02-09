
assert = require 'assert'
hive = require "#{__dirname}/.."
config = require './config'

client = hive.createClient config

module.exports =
    'Multi # Escape': (next) ->
        count_before = 0
        count_row = 0
        client.multi_query("""
        -- 
        create db
        -- 
        CREATE DATABASE IF NOT EXISTS #{config.db};
        /*
        create table
        -- with some dash
        CREATE TABLE IF NOT EXISTS #{config.table} ( 
            a_bigint BIGINT,
            an_int INT,
            a_date STRING
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ',';
        -- load data
        LOAD DATA LOCAL INPATH '#{__dirname}/data.csv' OVERWRITE INTO TABLE #{config.table};
        -- return data
        SELECT * FROM #{config.table};
        */
        show databases;
        """)
        .on 'before', (query) ->
            count_before++
        .on 'row', (row) ->
            count_row++
        .on 'error', (err) ->
            assert.ifError err
        .on 'end', (query) ->
            assert.eql count_before, 2
            assert.eql query, "show databases"
            next()
    'Close': (next) ->
        client.end()
        next()
