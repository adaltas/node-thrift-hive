
should = require 'should'
config = require './config'
hive = if process.env.HIVE_COV then require '../lib-cov/hive' else require '../lib/hive'

client = null
before ->
  client = hive.createClient config
after ->
  client.end()

describe 'escape', ->
  it 'should honor "--" and "/* */"', (next) ->
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
      should.not.exist err
    .on 'end', (query) ->
      count_before.should.eql 2
      query.should.eql "show databases"
      next()
