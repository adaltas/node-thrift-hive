
should = require 'should'
config = require './config'
hive = if process.env.HIVE_COV then require '../lib-cov/hive' else require '../lib/hive'

client = null
before ->
  client = hive.createClient config
after ->
  client.end()

describe 'Multi # Query', ->
  it 'String', (next) ->
    count_before = 0
    count_row = 0
    client.multi_query("""
    -- create db
    CREATE DATABASE IF NOT EXISTS #{config.db};
    -- create table
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
    """)
    .on 'before', (query) ->
      count_before++
    .on 'row', (row) ->
      count_row++
    .on 'error', (err) ->
      should.not.exist err
    .on 'end', (query) ->
      count_before.should.eql 4
      count_row.should.eql 54
      query.should.eql "SELECT * FROM #{config.table}"
      next()
  it 'Error in execute # No callback', (next) ->
    count_before = 0
    count_error = 0
    client.multi_query("""
    -- Throw err
    Whow, that should throw an exception!;
    -- create db
    CREATE DATABASE IF NOT EXISTS #{config.db};
    """)
    .on 'before', (query) ->
      count_before++
    .on 'error', (err) ->
      count_error++
    .on 'end', (query) ->
      false.should.not.be.ok
    .on 'both', (err) ->
      err.should.be.an.instanceof Error
      err.name.should.eql 'HiveServerException'
      count_before.should.eql 1
      count_error.should.eql 1
      next()
