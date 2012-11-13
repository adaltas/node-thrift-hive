
should = require 'should'
config = require './config'
hive = if process.env.HIVE_COV then require '../lib-cov/hive' else require '../lib/hive'

client = null
before ->
  client = hive.createClient config
after ->
  client.end()

describe 'Multi # Execute', ->
  it 'String', (next) ->
    count_before = 0
    count_end = 0
    count_both = 0
    execute = client.multi_execute """
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
    """, (err) ->
      should.not.exist err
      count_before.should.eql 3
      count_end.should.eql 1
      count_both.should.eql 1
      next()
    execute.on 'before', (query) ->
      count_before++
    execute.on 'end', (query) ->
      count_end++
    execute.on 'both', (query) ->
      count_both++
  it 'Error', (next) ->
    count_before = 0
    count_error = 0
    count_both = 0
    execute = client.multi_execute """
    -- Throw err
    Whow, that should throw an exception!;
    -- create db
    CREATE DATABASE IF NOT EXISTS #{config.db};
    """, (err) ->
      err.should.be.an.instanceof Error
      err.name.should.eql 'HiveServerException'
      count_before.should.eql 1
      count_error.should.eql 1
      count_both.should.eql 1
      next()
    execute.on 'before', (query) ->
      count_before++
    execute.on 'error', (err) ->
      err.should.be.an.instanceof Error
      err.name.should.eql 'HiveServerException'
      count_error++
    execute.on 'both', (query) ->
      count_both++
