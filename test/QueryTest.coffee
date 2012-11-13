
should = require 'should'
config = require './config'
hive = if process.env.HIVE_COV then require '../lib-cov/hive' else require '../lib/hive'

client = null
before ->
  client = hive.createClient config
after ->
  client.end()

describe 'Query', ->
  it 'Prepare', (next) ->
    client.execute "CREATE DATABASE IF NOT EXISTS #{config.db}", (err) ->
      should.not.exist err
      client.execute "USE #{config.db}", (err) ->
        should.not.exist err
        client.execute """
          CREATE TABLE IF NOT EXISTS #{config.table} ( 
            a_bigint BIGINT,
            an_int INT,
            a_date STRING
          )
          ROW FORMAT DELIMITED
          FIELDS TERMINATED BY ','
        """, (err) ->
          should.not.exist err
          client.execute """
          LOAD DATA LOCAL INPATH '#{__dirname}/data.csv' OVERWRITE INTO TABLE #{config.table}
          """, (err) ->
            should.not.exist err
            next()
  it 'all', (next) ->
    count = 0
    call_row_first = call_row_last = false
    client.query("SELECT * FROM #{config.table}")
    .on 'row', (row, index) ->
      index.should.eql count
      count++
      row.should.be.an.instanceof Array
      row.length.should.eql 3
    .on 'row-first', (row, index) ->
      count.should.eql 0
      call_row_first = true
    .on 'row-last', (row, index) ->
      count.should.eql 54
      call_row_last = true
    .on 'error', (err) ->
      false.should.not.be.ok
    .on 'end', ->
      count.should.eql 54
      call_row_first.should.be.ok
      call_row_last.should.be.ok
      next()
  it 'n', (next) ->
    count = 0
    client.query("select * from #{config.table}", 10)
    .on 'row', (row, index) ->
      index.should.eql count
      count++
    .on 'error', (err) ->
      false.should.not.be.ok
    .on 'end', ->
      count.should.eql 54
      next()
  it 'error', (next) ->
    error_called = false
    client.query("select * from undefined_table", 10)
    .on 'row', (row) ->
      false.should.not.be.ok
    .on 'error', (err) ->
      err.should.be.an.instanceof Error
      error_called = true
    .on 'end', ->
      false.should.not.be.ok
    .on 'both', (err) ->
      err.should.be.an.instanceof Error
      error_called.should.be.ok
      next()
  it 'pause/resume', (next) ->
    count = 0
    query = client.query("select * from #{config.table}", 10)
    .on 'row', (row, index) ->
      index.should.eql count
      count++
      query.pause()
      setTimeout ->
        query.resume()
      , 10
    .on 'error', (err) ->
      false.should.not.be.ok
    .on 'end', ->
      count.should.eql 54
      next()
  it 'header', (next) ->
    # Test where hive.cli.print.header impact Thrift
    # answer is no
    count = 0
    client.execute 'set hive.cli.print.header=true', (err) ->
      query = client.query("select * from #{config.table}", 10)
      .on 'row', (row, index) ->
        count++
      .on 'error', (err) ->
        false.should.not.be.ok
      .on 'end', ->
        count.should.eql 54
        next()
