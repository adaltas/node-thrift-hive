
assert = require 'assert'
hive = require "#{__dirname}/.."
config = require './config'

client = hive.createClient config

module.exports =
    'Prepare': (next) ->
        client.execute "CREATE DATABASE IF NOT EXISTS #{config.db}", (err) ->
            assert.ifError err
            client.execute "USE #{config.db}", (err) ->
                assert.ifError err
                client.execute """
                    CREATE TABLE IF NOT EXISTS #{config.table} ( 
                        a_bigint BIGINT,
                        an_int INT,
                        a_date STRING
                    )
                    ROW FORMAT DELIMITED
                    FIELDS TERMINATED BY ','
                """, (err) ->
                    assert.ifError err
                    client.execute """
                    LOAD DATA LOCAL INPATH '#{__dirname}/data.csv' OVERWRITE INTO TABLE #{config.table}
                    """, (err) ->
                        assert.ifError err
                        next()
    'Query # all': (next) ->
        count = 0
        call_row_first = call_row_last = false
        client.query("SELECT * FROM #{config.table}")
        .on 'row', (row, index) ->
            assert.eql index, count
            count++
            assert.ok Array.isArray row
            assert.eql row.length, 3
        .on 'row-first', (row, index) ->
            assert.eql count, 0
            call_row_first = true
        .on 'row-last', (row, index) ->
            assert.eql count, 54
            call_row_last = true
        .on 'error', (err) ->
            assert.ok false
        .on 'end', ->
            assert.eql count, 54
            assert.ok call_row_first
            assert.ok call_row_last
            next()
    'Query # n': (next) ->
        count = 0
        client.query("select * from #{config.table}", 10)
        .on 'row', (row, index) ->
            assert.eql index, count
            count++
        .on 'error', (err) ->
            assert.ok false
        .on 'end', ->
            assert.eql count, 54
            next()
    'Query # error': (next) ->
        error_called = false
        client.query("select * from undefined_table", 10)
        .on 'row', (row) ->
            assert.ok false
        .on 'error', (err) ->
            assert.ok err instanceof Error
            error_called = true
        .on 'end', ->
            assert.ok false
        .on 'both', (err) ->
            assert.ok err instanceof Error
            assert.ok error_called
            next()
    'Query # pause/resume': (next) ->
        count = 0
        query = client.query("select * from #{config.table}", 10)
        .on 'row', (row, index) ->
            assert.eql index, count
            count++
            query.pause()
            setTimeout ->
                query.resume()
            , 10
        .on 'error', (err) ->
            assert.ok false
        .on 'end', ->
            assert.eql count, 54
            next()
    'Query # header': (next) ->
        # Test where hive.cli.print.header impact Thrift
        # answer is no
        count = 0
        client.execute 'set hive.cli.print.header=true', (err) ->
            query = client.query("select * from #{config.table}", 10)
            .on 'row', (row, index) ->
                count++
            .on 'error', (err) ->
                assert.ok false
            .on 'end', ->
                assert.eql count, 54
                next()
    'Close': (next) ->
        client.end()
        next()
