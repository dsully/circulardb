require File.dirname(__FILE__) + '/test_helper'

require 'circulardb/storage'
require 'fileutils'
require 'tempfile'
require 'test/unit'

class TestCircularDB < Test::Unit::TestCase

  def setup
    @tempdir = Tempfile.new('circulardb').path
    @file    = File.join(@tempdir, 'basic.cdb')
    @name    = "Testing Ruby CDB"

    if File.exists?(@tempdir)
      FileUtils.rm_rf @tempdir
    end

    FileUtils.mkdir_p @tempdir
  end

  def teardown
    if File.exists?(@tempdir)
      FileUtils.rm_rf @tempdir
    end
  end

  def test_create
    cdb = CircularDB::Storage.new(@file, File::CREAT|File::RDWR|File::EXCL, nil, @name)
    cdb.max_value = 100
    assert(cdb)
    assert_equal(cdb.filename, @file)
    assert_equal(cdb.name, @name)
    assert_equal(cdb.type, :gauge)
    assert_equal(cdb.units, "absolute")
    assert_equal(cdb.min_value, 0)
    assert_equal(cdb.max_value, 100)
    cdb.close
  end

  def test_rw

    records = Array.new
    now     = Time.now

    flags       = File::CREAT|File::RDWR|File::EXCL
    mode        = nil
    max_records = nil
    type        = :gauge
    units       = "absolute"
    min_value   = 0
    max_value   = 0

    (1..10).each { |i|
      records.push([ now, i.to_f ])
      now += 1
    }

    cdb = CircularDB::Storage.new(@file, flags, mode, @name, max_records, type, units, min_value, max_value)
    assert(cdb)
    assert_equal(cdb.max_value, max_value)

    assert_equal(10, cdb.write_records(records))

    read = cdb.read_records

    assert_equal(10, read.length)
    assert_equal(10, cdb.num_records)

    # Can't use assert_same(records, read) here, because the object_id's will be different.
    records.each_with_index { |entry, i|
      assert_equal(records[i][0].to_i, read[i][0])
      assert_equal(records[i][1], read[i][1])
    }

    assert_equal(5.5, cdb.statistics.mean)
    assert_equal(5.5, cdb.statistics.median)
    assert_equal(55, cdb.statistics.sum)
    assert_equal(10, cdb.statistics.max)
    assert_equal(1, cdb.statistics.min)

    # Check setting a float and reading it back.
    assert_equal(1, cdb.update_record(records[5][0], 999.0005))

    float_check = cdb.read_records

    assert_equal(float_check[5][1], 999.0005)

    # Check setting value to nil
    assert_equal(1, cdb.update_record(records[6][0], nil))

    nil_check = cdb.read_records

    assert(nil_check[6][1].nan?)

    cdb.close
  end

  def test_counter_reset
    cdb = CircularDB::Storage.new(@file, File::CREAT|File::RDWR|File::EXCL, nil, @name, nil, :counter)
    assert(cdb)

    records = Array.new
    now     = Time.now.to_i

    [10, 11, 12, 0, 1].each_with_index { |val,i|
      records.push([ now+i, val.to_f ])
    }

    assert_equal(5, cdb.write_records(records))

    read = cdb.read_records(0, 0, 0, 1)

    assert_equal(10, read[0][1])
    assert_equal(1, read[1][1])
    assert_equal(1, read[2][1])
    assert(read[3][1].nan?)
    assert_equal(1, read[4][1])

    cdb.close
  end

  def test_step
    cdb = CircularDB::Storage.new(@file, File::CREAT|File::RDWR|File::EXCL, nil, @name, nil, :gauge)
    assert(cdb)

    records = Array.new
    start   = 1190860358

    20.times { |i| records.push([ start+i, i.to_f ]) }

    assert_equal(20, cdb.write_records(records))

    read = cdb.read_records(0, 0, 0, true, 5)

    # for first 5, should have time of 1190860360 and value of 2
    # for next  5, should have time of 1190860365 and value of 7
    assert_equal(4, read.size)

    assert_equal(1190860360, read[0][0])
    assert_equal(2, read[0][1])

    assert_equal(1190860365, read[1][0])
    assert_equal(7, read[1][1])

    assert_equal(1190860370, read[2][0])
    assert_equal(12, read[2][1])

    assert_equal(1190860375, read[3][0])
    assert_equal(17, read[3][1])

    cdb.close
  end

end
