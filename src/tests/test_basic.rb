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
    assert(cdb)
    assert_equal(cdb.filename, @file)
    assert_equal(cdb.name, @name)
    assert_equal(cdb.type, "gauge")
    assert_equal(cdb.units, "absolute")
    cdb.close
  end

  def test_rw

    records = Array.new
    now     = Time.now.to_i

    (1..10).each { |i|
      records.push([ now, i.to_f ])
      now += 1
    }

    cdb = CircularDB::Storage.new(@file, File::CREAT|File::RDWR|File::EXCL, nil, @name)
    assert(cdb)

    assert_equal(10, cdb.write_records(records))

    read = cdb.read_records

    assert_equal(10, read.length)
    assert_equal(10, cdb.num_records)

    # Can't use assert_same(records, read) here, because the object_id's will be different.
    records.each_with_index { |entry, i|
      assert_equal(records[i][0], read[i][0])
      assert_equal(records[i][1], read[i][1])
    }

    assert_equal(5.5, cdb.aggregate_using_function_for_records("average"))
    #assert_equal(5.5, cdb.aggregate_using_function_for_records("median"))
    assert_equal(55, cdb.aggregate_using_function_for_records("sum"))
    assert_equal(10, cdb.aggregate_using_function_for_records("max"))
    assert_equal(1, cdb.aggregate_using_function_for_records("min"))

    # Check setting a float and reading it back.
    assert_equal(1, cdb.update_record(records[5][0], 999.0005))

    float_check = cdb.read_records

    assert_equal(float_check[5][1], 999.0005)

    # Check setting value to nil
    assert_equal(1, cdb.update_record(records[6][0], nil))

    nil_check = cdb.read_records

    assert_equal(nil_check[6][1], nil)

    cdb.close
  end

end
