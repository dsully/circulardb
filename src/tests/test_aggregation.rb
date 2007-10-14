require File.dirname(__FILE__) + '/test_helper'

require 'circulardb/aggregate'
require 'circulardb/storage'
require 'fileutils'
require 'tempfile'
require 'test/unit'

class TestAggregateCircularDB < Test::Unit::TestCase

  def setup
    @tempdir = Tempfile.new('circulardb').path
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

  def test_aggregate

    times = (0..10).collect { |i| Time.now.to_i + i }

    agg = CircularDB::Aggregate.new("test")
    assert(agg)

    (1..3).each do |i|

      records = Array.new

      (1..10).each { |j| records.push([ times[j], j.to_f ]) }

      cdb = CircularDB::Storage.new(File.join(@tempdir, "#{i}.cdb"), File::CREAT|File::RDWR|File::EXCL, nil, @name)
      cdb.write_records(records)
      agg.cdbs << cdb
    end

    read = agg.read_records

    assert_equal(10, read.length)
    assert_equal(10, agg.num_records)

    (0..9).each do |i|
      assert_equal((i+1)*3, read[i][1])
    end

    agg.close
  end

end
