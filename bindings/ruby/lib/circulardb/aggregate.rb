# $Id
#
# TODO: Backing store.

module CircularDB

  class Aggregate

    require 'rubygems'
    require 'circulardb_ext'

    EXTENSION = '.acdb'
    PREFIX    = 'Aggregate Part '

    attr_reader :cdbs, :name, :type, :units

    def cdbs=(cdbs) 

      if cdbs.length > 0

        # Type & Units need to be the same.
        if @type.nil?
          @type = cdbs[0].type
        end

        if @units.nil?
          @units = cdbs[0].units
        end

        cdbs.each do |cdb|
          if cdb.type != @type
            raise StandardError, "CircularDB types must be the same for aggregation: #{cdb.type} != #{@type}\n" + cdb.filename
          end

          if cdb.units != @units
            raise StandardError, "CircularDB units must be the same for aggregation: #{cdb.units} != #{@units}\n" + cdb.filename
          end
        end

        # Largest number of records is the driver.
        @cdbs = cdbs.sort { |a,b| b.num_records <=> a.num_records }.collect { |r| r }
      end

      @cdbs
    end

    def reorder
      @cdbs << @cdbs.shift
    end

    def filename
      @cdbs[0].filename
    end

    def driver_start_time
      @cdbs[0].read_records(0, 0, -1)[0][0]
    end

    def driver_end_time
      @cdbs[0].read_records(0, 0, 0)[-1][0]
    end

    def min_value
      @cdbs[0].min_value
    end

    def max_value
      @cdbs[0].max_value
    end

    def num_records
      @cdbs[0].num_records
    end

    def last_updated
      @cdbs[0].last_updated
    end

    def size

      # Only aggregate percentage sizes by division, otherwise we want the
      # total - ie: Network traffic.
      if @units =~ /transmitted|recieved/
        return 1.0
      else
        return @cdbs.size.to_f
      end
    end

    def close
      if @cdbs.length > 0
        @cdbs.each { |cdb| cdb.close }
      end
    end

  end
end
