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

    def initialize(name)

      @name     = name
      @instance = name
      @cdbs     = []

      if nil
        if @name =~ /#{EXTENSION}$/
          @name.sub!(/#{EXTENSION}/, '')
        end

        if @instance !~ /^(\/|\.)/
          @instance.gsub!(/[^\w\d_:\.\/-]/, '_')
        end

        if @instance !~ /#{EXTENSION}$/
          @instance << EXTENSION
        end
      end
    end

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

        # Smallest number of records is the driver.
        @cdbs = cdbs.sort { |a,b| a.num_records <=> b.num_records }.collect { |r| r }
      end

      @cdbs
    end

    def driver_start_time
      @cdbs[0].read_records(0, 0, -1)[0][0]
    end

    def driver_end_time
      @cdbs[0].read_records(0, 0, 0)[-1][0]
    end

    def num_records
      @cdbs[0].num_records
    end

    def last_updated
      @cdbs[0].last_updated
    end

    # Dummy - only used by graph right now. May need to make this real in the future.
    def aggregate_using_function_for_records(function = nil, start_time = 0, end_time = 0)
      1.0
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
