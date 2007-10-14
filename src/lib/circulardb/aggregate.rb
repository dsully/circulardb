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
            raise StandardError, "CircularDB types must be the same for aggregation."
          end

          if cdb.units != @units
            raise StandardError, "CircularDB units must be the same for aggregation."
          end
        end

        @cdbs = cdbs
      end

      @cdbs
    end

    def num_records
      cdbs[0].num_records
    end

    def last_updated
      cdbs[0].last_updated
    end

    def close
      if @cdbs.length > 0
        @cdbs.each { |cdb| cdb.close }
      end
    end

  end
end
