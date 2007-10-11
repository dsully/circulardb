# $Id
#
# TODO: Backing store.

module CircularDB

  class Aggregate

    require 'rubygems'
    require 'circulardb_ext'

    EXTENSION = '.acdb'
    PREFIX    = 'Aggregate Part '

    attr_accessor :cdbs

    def initialize(name)

      @name     = name
      @instance = name

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
end
