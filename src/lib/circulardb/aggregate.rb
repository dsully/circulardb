# $Id
#
# TODO: Backing store.

module CircularDB

  class Aggregate

    require 'rubygems'
    require 'fileutils'
    require 'tempfile'

    begin
      require 'GSL'
      include GSL
    rescue LoadError => e
      puts "GSL (http://ruby-gsl.sourceforge.net/) is not installed. Exiting (#{e})"
      exit
    end

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

      @scratch_dir = Tempfile.new('gnuplot' << Time.now.to_i.to_s).close!.path
    end

    def read_records(start_time = 0, end_time = 0, num_req = 0)

      @cdbs[0].read_records(start_time, end_time, num_req)
    end

    def print_records(start_time = 0, end_time = 0, num_req = 0, outfh = STDOUT, for_graphing = 0)

      if not File.exists?(@scratch_dir)
        FileUtils.mkdir_p @scratch_dir
      end

      data_files = Array.new
      real_start = Array.new
      real_end   = Array.new
      parsed     = Hash.new

      # We use a hashtable to count how many time is used a cdb's name.
      # So if a cdb's name is used twice or more, we will use the cdb's 
      # longer name instead
      # XXX - utility function
      cdb_list = Hash.new
      cdb_list.default = 0

      @cdbs.each do |cdb|
        cdb_list[cdb.name || "Unknown"] = 1
      end

      @cdbs.each do |cdb|

        name = cdb.name

        # If the cdb name is used more than once we use it's longer name
        if cdb_list[name] > 1
          # TODO - need to implement longer_name
          name = cdb.longer_name
        end

        if name !~ /^PREFIX/
            name = "#{PREFIX} #{name}"
        end

        data_file = File.join(@scratch_dir, File.basename(cdb.filename) + ".dat")
        read_data = Hash.new

        data_files << data_file

        read_start, read_end = nil, nil

        unless File.exists?(data_file)

          File.open(data_file, File::RDWR|File::CREAT) do |fh|
            read_start, read_end = cdb.print_records(start_time, end_time, nil, fh, nil, for_graphing)
          end

        end

        File.open(data_file).each do |line|

          time, value = line.strip.split
          time  = time.to_i
          value = value.to_f

          read_start = time if read_start.nil?
          read_data[time] = value
          read_end = time
        end

        parsed[data_file] = read_data

        real_start << read_start
        real_end   << read_end
      end

      # do the linear interpolation
      driver = data_files.shift

      driver_x_values     = Array.new
      interpolated_values = Hash.new

      # setup initial xy mapping
      parsed[driver].sort { |a,b| a[0] <=> b[0] }.each do |data|
        driver_x_values << data[0]
        interpolated_values[data[0]] = data[1]
      end

      accel  = Interpolation::Accel.new
      interp = Interpolation::Interp.new(Interpolation::LINEAR, driver_x_values.size)

      data_files.each do |follower|

        follower_x_values = Array.new
        follower_y_values = Array.new

        parsed[follower].sort { |a,b| a[0] <=> b[0] }.each { |data|
          follower_x_values << data[0]
          follower_y_values << data[1]
        }

        driver_x_values.each do |x|

          interp.init(follower_x_values, follower_y_values)

          yi = interp.eval(follower_x_values, follower_y_values, x, accel)

          interpolated_values[x] += yi
        end
      end

      interpolated_values.sort { |a,b| a[0] <=> b[0] }.each do |data|
        outfh.puts "#{data[0]} #{data[1]}"
      end

      return real_start[0], real_end[0]

    end
  end
end
