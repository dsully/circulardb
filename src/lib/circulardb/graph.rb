module CircularDB

  class Graph

    require 'rubygems'
    require 'fileutils'
    require 'tempfile'

    begin
      require 'gnuplot'
    rescue LoadError
    end

    LEGEND_MAX_SIZE = 128
    SMALL_LEGEND_MAX_SIZE = 48
    ONE_HOUR = 60 * 60

    # Set the style of the graph. These are gnuplot styles. Possible values are:
    #
    # `lines`, `points`, `linespoints`, `impulses`, `dots`,
    # `steps`, `fsteps`, `histeps`, `errorbars`, `xerrorbars`, `yerrorbars`,
    # `xyerrorbars`, `boxes`, `boxerrorbars`, `boxxyerrorbars`, `financebars`,
    # `candlesticks` or `vector`
    #
    # GNUPlot 4.2 adds: filledcurves & histograms. Common usage would be:
    # 'filledcurves above x1'
    attr_accessor :style, :title, :fix_logscale, :type, :show_data, :show_trend, :logscale, :debug
    attr_reader :size

    def initialize(output, start_time = 0, end_time = 0, cdbs = [])

      if (start_time and end_time and start_time >= end_time)
        raise "Start time #{start_time} should be less than end time #{end_time}\n"
      end

      @output     = output
      @start_time = start_time
      @end_time   = end_time
      @cdbs       = cdbs
      @legend_max = SMALL_LEGEND_MAX_SIZE

      if File.extname(output) == ".svg"
        self.type = "svg"
      end

      self.style        = "lines"
      self.show_data    = 1
      self.show_trend   = 0
      self.fix_logscale = 0.0000001

      # For whatever reason, "tmpdir" doesn't exist as a standard Ruby library. This
      # seems like a reasonable substitute.
      @scratch_dir = Tempfile.new('gnuplot' << Time.now.to_i.to_s).close!.path

      self
    end

    def size=(size)

      @size = size

      if @size == "small"
        @legend_max = SMALL_LEGEND_MAX_SIZE
      else 
        @legend_max = LEGEND_MAX_SIZE
      end

      @size
    end

    # Generate the graph. It is possible to plot up to 8 different cdbs on the same
    # graph (we use 8 distinct colors).
    #
    # For it to be useful and comparable, it is recommended that data with similar
    # units be plotted on the same graph. This routine will handle at most 2 buckets
    # of dissimilar units, and plot them on y1 (left vertical axis) and y2 (right
    # vertical axis). For additional data that does not fit either of the buckets,
    # it will be simply not plotted.

    def graph

      x_start  = 0
      x_end    = 0
      ylabel   = nil
      y2label  = nil
      num_plots = 0
      for_graphing = 1

      axes   = Hash.new
      styles = [3, 1, 2, 9, 10, 8, 7, 13]

      unless File.exists?(@scratch_dir)
        FileUtils.mkdir_p @scratch_dir
      end

      # Debug gnuplot scripts with:
      #File.open("out.gplot", "w") do |gp|

      Gnuplot.open do |gp|
        Gnuplot::Plot.new(gp) do |plot|

          if @type == "svg"

            case size
              when "large"  then plot.terminal "svg size 1060 800 enhanced fname 'Trebuchet'"
              when "medium" then plot.terminal "svg size  840 600 enhanced fname 'Trebuchet'"
              else               plot.terminal "svg size  420 210 enhanced fname 'Tahoma' fsize 11"
            end

          else

            case size
              when "large"  then plot.terminal "png transparent small size 1060,800"
              when "medium" then plot.terminal "png transparent small size 840,600"
              else               plot.terminal "png transparent small size 450,250"
            end

          end

          if @output and @output !~ /^\s*$/

            dirname = File.dirname(@output)

            unless File.exists?(dirname)
              FileUtils.mkdir_p(dirname)
            end

            plot.output @output
          end

          if @logscale
            plot.logscale @logscale
          end

          if @title
            plot.title @title
          end

          plot.grid
          plot.key "below vertical"
          plot.xdata "time"
          plot.timefmt '"%s"'

          # We use a hashtable to count how many time is used a cdb's name.  So if a
          # cdb's name is used twice or more, we will use the cdb's longer name instead
          cdb_list = Hash.new
          cdb_list.default = 0

          @cdbs.each do |cdb|
            cdb_list[cdb.name] = 1
          end

          plots = @cdbs.size

          @cdbs.each do |cdb|

            if cdb.num_records == 0
              puts "No records to plot for: #{cdb.filename}"
              plots -= 1
              next
            end

            name = cdb.name.clone

            # If the cdb name is used more than once we use it's longer name
            if cdb_list[name] > 1
              # TODO - need to implement longer_name
              name = cdb.longer_name
            end

            name.gsub!(/Circular DB/, '')

            if cdb.type == "counter"
              name << " (#{cdb.units})"
            end

            if name.length > @legend_max
              name = name[0, ((@legend_max.length - 5 / 2))] << '[...]'
            end

            real_start = 0
            real_end   = 0

            # Check for empty and bogus values.
            sum = cdb.aggregate_using_function_for_records("sum", @start_time, @end_time)

            if sum.nan?
              puts "Sum is NaN for: #{cdb.filename}"
              plots -= 1
              next
            end

            # Write out the temporary data file for gnuplot to read.
            data_file = File.join(@scratch_dir, File.basename(cdb.filename) + ".dat")

            # Gnuplot can read data from a file much faster than us building up objects
            # for a dataset. Uses much less memory as well.
            File.open(data_file, File::RDWR|File::CREAT) { |fh|
              real_start, real_end = cdb.print_records(@start_time, @end_time, nil, fh, nil, for_graphing)
            }

            if File.stat(data_file).zero?
              puts "Data file is empty for: #{cdb.filename}"
              plots -= 1
              next
            end

            # Silence gnuplot warnings.
            if name =~ /temperature/i
              min = cdb.aggregate_using_function_for_records("min", @start_time, @end_time)
              max = cdb.aggregate_using_function_for_records("max", @start_time, @end_time)

              plot.yrange "[-#{min-1.0}:#{max+1.0}]" if min == max
            else 
              plot.yrange '[-1:1]' if sum == 0.0
            end

            axis = axes[cdb.units]

            unless axis

              if axes.length >= 1
                axis = 'x1y2'
              else
                axis = 'x1y1'
              end

              axes[cdb.units] = axis 
            end

            # Data can be processed on the fly by gnuplot. in the "using 1:2"
            # statement, "2" represents the y value to be read (2 means here
            # gnuplot has to read the 2nd column of data). So if a log
            # scale is used and the way to fix it is defined, each value below 0
            # will be replaced by the value provided by fix_logscale.
            yaxis = 2

            if @logscale and @fix_logscale

              # We use the ? operator :
              # condition ? TRUE : FALSE
              #
              # So if the y value is > 0 we use it.
              # If the y value <= 0, then we replace this value by the one provided
              #  by fixLogscale
              yaxis = '(2>0?2:' << @fix_logscale.to_s << ')'

              # If the value provided by fixLogscale is a numeric one
              # we signal to the user we have replaced all the values below zero by
              # the value provided by fixLogscale
              if @fix_logscale =~ /\d+\.?\d*/

                ylabel  << ' [' << @fix_logscale.to_s << ' means zero] '
                y2label << ' [' << @fix_logscale.to_s << ' means zero] '
              end
            end

            if @show_data or @show_trend

              # Fixup dev names.
              name.gsub!(/_/, '/')
              
              plot.cmd << " \"#{data_file}\" using 1:#{yaxis} "

              if @show_data and @show_trend == 0
                plot.cmd << "axes #{axis} title \"#{name}\" "
              elsif @show_trend
                plot.cmd << "smooth bezier axes #{axis} title \"#{name}\" "
              end

              plot.cmd << "with #{@style} lw 1.5 lt #{styles[num_plots]},"

              num_plots += 1
              num_plots %= styles.length
            end

            if x_start == 0
              x_start = real_start
            else
              x_start = real_start < x_start ? real_start : x_start
            end

            if x_end == 0
              x_end = real_end
            else 
              x_end = real_end > x_end ? real_end : x_end
            end
          end

          if plots == 0
            puts "Nothing to plot!"
            return
          end

          # Zap the trailing comma.
          plot.cmd.chop!

          axes.each_pair do |units,axis|

            # Default plot format - rounds to whole numbers and kilo/mega bytes
            if units =~ /bytes per/
              format = "\"%6.0s %cB\""
            else
              format = "\"%6.0s %c\""
            end

            # Automatically scale to percentage based
            if units == "percent"
              plot.yrange "[0:100]"
              format = "\"%3.0s %%\""
            end

            if units =~ /degrees/
              format = "\"%3.1s #{176.chr}\""
            end

            if axis =~ /y1/
              plot.format "y #{format}"
              plot.ylabel "\"#{units}\""
              plot.yrange "[0:100]" if units =~ /percent/
              ylabel = units
            else
              plot.format "y2 #{format}"
              plot.y2label "\"#{units}\""
              plot.y2range "[0:100]" if units =~ /percent/
              plot.y2tics
            end
          end

          plot.xrange "[\"#{x_start}\":\"#{x_end}\"]"

          # for plots longer than a quarter, skip day and hour information
          if (x_end - x_start <= 30 * ONE_HOUR)

            plot.format "x \"%H:%M\\n%m/%d\"" # day

          elsif (x_end - x_start <= 9 * 24 * ONE_HOUR)

            if (ylabel and ylabel == "per day")
              plot.format "x \"%b %d\""
            else
              plot.format "x \"%H:%M\\n%b %d\"" # week
            end

          elsif (x_end - x_start <= 35 * 24 * ONE_HOUR)
            plot.format "x \"%b %d\"" # month
          elsif (x_end - x_start <= 4 * 31 * 24 * ONE_HOUR)
            plot.format "x \"%b %d\"" # quarter
          else
            plot.format "x \"%m/%y\"" # > quarter
          end

        end
      end

      if File.exists?(@scratch_dir)
        FileUtils.rm_rf @scratch_dir
      end

    end

  end
end

# Override to_gplot to send the script to gnuplot without caring about data.
begin
  class Gnuplot::Plot

    def to_gplot (io = "")
      @sets.each { |var, val| io << "set #{var} #{val}\n" }
      io << @cmd << "\nunset output\n"
      io
    end
  end
rescue NameError
end
