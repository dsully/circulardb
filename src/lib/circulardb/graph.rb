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

    @@gnuplot_fh = nil

    # Set the style of the graph. These are gnuplot styles. Possible values are:
    #
    # `lines`, `points`, `linespoints`, `impulses`, `dots`,
    # `steps`, `fsteps`, `histeps`, `errorbars`, `xerrorbars`, `yerrorbars`,
    # `xyerrorbars`, `boxes`, `boxerrorbars`, `boxxyerrorbars`, `financebars`,
    # `candlesticks` or `vector`
    #
    # GNUPlot 4.2 adds: filledcurves & histograms. Common usage would be:
    # 'filledcurves above x1'
    attr_accessor :style, :title, :fix_logscale, :output_format, :show_data, :show_trend, :logscale, :debug
    attr_accessor :start_time, :end_time
    attr_reader :output, :size, :cdbs

    def initialize(output = nil, start_time = nil, end_time = nil, cdbs = [])

      @legend_max = SMALL_LEGEND_MAX_SIZE

      self.output       = output
      self.start_time   = start_time
      self.end_time     = end_time
      self.cdbs         = cdbs

      self.style        = "lines"
      self.show_data    = 1
      self.show_trend   = 0
      self.fix_logscale = 0.0000001

      # Keep this around until the program ends.
      #if @@gnuplot_fh.nil?
      #  @@gnuplot_fh = IO::popen((Gnuplot.gnuplot(true) or raise 'gnuplot not found'), 'r+')
      #end
    end

    def size=(size)

      if size == "small"
        @legend_max = SMALL_LEGEND_MAX_SIZE
      else 
        @legend_max = LEGEND_MAX_SIZE
      end

      @size = size
    end

    # Takes a hash of name => cdb
    def add_cdbs(cdbs)

      @cdbs ||= Hash.new

      cdbs.each_pair do |name,cdb|

        if cdb.type == "counter"
          name = "#{name} (#{cdb.units})"
        end

        @cdbs[name] = cdb

      end
    end

    def cdbs=(cdbs)

      @cdbs = Hash.new

      # Handle the singular case. 
      if cdbs.class == CircularDB::Storage
        cdbs = [cdbs]
      end

      cdbs.each do |cdb|

        name = cdb.name.clone

        # Use a longer name to eliminate duplicates.
        if @cdbs.has_key?(name)
          name = "#{cdb.filename.split(/\//)[-2]} #{name} #{cdb.description}"
        end

        name.gsub!(/Circular DB/, '')

        if cdb.type == "counter"
          name << " (#{cdb.units})"
        end

        #if name.length > @legend_max
          #name = name[0, (@legend_max - 5 / 2)] << '[...]'
        #end

        @cdbs[name] = cdb

      end
    end

    def output=(output)

      if output 
        ext = nil

        if output.respond_to?('path')
          ext = Filename.extname(output.path)
        elsif output.kind_of?(String)
          ext = File.extname(output)
        else
        end

        if ext == ".svg" 
           self.output_format = "svg"
        end
      else
        # output is nil. because gnuplot is kinda lame, create a temporary file.
        @tmpfile = Tempfile.new('gnuplot')
        output   = @tmpfile.path
      end

      @output = output
    end

    def set_output(plot)

      if @output_format == "svg"

        case self.size
          when "large"  then plot.terminal "svg size 1060 800 enhanced fname 'Trebuchet'"
          when "medium" then plot.terminal "svg size  840 600 enhanced fname 'Trebuchet'"
          else               plot.terminal "svg size  420 210 enhanced fname 'Tahoma' fsize 11"
        end

      elsif @output_format == "pngcairo"

        case self.size
          when "large"  then plot.terminal "pngcairo transparent size 1060,800"
          when "medium" then plot.terminal "pngcairo transparent size 780,600"
          else               plot.terminal "pngcairo transparent size 650,350"
        end

      else

        case self.size
          when "large"  then plot.terminal "png transparent small size 1060,800"
          when "medium" then plot.terminal "png transparent small size 780,600"
          else               plot.terminal "png transparent small size 650,350"
        end

      end

      if @output

        if @output.class == 'String' and @output !~ /^\s*$/
          dirname = File.dirname(@output)

          unless File.exists?(dirname)
            FileUtils.mkdir_p(dirname)
          end
        end

        plot.output @output
      else
        # Set output to be empty like gnuplot wants.
        plot.sets << 'output'
      end
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

      if (@start_time and @end_time and @start_time >= @end_time)
        #raise "Start time #{@start_time} should be less than end time #{@end_time}\n"
      end

      # Debug gnuplot scripts with:
      #File.open("/tmp/out.gplot", "w+") do |gp|
 
      Gnuplot.open do |gp|
        Gnuplot::Plot.new(gp) do |plot|

          total_sum = 0.0

          set_output(plot)

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

          plots = @cdbs.size

          debug = false
          @cdbs.keys.sort.each do |name|

            cdb = @cdbs[name]

            records    = cdb.read_records(@start_time, @end_time, nil, for_graphing)

            if records.empty? or records.first.nil?
              puts "Busted read_records for: #{cdb.filename}"
              plots -= 1
              next
            end

            # 5 is arbitrary
            if records.length < 5
              puts "No records to plot for: #{cdb.filename}"
              plots -= 1
              next
            end

            real_start = records.first.first
            real_end   = records.last.first
            stats      = cdb.statistics

            # Check for empty and bogus values.
            sum = stats.sum

            if sum.kind_of?(Float) and sum.nan?
              puts "Sum is NaN for: #{cdb.filename}"
              plots -= 1
              next
            else
              total_sum += sum
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

            # Silence gnuplot warnings.
            if name =~ /temperature/i
              min = stats.min
              max = stats.max

              if min == max
                range = "[-#{min-1.0}:#{max+1.0}]"

                if axis == 'x1y1'
                  plot.yrange range
                else
                  plot.y2range range
                end
              end

            elsif sum < 1
              range = '[0:1]'
              
              if axis == 'x1y1'
                plot.yrange range
              else
                plot.y2range range
              end
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

              div = cdb.size

              # Divide by number of cdbs (aggregate) or 1.0 (single).
              # This should be configurable.
              x = []
              y = []

              mad    = stats.mad
              median = stats.median
        
              records.each do |r|
                # Rudimentary outlier rejection
                # Zi = 0.6745 * abs(x - median) / MAD
                #if ((0.6745 * (r[1] - median).abs) / mad) > 3.5
                  #next
                #end

                x << r[0]
                y << r[1] / div
              end

              plot.data << Gnuplot::DataSet.new([x, y]) do |ds|

                ds.title = name.gsub(/_/, '/')
                ds.with  = "#{@style} lw 2 lt #{styles[num_plots]}"

                if @show_data and @show_trend == 0
                  ds.using = "1:#{yaxis} axes #{axis}"
                elsif @show_trend
                  ds.using = "1:#{yaxis} smooth bezier axes #{axis}"
                end
              end

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

          axes.each_pair do |units,axis|

            # Default plot format - rounds to whole numbers and kilo/mega
            if units =~ /bytes per/
              format = "\"%6.0s %cB\""
            elsif units =~ /bits per/
              format = "\"%6.0s %cb\""
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

              if units =~ /percent/
                plot.yrange "[0:100]"
              elsif units =~ /bytes|bits/
                plot.yrange "[0:*]" unless total_sum == 0.0
              end

              ylabel = units
            else
              plot.format "y2 #{format}"
              plot.y2label "\"#{units}\""

              if units =~ /percent/
                plot.y2range "[0:100]"
              elsif units =~ /bytes|bits/
                plot.y2range "[0:*]" unless total_sum == 0.0
              end

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

      if @tmpfile
        @tmpfile.open
        graph = @tmpfile.read
        @tmpfile.close!
        return graph
      end

    end

    def close
      @cdbs.values.each { |cdb| cdb.close }
      @cdbs = nil
    end

  end
end

# Replace to have sets be a Hash instead of an Array.
module Gnuplot
  class Plot

    def initialize (io = nil, cmd = "plot")
      @cmd = cmd
      @sets = {}
      @data = []
      yield self if block_given?

      io << to_gplot if io
    end

    def set ( var, value = "" )
      value = "'#{value}'" if QUOTED.include? var unless value =~ /^'.*'$/
      @sets[var] = value
    end

    def to_gplot (io = "")
      @sets.each_pair { |var, val| io << "set #{var} #{val}\n" }

      if @data.size > 0 then
        io << @cmd << " " << @data.collect { |e| e.plot_args }.join(", ")
        io << "\n"

        v = @data.collect { |ds| ds.to_gplot }
        io << v.compact.join("e\n")
      end

      io
    end
  end
end

class Array
  def to_gplot_fast
    self.collect { |a| a.join(" ") }.join("\n") + "\ne"
  end
end
