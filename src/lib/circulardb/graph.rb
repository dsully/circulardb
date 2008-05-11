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
    ONE_HOUR  = 60 * 60
    VERA_FONT = '/usr/share/fonts/truetype/ttf-bitstream-vera/Vera.ttf' 

    if File.exists?(VERA_FONT)
      FONT = "font '#{VERA_FONT},9'"
    else
      FONT = ""
    end

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
    attr_accessor :start_time, :end_time, :size, :aggregate
    attr_reader :output, :size, :cdbs

    def initialize(output = nil, start_time = nil, end_time = nil, cdbs = [])

      # Force to an array. 
      if cdbs.kind_of?(CircularDB::Storage) 
        cdbs = [cdbs]
      end

      @legend_max = SMALL_LEGEND_MAX_SIZE

      self.output       = output
      self.start_time   = start_time
      self.end_time     = end_time
      self.cdbs         = cdbs

      self.style        = "lines"
      self.show_data    = true
      self.show_trend   = false
      self.fix_logscale = 0.0000001

      # Keep this around until the program ends.
      #if @@gnuplot_fh.nil?
      #  @@gnuplot_fh = IO::popen((Gnuplot.gnuplot(true) or raise 'gnuplot not found'), 'r+')
      #end
    end

    # Takes a hash of name => cdb
    def add_cdbs(cdbs)

      @cdbs ||= Hash.new

      cdbs.each_pair do |name,cdb|

        #if cdb.type == "counter"
        #  name = "#{name} (#{cdb.units})"
        #end

        @cdbs[name] = cdb

      end
    end

    def add_cdbs=(cdbs)
      add_cdbs(cdbs)
    end

    def cdbs=(cdbs)

      @cdbs = Hash.new

      # Handle the singular case. 
      if cdbs.kind_of?(CircularDB::Storage)
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

        @tmpfile = nil
      else
        # output is nil. because gnuplot is kinda lame, create a temporary file.
        @tmpfile = Tempfile.new('gnuplot')
        output   = @tmpfile.path
      end

      @output = output
    end

    def set_output(plot)

      if @size.kind_of?(String)
        case @size
          when "large"  then @size = [1100,450]
          when "medium" then @size = [900,400]
          when "small"  then @size = [700,350]
        end
      elsif !@size.kind_of?(Array)
        @size = [700,350]
      end

      plot.terminal "#{@output_format || 'png'} transparent size #{@size.join(',')} enhanced #{FONT}"

      if @output

        if @output.kind_of?(String) and @output !~ /^\s*$/
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
      num_plots = 0
      for_graphing = 1

      labels = Hash.new
      axes   = Hash.new
      sums   = Hash.new
      styles = [3, 1, 2, 9, 10, 8, 7, 13]

      if (@start_time and @end_time and @start_time >= @end_time)
        #raise "Start time #{@start_time} should be less than end time #{@end_time}\n"
      end

      # Debug gnuplot scripts with:
      #File.open("/tmp/out.gplot", "a+") do |gp|
 
      Gnuplot.open do |gp|
        Gnuplot::Plot.new(gp) do |plot|

          set_output(plot)

          if @logscale
            plot.logscale @logscale
          end

          if @title
            plot.title @title
          end

          plot.grid
          plot.key "below horizontal"
          plot.xdata "time"
          plot.timefmt '"%s"'

          plots = @cdbs.size

          @cdbs.keys.sort.each do |name|

            cdb     = @cdbs[name]
            retries = 0
            records = []
      
            begin
              records = cdb.read_records(@start_time, @end_time, nil, for_graphing)
            rescue RuntimeError => e
              if retries < 10 and cdb.respond_to?('reorder')
                cdb.reorder
                retries += 1
                retry
              else
                puts "#{cdb.filename}: #{e} - trying to continue. Retried #{retries} times."
                plots -= 1
                next
              end
            rescue Exception => e
              puts "#{cdb.filename}: #{e} - trying to continue"
              plots -= 1
              next
            end

            if records.empty? or records.first.nil?
              puts "Busted read_records for: #{cdb.filename}" if @debug
              plots -= 1
              next
            end

            # 5 is arbitrary
            if records.length < 5
              puts "No records to plot for: #{cdb.filename}" if @debug
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
            end

            axis = axes[cdb.units]

            unless axis
              axis = (axes.length >= 1) ? 'x1y2' : 'x1y1'
              axes[cdb.units] = axis 
            end

            # Keep track of sum per axis for display purposes
            sums[axis] ||= 0.0
            sums[axis] += sum

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

                labels["x1y1"]  << ' [' << @fix_logscale.to_s << ' means zero] '
                labels["x1y2"]  << ' [' << @fix_logscale.to_s << ' means zero] '
              end
            end

            if @show_data or @show_trend

              # Divide by number of cdbs (aggregate) or 1.0 (single).
              # If the caller has asked for a sum (aggregate), rather than the average.
              div = aggregate.eql?(:sum) ? 1.0 : cdb.size

              x = []
              y = []

              #if @show_trend
              #  left  = records.size-1
              #  average(records, x, y, 160, left)

              #else
                records.each do |r|
                  x << r[0]
                  y << r[1] / div
                end
              #end

              plot.data << Gnuplot::DataSet.new([x, y]) do |ds|

                ds.title = name.gsub(/_/, '-')
                ds.with  = "#{@style} lw 2 lt #{styles[num_plots]}"

                if @show_data and @show_trend == false
                  ds.using = "1:#{yaxis} axes #{axis}"
                elsif @show_trend
                  ds.using = "1:#{yaxis} smooth bezier axes #{axis}"
                  #ds.using = "1:#{yaxis} axes #{axis}"
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
            puts "Nothing to plot!" if @debug
            return
          end

          custom_format = false
          ranges  = Hash.new
          formats = Hash.new

          axes.each_pair do |units,axis|

            if sums[axis] < 1
              ranges[axis] = "[0:1]"
            else
              ranges[axis] = "[0:*]"
            end

            # Default plot format - rounds to whole numbers and kilo/mega
            if units =~ /bytes per/
              formats[axis] = "\"%5.0s %cB\""
            elsif units =~ /bits per/
              formats[axis] = "\"%5.0s %cb\""
            elsif units == "percent"
              ranges[axis]  = "[0:100]"
              formats[axis] = "\"%3.0s %%\""
            elsif units == "milliseconds"
              formats[axis] = "\"%5.0f ms\""
            elsif units == "seconds"
              formats[axis] = "\"%3.2f s\""
            elsif units =~ /degrees/
              formats[axis] = "\"%3.1s #{176.chr}\""
            elsif units =~ /per sec/ or units == "qps"
            else
              formats[axis] = "\"%5.0s %c\""
            end

            if formats[axis].nil?
              labels[axis] = "\"#{units}\""
            end

            if axis =~ /y1/
              plot.format "y #{formats[axis]}"
              plot.yrange  ranges[axis]
              plot.ylabel  labels[axis]
            else
              plot.format "y2 #{formats[axis]}"
              plot.y2range ranges[axis]
              plot.y2label  labels[axis]
              #plot.y2label "\"#{units}\""
              #plot.y2label y2label
              plot.y2tics
            end
          end

          plot.xrange "[\"#{x_start}\":\"#{x_end}\"]"
          set_x_format(plot, x_start, x_end, labels["x1y1"])
        end
      end

      if @tmpfile
        @tmpfile.open
        graph = @tmpfile.read
        @tmpfile.close!
        return graph
      end

    end

    # Quick hack (till it can make it in C) to generate 
    def average(records, x, y, step, left)

      (0..left).step(step) do |i|

        xi = []
        yi = []

        records[i,step].each do |r|
          xi << r[0]
          yi << r[1]
        end

        x << xi.mean
        y << yi.mean
        left -= step
      end
    end

    def close
      @cdbs.values.each { |cdb| cdb.close }
      @cdbs = nil
    end

    def set_x_format(plot, x_start, x_end, ylabel)
      format = nil

      # for plots longer than a quarter, skip day and hour information
      if (x_end - x_start <= 2 * ONE_HOUR)
        format = ":%M"
      elsif (x_end - x_start <= 24 * ONE_HOUR)
        format = "%H"
      elsif (x_end - x_start <= 30 * ONE_HOUR)
        format  = "%H:%M\\n%m/%d"
      elsif (x_end - x_start <= 9 * 24 * ONE_HOUR)
        if ylabel =~ /per day/
          format = "%b %d"
        else
          format = "%H\\n%d"
        end
      elsif (x_end - x_start <= 35 * 24 * ONE_HOUR)
        format = "%b %d"
      elsif (x_end - x_start <= 4 * 31 * 24 * ONE_HOUR)
        format = "%b %d"
      else
        format = "%m/%y"
      end

      plot.format "x \"#{format}\""
    end
  end
end

class Array

  def sum
    inject(0) { |sum, x| sum ? sum + x : x }
  end

  def mean
    return 0 if self.size == 0
    sum = 0
    self.each { |v| sum += v }
    sum / self.size.to_f
  end

  def to_gplot_fast
    self.collect { |a| a.join(" ") }.join("\n") + "\ne"
  end
end
