module CircularDB

  class Graph

    require 'rubygems'
    require 'fileutils'
    require 'tempfile'
    #require 'tzinfo'

    begin
      require 'gnuplot'
    rescue LoadError
    end

    begin
      require 'facets/more/times'
    rescue LoadError
      require 'facets/times'
    end

    LEGEND_MAX_SIZE = 128
    SMALL_LEGEND_MAX_SIZE = 48

    # Look for a better font - Linux & Darwin
    ["/usr/share/fonts/bitstream-vera/Vera.ttf", "/usr/X11/lib/X11/fonts/TTF/Vera.ttf"].each do |f|
      if File.exists?(f)
        @@font = "font '#{f},9'"
        break
      else
        @@font = ""
      end
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
    attr_accessor :start_time, :end_time, :size, :aggregate, :average, :font, :minimal, :colors
    attr_reader :output, :size, :cdbs

    def initialize(output = nil, start_time = nil, end_time = nil, cdbs = [])

      # Force to an array. 
      if cdbs.kind_of?(CircularDB::Storage) 
        cdbs = [cdbs]
      end

      @legend_max = SMALL_LEGEND_MAX_SIZE
      @cooked     = true

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

    def title=(string)
      @title = string.gsub(/_/, '-')
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

        name.gsub!(/Circular DB/, ' ')
        name.gsub!(/^Mount Used: /, '')
        name.gsub!(/^System Utilized: /, '')
        name.gsub!(/^CPU Time for IO: /, '')
        name.gsub!(/^Average \w+ \w+: /, '')
        name.gsub!(/^(?:Read|Write) Requests: /, '')

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

      plot.terminal "#{@output_format || 'png'} size #{@size.join(',')} enhanced nocrop #{@font || @@font}"

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

          if @minimal
            plot.key "off"
          else
            plot.key "below horizontal"
          end

          plot.grid
          plot.xdata "time"
          plot.timefmt '"%s"'

          plots = @cdbs.size

          @cdbs.keys.sort.each do |name|

            cdb     = @cdbs[name]
            retries = 0
            records = []
      
            begin
              records = cdb.read_records(@start_time, @end_time, nil, @cooked, @average)
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

            # Aggregate data can trickle in, causing odd data. Slice off the last 5 records
            if cdb.kind_of?(CircularDB::Aggregate)
              #records.slice!(-5)
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

            # Check for empty and bogus values.
            #sum = cdb.statistics.sum
            sum = records[1].sum

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
              div = @aggregate == :sum ? 1.0 : cdb.size

              x = []
              y = []

              records.each do |r|
                x << r[0]
                y << r[1] / div
              end

              plot.data << Gnuplot::DataSet.new([x, y]) do |ds|

                ds.title = name.gsub(/_/, '-')
                ds.with  = "#{@style} lw 2 lt #{styles[num_plots]}"

                if @show_data and @show_trend == false
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
            puts "Nothing to plot!" if @debug
            return
          end

          ranges  = Hash.new
          formats = Hash.new

          axes.each_pair do |units,axis|

            if sums[axis] < 1
              ranges[axis] = "[0:1]"
            else
              ranges[axis] = "[0:*]"
            end

            # Default plot format - rounds to whole numbers and kilo/mega
            if units =~ /\bbytes\b/
              formats[axis] = "\"%5.0s %cB\""
            elsif units =~ /\bbits\b/
              formats[axis] = "\"%5.0s %cb\""
            elsif units =~ /percent/
              ranges[axis]  = "[0:100]"
              formats[axis] = "\"%3.0s %%\""
            elsif units == "milliseconds"
              formats[axis] = "\"%5.0f ms\""
            elsif units == "seconds"
              formats[axis] = "\"%3.2f s\""
            elsif units =~ /degrees/
              formats[axis] = "\"%3.1s #{176.chr}\""
            #else
            #  formats[axis] = "\"%5.0s %c\""
            end

            if formats[axis].nil?
              labels[axis] = "\"#{units}\""
            end

            if axis =~ /y1/
              plot.format "y #{formats[axis]}"
              plot.yrange  ranges[axis]
              plot.ylabel  labels[axis] unless @minimal
            else
              plot.format "y2 #{formats[axis]}"
              plot.y2range ranges[axis]
              plot.y2label  labels[axis] unless @minimal
              #plot.y2label "\"#{units}\""
              #plot.y2label y2label
              plot.y2tics
            end
          end

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

    def close
      @cdbs.values.each { |cdb| cdb.close }
      @cdbs = nil
    end

    def utc2local(t)
      ENV["TZ"] = 'US/Pacific'
      res = t.getlocal
      ENV["TZ"] = "UTC"
      res
    end

    def set_x_format(plot, x_start, x_end, ylabel)
      format = nil
      xtics  = nil
      delta  = x_end - x_start

      if (delta <= 2.hours)
        format = ":%M"
        xtics  = 10.minutes
      elsif (delta <= 1.5.days)
        format = "%H:%M"
        xtics  = 3.hours
      elsif (delta <= 8.days)
        format = "%a %d\\n%H:%M"
        xtics  = 18.hours
      elsif (delta <= 35.days)
        format = "Week %U"
        xtics  = 1.week
      elsif (delta <= 124.days)
        format = "%b"
        xtics  = 2.weeks
      else
        format = "%m"
        xtics  = 2.months
      end

      #now = Time.now
      #off = now.to_i - utc2local(now).to_i

      plot.xtics xtics if xtics
      plot.format "x \"#{format}\""
      plot.x2label "'Generated: #{Time.now}" unless @minimal
      #plot.xlabel "\"Generated: #{now}\\n#{TZInfo::Timezone.get('US/Pacific').utc_to_local(now)}\""
      plot.xrange "[\"#{x_start}\":\"#{x_end}\"]"
    end
  end
end

class Array
  def to_gplot_fast
    self.collect { |a| a.join(" ") }.join("\n") + "\ne"
  end

  def sum
    inject(0) { |sum, x| sum ? sum + x.abs : x.abs }
  end
end
