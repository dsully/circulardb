module CircularDB

  class Storage

    require 'circulardb_ext'

    [:name, :filename, :description, :units, :type, :num_records].each do |meth|
      class_eval <<-METH
        def #{meth}; @header[:#{meth}]; end
      METH
    end

    [:name, :description, :units, :type].each do |meth|
      class_eval <<-METH
        def #{meth}=(value); _set_header("#{meth}", value); end
      METH
    end

    def <=>(cdb)
      cdb.num_records
    end

    def last_updated
      File.stat(self.filename).mtime.to_i
    end

    # The number of cdbs in a cdb. Aggregate provides a real size.
    def size
      1.0
    end

    def graph(output = nil, start_time = nil, end_time = nil)
      cdbg = CircularDB::Graph.new(output, start_time, end_time, self)
      cdbg.title = self.name
      cdbg.size  = 'medium'
      data = cdbg.graph
      cdbg.close
      data
    end

    def validate(verbose = false)
      all_dates  = Hash.new
      nil_values = Array.new
      duplicates = Array.new
      wraps      = Hash.new
      bad_dates  = Array.new

      prev_date  = -1;
      prev_value = -1;

      if self.num_records == 0
        puts "No records!"
      end

      # XXX - why isn't this setting cooked properly
      self.read_records.each { |entry|
        date  = entry[0]
        value = entry[1]

        if all_dates.has_key?(date)
          duplicates.push(date)
        else
          all_dates[date] = 1
        end

        if value.nil?
          nil_values.push(date)
        end

        #if type == "counter" and value < prev_value
        #  wraps[date] = "#{value} < #{prev_value}"
        #end

        if date < prev_date
          bad_dates.push(date)
        end

        prev_date  = date
        prev_value = value
      }

      if verbose
        #self.print_header
      end

      ret = true

      unless bad_dates.empty?
        if verbose
          puts "Error: DB has #{bad_dates.size} record(s) with out of order timestamps"
          bad_dates.each { |date| puts date }
        end
        ret = false
      end

      unless duplicates.empty?
        if verbose
          puts "Error: DB has #{duplicates.size} record(s) with duplicate timestamps"
          duplicates.each { |date| puts date }
        end
        ret = false
      end

      unless nil_values.empty?
        if verbose
          puts "Error: DB has #{nil_values.size} record(s) with nil/undef/null values"
          nil_values.each { |date| puts date }
        end
        ret = false
      end

      unless wraps.empty?
        if verbose
          puts "Error: DB has #{wraps.size} record(s) with counter wraps"
          wraps.each { |date| puts date }
        end
        ret = false
      end

      ret
    end
  end
end
