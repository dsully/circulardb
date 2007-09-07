module CircularDB

  class Storage

    require 'circulardb_ext'

    def filename
      @header[:filename]
    end

    def name(pretty = 0)
      name = @header[:name]

      if pretty and description and description !~ /Circular DB.*entries/
        name << description
      end

      name
    end

    def description
      @header[:description]
    end

    def description=(description) 
      _set_header("description", description)
    end

    def units
      @header[:units]
    end

    def units=(units)
      _set_header("units", units)
    end

    def type
      @header[:type]
    end

    def num_records
      @header[:num_records]
    end

    def last_updated
      @header[:last_updated]
    end

    def validate
      all_dates  = Hash.new
      nil_values = Array.new
      duplicates = Array.new
      wraps      = Array.new
      bad_dates  = Array.new

      prev_date  = -1;
      prev_value = -1;

      records = self.read_records

      records.each { |entry|
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

        if type == "counter" and value < prev_value
          wraps.push(date)
        end

        if date < prev_date
          bad_dates.push(date)
        end

        prev_date  = date
        prev_value = value
      }

      self.print_header

      ret = false

      if bad_dates.empty?
        puts "There are no out of order timestamps in this DB"
      else
        puts "Error: DB has #{bad_dates.size} record(s) with out of order timestamps"
        bad_dates.each { |date| puts date }
        ret = true
      end

      if duplicates.empty?
        puts "There are no duplicate timestamps in this DB"
      else
        puts "Error: DB has #{duplicates.size} record(s) with duplicate timestamps"
        duplicates.each { |date| puts date }
        ret = true
      end

      if nil_values.empty?
        puts "There are no nil/undef/null values in this DB"
      else
        puts "Error: DB has #{nil_values.size} record(s) with nil/undef/null values"
        nil_values.each { |date| puts date }
        ret = true
      end

      if wraps.empty?
        puts "There are no counter wraps in this DB"
      else
        puts "Error: DB has #{wraps.size} record(s) with counter wraps"
        wraps.each { |date| puts date }
        ret = true
      end

      ret
    end
  end
end
