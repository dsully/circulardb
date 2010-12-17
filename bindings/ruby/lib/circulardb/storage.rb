module CircularDB

  class Storage

    require 'circulardb_ext'

    [:name, :filename, :max_records, :min_value, :max_value, :units, :type, :num_records].each do |meth|
      class_eval <<-METH
        def #{meth}; @header[:#{meth}]; end
      METH
    end

    [:name, :max_records, :min_value, :max_value, :units, :type].each do |meth|
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
  end
end
