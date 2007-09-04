#!/usr/bin/ruby

$: << File.join(File.dirname(__FILE__), '..')
$: << File.join(File.dirname(__FILE__), '../lib')
$: << File.join(File.dirname(__FILE__), 'lib')

require 'circulardb/storage'
require 'circulardb/graph'
require 'find'

def graph_cdb(files, title, output)

  cdbs = files.collect { |file| CircularDB::Storage.new(file) }

  cdbg = CircularDB::Graph.new(output, nil, nil, cdbs)
  cdbg.title = title
  cdbg.size  = "large"
  #cdbg.style = "filledcurves above x1"
  cdbg.graph
end

def main

  Find.find("/tmp/collect") do |f|

    if f =~ /\.cdb/

      cdb = CircularDB::Storage.new(f)
      svg = File.basename(f).gsub(/\.cdb/, '.png')

      graph_cdb([f], cdb.name, "/home/daniel/public_html/ruby/collect/#{svg}")

    end
  end

end

main()
