#!/usr/bin/ruby

require 'rubygems'
require 'circulardb/storage'

ARGV.each { |f| CircularDB::Storage.new(f).print }
