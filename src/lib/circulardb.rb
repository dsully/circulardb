# $Id$

# stdlib requires
require 'rubygems'

# 3rd party rubygem requires

$:.unshift File.dirname(__FILE__) # For use/testing when no gem is installed

# internal requires

require 'circulardb/storage'
require 'circulardb/graph'
require 'circulardb/aggregate'

# gem version
# KEEP THE VERSION CONSTANT BELOW THIS COMMENT
# IT IS AUTOMATICALLY UPDATED FROM THE VERSION
# SPECIFIED IN configure.ac DURING PACKAGING

module CircularDB
  VERSION = '0.1.0'
end
