#!/usr/bin/env ruby

require 'mkmf'

# Ugh. Hardcoded.

if ARGV[0] == "local"
  $CFLAGS  << " -I/home/dan/dev/circulardb/include"
  $LDFLAGS << " -L/home/dan/dev/circulardb/src/.libs"
else
  $CFLAGS  << " -I/p/include/circulardb-0"
  $LDFLAGS << " -L/p/lib"
end

exit unless have_header("circulardb.h")
exit unless have_library("circulardb", "cdb_new", "circulardb.h")

name = "circulardb_ext"

link_command("-lcirculardb")
dir_config(name)
create_makefile(name)
