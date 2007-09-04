#!/usr/bin/ruby

require 'mkmf'

# Ugh. Hardcoded.
$CFLAGS  << " -I/p/include/circulardb-0"
$LDFLAGS << " -L/p/lib"

exit unless have_header("circulardb.h")
exit unless have_library("circulardb", "cdb_new", "circulardb.h")

name = "circulardb_ext"

link_command("-lcirculardb")
dir_config(name)
create_makefile(name)
