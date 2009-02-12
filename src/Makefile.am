# The following targets can be used for shared objects and archives

cdb_read_SOURCES = \
	cdb_read.c

cdb_validate_SOURCES = \
	cdb_validate.cc

cdb_read_LDADD = \
	libcirculardb.la \
	@GSL_LIBS@

cdb_validate_LDADD = \
	libcirculardb.la \
	@GSL_LIBS@

bin_PROGRAMS = \
	cdb_read \
	cdb_validate

lib_sources = \
	circulardb.c

lib_LTLIBRARIES = \
	libcirculardb.la

libcirculardb_la_SOURCES = \
	${lib_sources}

libcirculardb_la_LIBADD = \
	@GSL_LIBS@

# libcirculardb_la_LDFLAGS =

INCLUDES = \
	-I$(top_srcdir)/include \
	@GSL_CFLAGS@
