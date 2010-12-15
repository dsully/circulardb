# The following target defines tests that you want
# to automatically run when make check occurs

mytests = \
	test_circulardb

test_circulardb_SOURCES = test_circulardb.c

test_circulardb_LDADD = \
	$(top_builddir)/src/libcirculardb.la \
	@GSL_LIBS@ \
	@CHECK_LIBS@

check_PROGRAMS = ${mytests}

TESTS = $(check_PROGRAMS)

INCLUDES = \
	-I$(top_srcdir)/include \
	@GSL_CFLAGS@
	@CHECK_CFLAGS@
