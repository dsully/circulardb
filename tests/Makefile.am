#
# The following target defines tests that you want
# to automatically run when make check occurs
#

mytests = \
	test_circulardb

test_circulardb_SOURCES = test_circulardb.c

test_circulardb_LDADD = \
	$(top_builddir)/src/libcirculardb.la \
	@PKGCONFIG_PREREQUISITES_LIBS@

check_PROGRAMS = ${mytests}

TESTS = $(check_PROGRAMS)

INCLUDES = \
	-I$(top_srcdir)/include \
	@PKGCONFIG_PREREQUISITES_CFLAGS@

include $(top_srcdir)/@ac_aux_dir@/powerset-make-rules.mk
