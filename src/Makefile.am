#
# The following targets can be used for shared objects and archives
#

lib_sources = \
	circulardb.c

lib_LTLIBRARIES = \
	libcirculardb.la

libcirculardb_la_SOURCES = \
	${lib_sources}

libcirculardb_la_LIBADD = \
	@PKGCONFIG_PREREQUISITES_LIBS@

libcirculardb_la_LDFLAGS = \
	-version-info @MAJOR_VERSION@:@MINOR_VERSION@:0

INCLUDES = \
	-I$(top_srcdir)/include \
	@PKGCONFIG_PREREQUISITES_CFLAGS@

include $(top_srcdir)/@ac_aux_dir@/powerset-make-rules.mk
