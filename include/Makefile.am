#
# By default, this makefile will install all headers that exist
# in the current directory (with exception of any file named
# @PACKAGE_NAME@.h).  
#

all_headers := $(filter-out $(srcdir)/@PACKAGE_NAME@.h,$(wildcard $(srcdir)/*.h))

#
# Commenting out the following line will eliminate the ability
# for developers to #include _specific_ header files in your 
# package (and force them to use the meta-header instead).
# This can be useful if you anticipate frequent header changes
#

install-data-hook: detail-link

#
# Most things below this point should not require modification
#

majorincludedir = $(includedir)/@PACKAGE_NAME@-@MAJOR_VERSION@
majorinclude_HEADERS = \
	@PACKAGE_NAME@.h

versionedincludedir = $(majorincludedir)/@PACKAGE_NAME@-@PACKAGE_VERSION@
versionedinclude_HEADERS = ${all_headers}

@PACKAGE_NAME@.h: Makefile
	@(printf "/* This header file is automatically generated */\n\n"; \
	for hdr in ${all_headers}; do \
		echo "#include <@PACKAGE_NAME@-@PACKAGE_VERSION@/$$(basename $$hdr)>"; \
	done) &> $@

CLEANFILES = \
	@PACKAGE_NAME@.h

detail-link:
	cd $(DESTDIR)$(majorincludedir) && \
	$(LN_S) -f @PACKAGE_NAME@-@PACKAGE_VERSION@ @PACKAGE_NAME@

include $(top_srcdir)/@ac_aux_dir@/powerset-make-rules.mk
