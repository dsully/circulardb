Makefile-perl.mk : Makefile.PL
	$(ENVARGS) $(PERL) Makefile.PL MAKEFILE=Makefile-perl.mk

all-local: Makefile-perl.mk
	$(MAKE) -f Makefile-perl.mk
	find blib -name 'Makefile*' -exec rm -f {} \;

check-local:
	$(MAKE) -f Makefile-perl.mk test

install-exec-local: Makefile-perl.mk
	$(MAKE) -f Makefile-perl.mk install DESTDIR=$(DESTDIR)$(prefix)

clean-local:
	if test -f Makefile-perl.mk ; then \
		$(MAKE) -f Makefile-perl.mk distclean ; \
	fi
	rm -f Makefile-perl.mk.old

EXTRA_DIST = \
	Changes \
	MANIFEST \
	Makefile.PL \
	CircularDB.xs \
	lib/CircularDB.pm \
	lib/CircularDB/Aggregate.pm \
	lib/CircularDB/Storage.pm \
	ppport.h \
	README \
	t/00use.t \
	t/01basic.t \
	t/02aggregation.t
