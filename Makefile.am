include $(srcdir)/bones-common/powerset-automake.am
DESTDIR ?= /
 
pkgconfig_DATA = @PACKAGE@.pc
@PACKAGE@.pc: pkgconfig-template.pc
	ln -f pkgconfig-template.pc @PACKAGE@.pc
 
install-exec-hook:
	@if test -e src/Rakefile ; then cd src && rake package > /dev/null ; fi
	@test -n "$(DESTDIR)" && $(mkdir_p) $(DESTDIR)
	stage_gem install -i $(DESTDIR) --local src/pkg/@GEM_NAME@-@PACKAGE_VERSION@.gem

check-local:
	@if test -e src/Rakefile ; then cd src && rake test ; fi

clean-local:
	@-cd src && rake clean
	@-rm -f @PACKAGE@.pc
