all-local:
	$(ENVARGS) $(PYTHON) setup.py build_ext --inplace --debug

install-exec-hook:
	$(ENVARGS) $(PYTHON) setup.py install --prefix=$(DESTDIR)$(prefix)

clean-local:
	$(ENVARGS) $(PYTHON) setup.py clean --all

check-local:
	LD_LIBRARY_PATH=@top_srcdir@/src/.libs $(ENVARGS) $(PYTHON) tests/test_basic.py
