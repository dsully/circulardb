all-local:
	$(ENVARGS) python setup.py build_ext --inplace --debug

install-exec-hook:
	$(ENVARGS) python setup.py install

clean-local:

check-local:
	python tests/test_basic.py
