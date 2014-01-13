VIRTUALENV = virtualenv
NOSE = local/bin/nosetests -s
TESTS = syncstorage/tests
PYTHON = local/bin/python
PIP = local/bin/pip
PIP_CACHE = /tmp/pip-cache.${USER}
BUILD_TMP = /tmp/syncstorage-build.${USER}
PYPI = https://pypi.python.org/simple
INSTALL = $(PIP) install -U -i $(PYPI)

.PHONY: all build test

all:	build

build:
	$(VIRTUALENV) --no-site-packages --distribute ./local
	$(INSTALL) Distribute
	$(INSTALL) pip
	$(INSTALL) nose
	$(INSTALL) flake8
	$(INSTALL) -r requirements.txt
	$(PYTHON) ./setup.py develop

test:
	# Check that flake8 passes before bothering to run anything.
	# This can really cut down time wasted by typos etc.
	./local/bin/flake8 syncstorage
	# Run the actual testcases.
	$(NOSE) $(TESTS)
	# Test that live functional tests can run correctly, by actually
	# spinning up a server and running them against it.
	./local/bin/paster serve syncstorage/tests/tests.ini & SERVER_PID=$$! ; sleep 2 ; ./local/bin/python syncstorage/tests/functional/test_storage.py http://localhost:5000 ; kill $$SERVER_PID
