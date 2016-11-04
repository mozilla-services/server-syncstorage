VIRTUALENV = virtualenv
NOSE = local/bin/nosetests -s
TESTS = syncstorage/tests
PYTHON = local/bin/python
PIP = local/bin/pip
PIP_CACHE = /tmp/pip-cache.${USER}
BUILD_TMP = /tmp/syncstorage-build.${USER}
PYPI = https://pypi.python.org/simple

export MOZSVC_SQLURI = sqlite:///:memory:

# Hackety-hack around OSX system python bustage.
# The need for this should go away with a future osx/xcode update.
ARCHFLAGS = -Wno-error=unused-command-line-argument-hard-error-in-future

INSTALL = ARCHFLAGS=$(ARCHFLAGS) $(PIP) install -U -i $(PYPI)

.PHONY: all build test

all:	build

build:
	$(VIRTUALENV) --no-site-packages --distribute ./local
	$(INSTALL) --upgrade Distribute pip
	$(INSTALL) -r requirements.txt
	$(PYTHON) ./setup.py develop

test:
	$(INSTALL) nose flake8
	# Check that flake8 passes before bothering to run anything.
	# This can really cut down time wasted by typos etc.
	./local/bin/flake8 syncstorage
	# Run the actual testcases.
	$(NOSE) $(TESTS)
	# Test that live functional tests can run correctly, by actually
	# spinning up a server and running them against it.
	./local/bin/gunicorn --paste ./syncstorage/tests/tests.ini --workers 1 --worker-class mozsvc.gunicorn_worker.MozSvcGeventWorker & SERVER_PID=$$! ; sleep 2 ; ./local/bin/python syncstorage/tests/functional/test_storage.py http://localhost:5000 ; kill $$SERVER_PID
