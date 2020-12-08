SYSTEMPYTHON = `which python2 pypy python | head -n 1`
VIRTUALENV = virtualenv --python=$(SYSTEMPYTHON)
NOSE = local/bin/nosetests -s
TESTS = syncstorage/tests
PYTHON = local/bin/python
EASY_INSTALL = local/bin/easy_install
PIP = local/bin/pip
PIP_CACHE = /tmp/pip-cache.${USER}
BUILD_TMP = /tmp/syncstorage-build.${USER}
PYPI = https://pypi.org/simple

export MOZSVC_SQLURI = sqlite:///:memory:

# Hackety-hack around OSX system python bustage.
# The need for this should go away with a future osx/xcode update.
ARCHFLAGS = -Wno-error=unused-command-line-argument-hard-error-in-future
CFLAGS = -Wno-error=write-strings

INSTALL = ARCHFLAGS=$(ARCHFLAGS) CFLAGS=$(CFLAGS) $(PIP) install -U -i $(PYPI)

.PHONY: all build test

all:	build

build:
	# The latest `pip` doesn't work with pypy 2.7 on some platforms.
	# Pin to a working version; ref https://github.com/pypa/pip/issues/8653
	$(VIRTUALENV) --no-pip ./local
	$(EASY_INSTALL) pip==20.1.1
	$(INSTALL) --upgrade "setuptools>=0.7"
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
	./local/bin/gunicorn --paste ./syncstorage/tests/tests.ini --workers 1 --worker-class mozsvc.gunicorn_worker.MozSvcGeventWorker & SERVER_PID=$$! ; sleep 2 ; ./local/bin/python syncstorage/tests/functional/test_storage.py http://localhost:5000 ; kill $$SERVER_PID

safetycheck:
	$(INSTALL) safety
	# Check for any dependencies with security issues.
	# We ignore a known issue with gevent, because we can't update to it yet.
	./local/bin/safety check --full-report --ignore 25837
