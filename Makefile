APPNAME = server-syncstorage
DEPS = 
VIRTUALENV = virtualenv
NOSE = bin/nosetests -s --with-xunit
TESTS = syncstorage/tests
PYTHON = bin/python
EZ = bin/easy_install
COVEROPTS = --cover-html --cover-html-dir=html --with-coverage --cover-package=keyexchange
COVERAGE = bin/coverage
PYLINT = bin/pylint
PKGS = syncstorage
EZOPTIONS = -U -i $(PYPI)
PYPI = http://pypi.python.org/simple
PYPI2RPM = bin/pypi2rpm.py --index=$(PYPI)
PYPIOPTIONS = -i $(PYPI)
BUILDAPP = bin/buildapp
BUILDRPMS = bin/buildrpms
PYPI = http://pypi.python.org/simple
CHANNEL = dev
RPM_CHANNEL = dev
PIP_CACHE = /tmp/pip-cache.${USER}
BUILD_TMP = /tmp/syncstorage-build.${USER}
INSTALL = bin/pip install
INSTALLOPTIONS = -U -i $(PYPI)

ifdef PYPIEXTRAS
	PYPIOPTIONS += -e $(PYPIEXTRAS)
	INSTALLOPTIONS += -f $(PYPIEXTRAS)
endif

ifdef PYPISTRICT
	PYPIOPTIONS += -s
	ifdef PYPIEXTRAS
		HOST = `python -c "import urlparse; print urlparse.urlparse('$(PYPI)')[1] + ',' + urlparse.urlparse('$(PYPIEXTRAS)')[1]"`

	else
		HOST = `python -c "import urlparse; print urlparse.urlparse('$(PYPI)')[1]"`
	endif

endif

INSTALL += $(INSTALLOPTIONS)

.PHONY: all build test cover build_rpms mach update

all:	build

build:
	$(VIRTUALENV) --no-site-packages --distribute .
	$(INSTALL) Distribute
	$(INSTALL) MoPyTools
	$(INSTALL) nose
	$(INSTALL) coverage
	$(INSTALL) WebTest
	$(BUILDAPP) -c $(CHANNEL) $(PYPIOPTIONS) $(DEPS)
	# repoze.lru install seems to conflict with repoze.who.
	# reinstalling fixes it
	./bin/pip uninstall -y repoze.lru
	$(INSTALL) repoze.lru
	# NOTE: we don't install pyzmq and related dependencies here.
	# They're not needed by default and they require extra system-level
	# libraries to build.  If you want them, run `make build_rpms` and
	# it will install them into the virtualenv.


update:
	$(BUILDAPP) -c $(CHANNEL) $(PYPIOPTIONS) $(DEPS)

test:
	$(NOSE) $(TESTS)

cover:
	$(NOSE) --with-coverage --cover-html --cover-package=syncstorage $(TESTS)

build_rpms:
	rm -rf rpms
	mkdir -p  rpms ${BUILD_TMP}
	$(BUILDRPMS) -c $(RPM_CHANNEL) $(PYPIOPTIONS) $(DEPS)
	# Install cython for zmq-related builds.
	$(INSTALL) cython
	# PyZMQ sdist bundles don't play nice with pypi2rpm.
	# We need to build from a checkout of the tag.
	# Also install it into the build env so gevent_zeromq will build.
	wget -O ${BUILD_TMP}/pyzmq-2.1.11.tar.gz https://github.com/zeromq/pyzmq/tarball/v2.1.11
	bin/pip install ${BUILD_TMP}/pyzmq-2.1.11.tar.gz
	$(PYPI2RPM) --dist-dir=$(CURDIR)/rpms ${BUILD_TMP}/pyzmq-2.1.11.tar.gz
	rm -f ${BUILD_TMP}/pyzmq-2.1.11.tar.gz
	# We need some extra patches to gevent_zeromq, use our forked version.
	# Explicitly set PYTHONPATH for the build so that it picks up the local
	# version of PyZMQ that we built above.
	wget -O ${BUILD_TMP}/gevent-zeromq.zip https://github.com/mozilla-services/gevent-zeromq/zipball/532d3df654233f1b91e58a52be709475c0cd49da
	bin/pip install ${BUILD_TMP}/gevent-zeromq.zip
	PYTHONPATH=$(CURDIR)/lib/*/site-packages $(PYPI2RPM) ${BUILD_TMP}/gevent-zeromq.zip --dist-dir=$(CURDIR)/rpms
	rm -f ${BUILD_TMP}/gevent-zeromq.zip
	# The simplejson rpms conflict with a RHEL6 system package.
	# Do a custom build so that they can overwrite rather than conflict.
	rm -f $(CURDIR)/rpms/python26-simplejson-2.4.0-1.x86_64.rpm
	wget -O ${BUILD_TMP}/simplejson-2.4.0.tar.gz http://pypi.python.org/packages/source/s/simplejson/simplejson-2.4.0.tar.gz
	cd ${BUILD_TMP}; tar -xzvf simplejson-2.4.0.tar.gz
	cd ${BUILD_TMP}/simplejson-2.4.0; python setup.py  --command-packages=pypi2rpm.command bdist_rpm2 --binary-only --name=python-simplejson --dist-dir=$(CURDIR)/rpms
	rm -rf ${BUILD_TMP}/simplejson-2.4.0
	rm -f ${BUILD_TMP}/simplejson-2.4.0.tar.gz

mock: build build_rpms
	mock init
	mock --install python26 python26-setuptools
	cd rpms; wget http://mrepo.mozilla.org/mrepo/5-x86_64/RPMS.mozilla-services/libmemcached-devel-0.50-1.x86_64.rpm
	cd rpms; wget http://mrepo.mozilla.org/mrepo/5-x86_64/RPMS.mozilla-services/libmemcached-0.50-1.x86_64.rpm
	cd rpms; wget http://mrepo.mozilla.org/mrepo/5-x86_64/RPMS.mozilla-services/gunicorn-0.11.2-1moz.x86_64.rpm
	cd rpms; wget http://mrepo.mozilla.org/mrepo/5-x86_64/RPMS.mozilla/nginx-0.7.65-4.x86_64.rpm
	mock --install rpms/*
	mock --chroot "python2.6 -m syncstorage.run"
