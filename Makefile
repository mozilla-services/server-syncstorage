APPNAME = server-storage
DEPS = server-core
VIRTUALENV = virtualenv
NOSE = bin/nosetests -s --with-xunit
TESTS = syncstorage/tests
PYTHON = bin/python
EZ = bin/easy_install
COVEROPTS = --cover-html --cover-html-dir=html --with-coverage --cover-package=keyexchange
COVERAGE = bin/coverage
PYLINT = bin/pylint
PKGS = syncstorage
BUILDAPP = bin/buildapp
EZOPTIONS = -U -i $(PYPI)
PYPI = http://pypi.python.org/simple
PYPI2RPM = bin/pypi2rpm.py --index=$(PYPI)
PYPIOPTIONS = -i $(PYPI)

ifdef PYPIEXTRAS
	PYPIOPTIONS += -e $(PYPIEXTRAS)
	EZOPTIONS += -f $(PYPIEXTRAS)
endif

ifdef PYPISTRICT
	PYPIOPTIONS += -s
	ifdef PYPIEXTRAS
		HOST = `python -c "import urlparse; print urlparse.urlparse('$(PYPI)')[1] + ',' + urlparse.urlparse('$(PYPIEXTRAS)')[1]"`

	else
		HOST = `python -c "import urlparse; print urlparse.urlparse('$(PYPI)')[1]"`
	endif
	EZOPTIONS += --allow-hosts=$(HOST)
endif

EZ += $(EZOPTIONS)


.PHONY: all build test build_rpms mach

all:	build

build:
	$(VIRTUALENV) --no-site-packages --distribute .
	$(EZ) MoPyTools
	$(BUILDAPP) $(PYPIOPTIONS) $(APPNAME) $(DEPS)
	$(EZ) nose
	$(EZ) WebTest
	$(EZ) Funkload
	$(EZ) pylint
	$(EZ) coverage
	$(EZ) pypi2rpm
	$(EZ) wsgi_intercept
	$(EZ) wsgiproxy

test:
	$(NOSE) $(TESTS)

build_rpms:
	rm -rf $(CURDIR)/rpms
	mkdir $(CURDIR)/rpms
	rm -rf build; $(PYTHON) setup.py --command-packages=pypi2rpm.command bdist_rpm2 --spec-file=SyncStorage.spec --dist-dir=$(CURDIR)/rpms --binary-only
	cd deps/server-core; rm -rf build; ../../$(PYTHON) setup.py --command-packages=pypi2rpm.command bdist_rpm2 --spec-file=Services.spec --dist-dir=$(CURDIR)/rpms --binary-only
	$(PYPI2RPM) --dist-dir=$(CURDIR)/rpms cef --version=0.2
	$(PYPI2RPM) --dist-dir=$(CURDIR)/rpms WebOb --version=1.0.7
	$(PYPI2RPM) --dist-dir=$(CURDIR)/rpms Paste --version=1.7.5.1
	$(PYPI2RPM) --dist-dir=$(CURDIR)/rpms PasteDeploy --version=1.3.4
	$(PYPI2RPM) --dist-dir=$(CURDIR)/rpms PasteScript --version=1.7.3
	$(PYPI2RPM) --dist-dir=$(CURDIR)/rpms Mako --version=0.4.1
	$(PYPI2RPM) --dist-dir=$(CURDIR)/rpms MarkupSafe --version=0.12
	$(PYPI2RPM) --dist-dir=$(CURDIR)/rpms simplejson --version=2.1.6
	$(PYPI2RPM) --dist-dir=$(CURDIR)/rpms Routes --version=1.12.3
	$(PYPI2RPM) --dist-dir=$(CURDIR)/rpms SQLAlchemy --version=0.6.6
	$(PYPI2RPM) --dist-dir=$(CURDIR)/rpms WSGIProxy --version=0.2.2
	$(PYPI2RPM) --dist-dir=$(CURDIR)/rpms pylibmc
	$(PYPI2RPM) --dist-dir=$(CURDIR)/rpms PyMySQL
	$(PYPI2RPM) --dist-dir=$(CURDIR)/rpms pymysql_sa
	$(PYPI2RPM) --dist-dir=$(CURDIR)/rpms gevent
	$(PYPI2RPM) --dist-dir=$(CURDIR)/rpms greenlet
	$(PYPI2RPM) --dist-dir=$(CURDIR)/rpms python-memcached
	$(PYPI2RPM) --dist-dir=$(CURDIR)/rpms MySQL-python

mock: build build_rpms
	mock init
	mock --install python26 python26-setuptools libmemcached-devel libmemcached
	cd rpms; wget http://mrepo.mozilla.org/mrepo/5-x86_64/RPMS.mozilla-services/gunicorn-0.11.2-1moz.x86_64.rpm
	cd rpms; wget http://mrepo.mozilla.org/mrepo/5-x86_64/RPMS.mozilla/nginx-0.7.65-4.x86_64.rpm
	mock --install rpms/*
	mock --chroot "python2.6 -m syncstorage.run"
