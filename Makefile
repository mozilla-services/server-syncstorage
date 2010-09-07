VIRTUALENV = virtualenv
BIN = bin

.PHONY: all build check coverage test mysqltest redisqltest doc alltest

all:	build test

build:
	$(VIRTUALENV) --no-site-packages .
	$(BIN)/easy_install nose
	$(BIN)/easy_install coverage
	$(BIN)/easy_install flake8
	$(BIN)/python setup.py develop

check:
	rm -rf syncserver/templates/*.py
	$(BIN)/flake8 syncserver

coverage:
	$(BIN)/nosetests -s --cover-html --cover-html-dir=html --with-coverage --cover-package=syncserver syncserver
	WEAVE_TESTFILE=mysql $(BIN)/nosetests -s --cover-html --cover-html-dir=html --with-coverage --cover-package=syncserver syncserver 

test:
	$(BIN)/nosetests -s syncserver

mysqltest:
	WEAVE_TESTFILE=mysql $(BIN)/nosetests -s syncserver

redisqltest:
	WEAVE_TESTFILE=redisql $(BIN)/nosetests -s syncserver

ldaptest:
	WEAVE_TESTFILE=ldap $(BIN)/nosetests -s syncserver


alltest: test mysqltest redisqltest ldaptest

doc:
	$(BIN)/sphinx-build doc/source/ doc/build/

