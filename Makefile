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
	$(BIN)/flake8 syncstorage

coverage:
	$(BIN)/nosetests -s --cover-html --cover-html-dir=html --with-coverage --cover-package=syncstorage syncstorage
	WEAVE_TESTFILE=mysql $(BIN)/nosetests -s --cover-html --cover-html-dir=html --with-coverage --cover-package=syncstorage syncstorage 

test:
	$(BIN)/nosetests -s syncstorage

mysqltest:
	WEAVE_TESTFILE=mysql $(BIN)/nosetests -s syncstorage

redisqltest:
	WEAVE_TESTFILE=redisql $(BIN)/nosetests -s syncstorage

ldaptest:
	WEAVE_TESTFILE=ldap $(BIN)/nosetests -s syncstorage


alltest: test mysqltest redisqltest ldaptest

doc:
	$(BIN)/sphinx-build doc/source/ doc/build/

