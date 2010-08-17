VIRTUALENV = virtualenv
BIN = bin

.PHONY: all build check coverage test mysqltest redisqltest

all:	build test

build:
	$(VIRTUALENV) --no-site-packages .
	$(BIN)/easy_install nose
	$(BIN)/easy_install coverage
	$(BIN)/easy_install flake8
	$(BIN)/python setup.py develop

check:
	rm -rf weaveserver/templates/*.py
	$(BIN)/flake8 weaveserver

coverage:
	$(BIN)/nosetests -s --cover-html --cover-html-dir=html --with-coverage --cover-package=weaveserver weaveserver

test:
	$(BIN)/nosetests -s weaveserver

mysqltest:
	WEAVE_TESTFILE=mysql $(BIN)/nosetests -s weaveserver

redisqltest:
	WEAVE_TESTFILE=redisql $(BIN)/nosetests -s weaveserver

