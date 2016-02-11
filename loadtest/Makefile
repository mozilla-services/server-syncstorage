SERVER_URL = https://token.stage.mozaws.net

# Hackety-hack around OSX system python bustage.
# The need for this should go away with a future osx/xcode update.
ARCHFLAGS = -Wno-error=unused-command-line-argument-hard-error-in-future
INSTALL = ARCHFLAGS=$(ARCHFLAGS) ./bin/pip install

.PHONY: build test bench

# Build virtualenv, to ensure we have all the dependencies.
build:
	virtualenv --no-site-packages .
	$(INSTALL) gevent
	$(INSTALL) pexpect
	$(INSTALL) configparser==3.5.0b2
	$(INSTALL) https://github.com/mozilla-services/loads/archive/master.zip
	$(INSTALL) PyBrowserID
	$(INSTALL) hawkauthlib
	$(INSTALL) tokenlib
	rm -rf ./local  # ubuntu, why you create this useless folder?

# Clean all the things installed by `make build`.
clean:
	rm -rf ./include ./bin ./lib ./lib64 *.pyc

# Run a single test from the local machine, for sanity-checking.
test:
	./bin/loads-runner --config=./config/test.ini --server-url=$(SERVER_URL) stress.StressTest.test_storage_session

# Run a bench of 20 concurrent users.
bench:
	./bin/loads-runner --config=./config/bench.ini --server-url=$(SERVER_URL) stress.StressTest.test_storage_session

# Run a much bigger bench, by submitting to broker in AWS.
megabench:
	./bin/loads-runner --config=./config/megabench.ini --user-id=$(USER) --server-url=$(SERVER_URL) stress.StressTest.test_storage_session

# Purge any currently-running loadtest runs.
purge:
	./bin/loads-runner --config=./config/megabench.ini --purge-broker
