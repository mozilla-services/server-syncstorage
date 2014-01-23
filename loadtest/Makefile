SERVER_URL = https://token.dev.lcip.org

.PHONY: build test bench

# Build virtualenv, to ensure we have all the dependencies.
build:
	virtualenv --no-site-packages .
	./bin/pip install gevent
	./bin/pip install https://github.com/mozilla-services/loads/archive/master.zip
	./bin/pip install PyBrowserID
	./bin/pip install hawkauthlib
	rm -rf ./local  # ubuntu, why you create this useless folder?

# Clean all the things installed by `make build`.
clean:
	rm -rf ./include ./bin ./lib ./lib64 *.pyc

# Run a single test from the local machine, for sanity-checking.
test:
	./bin/loads-runner --hits=1 --users=1 stress.StressTest.test_storage_session --server-url=$(SERVER_URL)

# Run a full bench, by submitting to broker in AWS.
bench:
	./bin/loads-runner --users=20 --duration=1800 --include-file=./stress.py --python-dep=hawkauthlib stress.StressTest.test_storage_session --server-url=$(SERVER_URL)
