#!/usr/bin/python

# This suite contains basic tests for the identity server.
# It does not exercise any of the per-protocol features.

from optparse import OptionParser
from unittest import TextTestRunner, defaultTestLoader
import logging
import sys

import server_tests
import module_test_runner
import test_config

def run():
    """Runs the test suite."""
    test_runner = module_test_runner.ModuleTestRunner(asXML='-xml' in sys.argv)
    test_runner.modules = [server_tests]
    return test_runner.RunAllTests()


# python run_server_tests.py --scheme=https --server=pm-weave04.mozilla.org --host=sj-weave01.services.mozilla.com --username=weavetest-sj01 --password=caid2raefoWi


if __name__ == '__main__':
    logging.basicConfig(level = logging.DEBUG)

    # process arguments
    usage = "usage: %prog [options] [ test-class | test-class.test-method ] "
    parser = OptionParser(usage=usage)
    parser.add_option("--scheme", help="http or https", dest="scheme")
    parser.add_option("--server",
                      help="the actual internet host to contact",
                      dest="server")
    parser.add_option("--host",
                      help="the Host name to present in the HTTP request",
                      dest="host")
    parser.add_option("--username",
                      help="the Weave username to send",
                      dest="username")
    parser.add_option("--password", help="the Weave password to send",
                      dest="password")
    parser.add_option("--with-memcache",
                      help=("whether the server is running with memcache "
                            "(1 if true; 0 if not)"),
                      dest="memcache")

    options, args = parser.parse_args()

    if options.scheme:
        test_config.STORAGE_SCHEME = options.scheme
    if options.server:
        test_config.STORAGE_SERVER =  options.server
    if options.host:
        test_config.HOST_NAME = options.host
    if options.username:
        test_config.USERNAME = options.username
    if options.password:
        test_config.PASSWORD = options.password

    test_config.memcache = options.memcache == "1"

    tests = args
    any_problems = False

    if len(tests) > 0:
        results = []
        runner = TextTestRunner(verbosity=3)
        for test in tests:
            test = defaultTestLoader.loadTestsFromName(test,
                                                       module=server_tests)
            results.append(runner.run(test))
    else:
        results = run()

    for result in results:
        if len(result.failures + result.errors) > 0:
            sys.exit(1)

    sys.exit(0)
