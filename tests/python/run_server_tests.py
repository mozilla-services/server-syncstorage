#!/usr/bin/python

# This suite contains basic tests for the identity server.
# It does not exercise any of the per-protocol features.

import server_tests

import module_test_runner
import unittest
import logging
import sys


def Run():
	test_runner = module_test_runner.ModuleTestRunner(asXML='-xml' in sys.argv)
	test_runner.modules = [server_tests]
	test_runner.RunAllTests()

if __name__ == '__main__':
	logging.basicConfig(level = logging.DEBUG)

	tests = filter(lambda x:x[0] != '-', sys.argv[1:])
	if len(tests) > 0:
		runner = unittest.TextTestRunner(verbosity=3)
		for a in tests:
			runner.run(unittest.defaultTestLoader.loadTestsFromName(a, module=server_tests))
	else:
		Run()