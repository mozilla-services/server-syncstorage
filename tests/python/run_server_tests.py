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
	return test_runner.RunAllTests()

if __name__ == '__main__':
	logging.basicConfig(level = logging.DEBUG)

	tests = filter(lambda x:x[0] != '-', sys.argv[1:])
	anyProblems = False
	if len(tests) > 0:
		results = []
		runner = unittest.TextTestRunner(verbosity=3)
		for a in tests:
			results.append(runner.run(unittest.defaultTestLoader.loadTestsFromName(a, module=server_tests)))
	else:
		results = Run()

	for r in results:
		if len(r.failures) > 0 or len(r.errors) > 0:
			sys.exit(1)

	sys.exit(0)