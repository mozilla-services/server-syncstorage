#!/usr/bin/python
#
# Copyright (C) 2007, 2008 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


__author__ = 'davidbyttow@google.com (David Byttow)'


import unittest
import xmlrunner

class ModuleTestRunner(object):

	def __init__(self, module_list=None, module_test_settings=None, asXML = False):
		self.modules = module_list or []
		self.settings = module_test_settings or {}
		self.asXML = asXML
		
	def RunAllTests(self):
		"""Executes all tests present in the list of modules."""
		if self.asXML:
			runner = xmlrunner.XMLTestRunner()
		else:
			runner = unittest.TextTestRunner(verbosity=3)

		results = []
		for module in self.modules:
			for setting, value in self.settings.iteritems():
				try:
					setattr(module, setting, value)
				except AttributeError:
					pass
			print '\nRunning all tests in module', module.__name__
			results.append(runner.run(unittest.defaultTestLoader.loadTestsFromModule(module)))
		return results

