# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
from metlog.senders import DebugCaptureSender
from mozsvc.metrics import get_metlog_client, monkey_service_class
from mozsvc.metrics import setup_metlog, teardown_metlog
from mozsvc.tests.support import get_test_configurator
import unittest


class TestMetlog(unittest.TestCase):
    def setUp(self):
        self.config = get_test_configurator(__file__)
        self.config.include('syncstorage')
        setup_metlog(self.config.registry.settings.getsection('metlog'))
        monkey_service_class()
        self.client = get_metlog_client()

    def tearDown(self):
        teardown_metlog()
        monkey_service_class(un=True)

    def test_metlog_setup(self):
        sender = self.client.sender
        self.assertTrue(isinstance(sender, DebugCaptureSender))

