# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
from metlog.senders import DebugCaptureSender
from mozsvc.metrics import get_metlog_client
from mozsvc.metrics import setup_metlog, teardown_metlog
from mozsvc.tests.support import get_test_configurator, make_request
import json
import unittest


class TestMetlog(unittest.TestCase):
    def setUp(self):
        self.config = get_test_configurator(__file__)
        setup_metlog(self.config.registry.settings.getsection('metlog'))
        self.config.include('syncstorage')
        self.client = get_metlog_client()

    def tearDown(self):
        teardown_metlog()

    def _make_request(self, *args, **kwds):
        req = make_request(self.config, *args, **kwds)
        req.user = {'uid': 'aa'}
        return req

    def test_sender_class(self):
        sender = self.client.sender
        self.assertTrue(isinstance(sender, DebugCaptureSender))

    def test_service_view_wrappers(self):
        from syncstorage.views import get_info_counts
        req = self._make_request(environ={"HTTP_HOST": "localhost"})
        get_info_counts(req)
        msgs = self.client.sender.msgs
        self.assertEqual(len(msgs), 2)
        timer_msg = json.loads(msgs[0])
        wsgi_msg = json.loads(msgs[1])
        self.assertEqual(timer_msg['type'], 'timer')
        self.assertEqual(timer_msg['fields']['name'],
                         'syncstorage.views:get_info_counts')
        self.assertEqual(wsgi_msg['type'], 'wsgi')
        self.assertEqual(wsgi_msg['fields']['headers'],
                         {'path': '/', 'host': 'localhost',
                          'User-Agent': ''})
