# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import json

from metlog.senders import DebugCaptureSender
from syncstorage.views import get_info_counts
from syncstorage.tests.support import StorageTestCase


class TestMetlog(StorageTestCase):

    def make_request(self, *args, **kwds):
        req = super(TestMetlog, self).make_request(*args, **kwds)
        req.user = {'uid': 'aa'}
        req.matchdict = {}
        return req

    def test_sender_class(self):
        sender = self.metlog.sender
        self.assertTrue(isinstance(sender, DebugCaptureSender))

    def test_service_view_wrappers(self):
        req = self.make_request(environ={"HTTP_HOST": "localhost"})
        get_info_counts(req)
        msgs = self.metlog.sender.msgs
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
