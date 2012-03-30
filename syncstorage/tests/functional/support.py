# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
""" Base test class, with an instanciated app.
"""

import os
import urlparse
import random

from mozsvc.tests.support import FunctionalTestCase

from syncstorage.tests.support import StorageTestCase


class StorageFunctionalTestCase(FunctionalTestCase, StorageTestCase):

    def setUp(self):
        super(StorageFunctionalTestCase, self).setUp()
        self.user_id = random.randint(1, 100000)

    def _cleanup_test_databases(self):
        # Don't cleanup databases unless we created them ourselves.
        if not self.distant:
            super(StorageFunctionalTestCase, self)._cleanup_test_databases()
