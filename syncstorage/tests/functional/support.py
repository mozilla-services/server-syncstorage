# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
""" Base test class, with an instanciated app.
"""

import os
import urlparse
import random

from mozsvc.metrics import setup_metlog
from mozsvc.tests.support import FunctionalTestCase


class StorageFunctionalTestCase(FunctionalTestCase):

    def setUp(self):
        super(StorageFunctionalTestCase, self).setUp()
        setup_metlog(self.config.registry.settings.getsection('metlog'))

        self.config.include("syncstorage")
        self.config.commit()

        # We only support mysql and sqlite databases.
        # Check that the config keys match this expectation.
        # Also get a list of temp database files to delete on cleanup.
        if not self.distant:
            self.sqlfiles = []
            for key, value in self.config.registry.settings.iteritems():
                if key.endswith(".sqluri"):
                    sqluri = urlparse.urlparse(value)
                    assert sqluri.scheme in ('mysql',  'sqlite')
                    if sqluri.scheme == 'sqlite':
                        self.sqlfiles.append(sqluri.path)

        # adding a user if needed
        self.user_id = random.randint(1, 100000)

    def tearDown(self):
        if not self.distant:
            for key, storage in self.config.registry.iteritems():
                if not key.startswith("syncstorage:storage:"):
                    continue
                if "mysql" in storage.sqluri:
                    storage._engine.execute('truncate collections')
                    storage._engine.execute('truncate bso')

            for sqlfile in self.sqlfiles:
                if os.path.exists(sqlfile):
                    os.remove(sqlfile)

        super(StorageFunctionalTestCase, self).tearDown()
