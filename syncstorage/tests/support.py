# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
import os

from syncstorage.storage import SyncStorage
from services.tests.support import TestEnv


def initenv(config=None):
    """Reads the config file and instantiates a storage.
    """
    # pre-registering plugins
    from syncstorage.storage.sql import SQLStorage
    SyncStorage.register(SQLStorage)
    try:
        from syncstorage.storage.memcachedsql import MemcachedSQLStorage
        SyncStorage.register(MemcachedSQLStorage)
    except ImportError:
        pass

    mydir = os.path.dirname(__file__)
    testenv = TestEnv(ini_path=config, ini_dir=mydir,
                      load_sections=['storage'])
    return testenv.ini_dir, testenv.config, testenv.storage
