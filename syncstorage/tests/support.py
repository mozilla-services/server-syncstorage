# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import os
import uuid
import urlparse
import functools

import sqlalchemy.event
from sqlalchemy.engine.base import Engine

from mozsvc.tests.support import TestCase


def restore_env(*keys):
    """Decorator that ensures os.environ gets restored after a test.

    Given a list of environment variable keys, this decorator will save the
    current values of those environment variables at the start of the call
    and restore them to those values at the end.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwds):
            values = [os.environ.get(key) for key in keys]
            try:
                return func(*args, **kwds)
            finally:
                for key, value in zip(keys, values):
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value
        return wrapper
    return decorator


# A global event listener to santity-check all queries sent to the DB.
# Unfortunately SQLAlchemy doesn't have a way to unregister a listener,
# so once you import this module the listener will be installed forever.
@sqlalchemy.event.listens_for(Engine, "before_cursor_execute")
def validate_database_query(conn, cursor, statement, *args):
    """Check that database queries have appripriate metadata."""
    statement = statement.strip()
    if statement.startswith("PRAGMA "):
        return
    if statement.startswith("CREATE "):
        return
    if statement.startswith("DESCRIBE "):
        return
    if statement.startswith("DROP "):
        return
    if " pg_class " in statement:
        return
    if "queryName=" not in statement:
        assert False, "SQL query does not have a name: %s" % (statement,)


class StorageTestCase(TestCase):
    """TestCase class with automatic cleanup of database files."""

    @restore_env("MOZSVC_TEST_INI_FILE")
    def setUp(self):
        # Put a fresh UUID into the environment.
        # This can be used in e.g. config files to create unique paths.
        os.environ["MOZSVC_UUID"] = str(uuid.uuid4())
        # Ensure a default sqluri if none is provided in the environment.
        # We use an in-memory sqlite db by default, except for tests that
        # explicitly require an on-disk file.
        if "MOZSVC_SQLURI" not in os.environ:
            os.environ["MOZSVC_SQLURI"] = "sqlite:///:memory:"
        if "MOZSVC_ONDISK_SQLURI" not in os.environ:
            ondisk_sqluri = os.environ["MOZSVC_SQLURI"]
            if ":memory:" in ondisk_sqluri:
                ondisk_sqluri = "sqlite:////tmp/tests-sync-%s.db"
                ondisk_sqluri %= (os.environ["MOZSVC_UUID"],)
            os.environ["MOZSVC_ONDISK_SQLURI"] = ondisk_sqluri
        # Allow subclasses to override default ini file.
        if hasattr(self, "TEST_INI_FILE"):
            if "MOZSVC_TEST_INI_FILE" not in os.environ:
                os.environ["MOZSVC_TEST_INI_FILE"] = self.TEST_INI_FILE
        super(StorageTestCase, self).setUp()

    def tearDown(self):
        self._cleanup_test_databases()
        # clear the pyramid threadlocals
        self.config.end()
        super(StorageTestCase, self).tearDown()
        del os.environ["MOZSVC_UUID"]

    def get_configurator(self):
        config = super(StorageTestCase, self).get_configurator()
        config.include("syncstorage")
        return config

    def _cleanup_test_databases(self):
        """Clean up any database used during the tests."""
        # Find and clean up any in-use databases
        for key, storage in self.config.registry.iteritems():
            if not key.startswith("syncstorage:storage:"):
                continue
            while hasattr(storage, "storage"):
                storage = storage.storage
            # For server-based dbs, drop the tables to clear them.
            if storage.dbconnector.driver in ("mysql", "postgres"):
                with storage.dbconnector.connect() as c:
                    c.execute('DROP TABLE bso')
                    c.execute('DROP TABLE user_collections')
                    c.execute('DROP TABLE collections')
                    c.execute('DROP TABLE batch_uploads')
                    c.execute('DROP TABLE batch_upload_items')
            # Explicitly free any pooled connections.
            storage.dbconnector.engine.dispose()
        # Find any sqlite database files and delete them.
        for key, value in self.config.registry.settings.iteritems():
            if key.endswith(".sqluri"):
                sqluri = urlparse.urlparse(value)
                if sqluri.scheme == 'sqlite' and ":memory:" not in value:
                    if os.path.isfile(sqluri.path):
                        os.remove(sqluri.path)
