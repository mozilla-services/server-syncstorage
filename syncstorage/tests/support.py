# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import os
import uuid
import urlparse
import functools

import sqlalchemy.event
from sqlalchemy.engine.base import Engine

from metlog.decorators.base import MetlogDecorator
from mozsvc.metrics import load_metlog_client
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
    if "queryName=" not in statement:
        assert False, "SQL query does not have a name: %s" % (statement,)


class StorageTestCase(TestCase):
    """TestCase class with automatic cleanup of database files."""

    @restore_env("MOZSVC_TEST_INI_FILE")
    def setUp(self):
        # Put a fresh UUID into the environment.
        # This can be used in e.g. config files to create unique paths.
        os.environ["MOZSVC_UUID"] = str(uuid.uuid4())
        # Allow subclasses to override default ini file.
        if hasattr(self, "TEST_INI_FILE"):
            if "MOZSVC_TEST_INI_FILE" not in os.environ:
                os.environ["MOZSVC_TEST_INI_FILE"] = self.TEST_INI_FILE
        super(StorageTestCase, self).setUp()

    def tearDown(self):
        self._cleanup_test_databases()

        # restore MetlogDecorator's `client` property
        MetlogDecorator.client = self.orig_client

        # clear the pyramid threadlocals
        self.config.end()
        super(StorageTestCase, self).tearDown()
        del os.environ["MOZSVC_UUID"]

    def get_configurator(self):
        config = super(StorageTestCase, self).get_configurator()
        self.metlog = load_metlog_client(config)

        # override MetlogDecorator's `client` property
        self.orig_client = MetlogDecorator.client
        MetlogDecorator.client = self.metlog

        config.registry['metlog'] = self.metlog
        config.include("syncstorage")
        return config

    def _cleanup_test_databases(self):
        """Clean up any database used during the tests."""
        # Find any in-use mysql database and drop the tables.
        for key, storage in self.config.registry.iteritems():
            if not key.startswith("syncstorage:storage:"):
                continue
            while hasattr(storage, "storage"):
                storage = storage.storage
            if "mysql" in storage.sqluri:
                with storage.dbconnector.connect() as c:
                    c.execute('DROP TABLE bso')
                    c.execute('DROP TABLE user_collections')
                    c.execute('DROP TABLE collections')
        # Find any sqlite database files and delete them.
        for key, value in self.config.registry.settings.iteritems():
            if key.endswith(".sqluri"):
                sqluri = urlparse.urlparse(value)
                if sqluri.scheme == 'sqlite' and ":memory:" not in value:
                    if os.path.isfile(sqluri.path):
                        os.remove(sqluri.path)
