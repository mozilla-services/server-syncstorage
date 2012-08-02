# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Low-level RDBMS client class.

This module implements a thin data access layer on top of an SQL database,
providing a simplified API over SQLAlchemy.
"""

import re
import urlparse
import traceback
import functools
from collections import defaultdict

from pyramid.threadlocal import get_current_registry

from sqlalchemy import create_engine, Table
from sqlalchemy.pool import NullPool, QueuePool
from sqlalchemy.sql import insert, update
from sqlalchemy.exc import (DBAPIError, OperationalError,
                            TimeoutError, IntegrityError)

from mozsvc.util import maybe_resolve_name
from mozsvc.exceptions import BackendError


# Regular expression matching strings that are safe to use as SQL field names.
# This gets applied as an addtional safety check when creating SQL by hand.
SAFE_FIELD_NAME_RE = re.compile("^[a-zA-Z0-9_]+$")


# Regular expression for extraction substitution names from a format string.
# It is used to extract e.g. "table" from the string "INSERT INTO {table}".
FORMAT_SUBST_NAME_RE = re.compile("\\{([a-zA-Z0-9]+)\\}")


class DBConnector(object):
    """Database connector class for SQL access layer.

    This class, along with its companion class DBConnection, provide a
    layer through which to access the SQL database.  It is a thin layer
    on top of the SQLAlchemy engine/connection machinery, with the following
    additional features:

        * use pre-defined queries rather than inline construction of SQL
        * accessor methods that automatically clean up database resources
        * automatic retry of connections that are invalidated by the server

    """

    def __init__(self, sqluri, create_tables=False, pool_size=100,
                 no_pool=False, pool_recycle=60, reset_on_return=True,
                 pool_max_overflow=10, pool_timeout=30, **kwds):

        parsed_sqluri = urlparse.urlparse(sqluri)
        self.sqluri = sqluri
        self.driver = parsed_sqluri.scheme.lower()
        if "mysql" in self.driver:
            self.driver = "mysql"

        if self.driver not in ("mysql", "sqlite"):
            msg = "Only MySQL and SQLite databases are officially supported"
            self.logger.warn(msg)

        # Construct the pooling-related arguments for SQLAlchemy engine.
        sqlkw = {}
        sqlkw["logging_name"] = "syncstorage"
        sqlkw["connect_args"] = {}
        if no_pool:
            sqlkw["poolclass"] = NullPool
        else:
            sqlkw["poolclass"] = QueuePool
            sqlkw["pool_size"] = int(pool_size)
            sqlkw["pool_recycle"] = int(pool_recycle)
            sqlkw["pool_timeout"] = int(pool_timeout)
            sqlkw["pool_reset_on_return"] = reset_on_return
            sqlkw["max_overflow"] = int(pool_max_overflow)

        # Connection handling in sqlite needs some extra care.
        # If it's a :memory: database, ensure we use only a single connection.
        if self.driver == "sqlite":
            # If pooling is in use, we must mark it as safe to share
            # connection objects between threads.
            if not no_pool:
                sqlkw["connect_args"]["check_same_thread"] = False
            # If using a :memory: database, we must use a QueuePool of size
            # 1 so that a single connection is shared by all threads.
            if parsed_sqluri.path.lower() in ("/", "/:memory:"):
                if no_pool:
                    msg = "You cannot specify no_pool=True "
                    msg += "when using a :memory: database"
                    raise ValueError(msg)
                sqlkw["pool_size"] = 1
                sqlkw["max_overflow"] = 0

        # Create the engine.
        self.engine = create_engine(sqluri, **sqlkw)

        self.create_tables = create_tables
        self._tables = {}
        self._queries = {}

    @property
    def logger(self):
        return get_current_registry()["metlog"]

    def load_table(self, table):
        assert isinstance(table, Table)
        self._tables[table.name] = table
        if self.create_tables:
            table.create(self.engine, checkfirst=True)

    def load_query(self, name, query):
        if query is not None:
            if not callable(query):
                if not isinstance(query, basestring):
                    msg = "Can't use {!r} as a query"
                    raise ValueError(msg.format(query))
        self._queries[name] = query

    def load_tables(self, name_or_module):
        module = maybe_resolve_name(name_or_module)
        for nm in dir(module):
            table = getattr(module, nm)
            if isinstance(table, Table):
                self.load_table(table)
        
    def load_queries(self, name_or_module):
        module = maybe_resolve_name(name_or_module)
        for nm in dir(module):
            if nm.isupper():
                self.load_query(nm, getattr(module, nm))

    def get_query(self, name, params):
        """Get the named pre-built query.

        This method returns an SQLAlchemy query object for the named query,
        after performing some sharding based on the given parameters.
        """
        # Get the pre-built query with that name.
        # It might be None, a string query, or a callable returning the query.
        try:
            query = self._queries[name]
        except KeyError:
            raise KeyError("No query named {!r}".format(name))
        # If it's None then just return it, indicating a no-op.
        if query is None:
            return None
        # If it's a callable, call it with a reference to this connector.
        if callable(query):
            return query(self, params)
        # If it's a string, do format string interpolation and return it.
        assert isinstance(query, basestring)
        return self._format_query(query, params)

    def _format_query(self, query, params):
        qvars = {}
        for name in set(FORMAT_SUBST_NAME_RE.findall(query)):
            qvars[name] = self._render_format_variable(name, params)
        if qvars:
            query = query.format(**qvars)
        return query

    def _render_format_variable(self, name, params):
        value = params[name]
        # If it's a list of values, turn it into a list of bindparams.
        if isinstance(value, (list, tuple)):
            bindparams = []
            for i, item in enumerate(value):
                param = "qvar_{}_{}".format(name, i)
                params[param] = item
                bindparams.append(":" + param)
            return "(" + ",".join(bindparams) + ")"
        # Otherwise, treat it as a literal string.
        # It must be a safe value.
        value = str(value)
        assert SAFE_FIELD_NAME_RE.match(value)
        return value

    def connect(self):
        """Create a new DBConnection object from this connector."""
        return DBConnection(self)

    def query(self, *args, **kwds):
        """Execute a database query, returning the rowcount."""
        with self.connect() as c:
            return c.query(*args, **kwds)

    def query_scalar(self, *args, **kwds):
        """Execute a named query, returning a single scalar value."""
        with self.connect() as c:
            return c.query_scalar(*args, **kwds)

    def query_fetchone(self, *args, **kwds):
        """Execute a named query, returning the first result row."""
        with self.connect() as c:
            return c.query_fetchone(*args, **kwds)

    def query_fetchall(self, *args, **kwds):
        """Execute a named query, returning iterator over the results."""
        with self.connect() as c:
            return c.query_fetchall(*args, **kwds)

    def insert_or_update(self, *args, **kwds):
        """Perform an efficient bulk "upsert" of the given items."""
        with self.connect() as c:
            return c.insert_or_update(*args, **kwds)


def report_backend_errors(func):
    """Method decorator to log and normalize unexpected DB errors.

    This method decorator catches unexpected database-level errors (such as
    connections dropping out or pool timeout errors) and transforms them
    into a BackendError instance.  The original error is logged for server-side
    debugging.
    """
    @functools.wraps(func)
    def report_backend_errors_wrapper(self, *args, **kwds):
        try:
            return func(self, *args, **kwds)
        except (OperationalError, TimeoutError), exc:
            # An unexpected database-level error.
            # Log the error, then normalize it into a BackendError instance.
            # Note that this will not not logic errors such as IntegrityError,
            # only unexpected operational errors from the database.
            err = traceback.format_exc()
            self.logger.error(err)
            raise BackendError(str(exc))
    return report_backend_errors_wrapper


class DBConnection(object):
    """Database connection class for SQL access layer.

    This class provides a light abstraction around SQLAlchemy's Connection
    object.  It offers automatic retry of invalidated connections and some
    higher-level utility methods for running pre-written named queries.

    DBConnection classes always operate within a single, implicit database
    transaction.  The transaction is opened the first time a query is
    executed and is closed by calling either the commit() or rollback()
    method.
    """

    def __init__(self, connector):
        self._connector = connector
        self._connection = None
        self._transaction = None

    @property
    def logger(self):
        return self._connector.logger

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.commit()
        else:
            self.rollback()

    @report_backend_errors
    def commit(self):
        """Commit the active transaction and close the connection."""
        try:
            if self._transaction is not None:
                self._transaction.commit()
                self._transaction = None
        finally:
            if self._connection is not None:
                self._connection.close()
                self._connection = None

    @report_backend_errors
    def rollback(self):
        """Abort the active transaction and close the connection."""
        try:
            if self._transaction is not None:
                self._transaction.rollback()
                self._transaction = None
        finally:
            if self._connection is not None:
                self._connection.close()
                self._connection = None

    @report_backend_errors
    def execute(self, query, params=None, annotations=None):
        """Execute a database query, with retry and exception-catching logic.

        This method executes the given query against the database, lazily
        establishing an actual live connection as required.  It catches
        operational database errors and normalizes them into a BackendError
        exception.
        """
        if params is None:
            params = {}
        if annotations is None:
            annotations = {}
        # If there is no active connection, create a fresh one.
        # This will affect the control flow below.
        connection = self._connection
        session_was_active = True
        if connection is None:
            connection = self._connector.engine.connect()
            transaction = connection.begin()
            session_was_active = False
        try:
            # It's possible for the backend to raise a "connection invalided"
            # error if e.g. the server timed out the connection we got from
            # the pool.  It's safe to retry with a new connection, but only
            # if the failed connection was never successfully used.
            try:
                query_str = self._render_query(query, params, annotations)
                return connection.execute(query_str, **params)
            except DBAPIError, exc:
                if not exc.connection_invalidated:
                    raise
                if session_was_active:
                    raise
                # The connection is dead, no need to close it here
                # before opening a fresh one.
                connection = self._connector.engine.connect()
                transaction = connection.begin()
                annotations["retry"] = "1"
                query_str = self._render_query(query, params, annotations)
                return connection.execute(query_str, **params)
        finally:
            # Now that the underlying connection has been used, remember it
            # so that all subsequent queries are part of the same transaction.
            if not session_was_active:
                self._connection = connection
                self._transaction = transaction

    def _render_query(self, query, params, annotations):
        """Render a query into its final string form, to send to database.

        This method does any final tweaks to the string form of the query
        immediately before it is sent to the database.  Currently its only
        job is to add annotations in a comment on the query.
        """
        # Convert SQLAlchemy expression objects into a string.
        if isinstance(query, basestring):
            query_str = query
        else:
            compiled = query.compile()
            for param, value in compiled.params.iteritems():
                params.setdefault(param, value)
            query_str = str(compiled)
        # Join all the annotations into a comment string.
        annotation_items = sorted(annotations.items())
        annotation_strs = ("{}={}".format(*item) for item in annotation_items)
        comment = "/* [" + ", ".join(annotation_strs) + "] */"
        # Add it to the query, at the front if possible.
        # SQLite chokes on leading comments, so put it at back on that driver.
        if self._connector.driver == "sqlite":
            query_str = query_str + " " + comment
        else:
            query_str = comment + " " + query_str
        return query_str

    def query(self, query_name, params=None, annotations=None):
        """Execute a database query, returning the rowcount."""
        query = self._connector.get_query(query_name, params)
        if query is None:
            return 0
        if annotations is None:
            annotations = {}
        annotations.setdefault("queryName", query_name)
        res = self.execute(query, params, annotations)
        try:
            return res.rowcount
        finally:
            res.close()

    def query_scalar(self, query_name, params=None, default=None,
                     annotations=None):
        """Execute a named query, returning a single scalar value."""
        query = self._connector.get_query(query_name, params)
        if query is None:
            return default
        if annotations is None:
            annotations = {}
        annotations.setdefault("queryName", query_name)
        res = self.execute(query, params, annotations)
        try:
            row = res.fetchone()
            if row is None or row[0] is None:
                return default
            return row[0]
        finally:
            res.close()

    def query_fetchone(self, query_name, params=None, annotations=None):
        """Execute a named query, returning the first result row."""
        query = self._connector.get_query(query_name, params)
        if query is None:
            return None
        if annotations is None:
            annotations = {}
        annotations.setdefault("queryName", query_name)
        res = self.execute(query, params, annotations)
        try:
            return res.fetchone()
        finally:
            res.close()

    def query_fetchall(self, query_name, params=None, annotations=None):
        """Execute a named query, returning iterator over the results."""
        query = self._connector.get_query(query_name, params)
        if query is not None:
            if annotations is None:
                annotations = {}
            annotations.setdefault("queryName", query_name)
            res = self.execute(query, params, annotations)
            try:
                for row in res:
                    yield row
            finally:
                res.close()

    def insert_or_update(self, table, items, annotations=None):
        """Perform an efficient bulk "upsert" of the given items.

        Given the name of a table and a list of data dicts to insert or update,
        this method performs the "upsert" in the most efficient way.  It's
        a separate method because the precise details of the operation depend
        on the database driver in use.

        For generic database backends, the best we can do is try each insert,
        catch any IntegrityErrors and retry as an update.  For MySQL however
        we can use the "ON DUPLICATE KEY UPDATE" syntax to do the operation
        in a single query.

        The number of newly-inserted rows is returned.
        """
        if annotations is None:
            annotations = {}
        annotations.setdefault("queryName", "UPSERT_{}".format(table))
        # Inserting zero items is strange, but allowed.
        if not items:
            return 0
        # Find the table object into which we're inserting.
        if isinstance(table, basestring):
            table = self._connector._format_query(table, items[0])
            table = self._connector._tables[table]
        # Dispatch to an appropriate implementation.
        if self._connector.driver == "mysql":
            return self._upsert_onduplicatekey(table, items, annotations)
        else:
            return self._upsert_generic(table, items, annotations)

    def _upsert_generic(self, table, items, annotations):
        """Upsert a batch of items one at a time, trying INSERT then UPDATE.

        This is a tremendously inefficient way to write a batch of items,
        but it's guaranteed to work without special cooperation from the
        database.  For MySQL we use the much improved _upsert_onduplicatekey.
        """
        num_created = 0
        for item in items:
            try:
                # Try to insert the item.
                # If it already exists, this fails with an integrity error.
                query = insert(table).values(**item)
                self.execute(query, item, annotations).close()
                num_created += 1
            except IntegrityError:
                # Update the item.
                # Use the table's primary key fields in the WHERE clause,
                # and put all other fields into the UPDATE clause.
                item = item.copy()
                query = update(table)
                for key in table.primary_key:
                    try:
                        query = query.where(key == item.pop(key.name))
                    except KeyError:
                        msg = "Item is missing primary key column {!r}"
                        raise ValueError(msg.format(key.name))
                query = query.values(**item)
                self.execute(query, item, annotations).close()
        return num_created

    def _upsert_onduplicatekey(self, table, items, annotations):
        """Upsert a batch of items using the ON DUPLICATE KEY UPDATE syntax.

        This is a custom batch upsert implementation based on non-standard
        features of MySQL.  The resulting query will be something like the
        following, where M is the number of fields in each item and N is the
        number of items being inserted:

            INSERT INTO table (c1, ..., cM)
            VALUES (:c11, ..., :cM1), ..., (:c1N, ... :cMN)
            ON DUPLICATE KEY UPDATE c1 = VALUES(c1), ..., cM = VALUES(cM)

        The values from the given items will be collected into a matching set
        of bind parameters :c11 through :cMN  when executing the query.
        """
        # Group the items to be inserted into batches that all have the same
        # set of fields.  Each batch will have the same ON DUPLICATE KEY UPDATE
        # clause and so can be sent as a single query.
        batches = defaultdict(list)
        for item in items:
            batches[frozenset(item.iterkeys())].append(item)
        # Now construct and send an appropriate query for each batch.
        num_created = 0
        for batch in batches.itervalues():
            # Since we're crafting SQL by hand, assert that each field is
            # actually a plain alphanum field name.  Can't be too careful...
            fields = batch[0].keys()
            assert all(SAFE_FIELD_NAME_RE.match(field) for field in fields)
            # Each item corresponds to a set of bindparams and a matching
            # entry in the "VALUES" clause of the query.
            query = "INSERT INTO {} ({}) VALUES "
            query = query.format(table.name, ",".join(fields))
            binds = [":{}{{num}}".format(field) for field in fields]
            pattern = "({}) ".format(",".join(binds))
            params = {}
            vclauses = []
            for num, item in enumerate(batch):
                vclauses.append(pattern.format(num=num))
                for field, value in item.iteritems():
                    params["{}{}".format(field, num)] = value
            query += ",".join(vclauses)
            # The ON DUPLICATE KEY CLAUSE updates all the given fields.
            updates = ["{f} = VALUES({f})".format(f=field) for field in fields]
            query += " ON DUPLICATE KEY UPDATE " + ",".join(updates)
            # Now we can execute it as one big query.
            res = self.execute(query, params, annotations)
            # MySQL adds one to the rowcount for each item that was inserted,
            # and adds two to the rowcount for each item that was updated.
            # Arithmetic lets us find the actual numbers.
            try:
                num_updated = res.rowcount - len(batch)
                assert num_updated >= 0
                num_created += (len(batch) - num_updated)
            finally:
                res.close()
        return num_created
