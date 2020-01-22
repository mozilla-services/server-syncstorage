# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Low-level SQL backend for syncstorage.

This module implements a thin data access layer on top of an SQL database,
providing the primitive operations on which to build a full SyncStorage
backend.  It provides three database tables:

  collections:  the names and ids of all collections in the store
  user_collections:  the per-user metadata associated with each collection
  bso:  the individual BSO items stored in each collection

For efficiency when dealing with large datasets, this module also supports
sharding of the BSO items into multiple tables named "bso0" through "bsoN".
This behaviour is off by default; pass shard=True to enable it.
"""

import os
import re
import sys
import copy
import logging
import urlparse
import traceback
import functools
from collections import defaultdict

import sqlalchemy.event
from sqlalchemy import create_engine
from sqlalchemy.util.queue import Queue
from sqlalchemy.pool import NullPool, QueuePool
from sqlalchemy.sql import insert, update, text as sqltext
from sqlalchemy.exc import DBAPIError, OperationalError, TimeoutError
from sqlalchemy import (Integer, String, Text, BigInteger,
                        MetaData, Column, Table, Index)
from sqlalchemy.dialects import postgresql, mysql

from mozsvc.metrics import metrics_timer, annotate_request
from mozsvc.exceptions import BackendError

from syncstorage.storage.sql import (queries_generic,
                                     queries_sqlite,
                                     queries_postgres,
                                     queries_mysql)


logger = logging.getLogger(__name__)

# Regex to match safe database field/column names.
SAFE_FIELD_NAME_RE = re.compile("^[a-zA-Z0-9_]+$")

# Regex to match specific kinds of query that are safe to kill.
# It's a SELECT, INSERT or UPDATE with optional leading comment.
SAFE_TO_KILL_QUERY = r"^\s*(/\*.*\*/)?\s*(SELECT|INSERT|UPDATE)\s"
SAFE_TO_KILL_QUERY = re.compile(SAFE_TO_KILL_QUERY, re.I)

# The ttl to use for rows that are never supposed to expire.
MAX_TTL = 2100000000

metadata = MetaData()


class Table(Table):
    """Custom Table class that sets some sensible default options."""

    def __new__(cls, *args, **kwds):
        kwds.setdefault("mysql_engine", "InnoDB")
        kwds.setdefault("mysql_charset", "latin1")
        return super(Table, cls).__new__(cls, *args, **kwds)


# Table mapping collection_name => collection_id.
#
# This table holds the names and corresponding ids of the collections in
# use on the storage node.  The collection id space is global, since we
# expect most users to have the same small, static set of collection names.

collections = Table(
    "collections",
    metadata,
    Column("collectionid", Integer, primary_key=True, nullable=False,
           autoincrement=True),
    Column("name", String(32), nullable=False, unique=True)
)


# Table mapping (user_id, collection_id) => collection-level metadata.
#
# This table holds collection-level metadata on a per-user basis.  Currently
# the only such metadata is the last-modified timestamp of the collection.

user_collections = Table(
    "user_collections",
    metadata,
    Column("userid", Integer, primary_key=True, nullable=False,
           autoincrement=False),
    Column("collection", Integer, primary_key=True, nullable=False,
           autoincrement=False),
    Column("last_modified", BigInteger, nullable=False)
)


# Table listing migration state for users
#
# This table contains the users in progress for migration. This table may
# be updated by the external user migration script
# A fxa_uid appearing in this table should cause the server to return a 5xx
# response. It's important that the server NEVER return a 2xx response.

migration = Table(
    "migration",
    metadata,
    Column("fxa_uid", String(255), primary_key=True, nullable=False),
    Column("started_at", BigInteger, nullable=False),
    Column("state", String(32))  # unknown/NULL, in_progress, complete
)


# Column definitions for BSO storage table/tables.
#
# This list class defines the columns used for storage of BSO records.
# It is used to create either sharded or non-shareded BSO storage tables,
# depending on the run-time settings of the application.

PAYLOAD_TYPE = Text()
PAYLOAD_TYPE = PAYLOAD_TYPE.with_variant(postgresql.TEXT(), 'postgresql')
PAYLOAD_TYPE = PAYLOAD_TYPE.with_variant(mysql.MEDIUMTEXT(), 'mysql')


# Common column definitions between BSO and batch upload item tables

def _get_bso_columns(table_name):
    return (
        Column("userid", Integer, primary_key=True, nullable=False,
               autoincrement=False),
        Column("collection", Integer, primary_key=True, nullable=False,
               autoincrement=False),
        Column("id", String(64), primary_key=True, autoincrement=False),
        Column("sortindex", Integer),
        Column("modified", BigInteger, nullable=False),
        # I'd like to default this to the emptry string, but
        # MySQL doesn't let you set a default on a TEXT column.
        Column("payload", PAYLOAD_TYPE, nullable=False),
        Column("payload_size", Integer, nullable=False,
               server_default=sqltext("0")),
        Column("ttl", Integer, nullable=False,
               server_default=sqltext(str(MAX_TTL))),
        # Declare indexes.
        # We need to include the tablename in the index name due to sharding,
        # because index names in sqlite are global, not per-table.
        # Index on "ttl" for easy pruning of expired items.
        Index("%s_ttl_idx" % (table_name,), "ttl"),
        # Index on "modified" for easy filtering by timestamp.
        Index("%s_usr_col_mod_idx" % (table_name,),
              "userid", "collection", "modified"),
        # There is intentinally no index on "sortindex".
        # Clients almost always filter on "modified" using the above index,
        # and cannot take advantage of a separate index for sorting.
    )


#  If the storage controller is not doing sharding based on userid,
#  then it will use the single "bso" table below for BSO storage.

bso = Table("bso", metadata, *_get_bso_columns("bso"))

# Table mapping (user_id, collection_id) => batch IDs

batch_uploads = Table(
    "batch_uploads",
    metadata,
    Column("batch", BigInteger, primary_key=True, nullable=False),
    Column("userid", Integer, primary_key=True, nullable=False,
           autoincrement=False),
    Column("collection", Integer, nullable=False)
)

# Column definitions for batch upload item table(s)
#
# These columns hold the batched uploads until either the client declares a
# commit or the TTL expires.


def _get_batch_item_columns(table_name):
    return (
        Column("batch", BigInteger, primary_key=True, nullable=False,
               autoincrement=False),
        Column("userid", Integer, primary_key=True, nullable=False,
               autoincrement=False),
        Column("id", String(64), primary_key=True, nullable=False,
               autoincrement=False),
        # All these need to be nullable, because the batch upload
        # may or may not set each individual field of each item.
        # Also note that there's no "modified" column because the
        # modification timestamp gets set on batch commit.
        Column("sortindex", Integer, nullable=True),
        Column("payload", PAYLOAD_TYPE, nullable=True),
        Column("payload_size", Integer, nullable=True),
        Column("ttl_offset", Integer, nullable=True)
    )


bui = Table("batch_upload_items", metadata,
            *_get_batch_item_columns("batch_upload_items"))


#  If the storage controller is doing sharding based on userid,
#  then it will use the below functions to select a table from "bso0"
#  to "bsoN" for each userid.  Ditto for batch_upload_items.

BSO_SHARDS = {}
BUI_SHARDS = {}


def get_sharded_table(index, which="bso"):
    """Get the Table object for table bso and batch_upload_items<N>."""
    global BSO_SHARDS, BUI_SHARDS
    if which == "bso":
        shards = BSO_SHARDS
        columns_func = _get_bso_columns
    elif which == "batch_upload_items":
        shards = BUI_SHARDS
        columns_func = _get_batch_item_columns
    else:
        raise ValueError("Invalid sharded table type: %s" % (which))

    table = shards.get(index)
    if table is None:
        table_name = "%s%d" % (which, index)
        table = Table(table_name, metadata, *columns_func(table_name))
        shards[index] = table
    return table


def get_bso_table(index):
    return get_sharded_table(index)


def get_batch_item_table(index):
    return get_sharded_table(index, which="batch_upload_items")


class _QueueWithMaxBacklog(Queue):
    """SQLAlchemy Queue subclass with a limit on the length of the backlog.

    This base Queue class sets no limit on the number of threads that can be
    simultaneously blocked waiting for an item on the queue.  This class
    adds a "max_backlog" parameter that can be used to bound this number.
    """

    def __init__(self, maxsize=0, max_backlog=-1):
        self.max_backlog = max_backlog
        self.cur_backlog = 0
        Queue.__init__(self, maxsize)

    def get(self, block=True, timeout=None):
        # The SQLAlchemy Queue class uses a re-entrant mutext by default,
        # so it's safe to acquire it both here and in the superclass method.
        with self.mutex:
            self.cur_backlog += 1
            try:
                if self.max_backlog >= 0:
                    if self.cur_backlog > self.max_backlog:
                        block = False
                        timeout = None
                return Queue.get(self, block, timeout)
            finally:
                self.cur_backlog -= 1


class QueuePoolWithMaxBacklog(QueuePool):
    """An SQLAlchemy QueuePool with a limit on the length of the backlog.

    The base QueuePool class sets no limit on the number of threads that can
    be simultaneously attempting to connect to the database.  This means that
    a misbehaving database can easily lock up all threads by keeping them
    waiting in the queue.

    This QueuePool subclass provides a "max_backlog" that limits the number
    of threads that can be in the queue waiting for a connection.  Once this
    limit has been reached, any further attempts to acquire a connection will
    be rejected immediately.
    """

    def __init__(self, creator, max_backlog=-1, **kwds):
        QueuePool.__init__(self, creator, **kwds)
        self._pool = _QueueWithMaxBacklog(self._pool.maxsize, max_backlog)

    def recreate(self):
        new_self = QueuePool.recreate(self)
        new_self._pool = _QueueWithMaxBacklog(self._pool.maxsize,
                                              self._pool.max_backlog)
        return new_self

    @metrics_timer("syncstorage.storage.sql.pool.get")
    def _do_get(self):
        return QueuePool._do_get(self)


class DBConnector(object):
    """Database connector class for SQL access layer.

    This class, along with its companion class DBConnection, provide the
    layer through which to access the SQL database.  It is a thin layer
    on top of the SQLAlchemy engine/connection machinery, with the following
    additional features:

        * transparent sharding of BSO storage tables
        * use pre-defined queries rather than inline construction of SQL
        * accessor methods that automatically clean up database resources
        * automatic retry of connections that are invalidated by the server

    """

    def __init__(self, sqluri, create_tables=False, pool_size=100,
                 no_pool=False, pool_recycle=60, reset_on_return=True,
                 pool_max_overflow=10, pool_max_backlog=-1, pool_timeout=30,
                 shard=False, shardsize=100, **kwds):

        parsed_sqluri = urlparse.urlparse(sqluri)
        self.sqluri = sqluri
        self.driver = parsed_sqluri.scheme.lower()
        if "mysql" in self.driver:
            self.driver = "mysql"
        elif "postgres" in self.driver:
            self.driver = "postgres"

        if self.driver not in ("mysql", "sqlite", "postgres"):
            msg = "your db driver is not officially supported"
            logger.warn(msg)

        self.shard = shard
        self.shardsize = shardsize

        # Construct the pooling-related arguments for SQLAlchemy engine.
        sqlkw = {}
        sqlkw["logging_name"] = "syncstorage"
        sqlkw["connect_args"] = {}
        if no_pool:
            sqlkw["poolclass"] = NullPool
        else:
            sqlkw["poolclass"] = QueuePoolWithMaxBacklog
            sqlkw["pool_size"] = int(pool_size)
            sqlkw["pool_recycle"] = int(pool_recycle)
            sqlkw["pool_timeout"] = int(pool_timeout)
            sqlkw["pool_reset_on_return"] = reset_on_return
            sqlkw["max_overflow"] = int(pool_max_overflow)
            sqlkw["max_backlog"] = int(pool_max_backlog)

        # Connection handling in sqlite needs some extra care.
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
        # We set the umask during this call, to ensure that any sqlite
        # databases will be created with secure permissions by default.
        old_umask = os.umask(0077)
        try:
            self.engine = create_engine(sqluri, **sqlkw)
        finally:
            os.umask(old_umask)

        # Create the tables if necessary.
        if create_tables:
            collections.create(self.engine, checkfirst=True)
            user_collections.create(self.engine, checkfirst=True)
            batch_uploads.create(self.engine, checkfirst=True)
            migration.create(self.engine, checkfirst=True)
            if not self.shard:
                bso.create(self.engine, checkfirst=True)
                bui.create(self.engine, checkfirst=True)
            else:
                for idx in xrange(self.shardsize):
                    bsoN = get_bso_table(idx)
                    bsoN.create(self.engine, checkfirst=True)
                    buiN = get_batch_item_table(idx)
                    buiN.create(self.engine, checkfirst=True)

        # Load the pre-built queries to use with this database backend.
        # Currently we have a generic set of queries, and some queries specific
        # to SQLite.  We may add more backend-specific queries in future.
        self._prebuilt_queries = {}
        query_modules = [queries_generic]
        if self.driver == "sqlite":
            query_modules.append(queries_sqlite)
        elif self.driver == "mysql":
            query_modules.append(queries_mysql)
        elif self.driver == "postgres":
            query_modules.append(queries_postgres)
        for queries in query_modules:
            for nm in dir(queries):
                if nm.isupper():
                    self._prebuilt_queries[nm] = getattr(queries, nm)

        # Constuct a Dialect object to use for rendering query objects.
        # This forces rendering of bindparams using the "named" style,
        # so that the resulting string is compatible with sqltext().
        self._render_query_dialect = copy.copy(self.engine.dialect)
        self._render_query_dialect.paramstyle = "named"

        # PyMySQL Connection objects hold a reference to their most recent
        # Result object, which can cause large datasets to remain in memory.
        # Explicitly clear it when returning a connection to the pool.
        if parsed_sqluri.scheme.lower().startswith("pymysql"):

            def clear_result_on_pool_checkin(conn, conn_record):
                if conn:
                    conn._result = None

            sqlalchemy.event.listen(self.engine.pool, "checkin",
                                    clear_result_on_pool_checkin)

    def connect(self, *args, **kwds):
        """Create a new DBConnection object from this connector."""
        return DBConnection(self)

    def get_query(self, name, params):
        """Get the named pre-built query.

        This method returns an SQLAlchemy query object for the named query,
        after performing some sharding based on the given parameters.
        """
        # Get the pre-built query with that name.
        # It might be None, a string query, or a callable returning the query.
        try:
            query = self._prebuilt_queries[name]
        except KeyError:
            raise KeyError("No query named %r" % (name,))
        # If it's None then just return it, indicating a no-op.
        if query is None:
            return None
        # If it's a callable, call it with the sharded bso table.
        if callable(query):
            bso = self.get_bso_table(params.get("userid"))
            return query(bso, params)
        # If it's a string, do some interpolation and return it.
        # XXX TODO: we could pre-parse these queries at load time to look for
        # string interpolation variables, saving some time on each call.
        assert isinstance(query, basestring)
        qvars = {}
        if "%(bso)s" in query:
            if "bso" in params:
                qvars["bso"] = params["bso"]
            else:
                qvars["bso"] = self.get_bso_table(params["userid"])
        if "%(bui)s" in query:
            if "bui" in params:
                qvars["bui"] = params["bui"]
            else:
                qvars["bui"] = self.get_batch_item_table(params["batch"])
        if "%(ids)s" in query:
            bindparams = []
            for i, id in enumerate(params["ids"]):
                params["id%d" % (i,)] = id
                bindparams.append(":id%d" % (i,))
            qvars["ids"] = "(" + ",".join(bindparams) + ")"
        if qvars:
            query = query % qvars
        return query

    def get_bso_table(self, userid):
        """Get the BSO table object for the given userid."""
        if not self.shard or userid is None:
            return bso
        return get_bso_table(userid % self.shardsize)

    def get_batch_item_table(self, batchid):
        """Get the batch_upload_items table object for the given userid."""
        if not self.shard or batchid is None:
            return bui
        return get_batch_item_table(batchid % self.shardsize)


def is_retryable_db_error(engine, exc):
    """Check whether we can safely retry in response to the given db error."""
    # Any connection-related errors can be safely retried.
    if exc.connection_invalidated:
        return True
    # Try to get the MySQL error number.
    # Unfortunately this requires use of a private API.
    # The try-except will also catch cases where we're not running MySQL.
    try:
        mysql_error_code = engine.dialect._extract_error_code(exc.orig)
    except AttributeError:
        pass
    else:
        # We're seeing a sporadic bug in production with TokuDB, where an
        # INSERT ON DUPLICATE KEY UPDATE will sometimes error out with:
        #    1032: could not find record in table
        # The below flags such errors in the request metrics log, so we can
        # easily track which requests are triggering the error.
        # See https://bugzilla.mozilla.org/show_bug.cgi?id=1057892
        if mysql_error_code == 1032:
            metric_name = "syncstorage.storage.sql.tokudb_error"
            annotate_request(None, metric_name, 1)
        # The following MySQL lock-related errors can be safely retried:
        #    1205: lock wait timeout exceeded
        #    1206: lock table full
        #    1213: deadlock found when trying to get lock
        #    1689: lock aborted
        # We also retry the TokuDB ON DUPLICATE KEY error noted above.
        if mysql_error_code in (1205, 1206, 1213, 1689, 1032):
            return True
    # Any other error is assumed not to be retryable.  Better safe than sorry.
    return False


def is_operational_db_error(engine, exc):
    """Check whether the given error is an operations-related db error.

    An operations-related error is loosely defined as something caused by
    the operational environment, e.g. the database being overloaded or
    unreachable.  It doesn't represent a bug in the code.
    """
    # All OperationalError or TimeoutError instances are operational.
    if isinstance(exc, (OperationalError, TimeoutError)):
        return True
    # Any retryable error is operational.
    if isinstance(exc, DBAPIError) and is_retryable_db_error(engine, exc):
        return True
    # Everything else counts as a programming error.
    return False


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
        except Exception, exc:
            if not is_operational_db_error(self._connector.engine, exc):
                raise
            # An unexpected database-level error.
            # Log the error, then normalize it into a BackendError instance.
            # Note that this will not catch logic errors such as e.g. an
            # IntegrityError, only unexpected operational errors from the
            # database such as e.g. disconnects and timeouts.
            err = traceback.format_exc()
            err = "Caught operational db error: %s\n%s" % (exc, err)
            logger.error(err)
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
            # It's possible for the backend to fail in a way that the query
            # can be retried,  e.g. the server timed out the connection we
            # got from the pool.  If so then we can retry the query with a
            # new connection, but only if the failed connection was never
            # successfully used as part of this transaction.
            try:
                query_str = self._render_query(query, params, annotations)
                return self._exec_with_cleanup(connection, query_str, **params)
            except DBAPIError, exc:
                if not is_retryable_db_error(self._connector.engine, exc):
                    raise
                if session_was_active:
                    raise
                # Don't try to close the connection if it's already dead.
                if not exc.connection_invalidated:
                    transaction.rollback()
                    connection.close()
                connection = self._connector.engine.connect()
                transaction = connection.begin()
                annotations["retry"] = "1"
                query_str = self._render_query(query, params, annotations)
                return self._exec_with_cleanup(connection, query_str, **params)
        finally:
            # Now that the underlying connection has been used, remember it
            # so that all subsequent queries are part of the same transaction.
            if not session_was_active:
                self._connection = connection
                self._transaction = transaction

    @metrics_timer("syncstorage.storage.sql.db.execute")
    def _exec_with_cleanup(self, connection, query_str, **params):
        """Execution wrapper that kills queries if it is interrupted.

        This is a wrapper around connection.execute() that will clean up
        any running query if the execution is interrupted by a control-flow
        exception such as KeyboardInterrupt or gevent.Timeout.

        The cleanup currently works only for the PyMySQL driver.  Other
        drivers will still execute fine, they just won't get the cleanup.
        """
        try:
            return connection.execute(sqltext(query_str), **params)
        except Exception:
            # Normal exceptions are passed straight through.
            raise
        except BaseException:
            # Control-flow exceptions trigger the cleanup logic.
            exc, val, tb = sys.exc_info()
            logger.warn("query was interrupted by %s", val)
            # Only cleanup SELECT, INSERT or UPDATE statements.
            # There are concerns that rolling back DELETEs is too costly.
            if not SAFE_TO_KILL_QUERY.match(query_str):
                msg = "  refusing to kill unsafe query: %s"
                logger.warn(msg, query_str[:100])
                raise
            try:
                # The KILL command is specific to MySQL, and this method of
                # getting the threadid is specific to the PyMySQL driver.
                # Other drivers will cause an AttributeError, failing through
                # to the "finally" clause at the end of this block.
                thread_id = connection.connection.server_thread_id[0]
                logger.warn("  killing connection %d", thread_id)
                cleanup_query = "KILL %d" % (thread_id,)
                # Use a freshly-created connection so that we don't block
                # waiting for something from the pool.  Unfortunately this
                # requires use of a private API and raw cursor access.
                cleanup_conn = self._connector.engine.pool._create_connection()
                try:
                    cleanup_cursor = cleanup_conn.connection.cursor()
                    try:
                        cleanup_cursor.execute(cleanup_query)
                    except Exception:
                        msg = "  failed to kill %d"
                        logger.exception(msg, thread_id)
                        raise
                    finally:
                        cleanup_cursor.close()
                    msg = "  successfully killed %d"
                    logger.warn(msg, thread_id)
                finally:
                    cleanup_conn.close()
            finally:
                try:
                    # Don't return this connection to the pool.
                    connection.invalidate()
                finally:
                    # Always re-raise the original error.
                    raise exc, val, tb

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
            dialect = self._connector._render_query_dialect
            compiled = query.compile(dialect=dialect)
            for param, value in compiled.params.iteritems():
                params.setdefault(param, value)
            query_str = str(compiled)
        # Join all the annotations into a comment string.
        if annotations:
            annotation_items = sorted(annotations.items())
            annotation_strs = ("%s=%s" % item for item in annotation_items)
            comment = "/* [" + ", ".join(annotation_strs) + "] */"
            # Add it to the query, at the front if possible.
            # SQLite chokes on leading comments, in that case put it at back.
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

    def insert_or_update(self, table, items, defaults=None, annotations=None):
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
        annotations.setdefault("queryName", "UPSERT_%s" % (table,))
        # Inserting zero items is strange, but allowed.
        if not items:
            return 0
        # Find the table object into which we're inserting.
        if table == "bso":
            # To work properly with sharding, all items must have same userid
            # so that we can select a single BSO table.
            userid = items[0].get("userid")
            table = self._connector.get_bso_table(userid)
        elif table == "batch_upload_items":
            # To work properly with sharding all items must have same batchid
            # so that we can select a single BUI table.
            batchid = items[0].get("batch")
            table = self._connector.get_batch_item_table(batchid)
        else:
            table = metadata.tables[table]
        # Dispatch to an appropriate implementation.
        if self._connector.driver == "mysql":
            return self._upsert_onduplicatekey(table, items, defaults,
                                               annotations)
        else:
            return self._upsert_generic(table, items, defaults, annotations)

    def _upsert_generic(self, table, items, defaults, annotations):
        """Upsert a batch of items one at a time, trying UPDATE then INSERT.

        This is a tremendously inefficient way to write a batch of items,
        but it's guaranteed to work without special cooperation from the
        database.  For MySQL we use the much improved _upsert_onduplicatekey.
        """
        userid = items[0].get("userid")
        num_created = 0
        for item in items:
            assert item.get("userid") == userid
            # Try to update the item.
            # Use the table's primary key fields in the WHERE clause,
            # and put all other fields into the UPDATE clause.
            # We have to do this first as the insert might cause a
            # conflict and abort the enclosing transacrtion.
            values = item.copy()
            query = update(table)
            for key in table.primary_key:
                try:
                    query = query.where(key == values.pop(key.name))
                except KeyError:
                    msg = "Item is missing primary key column %r"
                    raise ValueError(msg % (key.name,))
            query = query.values(**values)
            res = self.execute(query, {}, annotations)
            res.close()
            # If the item wasnt there, insert it instead.
            if res.rowcount == 0:
                query = insert(table)
                if defaults is not None:
                    query = query.values(**defaults)
                query = query.values(**item)
                self.execute(query, {}, annotations).close()
                num_created += 1
        return num_created

    def _upsert_onduplicatekey(self, table, items, defaults, annotations):
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
        userid = items[0].get("userid")
        # Group the items to be inserted into batches that all have the same
        # set of fields.  Each batch will have the same ON DUPLICATE KEY UPDATE
        # clause and so can be sent as a single query.
        batches = defaultdict(list)
        for item in items:
            assert item.get("userid") == userid
            batches[frozenset(item.iterkeys())].append(item)
        # Now construct and send an appropriate query for each batch.
        num_created = 0
        for batch in batches.itervalues():
            # Since we're crafting SQL by hand, assert that each field is
            # actually a plain alphanum field name.  Can't be too careful...
            update_fields = batch[0].keys()
            insert_fields = batch[0].keys()
            if defaults is not None:
                for field in defaults:
                    if field not in batch[0]:
                        insert_fields.append(field)
            assert all(SAFE_FIELD_NAME_RE.match(f) for f in update_fields)
            assert all(SAFE_FIELD_NAME_RE.match(f) for f in insert_fields)
            # Each item corresponds to a set of bindparams and a matching
            # entry in the "VALUES" clause of the query.
            query = "INSERT INTO %s (%s) VALUES "\
                    % (table.name, ",".join(insert_fields))
            binds = [":%s%%(num)d" % field for field in insert_fields]
            pattern = "(%s) " % ",".join(binds)
            params = {}
            vclauses = []
            for num, item in enumerate(batch):
                vclauses.append(pattern % {"num": num})
                for field in insert_fields:
                    try:
                        value = item[field]
                    except KeyError:
                        value = defaults[field]
                    params["%s%d" % (field, num)] = value
            query += ",".join(vclauses)
            # The ON DUPLICATE KEY CLAUSE updates all the given fields.
            updates = ["%s = VALUES(%s)" % (f, f) for f in update_fields]
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
