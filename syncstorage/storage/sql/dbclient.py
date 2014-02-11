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

from sqlalchemy import (Integer, String, Text, BigInteger,
                        MetaData, Column, Table, Index)

from mozsvc.storage.dbclient import DBClient

from syncstorage.storage.sql import (queries_generic,
                                     queries_sqlite,
                                     queries_mysql)


MAX_TTL = 2100000000

metadata = MetaData()


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


# Column definitions for BSO storage table/tables.
#
# This list class defines the columns used for storage of BSO records.
# It is used to create either sharded or non-shareded BSO storage tables,
# depending on the run-time settings of the application.

def _get_bso_columns(table_name):
    return (
        Column("id", String(64), primary_key=True, autoincrement=False),
        Column("userid", Integer, primary_key=True, nullable=False,
               autoincrement=False),
        Column("collection", Integer, primary_key=True, nullable=False,
               autoincrement=False),
        Column("sortindex", Integer),
        Column("modified", BigInteger),
        Column("payload", Text, nullable=False, server_default=""),
        Column("payload_size", Integer, nullable=False,
               server_default=sqltext("0")),
        Column("ttl", Integer, server_default=sqltext(str(MAX_TTL))),
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

#  If the storage controller is doing sharding based on userid,
#  then it will use the below functions to select a table from "bso0"
#  to "bsoN" for each userid.

BSO_SHARDS = {}


def get_bso_table(index):
    """Get the Table object for table bso<N>."""
    bso = BSO_SHARDS.get(index)
    if bso is None:
        table_name = "bso%d" % (index,)
        bso = Table(table_name, metadata, *_get_bso_columns(table_name))
        BSO_SHARDS[index] = bso
    return bso


class DBClient(DBClient):
    """Custom database connector class for SQL access layer."""

    def __init__(self, sqluri, shard=False, shardsize=100, **kwds):
        self.shard = shard
        self.shardsize = shardsize
        super(DBClient, self).__init__(sqluri, **kwds)

        # Load the pre-built queries to use with this database backend.
        # Currently we have a generic set of queries, and some queries specific
        # to SQLite.  We may add more backend-specific queries in future.
        self._prebuilt_queries = {}
        query_modules = [queries_generic]
        if self.driver == "sqlite":
            query_modules.append(queries_sqlite)
        elif self.driver == "mysql":
            query_modules.append(queries_mysql)
        for queries in query_modules:
            for nm in dir(queries):
                if nm.isupper():
                    self._prebuilt_queries[nm] = getattr(queries, nm)

    def get_all_tables(self):
        yield collections
        yield user_collections.create(self.engine, checkfirst=True)
        if not self.shard:
            yield bso
        else:
            for idx in xrange(self.shardsize):
                yield get_bso_table(idx)

    def get_table(self, name, params):
        if name != "bso":
            return getattr(metadata, name)
        userid = params.get("userid", None)
        if not self.shard or userid is None:
            return bso
        return get_bso_table(userid % self.shardsize)

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
        if "%(ids)s" in query:
            bindparams = []
            for i, id in enumerate(params["ids"]):
                params["id%d" % (i,)] = id
                bindparams.append(":id%d" % (i,))
            qvars["ids"] = "(" + ",".join(bindparams) + ")"
        if qvars:
            query = query % qvars
        return query
