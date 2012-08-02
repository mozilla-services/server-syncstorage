# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
RDBMS Table Definitions for SyncStorage.

This module provides the table definitions used to store sync data.
It provides three database tables:

  collections:  the names and ids of all collections in the store
  collection_timestamps:  the per-user timestamps for each collection
  bso:  the individual BSO items stored in each collection

For efficiency when dealing with large datasets, this module also supports
sharding of the BSO items into multiple tables named "bso0" through "bsoN".
This behaviour is off by default; pass shard=True to enable it.
"""

from sqlalchemy import (Integer, String, Text, BigInteger,
                        MetaData, Column, Table, Index)


MAX_TTL = 2100000000

metadata = MetaData()

# Table mapping collection_name => collection_id.
#
# This table holds the names and corresponding ids of the collections in
# use on the storage node.  The collection id space is global, since we
# expect most users to have the same small, static set of collection names.

collections = Table("collections", metadata,
    Column("collectionid", Integer, primary_key=True, nullable=False,
                           autoincrement=True),
    Column("name", String(32), nullable=False, unique=True)
)


# Table mapping (user_id, collection_id) => collection-level metadata.
#
# This table holds collection-level metadata on a per-user basis.  Currently
# the only such metadata is the last-modified time of the collection.

user_collections = Table("user_collections", metadata,
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

def make_bso_table(table_name):
    if table_name in metadata.tables:
        return metadata.tables[table_name]
    return Table(table_name, metadata,
        Column("id", String(64), primary_key=True, autoincrement=False),
        Column("userid", Integer, primary_key=True, nullable=False,
                         autoincrement=False),
        Column("collection", Integer, primary_key=True, nullable=False,
                             autoincrement=False),
        Column("sortindex", Integer),
        Column("modified", BigInteger),
        Column("payload", Text, nullable=False, default=""),
        Column("payload_size", Integer, nullable=False, default=0),
        Column("ttl", Integer, default=MAX_TTL),
        # Declare indexes.
        # We need to include the tablename in the index name due to sharding,
        # because index names in sqlite are global, not per-table.
        # Index on "ttl" for easy pruning of expired items.
        Index("{}_ttl_idx".format(table_name), "ttl"),
        # Index on "modified" for easy filtering by older/newer.
        Index("{}_usr_col_mod_idx".format(table_name),
              "userid", "collection", "modified"),
        # There is intentionally no index on "sortindex".
        # Clients almost always filter on "modified" using the above index,
        # and cannot take advantage of a separate index for sorting.
    )
