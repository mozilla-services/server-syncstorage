# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Pre-built queries for the SQL storage backend.

This module defines a set of pre-built queries for the SQL storage backend.
The function get_query(name, user_id) will retrieve the text for the named
query while taking BSO table sharding into account.
"""

from syncstorage.storage.sqlmappers import collections, bso, get_bso_table
from sqlalchemy.sql import select, bindparam, delete, and_, text

_USER_N_COLL = and_(collections.c.userid == bindparam('user_id'),
                    collections.c.name == bindparam('collection_name'))

queries = {
    'DELETE_SOME_USER_BSO': 'DELETE FROM %(bso)s WHERE userid=:user_id AND '
                            'collection=:collection_id AND id=:item_id',

    'DELETE_USER_COLLECTIONS': 'DELETE FROM collections WHERE '
                               'userid=:user_id',

    'DELETE_USER_COLLECTION': delete(collections).where(_USER_N_COLL),

    'DELETE_USER_BSOS': 'DELETE FROM %(bso)s WHERE userid=:user_id',

    'COLLECTION_EXISTS': select([collections.c.collectionid], _USER_N_COLL),

    'COLLECTION_NEXTID': 'SELECT MAX(collectionid) FROM collections '
                         'WHERE userid=:user_id',

    'COLLECTION_MODIFIED': 'SELECT bso_a.modified FROM %(bso)s AS bso_a, '
                           '%(bso)s WHERE bso_a.userid=%(bso)s.userid '
                           'AND bso_a.collection=%(bso)s.collection '
                           'ORDER BY bso_a.userid DESC, '
                           'bso_a.collection DESC, bso_a.modified DESC '
                           'LIMIT 1',

    'COLLECTION_STAMPS': 'SELECT collection, MAX(modified) FROM %(bso)s '
                         'WHERE userid=:user_id GROUP BY userid, '
                         'collection',

    'COLLECTION_COUNTS': 'SELECT collection, COUNT(collection) FROM %(bso)s '
                         'WHERE userid=:user_id AND ttl>:ttl '
                         'GROUP BY collection',

    'COLLECTION_MAX_STAMPS': 'SELECT MAX(modified) FROM %(bso)s WHERE '
                             'collection=:collection_id AND '
                             'userid=:user_id',

    'ITEM_EXISTS': 'SELECT modified FROM %(bso)s WHERE '
                   'collection=:collection_id AND userid=:user_id '
                   'AND id=:item_id AND ttl>:ttl',

    'DELETE_ITEMS': 'DELETE FROM %(bso)s WHERE collection=:collection_id AND '
                    'userid=:user_id AND ttl>:ttl',

    'USER_STORAGE_SIZE': 'SELECT SUM(payload_size) FROM %(bso)s WHERE '
                         'userid=:user_id AND ttl>:ttl',

    'COLLECTIONS_STORAGE_SIZE': 'SELECT collection, SUM(payload_size) '
                                'FROM %(bso)s WHERE userid=:user_id AND '
                                'ttl>:ttl GROUP BY collection',

    'USER_COLLECTION_NAMES': 'SELECT collectionid, name FROM collections '
                             'WHERE userid=:user_id',
    }


def get_query(name, user_id=None):
    """Get the named pre-built query, sharding on user_id if given.

    This is a helper function to return an appropriate pre-built SQL query
    while taking sharding of the BSO table into account.  Call it with the
    name of the query and optionally the user_id on which to shard.
    """
    if user_id is None:
        table = bso
    else:
        table = get_bso_table(user_id)

    queries['ITEM_ID_COL_USER'] = and_(
        table.c.collection == bindparam('collection_id'),
        table.c.userid == bindparam('user_id'),
        table.c.id == bindparam('item_id'),
        table.c.ttl > bindparam('ttl'))

    query = queries.get(name)
    if query is None:
        raise ValueError(name)

    if isinstance(query, str):
        if '%(bso)s' in query:
            query = query % {'bso': table.name}
        query = text(query)

    return query
