# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Pre-built queries for the SQL storage backend.

This module defines a set of pre-built queries for the SQL storage backend.
The function get_query(name, user_id) will retrieve the text for the named
query while taking WBO table sharding into account.
"""

from syncstorage.storage.sqlmappers import collections, wbo, get_wbo_table
from sqlalchemy.sql import select, bindparam, delete, and_, text

_USER_N_COLL = and_(collections.c.userid == bindparam('user_id'),
                    collections.c.name == bindparam('collection_name'))

queries = {
    'DELETE_SOME_USER_WBO': 'DELETE FROM %(wbo)s WHERE username=:user_id AND '
                            'collection=:collection_id AND id=:item_id',

    'DELETE_USER_COLLECTIONS': 'DELETE FROM collections WHERE '
                               'userid=:user_id',

    'DELETE_USER_COLLECTION': delete(collections).where(_USER_N_COLL),

    'DELETE_USER_WBOS': 'DELETE FROM %(wbo)s WHERE username=:user_id',

    'COLLECTION_EXISTS': select([collections.c.collectionid], _USER_N_COLL),

    'COLLECTION_NEXTID': 'SELECT MAX(collectionid) FROM collections '
                         'WHERE userid=:user_id',

    'COLLECTION_MODIFIED': 'SELECT wbo_a.modified FROM %(wbo)s AS wbo_a, '
                           '%(wbo)s WHERE wbo_a.username=%(wbo)s.username '
                           'AND wbo_a.collection=%(wbo)s.collection '
                           'ORDER BY wbo_a.username DESC, '
                           'wbo_a.collection DESC, wbo_a.modified DESC '
                           'LIMIT 1',

    'COLLECTION_STAMPS': 'SELECT collection, MAX(modified) FROM %(wbo)s '
                         'WHERE username=:user_id GROUP BY username, '
                         'collection',

    'COLLECTION_COUNTS': 'SELECT collection, COUNT(collection) FROM %(wbo)s '
                         'WHERE username=:user_id AND ttl>:ttl '
                         'GROUP BY collection',

    'COLLECTION_MAX_STAMPS': 'SELECT MAX(modified) FROM %(wbo)s WHERE '
                             'collection=:collection_id AND '
                             'username=:user_id',

    'ITEM_EXISTS': 'SELECT modified FROM %(wbo)s WHERE '
                   'collection=:collection_id AND username=:user_id '
                   'AND id=:item_id AND ttl>:ttl',

    'DELETE_ITEMS': 'DELETE FROM %(wbo)s WHERE collection=:collection_id AND '
                    'username=:user_id AND ttl>:ttl',

    'USER_STORAGE_SIZE': 'SELECT SUM(payload_size) FROM %(wbo)s WHERE '
                         'username=:user_id AND ttl>:ttl',

    'COLLECTIONS_STORAGE_SIZE': 'SELECT collection, SUM(payload_size) '
                                'FROM %(wbo)s WHERE username=:user_id AND '
                                'ttl>:ttl GROUP BY collection',

    'USER_COLLECTION_NAMES': 'SELECT collectionid, name FROM collections '
                             'WHERE userid=:user_id',
    }


def get_query(name, user_id=None):
    """Get the named pre-built query, sharding on user_id if given.

    This is a helper function to return an appropriate pre-built SQL query
    while taking sharding of the WBO table into account.  Call it with the
    name of the query and optionally the user_id on which to shard.
    """
    if user_id is None:
        table = wbo
    else:
        table = get_wbo_table(user_id)

    queries['ITEM_ID_COL_USER'] = and_(
        table.c.collection == bindparam('collection_id'),
        table.c.username == bindparam('user_id'),
        table.c.id == bindparam('item_id'),
        table.c.ttl > bindparam('ttl'))

    query = queries.get(name)
    if query is None:
        raise ValueError(name)

    if isinstance(query, str):
        if '%(wbo)s' in query:
            query = query % {'wbo': table.name}
        query = text(query)

    return query
