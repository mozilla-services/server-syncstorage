# ***** BEGIN LICENSE BLOCK *****
# Version: MPL 1.1/GPL 2.0/LGPL 2.1
#
# The contents of this file are subject to the Mozilla Public License Version
# 1.1 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
# http://www.mozilla.org/MPL/
#
# Software distributed under the License is distributed on an "AS IS" basis,
# WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
# for the specific language governing rights and limitations under the
# License.
#
# The Original Code is Sync Server
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2010
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Tarek Ziade (tarek@mozilla.com)
#
# Alternatively, the contents of this file may be used under the terms of
# either the GNU General Public License Version 2 or later (the "GPL"), or
# the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
# in which case the provisions of the GPL or the LGPL are applicable instead
# of those above. If you wish to allow use of your version of this file only
# under the terms of either the GPL or the LGPL, and not to allow others to
# use your version of this file under the terms of the MPL, indicate your
# decision by deleting the provisions above and replace them with the notice
# and other provisions required by the GPL or the LGPL. If you do not delete
# the provisions above, a recipient may use your version of this file under
# the terms of any one of the MPL, the GPL or the LGPL.
#
# ***** END LICENSE BLOCK *****
"""
Pre-built queries
"""
from syncstorage.storage.sqlmappers import collections, wbo, get_wbo_table
from sqlalchemy.sql import select, bindparam, delete, and_, text

_USER_N_COLL = and_(collections.c.userid == bindparam('user_id'),
                    collections.c.name == bindparam('collection_name'))

queries = {
    'USER_EXISTS': 'SELECT id FROM users where id = :user_id',

    'DELETE_SOME_USER_WBO': 'DELETE FROM %(wbo)s WHERE username=:user_id AND '
                            'collection=:collection_id AND id=:item_id',

    'DELETE_USER_COLLECTIONS': 'DELETE FROM collections WHERE '
                               'userid=:user_id',

    'DELETE_USER_COLLECTION': delete(collections).where(_USER_N_COLL),

    'DELETE_USER_WBOS': 'DELETE FROM %(wbo)s WHERE username=:user_id',

    'DELETE_USER': 'DELETE FROM users WHERE id=:user_id',

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
