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
from syncstorage.storage.sqlmappers import (users, collections, wbo,
                                            get_wbo_table)
from sqlalchemy.sql import select, bindparam, delete, func, and_


_USER_N_COLL = and_(collections.c.userid == bindparam('user_id'),
                    collections.c.name == bindparam('collection_name'))


def get_query(name, user_id=None):
    if user_id is None:
        table = wbo
        wbo_alias = wbo.alias()
    else:
        table = get_wbo_table(user_id)

    wbo_alias = table.alias()

    if name == 'USER_EXISTS':
        return select([users.c.id], users.c.id == bindparam('user_id'))
    elif name == 'DELETE_SOME_USER_WBO':
        return delete(table).where( \
                                 and_(table.c.username == bindparam('user_id'),
                                      table.c.collection ==
                                      bindparam('collection_id'),
                                      table.c.id == bindparam('item_id')))
    elif name == 'DELETE_USER_COLLECTIONS':
        return delete(collections).where( \
                                 collections.c.userid == bindparam('user_id'))
    elif name == 'DELETE_USER_COLLECTION':
        return delete(collections).where(_USER_N_COLL)
    elif name == 'DELETE_USER_WBOS':
        return delete(table, table.c.username == bindparam('user_id'))
    elif name == 'DELETE_USER':
        return delete(users, users.c.id == bindparam('user_id'))
    elif name == 'COLLECTION_EXISTS':
        return select([collections.c.collectionid], _USER_N_COLL)
    elif name == 'COLLECTION_NEXTID':
        return select([func.max(collections.c.collectionid)],
                      collections.c.userid == bindparam('user_id'))
    elif name == 'COLLECTION_MODIFIED':
        return select([wbo_alias.c.modified],
              and_(wbo_alias.c.username == table.c.username,
                   wbo_alias.c.collection == table.c.collection)).\
              order_by(wbo_alias.c.username.desc(),
                       wbo_alias.c.collection.desc(),
                       wbo_alias.c.modified.desc()).limit(1).as_scalar()
    elif name == 'COLLECTION_STAMPS':
        return select([table.c.collection, func.max(table.c.modified)],
             and_(table.c.username == bindparam('user_id'),
                  table.c.ttl > bindparam('ttl'))).group_by(table.c.username,
                                                            table.c.collection)
    elif name == 'COLLECTION_COUNTS':
        return select([table.c.collection, func.count(table.c.collection)],
           and_(table.c.username == bindparam('user_id'),
                table.c.ttl > bindparam('ttl'))).group_by(table.c.collection)
    elif name == 'COLLECTION_MAX_STAMPS':
        return select([func.max(table.c.modified)],
            and_(table.c.collection == bindparam('collection_id'),
                 table.c.username == bindparam('user_id'),
                 table.c.ttl > bindparam('ttl')))
    elif name == 'ITEM_ID_COL_USER':
        return and_(table.c.collection == bindparam('collection_id'),
                    table.c.username == bindparam('user_id'),
                    table.c.id == bindparam('item_id'),
                    table.c.ttl > bindparam('ttl'))

    elif name == 'ITEM_EXISTS':

        col_user = and_(table.c.collection == bindparam('collection_id'),
                         table.c.username == bindparam('user_id'),
                         table.c.id == bindparam('item_id'),
                         table.c.ttl > bindparam('ttl'))
        return select([table.c.modified], col_user)
    elif name == 'DELETE_ITEMS':
        return delete(table,
                       and_(table.c.collection == bindparam('collection_id'),
                            table.c.username == bindparam('user_id'),
                            table.c.ttl > bindparam('ttl')))
    elif name == 'USER_STORAGE_SIZE':
        return select([func.sum(table.c.payload_size)],
                       and_(table.c.username == bindparam('user_id'),
                            table.c.ttl > bindparam('ttl')))
    elif name == 'COLLECTIONS_STORAGE_SIZE':
        return select([table.c.collection,
            func.sum(table.c.payload_size)],
            and_(table.c.username == bindparam('user_id'),
                 table.c.ttl > bindparam('ttl'))).group_by(table.c.collection)
    elif name == 'USER_COLLECTION_NAMES':
        return select([collections.c.collectionid,
                       collections.c.name],
                      collections.c.userid == bindparam('user_id'))

    raise ValueError(name)
