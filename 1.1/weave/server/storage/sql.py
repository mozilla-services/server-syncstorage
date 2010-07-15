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
SQL backend
"""
from sqlalchemy import create_engine
from sqlalchemy.sql import text

from weave.server.storage import register
from weave.server.storage.sqlmappers import tables

_SQLURI = 'mysql://sync:sync@localhost/sync'
engine = create_engine(_SQLURI)

for table in tables:
    table.metadata.bind = engine


class WeaveSQLStorage(object):

    def __init__(self):
        self._conn = engine.connect()

    def get_name(self):
        """Returns the name of the storage"""
        return 'sql'

    #
    # Users APIs -- the user identifier is the 'username' field
    #

    def user_exists(self, user_name):
        """Returns true if the user exists."""
        query = text('select id from users where username = :username')
        res = self._conn.execute(query, username=user_name).fetchone()
        return res is not None

    def set_user(self, user_name, **values):
        """set information for a user. values contains the fields to set.

        If the user doesn't exists, it will be created."""
        values['username'] = user_name
        if not self.user_exists(user_name):
            fields = values.keys()
            params = ','.join([':%s' % field for field in fields])
            fields = ','.join(fields)
            query = text('insert into users (%s) values (%s)' % \
                            (fields, params))
        else:
            fields = values.keys()
            params = ','.join(['%s = :%s' % (field, field)
                               for field in fields])
            query = text('update users set %s where username = :username' \
                         % params)

        return self._conn.execute(query, **values)

    def get_user(self, user_name, fields=None):
        """Returns user information.

        If fields is provided, its a list of fields to return
        """
        if fields is None:
            fields = ['*']
        fields = ', '.join(fields)
        query = text('select %s from users where username = :username' \
                     % fields)
        return self._conn.execute(query, username=user_name).first()

    def delete_user(self, user_name):
        """Removes a user (and all its data)"""
        user_id = self._get_user_id(user_name)

        # remocing collections
        query = text('delete from collections where '
                     'userid = :userid')
        self._conn.execute(query, userid=user_id)

        # removing items
        query = text('delete from wbo where '
                     'username = :userid')
        self._conn.execute(query, userid=user_id)

        # XXX remove reset codes

        # removing user
        query = 'delete from users where username = :username'
        return self._conn.execute(query, username=user_name)

    def _get_user_id(self, user_name):
        """Returns a user id, given the name

        XXX We need to cache this, or to alter the DB (userid -> username)
        """
        data = self.get_user(user_name, ['id'])
        if data is None:
            return None
        return data[0]

    def _get_collection_id(self, user_name, collection_name):
        """Returns a collection id, given the name

        XXX We need to cache this, or to alter the DB
        """
        data = self.get_collection(user_name, collection_name,
                                   ['collectionid'])
        if data is None:
            return None
        return data[0]

    #
    # Collections APIs
    #

    def delete_collection(self, user_name, collection_name):
        """deletes a collection"""
        if not self.collection_exists(user_name, collection_name):
            return

        # removing items first
        self.delete_items(user_name, collection_name)

        user_id = self._get_user_id(user_name)
        query = text('delete from collections where '
                     'userid = :userid and name = :name')

        return self._conn.execute(query, userid=user_id, name=collection_name)

    def collection_exists(self, user_name, collection_name):
        """Returns True if the collection exists"""
        user_id = self._get_user_id(user_name)
        query = text('select collectionid from collections where '
                     'userid = :userid and name = :name')
        res = self._conn.execute(query, userid=user_id,
                                 name=collection_name)
        res = res.fetchone()
        return res is not None

    def set_collection(self, user_name, collection_name, **values):
        """Creates a collection"""
        # XXX values is not used for now because there are no values besides
        # the name
        if self.collection_exists(user_name, collection_name):
            return

        user_id = self._get_user_id(user_name)
        values['userid'] = user_id
        values['name'] = collection_name

        # getting the max collection_id
        # XXX why don't we have an autoinc here ?
        # instead
        max = ('select max(collectionid) from collections where '
                'userid = :userid')
        max = self._conn.execute(max, userid=user_id).first()
        if max[0] is None:
            next_id = 1
        else:
            next_id = max[0] + 1

        # insertion
        values['collectionid'] = next_id
        fields = values.keys()
        params = ','.join([':%s' % field for field in fields])
        fields = ','.join(fields)
        query = text('insert into collections (%s) values (%s)' % \
                        (fields, params))
        return self._conn.execute(query, **values)

    def get_collection(self, user_name, collection_name, fields=None):
        """Return information about a collection."""
        user_id = self._get_user_id(user_name)
        if fields is None:
            fields = ['*']
        fields = ', '.join(fields)
        query = text('select %s from collections where '
                     'userid = :userid and name = :name'\
                     % fields)
        return self._conn.execute(query, userid=user_id,
                                  name=collection_name).first()

    def get_collections(self, user_name, fields=None):
        """returns the collections information """
        user_id = self._get_user_id(user_name)
        if fields is None:
            fields = ['*']
        fields = ', '.join(fields)
        query = text('select %s from collections where userid = :userid'
                     % fields)
        return self._conn.execute(query, userid=user_id).fetchall()

    def get_collection_names(self, user_name):
        """return the collection names for a given user"""
        user_id = self._get_user_id(user_name)
        query = text('select name from collections '
                     'where userid = :userid')
        return self._conn.execute(query, userid=user_id).fetchall()


    #
    # Items APIs
    #

    def item_exists(self, user_name, collection_name, item_id):
        """Returns True if an item exists."""
        user_id = self._get_user_id(user_name)
        collection_id = self._get_collection_id(user_name, collection_name)
        query = text('select id from wbo where '
                     'username = :user_id and collection = :collection_id '
                     'and id = :item_id')
        res = self._conn.execute(query, user_id=user_id, item_id=item_id,
                                 collection_id=collection_id)
        res = res.fetchone()
        return res is not None

    def get_items(self, user_name, collection_name, fields=None):
        """returns items from a collection"""
        user_id = self._get_user_id(user_name)
        collection_id = self._get_collection_id(user_name, collection_name)
        if fields is None:
            fields = ['*']
        fields = ', '.join(fields)
        query = text('select %s from wbo where '
                     'username = :user_id and collection = :collection_id'\
                     % fields)
        return self._conn.execute(query, user_id=user_id,
                                  collection_id=collection_id).fetchall()

    def get_item(self, user_name, collection_name, item_id, fields=None):
        """returns one item"""
        user_id = self._get_user_id(user_name)
        collection_id = self._get_collection_id(user_name, collection_name)
        if fields is None:
            fields = ['*']
        fields = ', '.join(fields)
        query = text('select %s from wbo where '
                     'username = :user_id and collection = :collection_id '
                     'and id = :item_id ' % fields)
        return self._conn.execute(query, user_id=user_id, item_id=item_id,
                                  collection_id=collection_id).first()

    def set_item(self, user_name, collection_name, item_id, **values):
        """Adds or update an item"""
        values['username'] = self._get_user_id(user_name)
        values['collection'] = self._get_collection_id(user_name,
                                                       collection_name)
        values['id'] = item_id

        if not self.item_exists(user_name, collection_name, item_id):
            fields = values.keys()
            params = ','.join([':%s' % field for field in fields])
            fields = ','.join(fields)
            query = text('insert into wbo (%s) values (%s)' % \
                            (fields, params))
        else:
            fields = values.keys()
            params = ','.join(['%s = :%s' % (field, field)
                               for field in fields if field != ''])
            query = text('update wbo set %s where id = :id' \
                         % params)

        return self._conn.execute(query, **values)

    def delete_item(self, user_name, collection_name, item_id):
        """Deletes an item"""
        user_id = self._get_user_id(user_name)
        collection_id = self._get_collection_id(user_name, collection_name)
        query = text('delete from wbo where username = :user_id and '
                     'collection = :collection_id and id = :item_id')
        return self._conn.execute(query, user_id=user_id,
                                  collection_id=collection_id, item_id=item_id)

    def delete_items(self, user_name, collection_name, item_ids=None):
        """Deletes items. All items are removed unless item_ids is provided"""
        user_id = self._get_user_id(user_name)
        collection_id = self._get_collection_id(user_name, collection_name)

        if item_ids is None:
            query = text('delete from wbo where username = :user_id and '
                         'collection = :collection_id')
        else:
            ids = ', '.join(item_ids)
            query = text('delete from wbo where username = :user_id and '
                         'collection = :collection_id and id in (%s)' % ids)

        return self._conn.execute(query, user_id=user_id,
                                  collection_id=collection_id)


register(WeaveSQLStorage)
