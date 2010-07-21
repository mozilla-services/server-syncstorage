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
# The Initial Developer of the Original Code is Mozilla Foundation.
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
Storage controller. Implements all info, user APIs from:

https://wiki.mozilla.org/Labs/Weave/Sync/1.0/API

"""
from webob.exc import HTTPNotImplemented, HTTPBadRequest
from weave.server.util import json_response


class StorageController(object):

    def __init__(self, storage):
        self.storage = storage

    def index(self, request):
        return "Sync Server"

    def get_collections_info(self, request):
        """Returns a hash of collections associated with the account,
        Along with the last modified timestamp for each collection
        """
        user_id = request.sync_info['userid']
        collections = self.storage.get_collection_timestamps(user_id)
        # XXX see if we need more processing here
        res = dict([(name, stamp) for name, stamp in collections])
        return json_response(res)

    def get_collections_count(self, request):
        """Returns a hash of collections associated with the account,
        Along with the total number of items for each collection.
        """
        user_id = request.sync_info['userid']
        counts = self.storage.get_collection_counts(user_id)
        res = dict([(name, count) for name, count in counts])

        # XXX see if we need more processing here
        return json_response(res)

    def get_quota(self, request):
        raise HTTPNotImplemented

    # XXX see if we want to use kwargs here instead
    def get_collection(self, request, ids=None, predecessorid=None,
                       parentid=None, older=None, newer=None, full=False,
                       index_above=None, index_below=None, limit=None,
                       offset=None, sort=None):
        """Returns a list of the WBO ids contained in a collection."""
        # XXX sanity check on arguments (detect incompatible params here, or
        # unknown values)
        filters = {}
        if ids is not None:
            ids = [int(id_) for id_ in ids.split(',')]
            filters['id'] = 'in', ids
        if predecessorid is not None:
            filters['predecessorid'] = '=', predecessorid
        if parentid is not None:
            filters['parentid'] = '=', parentid
        if older is not None:
            filters['modified'] = '<', float(older)
        if newer is not None:
            filters['modified'] = '>', float(newer)
        if index_above is not None:
            filters['sortindex'] = '>', float(index_above)
        if index_below is not None:
            filters['sortindex'] = '<', float(index_below)

        if limit is not None:
            limit = int(limit)

        if offset is not None:
            # we need both
            if limit is None:
                raise HTTPBadRequest('"offset" cannot be used without "limit"')
            offset = int(offset)

        collection_name = request.sync_info['params'][0]
        user_id = request.sync_info['userid']
        if not full:
            fields = ['id']
        else:
            fields = None

        res = self.storage.get_items(user_id, collection_name, fields, filters,
                                     limit, offset, sort)
        return json_response([dict(line) for line in res])
