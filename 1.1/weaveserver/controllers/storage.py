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
import simplejson as json

from webob.exc import HTTPBadRequest, HTTPNotFound, HTTPPreconditionFailed

from weaveserver.util import convert_response, json_response, round_time
from weaveserver.wbo import WBO
from weaveserver.respcodes import (WEAVE_MALFORMED_JSON, WEAVE_INVALID_WBO,
                                   WEAVE_INVALID_WRITE, WEAVE_OVER_QUOTA)

_WBO_FIELDS = ['id', 'parentid', 'predecessorid', 'sortindex', 'modified',
               'payload', 'payload_size']


class StorageController(object):

    def __init__(self, storage):
        self.storage = storage

    def index(self, request):
        return "Sync Server"

    def _has_modifiers(self, data):
        return 'payload' in data

    def _was_modified(self, request, user_id, collection_name):
        """Checks the X-If-Unmodified-Since header."""
        unmodified = request.headers.get('X-If-Unmodified-Since')
        if unmodified is None:
            return False

        unmodified = round_time(unmodified)
        max = self.storage.get_collection_max_timestamp(user_id,
                                                        collection_name)
        if max is None:
            return False
        return max > unmodified

    def get_storage(self, request):
        # XXX returns a 400 if the root is called
        raise HTTPBadRequest()

    def get_collections_info(self, request, v=None):
        """Returns a hash of collections associated with the account,
        Along with the last modified timestamp for each collection
        """
        # 'v' is the version of the client, given the first time
        user_id = request.sync_info['user_id']
        collections = self.storage.get_collection_timestamps(user_id)
        response = convert_response(request, collections)
        response.headers['X-Weave-Records'] = str(len(collections))
        return response

    def get_collections_count(self, request):
        """Returns a hash of collections associated with the account,
        Along with the total number of items for each collection.
        """
        user_id = request.sync_info['user_id']
        counts = self.storage.get_collection_counts(user_id)
        response = convert_response(request, counts)
        response.headers['X-Weave-Records'] = str(len(counts))
        return response

    def get_quota(self, request):
        if not self.storage.use_quota:
            return json_response((0.0, 0))
        user_id = request.sync_info['user_id']
        used = self.storage.get_total_size(user_id)
        return json_response((used, self.storage.quota_size))

    def get_collection_sizes(self, request):
        user_id = request.sync_info['user_id']
        return json_response(self.storage.get_collection_sizes(user_id))

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
            filters['id'] = 'in', ids.split(',')
        if predecessorid is not None:
            filters['predecessorid'] = '=', predecessorid
        if parentid is not None:
            filters['parentid'] = '=', parentid
        if older is not None:
            filters['modified'] = '<', older
        if newer is not None:
            filters['modified'] = '>', newer
        if index_above is not None:
            filters['sortindex'] = '>', float(index_above)
        if index_below is not None:
            filters['sortindex'] = '<', float(index_below)

        if limit is not None:
            limit = int(limit)

        if offset is not None:
            # we need both
            if limit is None:
                offset = None
            else:
                offset = int(offset)

        collection_name = request.sync_info['collection']
        user_id = request.sync_info['user_id']
        if not full:
            fields = ['id']
        else:
            fields = _WBO_FIELDS

        res = self.storage.get_items(user_id, collection_name, fields, filters,
                                     limit, offset, sort)
        if not full:
            res = [line['id'] for line in res]

        response = convert_response(request, res)
        response.headers['X-Weave-Records'] = str(len(res))
        return response

    def get_item(self, request, full=True):  # always full
        """Returns a single WBO object."""
        collection_name = request.sync_info['collection']
        item_id = request.sync_info['item']
        user_id = request.sync_info['user_id']
        fields = _WBO_FIELDS
        res = self.storage.get_item(user_id, collection_name, item_id,
                                    fields=fields)
        if res is None:
            raise HTTPNotFound()

        return json_response(res)

    def _check_quota(self, request):
        """Checks the quota.

        If under the treshold, adds a header
        If the quota is reached, issues a 400
        """
        user_id = request.sync_info['user_id']
        left = self.storage.get_size_left(user_id)
        if left <= 0.:  # no space left
            raise HTTPBadRequest(WEAVE_OVER_QUOTA)
        return left

    def set_item(self, request):
        """Sets a single WBO object."""
        left = self._check_quota(request)
        user_id = request.sync_info['user_id']
        collection_name = request.sync_info['collection']
        item_id = request.sync_info['item']

        if self._was_modified(request, user_id, collection_name):
            raise HTTPPreconditionFailed(collection_name)

        try:
            data = json.loads(request.body)
        except ValueError:
            raise HTTPBadRequest(WEAVE_MALFORMED_JSON)

        wbo = WBO(data)
        consistent, msg = wbo.validate()

        if not consistent:
            raise HTTPBadRequest(msg)

        if self._has_modifiers(wbo):
            wbo['modified'] = request.server_time

        res = self.storage.set_item(user_id, collection_name, item_id, **wbo)
        response = json_response(res)
        if left <= 1024:
            response.headers['X-Weave-Quota-Remaining'] = str(left)
        return response

    def delete_item(self, request):
        """Deletes a single WBO object."""
        collection_name = request.sync_info['collection']
        item_id = request.sync_info['item']
        user_id = request.sync_info['user_id']
        if self._was_modified(request, user_id, collection_name):
            raise HTTPPreconditionFailed(collection_name)
        self.storage.delete_item(user_id, collection_name, item_id)
        return json_response(request.server_time)

    def set_collection(self, request):
        """Sets a batch of WBO objects into a collection."""
        user_id = request.sync_info['user_id']
        collection_name = request.sync_info['collection']

        if self._was_modified(request, user_id, collection_name):
            raise HTTPPreconditionFailed(collection_name)

        try:
            wbos = json.loads(request.body)
        except ValueError:
            raise HTTPBadRequest(WEAVE_MALFORMED_JSON)

        if not isinstance(wbos, (tuple, list)):
            # thats a batch of one
            if 'id' not in wbos:
                raise HTTPBadRequest(WEAVE_INVALID_WBO)
            id_ = str(wbos['id'])
            if '/' in id_:
                raise HTTPBadRequest(WEAVE_INVALID_WBO)

            request.sync_info['item'] = id_
            return self.set_item(request)

        res = {'modified': request.server_time, 'success': [], 'failed': {}}

        # sanity chech
        kept_wbos = []
        for wbo in wbos:
            wbo = WBO(wbo)

            if 'id' not in wbo:
                res['failed'][''] = ['invalid id']
                continue

            if self._has_modifiers(wbo):
                wbo['modified'] = request.server_time

            consistent, msg = wbo.validate()
            item_id = wbo['id']

            if not consistent:
                res['failed'][item_id] = [msg]
            else:
                kept_wbos.append(wbo)

        left = self._check_quota(request)
        self.storage.set_items(user_id, collection_name, kept_wbos)

        # XXX how to get back the real successes w/o an extra query
        res['success'] = [wbo['id'] for wbo in kept_wbos]
        response = json_response(res)
        if left <= 1024:
            response.headers['X-Weave-Quota-Remaining'] = str(left)
        return response

    def delete_collection(self, request, ids=None, parentid=None, older=None,
                          newer=None, index_above=None, index_below=None,
                          predecessorid=None, limit=None, offset=None,
                          sort=None):
        """Deletes the collection and all contents.

        Additional request parameters may modify the selection of which
        items to delete.
        """
        # XXX sanity check on arguments (detect incompatible params here, or
        # unknown values)
        collection_name = request.sync_info['collection']
        user_id = request.sync_info['user_id']
        if self._was_modified(request, user_id, collection_name):
            raise HTTPPreconditionFailed(collection_name)

        filters = {}
        if ids is not None:
            ids = [id_.strip() for id_ in ids.split(',')]
        if parentid is not None:
            filters['parentid'] = '=', parentid
        if predecessorid is not None:
            filters['predecessorid'] = '=', predecessorid
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
                offset = None
            else:
                offset = int(offset)

        res = self.storage.delete_items(user_id, collection_name, ids, filters,
                                        limit=limit, offset=offset, sort=sort)
        return json_response(res)

    def delete_storage(self, request):
        """Deletes all records for the user.

        Will return a precondition error unless an X-Confirm-Delete header
        is included.
        """
        if 'X-Confirm-Delete' not in request.headers:
            raise HTTPBadRequest(WEAVE_INVALID_WRITE)
        user_id = request.sync_info['user_id']
        self.storage.delete_storage(user_id)  # XXX failures ?
        return json_response(True)
