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
import pprint
from StringIO import StringIO
import simplejson as json

from webob.exc import HTTPBadRequest, HTTPNotFound, HTTPPreconditionFailed
from services.util import (convert_response, json_response, round_time,
                           batch, HTTPJsonBadRequest)
from services.respcodes import (WEAVE_MALFORMED_JSON, WEAVE_INVALID_WBO,
                                WEAVE_INVALID_WRITE, WEAVE_OVER_QUOTA)
from services.util import html_response
from syncstorage.wbo import WBO

_WBO_FIELDS = ['id', 'parentid', 'predecessorid', 'sortindex', 'modified',
               'payload', 'payload_size']
_ONE_MEG = 1024


class StorageController(object):

    def __init__(self, app):
        self.app = app

    def index(self, request):
        if not self.app.config.get('storage.display_config', False):
            return HTTPNotFound()

        # let's print out the info
        res = ['<html><body><h1>Request</h1>']
        # environ
        out = StringIO()
        pprint.pprint(request.environ, out)
        out.seek(0)
        environ = out.read()
        res.append('<pre>%s</pre>' % environ)

        # config
        res.append('<h1>Storage in usage</h1>')
        storage = self._get_storage(request)
        res.append('<ul>')
        res.append('<li>backend: %s</li>' % storage.get_name())
        if storage.get_name() in ('sql',):
            res.append('<li>sqluri: %s</li>' % storage.sqluri)
        res.append('</ul>')
        res.append('</body></html>')
        return html_response(''.join(res))

    def _has_modifiers(self, data):
        return 'payload' in data

    def _get_storage(self, request):
        return self.app.get_storage(request)

    def _was_modified(self, request, user_id, collection_name):
        """Checks the X-If-Unmodified-Since header."""
        unmodified = request.headers.get('X-If-Unmodified-Since')
        if unmodified is None:
            return False
        unmodified = round_time(unmodified)
        max = self._get_storage(request).get_collection_max_timestamp(user_id,
                                                        collection_name)
        if max is None:
            return False
        return max > unmodified

    def get_storage(self, request):
        # XXX returns a 400 if the root is called
        raise HTTPBadRequest()

    def get_collections(self, request, v=None):
        """Returns a hash of collections associated with the account,
        Along with the last modified timestamp for each collection
        """
        # 'v' is the version of the client, given the first time
        user_id = request.sync_info['user_id']
        storage = self._get_storage(request)
        collections = storage.get_collection_timestamps(user_id)
        response = convert_response(request, collections)
        response.headers['X-Weave-Records'] = str(len(collections))
        return response

    def get_collection_counts(self, request):
        """Returns a hash of collections associated with the account,
        Along with the total number of items for each collection.
        """
        user_id = request.sync_info['user_id']
        counts = self._get_storage(request).get_collection_counts(user_id)
        response = convert_response(request, counts)
        response.headers['X-Weave-Records'] = str(len(counts))
        return response

    def get_quota(self, request):
        if not self._get_storage(request).use_quota:
            return json_response((0.0, 0))
        user_id = request.sync_info['user_id']
        used = self._get_storage(request).get_total_size(user_id)
        return json_response((used, self._get_storage(request).quota_size))

    def get_collection_usage(self, request):
        user_id = request.sync_info['user_id']
        storage = self._get_storage(request)
        return json_response(storage.get_collection_sizes(user_id))

    def _convert_args(self, kw):
        """Converts incoming arguments for GET and DELETE on collections.

        This function will also raise a 400 on bad args.
        Unknown args are just dropped.
        XXX see if we want to raise a 400 in that case
        """
        args = {}
        filters = {}
        convert_name = {'older': 'modified',
                        'newer': 'modified',
                        'index_above': 'sortindex',
                        'index_below': 'sortindex'}

        for arg in  ('older', 'newer', 'index_above', 'index_below'):
            value = kw.get(arg)
            if value is None:
                continue
            try:
                value = float(value)
            except ValueError:
                raise HTTPBadRequest('Invalid value for "%s"' % arg)
            if arg in ('older', 'index_below'):
                filters[convert_name[arg]] = '<', value
            else:
                filters[convert_name[arg]] = '>', value

        # convert limit and offset
        limit = offset = None
        for arg in ('limit', 'offset'):
            value = kw.get(arg)
            if value is None:
                continue
            try:
                value = int(value)
            except ValueError:
                raise HTTPBadRequest('Invalid value for "%s"' % arg)
            if arg == 'limit':
                limit = value
            else:
                offset = value

        # we can't have offset without limit
        if limit is not None:
            args['limit'] = limit

        if offset is not None and limit is not None:
            args['offset'] = offset

        for arg in ('predecessorid', 'parentid'):
            value = kw.get(arg)
            if value is None:
                continue
            filters[arg] = '=', value

        # XXX should we control id lengths ?
        for arg in ('ids',):
            value = kw.get(arg)
            if value is None:
                continue
            filters['id'] = 'in', value.split(',')

        sort = kw.get('sort')
        if sort in ('oldest', 'newest', 'index'):
            args['sort'] = sort
        args['full'] = kw.get('full', False)
        args['filters'] = filters
        return args

    def get_collection(self, request, **kw):
        """Returns a list of the WBO ids contained in a collection."""
        kw = self._convert_args(kw)
        collection_name = request.sync_info['collection']
        user_id = request.sync_info['user_id']
        full = kw['full']

        if not full:
            fields = ['id']
        else:
            fields = _WBO_FIELDS

        storage = self._get_storage(request)
        res = storage.get_items(user_id, collection_name, fields,
                                kw['filters'],
                                kw.get('limit'), kw.get('offset'),
                                kw.get('sort'))
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
        storage = self._get_storage(request)
        res = storage.get_item(user_id, collection_name, item_id,
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
        storage = self._get_storage(request)
        left = storage.get_size_left(user_id)
        if left < _ONE_MEG:
            left = storage.get_size_left(user_id, recalculate=True)
        if left <= 0.:  # no space left
            raise HTTPJsonBadRequest(WEAVE_OVER_QUOTA)
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
            raise HTTPJsonBadRequest(WEAVE_MALFORMED_JSON)

        wbo = WBO(data)
        consistent, msg = wbo.validate()

        if not consistent:
            raise HTTPBadRequest(msg)

        if self._has_modifiers(wbo):
            wbo['modified'] = request.server_time

        res = self._get_storage(request).set_item(user_id, collection_name,
                                                  item_id, **wbo)
        response = json_response(res)
        if left <= _ONE_MEG:
            response.headers['X-Weave-Quota-Remaining'] = str(left)
        return response

    def delete_item(self, request):
        """Deletes a single WBO object."""
        collection_name = request.sync_info['collection']
        item_id = request.sync_info['item']
        user_id = request.sync_info['user_id']
        if self._was_modified(request, user_id, collection_name):
            raise HTTPPreconditionFailed(collection_name)
        self._get_storage(request).delete_item(user_id, collection_name,
                                               item_id)
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
            raise HTTPJsonBadRequest(WEAVE_MALFORMED_JSON)

        if not isinstance(wbos, (tuple, list)):
            # thats a batch of one
            if 'id' not in wbos:
                raise HTTPJsonBadRequest(WEAVE_INVALID_WBO)
            id_ = str(wbos['id'])
            if '/' in id_:
                raise HTTPJsonBadRequest(WEAVE_INVALID_WBO)

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

        for wbos in batch(kept_wbos):
            wbos = list(wbos)   # to avoid exhaustion
            try:
                self._get_storage(request).set_items(user_id, collection_name,
                                                     wbos)
            except Exception, e:   # we want to swallow the 503 in that case
                # something went wrong
                for wbo in wbos:
                    res['failed'][wbo['id']] = str(e)
            else:
                res['success'].extend([wbo['id'] for wbo in wbos])

        response = json_response(res)
        if left <= 1024:
            response.headers['X-Weave-Quota-Remaining'] = str(left)
        return response

    def delete_collection(self, request, **kw):
        """Deletes the collection and all contents.

        Additional request parameters may modify the selection of which
        items to delete.
        """
        kw = self._convert_args(kw)
        collection_name = request.sync_info['collection']
        user_id = request.sync_info['user_id']
        if self._was_modified(request, user_id, collection_name):
            raise HTTPPreconditionFailed(collection_name)

        res = self._get_storage(request).delete_items(user_id,
                                        collection_name,
                                        kw.get('ids'), kw['filters'],
                                        limit=kw.get('limit'),
                                        offset=kw.get('offset'),
                                        sort=kw.get('sort'))
        return json_response(res)

    def delete_storage(self, request):
        """Deletes all records for the user.

        Will return a precondition error unless an X-Confirm-Delete header
        is included.
        """
        if 'X-Confirm-Delete' not in request.headers:
            raise HTTPJsonBadRequest(WEAVE_INVALID_WRITE)
        user_id = request.sync_info['user_id']
        self._get_storage(request).delete_storage(user_id)  # XXX failures ?
        return json_response(True)
