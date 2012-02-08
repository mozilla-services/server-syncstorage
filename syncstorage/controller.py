# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Storage controller. Implements all info, user APIs from:

http://docs.services.mozilla.com/storage/apis-2.0.html

"""
import simplejson as json
import itertools

from pyramid.httpexceptions import (HTTPBadRequest,
                                    HTTPNotFound,
                                    HTTPPreconditionFailed)

from mozsvc.util import round_time
from mozsvc.exceptions import (ERROR_MALFORMED_JSON, ERROR_INVALID_WBO,
                               ERROR_INVALID_WRITE, ERROR_OVER_QUOTA)

from syncstorage import logger
from syncstorage.wbo import WBO
from syncstorage.storage import get_storage


_WBO_FIELDS = ['id', 'parentid', 'predecessorid', 'sortindex', 'modified',
               'payload']
_ONE_MEG = 1024


def HTTPJsonBadRequest(data, **kwds):
    kwds.setdefault("content_type", "application/json")
    return HTTPBadRequest(body=json.dumps(data, use_decimal=True), **kwds)


def batch(iterable, size=100):
    """Returns the given iterable split into batches of the given size."""
    counter = itertools.count()

    def ticker(key):
        return next(counter) // size

    for key, group in itertools.groupby(iter(iterable), ticker):
        yield group


class StorageController(object):

    def __init__(self, config):
        settings = config.registry.settings
        self.batch_size = settings.get('storage.batch_size', 100)
        self.batch_max_count = settings.get('storage.batch_max_count',
                                            100)
        self.batch_max_bytes = settings.get('storage.batch_max_bytes',
                                            1024 * 1024)

    def _has_modifiers(self, data):
        return 'payload' in data

    def _get_storage(self, request):
        return get_storage(request)

    def _was_modified(self, request, user_id, collection_name):
        """Checks the X-If-Unmodified-Since header."""
        unmodified = request.headers.get('X-If-Unmodified-Since')
        if unmodified is None:
            return False
        unmodified = round_time(unmodified)
        storage = self._get_storage(request)
        max = storage.get_collection_max_timestamp(user_id,
                                                   collection_name)
        if max is None:
            return False

        return max > unmodified

    def get_storage(self, request):
        # XXX returns a 400 if the root is called
        raise HTTPBadRequest()

    def get_collections(self, request, **metrics):
        """Returns a hash of collections associated with the account,
        Along with the last modified timestamp for each collection
        """
        user_id = request.user['userid']
        storage = self._get_storage(request)
        collections = storage.get_collection_timestamps(user_id)
        request.response.headers['X-Weave-Records'] = str(len(collections))
        return collections

    def get_collection_counts(self, request):
        """Returns a hash of collections associated with the account,
        Along with the total number of items for each collection.
        """
        user_id = request.user['userid']
        counts = self._get_storage(request).get_collection_counts(user_id)
        request.response.headers['X-Weave-Records'] = str(len(counts))
        return counts

    def get_quota(self, request):
        user_id = request.user['userid']
        used = self._get_storage(request).get_total_size(user_id)
        if not self._get_storage(request).use_quota:
            limit = None
        else:
            limit = self._get_storage(request).quota_size
        return used, limit

    def get_collection_usage(self, request):
        user_id = request.user['userid']
        storage = self._get_storage(request)
        return storage.get_collection_sizes(user_id)

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
                if arg in ('older', 'newer'):
                    value = round_time(value)
                else:
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
        collection_name = request.matchdict['collection']
        user_id = request.user['userid']
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

        request.response.headers['X-Weave-Records'] = str(len(res))
        return res

    def get_item(self, request, full=True):  # always full
        """Returns a single WBO object."""
        collection_name = request.matchdict['collection']
        item_id = request.matchdict['item']
        user_id = request.user['userid']
        fields = _WBO_FIELDS
        storage = self._get_storage(request)
        res = storage.get_item(user_id, collection_name, item_id,
                               fields=fields)
        if res is None:
            raise HTTPNotFound()

        return res

    def _check_quota(self, request):
        """Checks the quota.

        If under the treshold, adds a header
        If the quota is reached, issues a 400
        """
        user_id = request.user['userid']
        storage = self._get_storage(request)
        left = storage.get_size_left(user_id)
        if left < _ONE_MEG:
            left = storage.get_size_left(user_id, recalculate=True)
        if left <= 0.:  # no space left
            raise HTTPJsonBadRequest(ERROR_OVER_QUOTA)
        return left

    def set_item(self, request):
        """Sets a single WBO object."""
        storage = self._get_storage(request)
        if storage.use_quota:
            left = self._check_quota(request)
        else:
            left = 0.

        user_id = request.user['userid']
        collection_name = request.matchdict['collection']
        item_id = request.matchdict['item']

        if self._was_modified(request, user_id, collection_name):
            raise HTTPPreconditionFailed(collection_name)

        try:
            data = json.loads(request.body)
        except ValueError:
            raise HTTPJsonBadRequest(ERROR_MALFORMED_JSON)

        try:
            wbo = WBO(data)
        except ValueError:
            raise HTTPJsonBadRequest(ERROR_INVALID_WBO)

        consistent, msg = wbo.validate()
        if not consistent:
            raise HTTPJsonBadRequest(ERROR_INVALID_WBO)

        if self._has_modifiers(wbo):
            wbo['modified'] = request.server_time

        res = storage.set_item(user_id, collection_name, item_id, **wbo)
        if storage.use_quota and left <= _ONE_MEG:
            request.response.headers['X-Weave-Quota-Remaining'] = str(left)
        return res

    def delete_item(self, request):
        """Deletes a single WBO object."""
        collection_name = request.matchdict['collection']
        item_id = request.matchdict['item']

        user_id = request.user['userid']
        if self._was_modified(request, user_id, collection_name):
            raise HTTPPreconditionFailed(collection_name)

        self._get_storage(request).delete_item(user_id, collection_name,
                                              item_id,
                                              storage_time=request.server_time)

        return request.server_time

    def set_collection(self, request):
        """Sets a batch of WBO objects into a collection."""

        user_id = request.user['userid']
        collection_name = request.matchdict['collection']

        if self._was_modified(request, user_id, collection_name):
            raise HTTPPreconditionFailed(collection_name)

        try:
            wbos = json.loads(request.body)
        except ValueError:
            raise HTTPJsonBadRequest(ERROR_MALFORMED_JSON)

        if not isinstance(wbos, (tuple, list)):
            # thats a batch of one
            try:
                id_ = str(wbos['id'])
            except (KeyError, TypeError):
                raise HTTPJsonBadRequest(ERROR_INVALID_WBO)
            if '/' in id_:
                raise HTTPJsonBadRequest(ERROR_INVALID_WBO)

            request.matchdict['item'] = id_
            return self.set_item(request)

        res = {'success': [], 'failed': {}}

        # Sanity-check each of the WBOs.
        # Limit the batch based on both count and payload size.
        kept_wbos = []
        total_bytes = 0
        for count, wbo in enumerate(wbos):
            try:
                wbo = WBO(wbo)
            except ValueError:
                res['failed'][''] = ['invalid wbo']
                continue

            if 'id' not in wbo:
                res['failed'][''] = ['invalid id']
                continue

            consistent, msg = wbo.validate()
            item_id = wbo['id']
            if not consistent:
                res['failed'][item_id] = [msg]
                continue

            if count >= self.batch_max_count:
                res['failed'][item_id] = ['retry wbo']
                continue
            if 'payload' in wbo:
                total_bytes += len(wbo['payload'])
            if total_bytes >= self.batch_max_bytes:
                res['failed'][item_id] = ['retry bytes']
                continue

            if self._has_modifiers(wbo):
                wbo['modified'] = request.server_time

            kept_wbos.append(wbo)

        storage = self._get_storage(request)
        if storage.use_quota:
            left = self._check_quota(request)
        else:
            left = 0.

        storage_time = request.server_time

        for wbos in batch(kept_wbos, size=self.batch_size):
            wbos = list(wbos)   # to avoid exhaustion
            try:
                storage.set_items(user_id, collection_name,
                                  wbos, storage_time=storage_time)

            except Exception, e:   # we want to swallow the 503 in that case
                # something went wrong
                logger.error('Could not set items')
                logger.error(str(e))
                for wbo in wbos:
                    res['failed'][wbo['id']] = str(e)
            else:
                res['success'].extend([wbo['id'] for wbo in wbos])

        res['modified'] = storage_time
        if storage.use_quota and left <= 1024:
            request.response.headers['X-Weave-Quota-Remaining'] = str(left)
        return res

    def delete_collection(self, request, **kw):
        """Deletes the collection and all contents.

        Additional request parameters may modify the selection of which
        items to delete.
        """
        kw = self._convert_args(kw)
        collection_name = request.matchdict['collection']
        user_id = request.user['userid']
        if self._was_modified(request, user_id, collection_name):
            raise HTTPPreconditionFailed(collection_name)

        self._get_storage(request).delete_items(user_id,
                                        collection_name,
                                        kw.get('ids'), kw['filters'],
                                        limit=kw.get('limit'),
                                        offset=kw.get('offset'),
                                        sort=kw.get('sort'),
                                        storage_time=request.server_time)

        return request.server_time

    def delete_storage(self, request):
        """Deletes all records for the user.

        Will return a precondition error unless an X-Confirm-Delete header
        is included.
        """
        if 'X-Confirm-Delete' not in request.headers:
            raise HTTPJsonBadRequest(ERROR_INVALID_WRITE)
        user_id = request.user['userid']
        self._get_storage(request).delete_storage(user_id)  # XXX failures ?
        return request.server_time
