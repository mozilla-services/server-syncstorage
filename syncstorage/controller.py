# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Storage controller. Implements all info, user APIs from:

http://docs.services.mozilla.com/storage/apis-2.0.html

"""
import simplejson as json
import functools

from pyramid.httpexceptions import (HTTPBadRequest,
                                    HTTPNotFound,
                                    HTTPConflict,
                                    HTTPPreconditionFailed,
                                    HTTPCreated,
                                    HTTPNoContent,
                                    HTTPNotModified,
                                    HTTPUnsupportedMediaType)

from mozsvc.exceptions import (ERROR_MALFORMED_JSON, ERROR_INVALID_OBJECT,
                               ERROR_OVER_QUOTA)

from syncstorage.bso import BSO
from syncstorage.storage import get_storage, ConflictError, NotFoundError

_ONE_MEG = 1024 * 1024

# The maximum number of ids that can be deleted in a single batch operation.
MAX_IDS_PER_BATCH = 100

# How long the client should wait before retrying a conflicting write.
RETRY_AFTER = 5


def HTTPJsonBadRequest(data, **kwds):
    kwds.setdefault("content_type", "application/json")
    return HTTPBadRequest(body=json.dumps(data, use_decimal=True), **kwds)


def with_read_lock(func):
    """Method decorator to take a collection-level read lock.

    Methods decorated with this decorator will take a read lock on their
    target collection for the duration of the method.
    """
    @functools.wraps(func)
    def with_read_lock_wrapper(self, request, *args, **kwds):
        storage = self._get_storage(request)
        userid = request.user["uid"]
        collection = request.matchdict.get("collection")
        with storage.lock_for_read(userid, collection):
            return func(self, request, *args, **kwds)
    return with_read_lock_wrapper


def with_write_lock(func):
    """Method decorator to take a collection-level write lock.

    Methods decorated with this decorator will take a write lock on their
    target collection for the duration of the method.
    """
    @functools.wraps(func)
    def with_write_lock_wrapper(self, request, *args, **kwds):
        storage = self._get_storage(request)
        userid = request.user["uid"]
        collection = request.matchdict.get("collection")
        with storage.lock_for_write(userid, collection):
            return func(self, request, *args, **kwds)
    return with_write_lock_wrapper


def convert_storage_errors(func):
    """Function decorator to turn storage backend errors into HTTP errors.

    This decorator does a simple mapping from the following storage backend
    errors to their corresponding HTTP errors:

        NotFoundError => HTTPNotFound
        ConflictError => HTTPConflict

    """
    @functools.wraps(func)
    def error_converter_wrapper(*args, **kwds):
        try:
            return func(*args, **kwds)
        except NotFoundError:
            raise HTTPNotFound
        except ConflictError:
            raise HTTPConflict(headers={"Retry-After": str(RETRY_AFTER)})
    return error_converter_wrapper


class StorageController(object):

    def __init__(self, config):
        settings = config.registry.settings
        self.logger = config.registry['metlog']
        self.quota_size = settings.get("storage.quota_size", None)
        self.batch_max_count = settings.get('storage.batch_max_count',
                                            100)
        self.batch_max_bytes = settings.get('storage.batch_max_bytes',
                                            _ONE_MEG)

    def _get_storage(self, request):
        return get_storage(request)

    def _check_if_unmodified(self, request):
        """Check the X-If-Unmodified-Since header."""
        unmodified = request.headers.get("X-If-Unmodified-Since")
        if unmodified is None:
            return None

        try:
            unmodified = int(unmodified)
        except ValueError:
            msg = 'Invalid value for "X-If-Unmodified-Since": %r'
            raise HTTPBadRequest(msg % (unmodified,))

        ts = self._get_resource_timestamp(request)
        if ts is not None and ts > unmodified:
            raise HTTPPreconditionFailed()
        return ts

    def _check_if_modified(self, request):
        """Check the X-If-Modified-Since header.

        This method checks whether the target resource has been modified
        since the timestamp given in the X-If-Modified-Since header.  If
        so a "304 Not Modified" is generated.

        It also has the side-effect of setting X-Last-Modified in the
        response headers.
        """
        modified = request.headers.get("X-If-Modified-Since")
        if modified is not None:
            try:
                modified = int(modified)
            except ValueError:
                msg = "Bad value for X-If-Modified-Since: %r" % (modified,)
                raise HTTPBadRequest(msg)

        ts = self._get_resource_timestamp(request)
        request.response.headers["X-Last-Modified"] = str(ts)

        if ts is not None and modified is not None and ts <= modified:
            raise HTTPNotModified()
        return ts

    def _get_resource_timestamp(self, request):
        """Get last-modified timestamp for the target resource of the request.

        This method retreives the last-modified timestamp of the storage
        itself, a specific collection in the storage, or a specific item
        in a collection, depending on what resouce is targeted by the request.
        """
        storage = self._get_storage(request)
        user_id = request.user["uid"]
        collection = request.matchdict.get("collection")
        # No collection name; return overall storage timestamp.
        if collection is None:
            return storage.get_storage_timestamp(user_id)
        item_id = request.matchdict.get("item")
        # No item id; return timestamp of whole collection.
        if item_id is None:
            try:
                return storage.get_collection_timestamp(user_id, collection)
            except NotFoundError:
                return None
        # Otherwise, return timestamp of specific item.
        try:
            return storage.get_item_timestamp(user_id, collection, item_id)
        except NotFoundError:
            return None

    def get_storage(self, request):
        # XXX returns a 400 if the root is called
        raise HTTPBadRequest()

    @convert_storage_errors
    def get_collection_timestamps(self, request):
        """Returns a hash of collections associated with the account,
        Along with the last modified timestamp for each collection
        """
        self._check_if_modified(request)
        storage = self._get_storage(request)
        user_id = request.user["uid"]
        collections = storage.get_collection_timestamps(user_id)
        return collections

    @convert_storage_errors
    def get_collection_counts(self, request):
        """Returns a hash of collections associated with the account,
        Along with the total number of items for each collection.
        """
        self._check_if_modified(request)
        storage = self._get_storage(request)
        user_id = request.user["uid"]
        counts = storage.get_collection_counts(user_id)
        return counts

    @convert_storage_errors
    def get_quota(self, request):
        self._check_if_modified(request)
        storage = self._get_storage(request)
        user_id = request.user["uid"]
        used = storage.get_total_size(user_id)
        return {
            "usage": used,
            "quota": self.quota_size,
        }

    @convert_storage_errors
    def get_collection_usage(self, request):
        self._check_if_modified(request)
        storage = self._get_storage(request)
        user_id = request.user["uid"]
        return storage.get_collection_sizes(user_id)

    def _convert_args(self, kw):
        """Converts incoming arguments for GET and DELETE on collections.

        This function will also raise a 400 on bad args.
        Unknown args are just dropped.
        """
        args = {}

        for arg in  ('older', 'newer', 'index_above', 'index_below'):
            value = kw.get(arg)
            if value is None:
                continue
            try:
                value = int(value)
            except ValueError:
                msg = 'Invalid value for "%s": %r' % (arg, value)
                raise HTTPBadRequest(msg)
            if arg in ('older', 'index_below'):
                args[arg] = value
            else:
                args[arg] = value

        # convert limit
        limit = kw.get('limit')
        if limit is not None:
            try:
                limit = int(limit)
            except ValueError:
                msg = 'Invalid value for "limit": %r' % (limit,)
                raise HTTPBadRequest(msg)
            args['limit'] = limit

        for arg in ('ids',):
            value = kw.get(arg)
            if value is None:
                continue
            args['items'] = value.split(',')

        sort = kw.get('sort')
        if sort in ('oldest', 'newest', 'index'):
            args['sort'] = sort
        return args

    @convert_storage_errors
    @with_read_lock
    def get_collection(self, request, **kw):
        """Returns a list of the BSO ids contained in a collection."""
        self._check_if_modified(request)

        filters = self._convert_args(kw)
        collection_name = request.matchdict['collection']
        user_id = request.user["uid"]
        full = kw.get('full', False)

        storage = self._get_storage(request)
        if full:
            res = storage.get_items(user_id, collection_name, **filters)
        else:
            res = storage.get_item_ids(user_id, collection_name, **filters)

        request.response.headers['X-Num-Records'] = str(len(res))
        return {
            "items": res
        }

    @convert_storage_errors
    @with_read_lock
    def get_item(self, request):
        """Returns a single BSO object."""
        self._check_if_modified(request)
        storage = self._get_storage(request)
        collection_name = request.matchdict['collection']
        item_id = request.matchdict['item']
        user_id = request.user["uid"]
        return storage.get_item(user_id, collection_name, item_id)

    def _check_quota(self, request, new_bsos):
        """Checks the quota.

        If the quota is reached, issues a 400.
        """
        user_id = request.user["uid"]
        storage = self._get_storage(request)
        if self.quota_size is None:
            return 0

        used = storage.get_total_size(user_id)
        left = self.quota_size - used
        if left < _ONE_MEG:
            used = storage.get_total_size(user_id, recalculate=True)
            left = self.quota_size - used

        for bso in new_bsos:
            left -= len(bso.get("payload", ""))

        if left <= 0:  # no space left
            raise HTTPJsonBadRequest(ERROR_OVER_QUOTA)
        return left

    @convert_storage_errors
    @with_write_lock
    def set_item(self, request):
        """Sets a single BSO object."""
        self._check_if_unmodified(request)

        storage = self._get_storage(request)
        user_id = request.user["uid"]
        collection_name = request.matchdict['collection']
        item_id = request.matchdict['item']

        content_type = request.content_type
        if request.content_type not in ("application/json", None):
            msg = "Unsupported Media Type: %s" % (content_type,)
            raise HTTPUnsupportedMediaType(msg)

        try:
            data = json.loads(request.body)
        except ValueError:
            raise HTTPJsonBadRequest(ERROR_MALFORMED_JSON)

        try:
            bso = BSO(data)
        except ValueError:
            raise HTTPJsonBadRequest(ERROR_INVALID_OBJECT)

        consistent, msg = bso.validate()
        if not consistent:
            raise HTTPJsonBadRequest(ERROR_INVALID_OBJECT)

        left = self._check_quota(request, [bso])
        res = storage.set_item(user_id, collection_name, item_id, bso)

        if not res["created"]:
            response = HTTPNoContent()
        else:
            response = HTTPCreated()

        response.headers["X-Last-Modified"] = str(res["modified"])
        if 0 < left < _ONE_MEG:
            response.headers['X-Quota-Remaining'] = str(left)
        return response

    @convert_storage_errors
    @with_write_lock
    def delete_item(self, request):
        """Deletes a single BSO object."""
        self._check_if_unmodified(request)
        storage = self._get_storage(request)
        collection_name = request.matchdict['collection']
        item_id = request.matchdict['item']
        user_id = request.user["uid"]
        storage.delete_item(user_id, collection_name, item_id)
        return HTTPNoContent()

    @convert_storage_errors
    @with_write_lock
    def set_collection(self, request):
        """Sets a batch of BSO objects into a collection."""
        modified = self._check_if_unmodified(request)

        storage = self._get_storage(request)
        user_id = request.user["uid"]
        collection_name = request.matchdict['collection']

        # TODO: it would be lovely to support streaming uploads here...
        content_type = request.content_type
        try:
            if content_type in ("application/json", None):
                bsos = json.loads(request.body)
            elif content_type == "application/newlines":
                bsos = [json.loads(ln) for ln in request.body.split("\n")]
            else:
                msg = "Unsupported Media Type: %s" % (content_type,)
                raise HTTPUnsupportedMediaType(msg)
        except ValueError:
            raise HTTPJsonBadRequest(ERROR_MALFORMED_JSON)

        if not isinstance(bsos, (tuple, list)):
            raise HTTPJsonBadRequest(ERROR_INVALID_OBJECT)

        res = {'success': [], 'failed': {}}

        # Sanity-check each of the BSOs.
        # Limit the batch based on both count and payload size.
        kept_bsos = []
        total_bytes = 0
        for count, bso in enumerate(bsos):
            try:
                bso = BSO(bso)
            except ValueError:
                res['failed'][''] = ['invalid bso']
                continue

            if 'id' not in bso:
                res['failed'][''] = ['invalid id']
                continue

            consistent, msg = bso.validate()
            item_id = bso['id']
            if not consistent:
                res['failed'][item_id] = [msg]
                continue

            if count >= self.batch_max_count:
                res['failed'][item_id] = ['retry bso']
                continue
            if 'payload' in bso:
                total_bytes += len(bso['payload'])
            if total_bytes >= self.batch_max_bytes:
                res['failed'][item_id] = ['retry bytes']
                continue

            kept_bsos.append(bso)

        left = self._check_quota(request, kept_bsos)

        try:
            modified = storage.set_items(user_id, collection_name, kept_bsos)
        except Exception, e:
            # Something went wrong.
            # We want to swallow the 503 in that case.
            self.logger.error('Could not set items')
            self.logger.error(str(e))
            for bso in kept_bsos:
                res['failed'][bso['id']] = str(e)
        else:
            res['success'].extend([bso['id'] for bso in kept_bsos])

        request.response.headers["X-Last-Modified"] = str(modified)
        if 0 < left < _ONE_MEG:
            request.response.headers['X-Quota-Remaining'] = str(left)
        return res

    @convert_storage_errors
    @with_write_lock
    def delete_collection(self, request, **kw):
        """Deletes the collection and all contents.

        Additional request parameters may modify the selection of which
        items to delete.
        """
        self._check_if_unmodified(request)

        ids = kw.get("ids")
        if ids is not None:
            ids = ids.split(",")
            if len(ids) > MAX_IDS_PER_BATCH:
                msg = 'Cannot delete more than %s BSOs at a time'
                raise HTTPBadRequest(msg % (MAX_IDS_PER_BATCH,))

        storage = self._get_storage(request)
        collection_name = request.matchdict['collection']
        user_id = request.user["uid"]
        if ids is None:
            storage.delete_collection(user_id, collection_name)
            response = HTTPNoContent()
        else:
            modified = storage.delete_items(user_id, collection_name, ids)
            response = HTTPNoContent()
            response.headers["X-Last-Modified"] = str(modified)
        return response

    @convert_storage_errors
    def delete_storage(self, request):
        """Deletes all records for the user."""
        self._check_if_unmodified(request)
        storage = self._get_storage(request)
        user_id = request.user["uid"]
        storage.delete_storage(user_id)  # XXX failures ?
        return HTTPNoContent()
