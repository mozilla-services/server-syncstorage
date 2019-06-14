# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import logging

from base64 import b64encode

from pyramid.security import Allow

from cornice import Service

from syncstorage.bso import VALID_ID_REGEX
from syncstorage.util import get_timestamp
from syncstorage.storage import (ConflictError,
                                 NotFoundError,
                                 InvalidBatch)

from syncstorage.views.validators import (extract_target_resource,
                                          extract_precondition_headers,
                                          extract_query_params,
                                          extract_batch_state,
                                          parse_multiple_bsos,
                                          parse_single_bso,
                                          check_for_known_bad_payloads)
from syncstorage.views.decorators import (convert_storage_errors,
                                          sleep_and_retry_on_conflict,
                                          with_collection_lock,
                                          check_precondition_headers,
                                          check_storage_quota)
from syncstorage.views.util import get_resource_timestamp, get_limit_config


logger = logging.getLogger(__name__)

DEFAULT_VALIDATORS = (
    extract_target_resource,
    extract_precondition_headers,
    extract_query_params,
)

POST_VALIDATORS = DEFAULT_VALIDATORS + (
    extract_batch_state,
    parse_multiple_bsos,
    check_for_known_bad_payloads
)

PUT_VALIDATORS = DEFAULT_VALIDATORS + (
    parse_single_bso,
    check_for_known_bad_payloads
)


def default_decorators(func):
    func = check_storage_quota(func)
    func = check_precondition_headers(func)
    func = with_collection_lock(func)
    func = sleep_and_retry_on_conflict(func)
    func = convert_storage_errors(func)
    return func


BSO_ID_REGEX = VALID_ID_REGEX.pattern.lstrip("^").rstrip("$")
COLLECTION_ID_REGEX = "[a-zA-Z0-9._-]{1,32}"

ONE_KB = 1024.0


def default_acl(request):
    """Default ACL: only the owner is allowed access.

    This must be a function, not a method on SyncStorageService, because
    cornice takes a copy of it when constructing the pyramid view.
    """
    return [(Allow, int(request.matchdict["userid"]), "owner")]


def expired_token_acl(request):
    """ACL allowing holders to expired token to still access the resource.

    This is useful for allowing access to certain non-security-sensitive
    APIs with less client burden.  It must be a function, not a method on
    SyncStorageService, because cornice takes a copy of it when constructing
    the pyramid view.
    """
    return [
        (Allow, int(request.matchdict["userid"]), "owner"),
        (Allow, "expired:%s" % (request.matchdict["userid"],), "owner")
    ]


class SyncStorageService(Service):
    """Custom Service class to assist DRY in the SyncStorage project.

    This Service subclass provides useful defaults for SyncStorage service
    endpoints, such as configuring authentication and path prefixes.
    """

    # Cornice warns about a JSON XSRF vuln that's not relevant to us.
    # Disable the filter that checks for this to avoid annoying log lines.
    default_filters = []

    def __init__(self, **kwds):
        # Configure DRY defaults for the path.
        kwds["path"] = self._configure_the_path(kwds["path"])
        # Ensure all views require authenticated user.
        kwds.setdefault("permission", "owner")
        kwds.setdefault("acl", default_acl)
        # Add default set of validators
        kwds.setdefault("validators", DEFAULT_VALIDATORS)
        super(SyncStorageService, self).__init__(**kwds)

    def _configure_the_path(self, path):
        """Helper method to apply default configuration of the service path."""
        # Insert pattern-matching regexes into the path
        path = path.replace("{collection}",
                            "{collection:%s}" % (COLLECTION_ID_REGEX,))
        path = path.replace("{item}",
                            "{item:%s}" % (BSO_ID_REGEX,))
        # Add path prefix for the API version number and userid.
        path = "/{api:1\\.5}/{userid:[0-9]{1,10}}" + path
        return path


# We define a simple "It Works!" view at the site root, so that
# it's easy to see if the service is correctly running.
site_root = Service(name="site_root", path="/")


@site_root.get()
def get_site_root(request):
    return "It Works!  SyncStorage is successfully running on this host."


# The /__lbheartbeat__ route is used by mozsvc load-balancer infra
# to detect when a webhead is out of service.

lbheartbeat = Service(name="lbheartbeat", path='/__lbheartbeat__',
                      description="Web head health")


@lbheartbeat.get()
def get_lbheartbeat(request):
    """Return successful healthy response.

    If the load-balancer tries to access this URL and fails, this means the
    web head is not operational and should be dropped.
    """
    return {}


# The remianing routes are for the actual syncstorage service.

service_root = SyncStorageService(name="service_root",
                                  path="")

info = SyncStorageService(name="info",
                          path="/info")
info_quota = SyncStorageService(name="info_quota",
                                path="/info/quota")
info_timestamps = SyncStorageService(name="info_timestamps",
                                     path="/info/collections")
info_usage = SyncStorageService(name="info_usage",
                                path="/info/collection_usage")
info_counts = SyncStorageService(name="info_counts",
                                 path="/info/collection_counts")
info_configuration = SyncStorageService(name="info_configuration",
                                        path="/info/configuration")

storage = SyncStorageService(name="storage",
                             path="/storage")
collection = SyncStorageService(name="collection",
                                path="/storage/{collection}")
item = SyncStorageService(name="item",
                          path="/storage/{collection}/{item}")


@info_timestamps.get(accept="application/json", renderer="sync-json",
                     acl=expired_token_acl)
@default_decorators
def get_info_timestamps(request):
    storage = request.validated["storage"]
    # This route allows access with recently-expired tokens,
    # rejigger request.user to permit this.
    if "expired_uid" in request.user:
        request.user["uid"] = request.user.pop("expired_uid")
    timestamps = storage.get_collection_timestamps(request.user)
    request.response.headers["X-Weave-Records"] = str(len(timestamps))
    return timestamps


@info_counts.get(accept="application/json", renderer="sync-json")
@default_decorators
def get_info_counts(request):
    storage = request.validated["storage"]
    counts = storage.get_collection_counts(request.user)
    request.response.headers["X-Weave-Records"] = str(len(counts))
    return counts


@info_quota.get(accept="application/json", renderer="sync-json")
@default_decorators
def get_info_quota(request):
    storage = request.validated["storage"]
    used = storage.get_total_size(request.user) / ONE_KB
    quota = request.registry.settings.get("storage.quota_size", None)
    if quota is not None:
        quota = quota / ONE_KB
    return [used, quota]


@info_usage.get(accept="application/json", renderer="sync-json")
@default_decorators
def get_info_usage(request):
    storage = request.validated["storage"]
    sizes = storage.get_collection_sizes(request.user)
    for collection, size in sizes.iteritems():
        sizes[collection] = size / ONE_KB
    request.response.headers["X-Weave-Records"] = str(len(sizes))
    return sizes


@info_configuration.get(accept="application/json", renderer="sync-json")
@default_decorators
def get_info_configuration(request):
    # Don't return batch-related limits if the feature isn't enabled.
    if request.registry.settings.get("storage.batch_upload_enabled", False):
        LIMIT_NAMES = (
            "max_request_bytes",
            "max_record_payload_bytes",
            "max_post_records",
            "max_post_bytes",
            "max_total_records",
            "max_total_bytes",
        )
    else:
        LIMIT_NAMES = (
            "max_request_bytes",
            "max_record_payload_bytes",
        )
    limits = {}
    for name in LIMIT_NAMES:
        limits[name] = get_limit_config(request, name)
    return limits


@storage.delete(renderer="sync-json")
@default_decorators
def delete_storage(request):
    storage = request.validated["storage"]
    storage.delete_storage(request.user)
    return {}


@service_root.delete(renderer="sync-json")
@default_decorators
def delete_all(request):
    storage = request.validated["storage"]
    storage.delete_storage(request.user)
    return {}


@collection.get(accept="application/json", renderer="sync-json")
@collection.get(accept="application/newlines", renderer="sync-newlines")
@convert_storage_errors
def get_collection_with_internal_pagination(request):
    """Get the contents of a collection, in a respectful manner.

    We provide a client-driven pagination API, but some clients don't
    use it.  Instead they make humungous queries such as "give me all
    100,000 history items as a single batch" and unfortunately, we have
    to comply.

    This wrapper view breaks up such requests so that they use the
    pagination API internally, which is more respectful of server
    resources and avoids bogging down queries from other users.
    """
    try:
        settings = request.registry.settings
        batch_size = settings.get("storage.pagination_batch_size")
        # If we're not doing internal pagination, fulfill it directly.
        if batch_size is None:
            return get_collection(request)
        # If the request is already limited, fulfill it directly.
        limit = request.validated.get("limit", None)
        if limit is not None and limit < batch_size:
            return get_collection(request)
        # Otherwise, we'll have to paginate internally for reduce db load.
        items = []
        request.validated["limit"] = batch_size
        while True:
            # Do the actual fetch, knowing it won't be too big.
            res = get_collection(request)
            items.extend(res)
            if limit is not None:
                max_left = limit - len(items)
                # If we've fetched up to the requested limit then stop,
                # leaving the X-Weave-Next-Offset header intact.
                if max_left <= 0:
                    break
                request.validated["limit"] = min(max_left, batch_size)
            # Check Next-Offset to see if we've fetched all available items.
            try:
                offset = request.response.headers.pop("X-Weave-Next-Offset")
            except KeyError:
                break
            # Fetch again, using the given offset token and sanity-checking
            # that the collection has not been concurrently modified.
            # Taking a collection lock here would defeat the point of this
            # pagination, which is to free up db resources.
            request.validated["offset"] = offset
            if "if_unmodified_since" not in request.validated:
                last_modified = request.response.headers["X-Last-Modified"]
                last_modified = get_timestamp(last_modified)
                request.validated["if_unmodified_since"] = last_modified
        return items
    except NotFoundError:
        # For b/w compat, non-existent collections must return an empty list.
        return []


@sleep_and_retry_on_conflict
@with_collection_lock
@check_precondition_headers
@check_storage_quota
def get_collection(request):
    storage = request.validated["storage"]
    user = request.user
    collection = request.validated["collection"]

    filters = {}
    filter_names = ("ids", "newer", "older", "limit", "offset", "sort")
    for name in filter_names:
        if name in request.validated:
            filters[name] = request.validated[name]

    if request.validated.get("full", False):
        res = storage.get_items(user, collection, **filters)
        for bso in res["items"]:
            bso.pop("ttl", None)
    else:
        res = storage.get_item_ids(user, collection, **filters)
    next_offset = res.get("next_offset")
    if next_offset is not None:
        request.response.headers["X-Weave-Next-Offset"] = str(next_offset)
    # Ensure that X-Last-Modified is present, since it's needed when
    # doing pagination.  This lookup is essentially free since we already
    # loaded and cached the timestamp when taking the collection lock.
    ts = get_resource_timestamp(request)
    request.response.headers["X-Last-Modified"] = str(ts)
    return res["items"]


@collection.post(accept="application/json", renderer="sync-json",
                 validators=POST_VALIDATORS)
@default_decorators
def post_collection(request):
    storage = request.validated["storage"]
    user = request.user
    collection = request.validated["collection"]
    bsos = request.validated["bsos"]
    invalid_bsos = request.validated["invalid_bsos"]

    # For initial rollout, disable batch uploads by default.
    if request.registry.settings.get("storage.batch_upload_enabled", False):
        if request.validated["batch"] or request.validated["commit"]:
            return post_collection_batch(request)

    res = {'success': [], 'failed': {}}

    # If some BSOs failed to parse properly, include them
    # in the failure list straight away.
    for (id, error) in invalid_bsos.iteritems():
        res["failed"][id] = error

    ts = storage.set_items(user, collection, bsos)
    res["success"].extend([bso["id"] for bso in bsos])
    res['modified'] = ts
    request.response.headers["X-Last-Modified"] = str(ts)

    return res


def post_collection_batch(request):
    storage = request.validated["storage"]
    user = request.user
    collection = request.validated["collection"]
    bsos = request.validated["bsos"]
    invalid_bsos = request.validated["invalid_bsos"]
    batch = request.validated["batch"]
    commit = request.validated["commit"]

    request.response.status = 202

    # Bail early if we have nonsensical arguments
    if not batch:
        raise InvalidBatch

    # The "batch" key is set only on a multi-POST batch request prior to a
    # commit.  The "modified" key is only set upon a successful commit.
    # The two flags are mutually exclusive.
    res = {'success': [], 'failed': {}}

    for (id, error) in invalid_bsos.iteritems():
        res["failed"][id] = error

    try:
        if batch is True:
            try:
                batch = storage.create_batch(user, collection)
            except ConflictError, e:
                logger.error('Collision in batch creation!')
                logger.error(e)
                raise
            except Exception, e:
                logger.error('Could not create batch')
                logger.error(e)
                raise
        else:
            i = storage.valid_batch(user, collection, batch)
            if not i:
                raise InvalidBatch

        if bsos:
            try:
                storage.append_items_to_batch(user, collection, batch, bsos)
            except ConflictError:
                raise
            except Exception, e:
                logger.error('Could not append to batch("{0}")'.format(batch))
                logger.error(e)
                for bso in bsos:
                    res["failed"][bso["id"]] = "db error"
            else:
                res["success"].extend([bso["id"] for bso in bsos])

        if commit:
            try:
                ts = storage.apply_batch(user, collection, batch)
            except ConflictError, e:
                logger.error('Collision in batch commit!')
                logger.error(e)
                raise
            except Exception, e:
                logger.error("Could not apply batch")
                logger.error(e)
                raise
            else:
                res['modified'] = ts
                request.response.headers["X-Last-Modified"] = str(ts)
                storage.close_batch(user, collection, batch)
                request.response.status = 200
        else:
            res["batch"] = b64encode(str(batch))
    except ConflictError:
        raise

    return res


@collection.delete(renderer="sync-json")
@default_decorators
def delete_collection(request):
    storage = request.validated["storage"]
    user = request.user
    collection = request.validated["collection"]
    ids = request.validated.get("ids")

    # For b/w compat, non-existent collections must not give an error.
    try:
        if ids is None:
            ts = storage.delete_collection(user, collection)
        else:
            ts = storage.delete_items(user, collection, ids)
            request.response.headers["X-Last-Modified"] = str(ts)
        return {"modified": ts}
    except NotFoundError:
        return {"modified": storage.get_storage_timestamp(user)}


@item.get(accept="application/json", renderer="sync-json")
@default_decorators
def get_item(request):
    storage = request.validated["storage"]
    user = request.user
    collection = request.validated["collection"]
    item = request.validated["item"]
    bso = storage.get_item(user, collection, item)
    bso.pop("ttl", None)
    return bso


@item.put(renderer="sync-json", validators=PUT_VALIDATORS)
@default_decorators
def put_item(request):
    storage = request.validated["storage"]
    user = request.user
    collection = request.validated["collection"]
    item = request.validated["item"]
    bso = request.validated["bso"]

    res = storage.set_item(user, collection, item, bso)
    ts = res["modified"]
    request.response.headers["X-Last-Modified"] = str(ts)
    return ts


@item.delete(renderer="sync-json")
@default_decorators
def delete_item(request):
    storage = request.validated["storage"]
    user = request.user
    collection = request.validated["collection"]
    item = request.validated["item"]

    ts = storage.delete_item(user, collection, item)
    return {"modified": ts}


def includeme(config):
    # Commit the config to work around some conflicts raised by cornice,
    # which also does a config.commit() during view processing.
    config.commit()
    config.include("syncstorage.views.authentication")
    config.include("syncstorage.views.renderers")
    config.scan("syncstorage.views")
