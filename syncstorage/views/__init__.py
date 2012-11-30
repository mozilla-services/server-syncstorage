# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

from pyramid.security import Allow

from mozsvc.metrics import MetricsService

from syncstorage.bso import FIELD_DEFAULTS

from syncstorage.views.util import json_error
from syncstorage.views.validators import (extract_target_resource,
                                          extract_precondition_headers,
                                          extract_query_params,
                                          parse_multiple_bsos,
                                          parse_single_bso)
from syncstorage.views.decorators import (convert_storage_errors,
                                          with_collection_lock,
                                          check_precondition_headers,
                                          check_storage_quota)


DEFAULT_VALIDATORS = (
    extract_target_resource,
    extract_precondition_headers,
    extract_query_params,
)

DEFAULT_DECORATORS = (
    convert_storage_errors,
    with_collection_lock,
    check_precondition_headers,
    check_storage_quota,
)


class SyncStorageService(MetricsService):
    """Custom Service class to assist DRY in the SyncStorage project.

    This Service subclass provides useful defaults for SyncStorage service
    endpoints, such as configuring authentication and path prefixes.
    """

    def __init__(self, **kwds):
        # Configure DRY defaults for the path.
        kwds["path"] = self._configure_the_path(kwds["path"])
        # Ensure all views require authenticated user.
        kwds.setdefault("permission", "owner")
        kwds.setdefault("acl", self._default_acl)
        # Add default set of validators
        kwds.setdefault("validators", DEFAULT_VALIDATORS)
        super(SyncStorageService, self).__init__(**kwds)

    def _configure_the_path(self, path):
        """Helper method to apply default configuration of the service path."""
        # Insert pattern-matching regexes into the path
        path = path.replace("{collection}", "{collection:[a-zA-Z0-9_-]+}")
        path = path.replace("{item}", "{item:[a-zA-Z0-9_-]+}")
        # Add path prefix for the API version number and userid.
        path = "/{api:2.0}/{userid:[0-9]{1,10}}" + path
        return path

    def _default_acl(self, request):
        """Default ACL: only the owner is allowed access."""
        return [(Allow, int(request.matchdict["userid"]), "owner")]

    def get_view_wrapper(self, kwds):
        """Get view wrapper to appply the default decorators."""

        def view_wrapper(viewfunc):
            for decorator in reversed(DEFAULT_DECORATORS):
                viewfunc = decorator(viewfunc)
            return viewfunc

        return view_wrapper


# We define a service at the root, but don't give it any methods.
# This will generate a "415 Method Unsupported" for all attempts to use it,
# which is less confusing than having the root give a "404 Not Found".
root = SyncStorageService(name="root",
                          path="/")

info = SyncStorageService(name="info",
                          path="/info")
info_quota = SyncStorageService(name="info_quota",
                                path="/info/quota")
info_versions = SyncStorageService(name="info_versions",
                                   path="/info/collections")
info_usage = SyncStorageService(name="info_usage",
                                path="/info/collection_usage")
info_counts = SyncStorageService(name="info_counts",
                                 path="/info/collection_counts")

storage = SyncStorageService(name="storage",
                             path="/storage")
collection = SyncStorageService(name="collection",
                                path="/storage/{collection}")
item = SyncStorageService(name="item",
                          path="/storage/{collection}/{item}")


@info_versions.get(accept="application/json", renderer="sync-json")
def get_info_versions(request):
    storage = request.validated["storage"]
    return storage.get_collection_versions(request.validated["userid"])


@info_counts.get(accept="application/json", renderer="sync-json")
def get_info_counts(request):
    storage = request.validated["storage"]
    return storage.get_collection_counts(request.validated["userid"])


@info_quota.get(accept="application/json", renderer="sync-json")
def get_info_quota(request):
    storage = request.validated["storage"]
    used = storage.get_total_size(request.validated["userid"])
    return {
        "usage": used,
        "quota": request.registry.settings.get("storage.quota_size", None)
    }


@info_usage.get(accept="application/json", renderer="sync-json")
def get_info_usage(request):
    storage = request.validated["storage"]
    return storage.get_collection_sizes(request.validated["userid"])


@storage.delete(renderer="sync-void")
def delete_storage(request):
    storage = request.validated["storage"]
    storage.delete_storage(request.validated["userid"])
    return None


@collection.get(accept="application/json", renderer="sync-json")
@collection.get(accept="application/newlines", renderer="sync-newlines")
def get_collection(request):
    storage = request.validated["storage"]
    userid = request.validated["userid"]
    collection = request.validated["collection"]

    filters = {}
    filter_names = ("ids", "older", "newer", "limit", "offset", "sort")
    for name in filter_names:
        if name in request.validated:
            filters[name] = request.validated[name]

    if request.validated.get("full", False):
        res = storage.get_items(userid, collection, **filters)
    else:
        res = storage.get_item_ids(userid, collection, **filters)

    next_offset = res.get("next_offset")
    if next_offset is not None:
        request.response.headers["X-Next-Offset"] = str(next_offset)

    return res["items"]


@collection.post(accept="application/json", renderer="sync-json",
                 validators=DEFAULT_VALIDATORS + (parse_multiple_bsos,))
def post_collection(request):
    storage = request.validated["storage"]
    userid = request.validated["userid"]
    collection = request.validated["collection"]
    bsos = request.validated["bsos"]
    invalid_bsos = request.validated["invalid_bsos"]

    res = {'success': [], 'failed': {}}

    # If some BSOs failed to parse properly, include them
    # in the failure list straight away.
    for (id, error) in invalid_bsos.iteritems():
        res["failed"][id] = error

    try:
        version = storage.set_items(userid, collection, bsos)
    except Exception, e:
        request.registry["metlog"].error('Could not set items')
        request.registry["metlog"].error(str(e))
        for bso in bsos:
            res["failed"][bso["id"]] = "db error"
    else:
        res["success"].extend([bso["id"] for bso in bsos])
        request.response.headers["X-Last-Modified-Version"] = str(version)

    return res


@collection.delete(renderer="sync-void")
def delete_collection(request):
    storage = request.validated["storage"]
    userid = request.validated["userid"]
    collection = request.validated["collection"]
    ids = request.validated.get("ids")
    if ids is None:
        storage.delete_collection(userid, collection)
    else:
        version = storage.delete_items(userid, collection, ids)
        request.response.headers["X-Last-Modified-Version"] = str(version)
    return None


@item.get(accept="application/json", renderer="sync-json")
def get_item(request):
    storage = request.validated["storage"]
    userid = request.validated["userid"]
    collection = request.validated["collection"]
    item = request.validated["item"]
    return storage.get_item(userid, collection, item)


@item.put(renderer="sync-void",
          validators=DEFAULT_VALIDATORS + (parse_single_bso,))
def put_item(request):
    storage = request.validated["storage"]
    userid = request.validated["userid"]
    collection = request.validated["collection"]
    item = request.validated["item"]
    bso = request.validated["bso"]

    # A PUT request is a complete re-write of the item.
    # A payload must be specified, and any other missing fields are
    # explicitly set to their default value.
    if "payload" not in bso:
        raise json_error(400, "error", [{
            "location": "body",
            "name": "bso",
            "description": "BSO must specify a payload",
        }])

    for field in FIELD_DEFAULTS:
        if field not in bso:
            bso[field] = FIELD_DEFAULTS[field]

    res = storage.set_item(userid, collection, item, bso)
    request.response.headers["X-Last-Modified-Version"] = str(res["version"])
    return res


@item.post(renderer="sync-void",
           validators=DEFAULT_VALIDATORS + (parse_single_bso,))
def post_item(request):
    storage = request.validated["storage"]
    userid = request.validated["userid"]
    collection = request.validated["collection"]
    item = request.validated["item"]
    bso = request.validated["bso"]

    res = storage.set_item(userid, collection, item, bso)
    request.response.headers["X-Last-Modified-Version"] = str(res["version"])
    return res


@item.delete(renderer="sync-void")
def delete_item(request):
    storage = request.validated["storage"]
    userid = request.validated["userid"]
    collection = request.validated["collection"]
    item = request.validated["item"]

    storage.delete_item(userid, collection, item)
    return None


def includeme(config):
    config.scan("syncstorage.views")
    config.include("syncstorage.views.renderers")
