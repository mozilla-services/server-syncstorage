# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import logging

from pyramid.security import Allow

from cornice import Service

from syncstorage.bso import VALID_ID_REGEX
from syncstorage.storage import ConflictError, NotFoundError

from syncstorage.views.validators import (extract_target_resource,
                                          extract_precondition_headers,
                                          extract_query_params,
                                          parse_multiple_bsos,
                                          parse_single_bso)
from syncstorage.views.decorators import (convert_storage_errors,
                                          sleep_and_retry_on_conflict,
                                          with_collection_lock,
                                          check_precondition_headers,
                                          check_storage_quota)


logger = logging.getLogger("syncstorage")

DEFAULT_VALIDATORS = (
    extract_target_resource,
    extract_precondition_headers,
    extract_query_params,
)

DEFAULT_DECORATORS = (
    convert_storage_errors,
    sleep_and_retry_on_conflict,
    with_collection_lock,
    check_precondition_headers,
    check_storage_quota,
)


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
        # XXX TODO: current FF client hardcodes "1.1" as the version number.
        # We accept it for now but should disable this eventually.
        path = "/{api:1\\.5}/{userid:[0-9]{1,10}}" + path
        return path

    def get_view_wrapper(self, kwds):
        """Get view wrapper to appply the default decorators."""

        def view_wrapper(viewfunc):
            for decorator in reversed(DEFAULT_DECORATORS):
                viewfunc = decorator(viewfunc)
            return viewfunc

        return view_wrapper


# We define a simple "It Works!" view at the site root, so that
# it's easy to see if the service is correctly running.
site_root = Service(name="site_root", path="/")


@site_root.get()
def get_site_root(request):
    return "It Works!  SyncStorage is successfully running on this host."


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

storage = SyncStorageService(name="storage",
                             path="/storage")
collection = SyncStorageService(name="collection",
                                path="/storage/{collection}")
item = SyncStorageService(name="item",
                          path="/storage/{collection}/{item}")


@info_timestamps.get(accept="application/json", renderer="sync-json",
                     acl=expired_token_acl)
def get_info_timestamps(request):
    storage = request.validated["storage"]
    timestamps = storage.get_collection_timestamps(request.validated["userid"])
    request.response.headers["X-Weave-Records"] = str(len(timestamps))
    return timestamps


@info_counts.get(accept="application/json", renderer="sync-json")
def get_info_counts(request):
    storage = request.validated["storage"]
    counts = storage.get_collection_counts(request.validated["userid"])
    request.response.headers["X-Weave-Records"] = str(len(counts))
    return counts


@info_quota.get(accept="application/json", renderer="sync-json")
def get_info_quota(request):
    storage = request.validated["storage"]
    used = storage.get_total_size(request.validated["userid"]) / ONE_KB
    quota = request.registry.settings.get("storage.quota_size", None)
    if quota is not None:
        quota = quota / ONE_KB
    return [used, quota]


@info_usage.get(accept="application/json", renderer="sync-json")
def get_info_usage(request):
    storage = request.validated["storage"]
    sizes = storage.get_collection_sizes(request.validated["userid"])
    for collection, size in sizes.iteritems():
        sizes[collection] = size / ONE_KB
    request.response.headers["X-Weave-Records"] = str(len(sizes))
    return sizes


@storage.delete(renderer="sync-json")
def delete_storage(request):
    storage = request.validated["storage"]
    storage.delete_storage(request.validated["userid"])
    return {}


@service_root.delete(renderer="sync-json")
def delete_all(request):
    storage = request.validated["storage"]
    storage.delete_storage(request.validated["userid"])
    return {}


@collection.get(accept="application/json", renderer="sync-json")
@collection.get(accept="application/newlines", renderer="sync-newlines")
def get_collection(request):
    storage = request.validated["storage"]
    userid = request.validated["userid"]
    collection = request.validated["collection"]

    filters = {}
    filter_names = ("ids", "newer", "limit", "offset", "sort")
    for name in filter_names:
        if name in request.validated:
            filters[name] = request.validated[name]

    # For b/w compat, non-existent collections must return an empty list.
    try:
        if request.validated.get("full", False):
            res = storage.get_items(userid, collection, **filters)
        else:
            res = storage.get_item_ids(userid, collection, **filters)
        next_offset = res.get("next_offset")
        if next_offset is not None:
            request.response.headers["X-Weave-Next-Offset"] = str(next_offset)
        return res["items"]
    except NotFoundError:
        return []


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
        ts = storage.set_items(userid, collection, bsos)
    except ConflictError:
        raise
    except Exception, e:
        logger.error('Could not set items')
        logger.error(str(e))
        for bso in bsos:
            res["failed"][bso["id"]] = "db error"
    else:
        res["success"].extend([bso["id"] for bso in bsos])
        res['modified'] = ts
        request.response.headers["X-Last-Modified"] = str(ts)

    return res


@collection.delete(renderer="sync-json")
def delete_collection(request):
    storage = request.validated["storage"]
    userid = request.validated["userid"]
    collection = request.validated["collection"]
    ids = request.validated.get("ids")

    # For b/w compat, non-existent collections must not give an error.
    try:
        if ids is None:
            ts = storage.delete_collection(userid, collection)
        else:
            ts = storage.delete_items(userid, collection, ids)
            request.response.headers["X-Last-Modified"] = str(ts)
        return {"modified": ts}
    except NotFoundError:
        return {"modified": storage.get_storage_timestamp(userid)}


@item.get(accept="application/json", renderer="sync-json")
def get_item(request):
    storage = request.validated["storage"]
    userid = request.validated["userid"]
    collection = request.validated["collection"]
    item = request.validated["item"]
    return storage.get_item(userid, collection, item)


@item.put(renderer="sync-json",
          validators=DEFAULT_VALIDATORS + (parse_single_bso,))
def put_item(request):
    storage = request.validated["storage"]
    userid = request.validated["userid"]
    collection = request.validated["collection"]
    item = request.validated["item"]
    bso = request.validated["bso"]

    res = storage.set_item(userid, collection, item, bso)
    ts = res["modified"]
    request.response.headers["X-Last-Modified"] = str(ts)
    return ts


@item.delete(renderer="sync-json")
def delete_item(request):
    storage = request.validated["storage"]
    userid = request.validated["userid"]
    collection = request.validated["collection"]
    item = request.validated["item"]

    ts = storage.delete_item(userid, collection, item)
    return {"modified": ts}


def includeme(config):
    # Commit the config to work around some conflicts raised by cornice,
    # which also does a config.commit() during view processing.
    config.commit()
    config.include("syncstorage.views.authentication")
    config.include("syncstorage.views.renderers")
    config.scan("syncstorage.views")
