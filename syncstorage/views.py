# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import struct
import simplejson as json

from pyramid.security import Authenticated, Allow

from cornice.service import Service


class WhoisiRenderer(object):
    """Pyramid renderer producing lists in application/whoisi format."""

    def __init__(self, info):
        pass  # pyramid always calls the factory with a single argument

    def __call__(self, value, system):
        request = system.get('request')
        if request is not None:
            response = request.response
            if response.content_type == response.default_content_type:
                response.content_type = "application/whoisi"
        data = []
        for line in value:
            line = json.dumps(line, use_decimal=True)
            size = struct.pack('!I', len(line))
            data.append('%s%s' % (size, line))
        return ''.join(data)


class NewlinesRenderer(object):
    """Pyramid renderer producing lists in application/newlines format."""

    def __init__(self, info):
        pass  # pyramid always calls the factory with a single argument

    def __call__(self, value, system):
        request = system.get('request')
        if request is not None:
            response = request.response
            if response.content_type == response.default_content_type:
                response.content_type = "application/newlines"
        data = []
        for line in value:
            line = json.dumps(line, use_decimal=True).replace('\n', '\\u000a')
            data.append(line)
        return '\n'.join(data)


class SyncStorageService(Service):
    """Custom Service class to assist DRY in the SyncStorage project.

    This Service subclass provides useful defaults for SyncStorage service
    endpoints, such as configuring authentication and path prefixes.
    """

    def __init__(self, **kwds):
        # Configure DRY defaults for the path.
        kwds["path"] = self._configure_the_path(kwds["path"])
        # Ensure all views require authenticated user.
        kwds.setdefault("permission", "authn")
        kwds.setdefault("acl", lambda r: [(Allow, Authenticated, "authn")])
        super(SyncStorageService, self).__init__(**kwds)

    def _configure_the_path(self, path):
        """Helper method to apply default configuration of the service path."""
        # Insert pattern-matching regexes into the path
        # TODO: decide on the allowable characters for this, and document.
        path = path.replace("{collection}", "{collection:[a-zA-Z0-9._-]+}")
        path = path.replace("{item}", "{item:[a-zA-Z0-9._-]+}")
        # Add path prefix for the API version number and username.
        path = "/{api:2.0}/{username:[a-zA-Z0-9]+}" + path
        return path


root = SyncStorageService(name="root", path="/")

info = SyncStorageService(name="info",
                          path="/info")
info_quota = SyncStorageService(name="info_quota",
                                 path="/info/quota")
info_modified = SyncStorageService(name="info_modified",
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


def _ctrl(request):
    return request.registry["syncstorage.controller"]


@info_modified.get()
def get_info_modified(request):
    return _ctrl(request).get_collections(request)


@info_counts.get()
def get_info_counts(request):
    return _ctrl(request).get_collection_counts(request)


@info_quota.get()
def get_info_quota(request):
    return _ctrl(request).get_quota(request)


@info_usage.get()
def get_info_usage(request):
    return _ctrl(request).get_collection_usage(request)


@storage.delete()
def delete_storage(request):
    return _ctrl(request).delete_storage(request)


@collection.get(accept="application/json", renderer="simplejson")
@collection.get(accept="application/newlines", renderer="newlines")
@collection.get(accept="application/whoisi", renderer="whoisi")
def get_collection(request):
    return _ctrl(request).get_collection(request, **request.GET)


@collection.post()
def post_collection(request):
    return _ctrl(request).set_collection(request)


@collection.delete()
def delete_collection(request):
    return _ctrl(request).delete_collection(request, **request.GET)


@item.get()
def get_item(request):
    return _ctrl(request).get_item(request)


@item.put()
def put_item(request):
    return _ctrl(request).set_item(request)


@item.delete()
def delete_item(request):
    return _ctrl(request).delete_item(request)
