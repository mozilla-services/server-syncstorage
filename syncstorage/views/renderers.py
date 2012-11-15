# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import simplejson as json

from syncstorage.views.util import get_resource_version


class SyncStorageRenderer(object):
    """Base renderer class for syncstorage response rendering."""

    def __init__(self, info):
        # Pyramid always calls the factory with a single argument,
        # so we need to provide a stub __init__ method.
        pass

    def __call__(self, value, system):
        request = system.get('request')
        if request is not None:
            response = request.response
            self.adjust_response(value, request, response)
        return self.render_value(value)

    def adjust_response(self, value, request, response):
        # Ensure that every response reports the last-modified version.
        # In most cases this will have already been set when we looked it
        # up during processing of the request.
        if "X-Last-Modified-Version" not in response.headers:
            version = get_resource_version(request)
            response.headers["X-Last-Modified-Version"] = str(version)

    def render_value(self, value):
        raise NotImplementedError


class JsonRenderer(SyncStorageRenderer):
    """Pyramid renderer producing application/json output."""

    def adjust_response(self, value, request, response):
        super(JsonRenderer, self).adjust_response(value, request, response)
        if response.content_type == response.default_content_type:
            response.content_type = "application/json"
        if isinstance(value, (list, tuple)):
            response.headers["X-Num-Records"] = str(len(value))

    def render_value(self, value):
        # It's not safe to render lists as a raw json list.
        # Instead we produce a dict with the lone key "items".
        if isinstance(value, (list, tuple)):
            value = {"items": value}
        return json.dumps(value)


class NewlinesRenderer(SyncStorageRenderer):
    """Pyramid renderer producing lists in application/newlines format."""

    def adjust_response(self, value, request, response):
        super(NewlinesRenderer, self).adjust_response(value, request, response)
        if response.content_type == response.default_content_type:
            response.content_type = "application/newlines"
        response.headers["X-Num-Records"] = str(len(value))

    def render_value(self, value):
        data = []
        for line in value:
            line = json.dumps(line).replace('\n', '\\u000a')
            data.append(line)
        return '\n'.join(data)


class VoidRenderer(SyncStorageRenderer):
    """Pyramid renderer for 201/204 responses, where no content is needed.

    It's convenient to do this as a renderer to get header tweaking etc
    for free.
    """

    def adjust_response(self, value, request, response):
        super(VoidRenderer, self).adjust_response(value, request, response)
        if value and value.get("created", False):
            response.status_code = 201
        else:
            response.status_code = 204

    def render_value(self, value):
        return ""


def includeme(config):
    here = "syncstorage.views.renderers:"
    config.add_renderer("sync-json", here + "JsonRenderer")
    config.add_renderer("sync-newlines", here + "NewlinesRenderer")
    config.add_renderer("sync-void", here + "VoidRenderer")
