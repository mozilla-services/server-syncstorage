# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.


from syncstorage.util import json_dumps
from syncstorage.views.util import get_resource_timestamp


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
        # Ensure that every response reports the last-modified timestamp.
        # In most cases this will have already been set when we looked it
        # up during processing of the request.
        if "X-Last-Modified" not in response.headers:
            ts = get_resource_timestamp(request)
            response.headers["X-Last-Modified"] = str(ts)

    def render_value(self, value):
        raise NotImplementedError


class JsonRenderer(SyncStorageRenderer):
    """Pyramid renderer producing application/json output."""

    def adjust_response(self, value, request, response):
        super(JsonRenderer, self).adjust_response(value, request, response)
        if response.content_type == response.default_content_type:
            response.content_type = "application/json"
        if isinstance(value, (list, tuple)):
            response.headers["X-Weave-Records"] = str(len(value))

    def render_value(self, value):
        return json_dumps(value)


class NewlinesRenderer(SyncStorageRenderer):
    """Pyramid renderer producing lists in application/newlines format."""

    def adjust_response(self, value, request, response):
        super(NewlinesRenderer, self).adjust_response(value, request, response)
        if response.content_type == response.default_content_type:
            response.content_type = "application/newlines"
        response.headers["X-Weave-Records"] = str(len(value))

    def render_value(self, value):
        data = []
        for line in value:
            line = json_dumps(line)
            line = line.replace('\n', '\\u000a')
            data.append(line)
            data.append('\n')
        return ''.join(data)


def includeme(config):
    here = "syncstorage.views.renderers:"
    config.add_renderer("sync-json", here + "JsonRenderer")
    config.add_renderer("sync-newlines", here + "NewlinesRenderer")
