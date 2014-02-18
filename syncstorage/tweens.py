# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import json

from pyramid.httpexceptions import HTTPException, HTTPServiceUnavailable

from syncstorage.util import get_timestamp

try:
    from mozsvc.storage.mcclient import MemcachedClient
except ImportError:
    MemcachedClient = None  # NOQA


WEAVE_UNKNOWN_ERROR = 0
WEAVE_ILLEGAL_METH = 1              # Illegal method/protocol
WEAVE_MALFORMED_JSON = 6            # Json parse failure
WEAVE_INVALID_WBO = 8               # Invalid Weave Basic Object
WEAVE_OVER_QUOTA = 14               # User over quota


def set_x_timestamp_header(handler, registry):
    """Tween to set the X-Weave-Timestamp header on all responses."""

    def set_x_timestamp_header_tween(request):
        ts1 = get_timestamp()
        response = handler(request)
        # The storage might have created a new timestamp when processing
        # a write.  Report that one if it's newer than the current one.
        ts2 = get_timestamp(response.headers.get("X-Last-Modified"))
        response.headers["X-Weave-Timestamp"] = str(max(ts1, ts2))
        return response

    return set_x_timestamp_header_tween


def check_for_blacklisted_nodes(handler, registry):
    """Tween to check for blacklisted nodes by querying memcache."""

    # Check whether this tween is enabled in the config.
    if not registry.settings.get("storage.check_blacklisted_nodes", False):
        return handler

    # Create a memcached client, error out if that's not possible.
    if MemcachedClient is None:
        raise ValueError('The "check_blacklisted_nodes" option '
                         'requires a memcached server')

    servers = registry.settings.get('storage.cache_servers', '127.0.0.1:11211')
    cache = MemcachedClient(servers.split(','))

    def check_for_blacklisted_nodes_tween(request):
        # Is it down?  We just error out straight away.
        host = request.host
        if cache.get("down:" + host) is not None:
            raise HTTPServiceUnavailable("Server Problem Detected")

        response = handler(request)

        # Is it backed-off?  We process the request but add a header.
        backoff = cache.get('backoff:%s' % host)
        if backoff is not None:
            response.headers["X-Weave-Backoff"] = str(backoff)
        return response

    return check_for_blacklisted_nodes_tween


def set_default_accept_header(handler, registry):
    """Tween to set a default Accept header on incoming requests.

    This tween intercepts requests without an Accept header, adding one
    which indicates a preference for application/json responses.  This
    helps resolve ambiguity inside Pyramid's accept-handling logic, which
    doesn't seem to have a way to specify the server's preferred type.
    """

    def set_default_accept_header_tween(request):
        if not getattr(request, "accept", None):
            request.accept = "application/json, */*; q=0.9"

        return handler(request)

    return set_default_accept_header_tween


def convert_cornice_errors_to_respcodes(handler, registry):
    """Tween to convert cornice error objects into integer response codes.

    This is an uglifying pass that inspects cornice error information and
    decides on the appropriate Weave error response code to send in its
    place.  It makes the author very sad, but it is necessary for backwards
    compatibility in the sync1.5 protocol.
    """

    def pick_weave_error_code(body):
        try:
            if body["status"] == "quota-exceeded":
                return WEAVE_OVER_QUOTA
            error = body["errors"][0]
            if error["location"] == "body":
                if error["name"] in ("bso", "bsos"):
                    if "invalid json" in error["description"].lower():
                        return WEAVE_MALFORMED_JSON
                    return WEAVE_INVALID_WBO
        except (KeyError, IndexError):
            pass
        return None

    def convert_cornice_response(request, response):
        try:
            body = json.loads(response.body)
        except ValueError:
            pass
        else:
            code = pick_weave_error_code(body)
            if code is None:
                # We have to return an integer, so use this as
                # a generic "unexpected error" code.
                code = WEAVE_ILLEGAL_METH
            response.body = str(code)
            response.content_length = len(response.body)

    def convert_cornice_errors_to_respcodes_tween(request):
        try:
            response = handler(request)
        except HTTPException, response:
            if response.content_type == "application/json":
                convert_cornice_response(request, response)
            raise
        else:
            if response.status_code == 400:
                if response.content_type == "application/json":
                    convert_cornice_response(request, response)
            return response

    return convert_cornice_errors_to_respcodes_tween


def convert_non_json_responses(handler, registry):
    """Tween to convert non-json response bodies to json.

    The framework can sometimes generate a HTML response page, e.g. for a
    404 or 401 response.  Clients don't really expect to see HTMl pages,
    so we intercept them and replace them with a simple json body.
    """

    def convert_non_json_responses_tween(request):
        try:
            response = handler(request)
        except HTTPException, response:
            if response.content_type != "application/json":
                response.body = str(WEAVE_UNKNOWN_ERROR)
                response.content_length = len(response.body)
                response.content_type = "application/json"
            raise
        else:
            if response.status_code >= 400:
                if response.content_type != "application/json":
                    response.body = str(WEAVE_UNKNOWN_ERROR)
                    response.content_length = len(response.body)
                    response.content_type = "application/json"
            return response

    return convert_non_json_responses_tween


def includeme(config):
    """Include all the SyncServer tweens into the given config."""
    config.add_tween("syncstorage.tweens.check_for_blacklisted_nodes")
    config.add_tween("syncstorage.tweens.set_x_timestamp_header")
    config.add_tween("syncstorage.tweens.set_default_accept_header")
    config.add_tween("syncstorage.tweens.convert_cornice_errors_to_respcodes")
    config.add_tween("syncstorage.tweens.convert_non_json_responses")
