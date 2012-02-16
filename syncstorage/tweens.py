# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

from pyramid.httpexceptions import HTTPServiceUnavailable

from syncstorage.util import get_timestamp


try:
    from memcache import Client
except ImportError:
    Client = None       # NOQA


def set_x_timestamp_header(handler, registry):
    """Tween to set the X-Timestamp header on all responses."""

    def set_x_timestamp_header_tween(request):
        request.server_time = get_timestamp()
        response = handler(request)
        response.headers["X-Timestamp"] = str(request.server_time)
        return response

    return set_x_timestamp_header_tween


def check_for_blacklisted_nodes(handler, registry):
    """Tween to check for blacklisted nodes by querying memcache."""

    # Check whether this tween is enabled in the config.
    if not registry.settings.get("storage.check_blacklisted_nodes", False):
        return handler

    # Create a memcached client, error out if that's not possible.
    if Client is None:
        raise ValueError('The "check_blacklisted_nodes" option '
                         'requires a memcached server')

    servers = registry.settings.get('storage.cache_servers', '127.0.0.1:11211')
    cache = registry["cache"] = Client(servers.split(','))

    def check_for_blacklisted_nodes_tween(request):
        # Is it down?  We just error out straight away.
        host = request.host
        if cache.get("down:" + host) is not None:
            raise HTTPServiceUnavailable("Server Problem Detected")

        response = handler(request)

        # Is it backed-off?  We process the request but add a header.
        backoff = cache.get('backoff:%s' % host)
        if backoff is not None:
            response.headers["X-Backoff"] = str(backoff)
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


def includeme(config):
    """Include all the SyncServer tweens into the given config."""
    config.add_tween("syncstorage.tweens.check_for_blacklisted_nodes")
    config.add_tween("syncstorage.tweens.set_x_timestamp_header")
    config.add_tween("syncstorage.tweens.set_default_accept_header")
