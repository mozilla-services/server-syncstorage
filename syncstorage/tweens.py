# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import time
import random

from pyramid.httpexceptions import HTTPException, HTTPServiceUnavailable


try:
    from mozsvc.storage.mcclient import MemcachedClient
except ImportError:
    MemcachedClient = None       # NOQA


def set_x_timestamp_header(handler, registry):
    """Tween to set the X-Timestamp header on all responses."""

    def set_x_timestamp_header_tween(request):
        request.server_time = int(time.time() * 1000)
        response = handler(request)
        response.headers["X-Timestamp"] = str(request.server_time)
        return response

    return set_x_timestamp_header_tween


def check_node_status(handler, registry):
    """Tween to check storage node status by querying memcache.

    If configured to do so, this function can check memcache for the status
    of the target node and possibly avoid calling out to the storage backend.
    It looks for a memcache key named "status:<hostname>" with one of the
    following values:

       "down":   the node is explicitly marked as down
       "draining":   the node is being decommissioned
       "unhealthy":  the node has not responded to health checks
       "backoff" or "backoff:NN":  the node is under heavy load and
                                   clients should back off for a while.

    """

    # Check whether we're required to perform the checks.
    # If so then we must be able to create a memcached client.
    if not registry.settings.get("storage.check_node_status", False):
        registry["check_node_status:cache"] = None
    else:
        if MemcachedClient is None:
            msg = 'The "check_node_status" option requires a memcached server'
            raise ValueError(msg)
        servers = registry.settings.get('storage.cache_servers')
        if servers is None:
            servers = ["127.0.0.1:11211"]
        else:
            servers = servers.split(",")
        registry["check_node_status:cache"] = MemcachedClient(servers)

    def check_node_status_tween(request):

        # Don't perform the check unless configured to do so.
        cache = request.registry.get("check_node_status:cache")
        if cache is None:
            return handler(request)

        headers = {}
        response = None
        settings = request.registry.settings

        # A helper function to create HTTPServiceUnavailable responses,
        # while providing all the necessary headers etc.
        def resp_service_unavailable(msg):
            retry_after = str(settings.get("mozsvc.retry_after", 1800))
            headers.setdefault("Retry-After", retry_after)
            headers.setdefault("X-Backoff", retry_after)
            return HTTPServiceUnavailable(body=msg, headers=headers)

        # Get the node name from the host header,
        # and check that it's one we know about.
        node = request.host
        if not node:
            msg = "host header not received from client"
            return resp_service_unavailable(msg)

        if "syncstorage:storage:host:" + node not in request.registry:
            msg = "database lookup failed"
            return resp_service_unavailable(msg)

        # Check the node's status in memcache,
        # and return early with an error if it's not as expected.
        status = cache.get('status:%s' % (node,))
        if status is not None:

            # If it's marked as draining then send a 503 response.
            # XXX TODO: consider sending a 401 to trigger migration?
            if status == "draining":
                msg = "node reassignment"
                return resp_service_unavailable(msg)

            # If it's marked as being down then send a 503 response.
            if status == "down":
                msg = "database marked as down"
                return resp_service_unavailable(msg)

            # If it's marked as being unhealthy then send a 503 response.
            if status == "unhealthy":
                msg = "database is not healthy"
                return resp_service_unavailable(msg)

            # If it's marked for backoff, proceed with the request
            # but set appropriate headers on the response.
            if status == "backoff" or status.startswith("backoff:"):
                try:
                    retry_after = status.split(":", 1)[1]
                except IndexError:
                    retry_after = str(settings.get("mozsvc.retry_after", 1800))
                headers["X-Backoff"] = retry_after

        # If we get to here then we can proceed with contacting the storage
        # note, but may have some control headers to add to the response.
        response = handler(request)
        response.headers.update(headers)
        return response

    return check_node_status_tween


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


def fuzz_retry_after_header(handler, registry):
    """Add some random fuzzing to the value of the Retry-After header."""

    def fuzz_response(response):
        retry_after = response.headers.get("Retry-After")
        if retry_after is not None:
            retry_after = int(retry_after) + random.randint(0, 5)
            response.headers["Retry-After"] = str(retry_after)

    def fuzz_retry_after_header_tween(request):
        try:
            response = handler(request)
        except HTTPException, response:
            fuzz_response(response)
            raise
        else:
            fuzz_response(response)
            return response

    return fuzz_retry_after_header_tween


def includeme(config):
    """Include all the SyncServer tweens into the given config."""
    config.add_tween("syncstorage.tweens.check_node_status")
    config.add_tween("syncstorage.tweens.set_x_timestamp_header")
    config.add_tween("syncstorage.tweens.set_default_accept_header")
    config.add_tween("syncstorage.tweens.fuzz_retry_after_header")
