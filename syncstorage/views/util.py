# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import json
import functools

from pyramid.httpexceptions import HTTPError

from syncstorage.storage import NotFoundError


def json_error(status_code=400, status_message="error", errors=()):
    """Construct a cornice-format JSON error response."""
    err = HTTPError()
    err.content_type = "application/json"
    err.status_code = status_code
    err.body = json.dumps({
        "status": status_message,
        "errors": errors,
    })
    return err


def make_decorator(decorator_func):
    """Decorator to make other functions into decorators.

    Apply this function to make decorators with a less repetative syntax.
    It causes the wrapped function to be passed in as first argument, so
    that this::

        @make_decorator
        def wrap_function(func, *args, **kwds):
            with some_context():
                return func(*args, **kwds)

    Is equivalent to this::

        def wrap_function(func):
            def wrapper(*args, **kwds):
                with some_context():
                    return func(*args, **kwds)
            return wrapper

    The result is substantially cleaner code if you're defining many
    decorators in a row.
    """
    @functools.wraps(decorator_func)
    def decorator(target_func):

        @functools.wraps(target_func)
        def wrapper(*args, **kwds):
            return decorator_func(target_func, *args, **kwds)

        return wrapper

    return decorator


def get_resource_version(request):
    """Get last-modified version for the target resource of a request.

    This method retreives the last-modified version of the storage
    itself, a specific collection in the storage, or a specific item
    in a collection, depending on what resouce is targeted by the request.
    If the target resource does not exist, it returns zero.
    """
    storage = request.validated["storage"]
    userid = request.validated["userid"]
    collection = request.validated.get("collection")
    item = request.validated.get("item")

    # No collection name => return overall storage version.
    if collection is None:
        return storage.get_storage_version(userid)

    # No item id => return version of whole collection.
    if item is None:
        try:
            return storage.get_collection_version(userid, collection)
        except NotFoundError:
            return 0

    # Otherwise, return version of specific item.
    try:
        return storage.get_item_version(userid, collection, item)
    except NotFoundError:
        return 0
