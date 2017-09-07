# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import re
import logging
from base64 import b64decode

from mozsvc.metrics import annotate_request

from syncstorage.bso import BSO, VALID_ID_REGEX
from syncstorage.util import get_timestamp, json_loads
from syncstorage.storage import get_storage
from syncstorage.views.util import json_error, get_limit_config


logger = logging.getLogger(__name__)


BATCH_MAX_IDS = 100
TRUE_REGEX = re.compile("^true$", re.I)
KNOWN_BAD_PAYLOAD_REGEX = re.compile(r'"IV":\s*"AAAAAAAAAAAAAAAAAAAAAA=="')


def extract_target_resource(request):
    """Validator to extract the target resource of a request.

    This validator will extract the userid, collection name and item id if
    they appear in the matched URL of the request.  It assumes they have
    already been checked for validity by the authentication and url-matching
    logic of the application.

    It also looks up the appropriate storage backend based on the hostname
    in the request.

    It places these items under the keys "storage", "userid", "collection"
    and "item".
    """
    request.validated["storage"] = get_storage(request)
    request.validated["userid"] = int(request.matchdict["userid"])
    if "collection" in request.matchdict:
        request.validated["collection"] = request.matchdict["collection"]
    if "item" in request.matchdict:
        request.validated["item"] = request.matchdict["item"]


def extract_precondition_headers(request):
    """Validator to extract the X-If-[Unm|M]odified-Since headers.

    This validator extracts the X-If-Modified-Since- header or the
    X-If-Unmodified-Since header, validates it and parses it into a float.
    an integer.  The result is stored under the key "if_modified_since" or
    "if_unmodified_since" as appropriate.

    It is an error to specify both headers in a single request.
    """
    if_modified_since = request.headers.get("X-If-Modified-Since")
    if if_modified_since is not None:
        try:
            if_modified_since = get_timestamp(if_modified_since)
            if if_modified_since < 0:
                raise ValueError
        except ValueError:
            msg = "Bad value for X-If-Modified-Since: %r"
            request.errors.add("header", "X-If-Modified-Since",
                               msg % (if_modified_since))
        else:
            request.validated["if_modified_since"] = if_modified_since

    if_unmodified_since = request.headers.get("X-If-Unmodified-Since")
    if if_unmodified_since is not None:
        try:
            if_unmodified_since = get_timestamp(if_unmodified_since)
            if if_unmodified_since < 0:
                raise ValueError
        except ValueError:
            msg = 'Invalid value for "X-If-Unmodified-Since": %r'
            request.errors.add("header", "X-If-Unmodified-Since",
                               msg % (if_unmodified_since,))
        else:
            if if_modified_since is not None:
                msg = "Cannot specify both X-If-Modified-Since and "\
                      "X-If-Unmodified-Since on a single request"
                request.errors.add("header", "X-If-Unmodified-Since", msg)
            else:
                request.validated["if_unmodified_since"] = if_unmodified_since


def extract_query_params(request):
    """Validator to extract BSO search parameters from the query string.

    This validator will extract and validate the following search params:

        * newer: lower-bound on last-modified time (float timestamp)
        * older: upper-bound on last-modified time (float timestamp)
        * sort:  order in which to return results (string)
        * limit:  maximum number of items to return (integer)
        * offset:  position at which to restart search (string)
        * ids: a comma-separated list of BSO ids (list of strings)
        * full: flag, whether to include full bodies (bool)

    """
    newer = request.GET.get("newer")
    if newer is not None:
        try:
            newer = get_timestamp(newer)
            if newer < 0:
                raise ValueError
        except ValueError:
            msg = "Invalid value for newer: %r" % (newer,)
            request.errors.add("querystring", "newer", msg)
        else:
            request.validated["newer"] = newer

    older = request.GET.get("older")
    if older is not None:
        try:
            older = get_timestamp(older)
            if older < 0:
                raise ValueError
        except ValueError:
            msg = "Invalid value for older: %r" % (older,)
            request.errors.add("querystring", "older", msg)
        else:
            request.validated["older"] = older

    limit = request.GET.get("limit")
    if limit is not None:
        try:
            limit = int(limit)
            if limit < 0:
                raise ValueError
        except ValueError:
            msg = "Invalid value for limit: %r" % (limit,)
            request.errors.add("querystring", "limit", msg)
        else:
            request.validated["limit"] = limit

    # The offset token is an opaque string, with semantics determined by
    # the storage backend, so we can't parse or validate it here.  Rather,
    # we must catch InvalidOffsetError if something goes wrong.
    offset = request.GET.get("offset")
    if offset is not None:
        request.validated["offset"] = offset

    sort = request.GET.get("sort")
    if sort is not None:
        if sort not in ("newest", "oldest", "index"):
            msg = "Invalid value for sort: %r" % (sort,)
            request.errors.add("querystring", "sort", msg)
        else:
            request.validated["sort"] = sort

    ids = request.GET.get("ids")
    if ids is not None:
        ids = [id.strip() for id in ids.split(",")]
        if len(ids) > BATCH_MAX_IDS:
            msg = 'Cannot process more than %s BSOs at a time'
            msg = msg % (BATCH_MAX_IDS,)
            request.errors.add("querysting", "items", msg)
        else:
            for id in ids:
                if not VALID_ID_REGEX.match(id):
                    msg = "Invalid BSO id: %r" % (id,)
                    request.errors.add("querystring", "ids", msg)
        request.validated["ids"] = ids

    if "full" in request.GET:
        request.validated["full"] = True


def extract_batch_state(request):
    """Validator to extract the batch state of a request for slightly
    tidier code in the views.

    If the "batch" parameter is has no value or has a value of "true" then
    a new batch will be created.

    If the "commit" parameter is has a value of "true", this batch
    is to be committed and deleted.
    """
    request.validated["batch"] = False
    batch_id = request.GET.get("batch")
    if batch_id is not None:
        if TRUE_REGEX.match(batch_id):
            batch_id = True
        else:
            try:
                batch_id = int(b64decode(batch_id))
            except TypeError:
                try:
                    batch_id = int(batch_id)
                except ValueError:
                    msg = "Invalid batch ID: \"%s\"" % (batch_id,)
                    request.errors.add("batch", "id", msg)
        request.validated["batch"] = batch_id
    elif batch_id is None and "batch" in request.GET:
        request.validated["batch"] = True

    request.validated["commit"] = False
    commit = request.GET.get("commit")
    if commit is not None:
        if TRUE_REGEX.match(commit):
            request.validated["commit"] = True
        else:
            msg = "commit parameter must be \"true\" to apply batches"
            request.errors.add("batch", "commit", msg)

    # If batch uploads are not enabled in the config then
    # we want to:
    #  * silently ignore attempts to start a new batch, which
    #    will cause clients to fall back to non-batch mode.
    #  * error out on attempts to continue an existing batch,
    #    since we can't possibly do what the client expects.
    settings = request.registry.settings
    if not settings.get("storage.batch_upload_enabled", False):
        if request.validated["batch"]:
            if request.validated["batch"] is not True:
                request.errors.add("batch", "id", "Batch uploads disabled")

    LIMITS = (
      ("X-Weave-Records", "max_post_records"),
      ("X-Weave-Bytes", "max_post_bytes"),
      ("X-Weave-Total-Records", "max_total_records"),
      ("X-Weave-Total-Bytes", "max_total_bytes"),
    )
    for (header, setting) in LIMITS:
        try:
            count = int(request.headers[header])
        except ValueError:
            msg = "Invalid integer value: %s" % (request.headers[header],)
            request.errors.add("header", header, msg)
            continue
        except KeyError:
            continue
        if count > get_limit_config(request, setting):
            raise json_error(400, "size-limit-exceeded")


def parse_multiple_bsos(request):
    """Validator to parse a list of BSOs from the request body.

    This validator accepts a list of BSOs in either application/json or
    application/newlines format, parses and validates them.

    Valid BSOs are placed under the key "bsos".  Invalid BSOs are placed
    under the key "invalid_bsos".
    """
    content_type = request.content_type
    try:
        if content_type in ("application/json", "text/plain", None):
            bso_datas = json_loads(request.body)
        elif content_type == "application/newlines":
            bso_datas = []
            if request.body:
                for ln in request.body.split("\n"):
                    bso_datas.append(json_loads(ln))
        else:
            msg = "Unsupported Media Type: %s" % (content_type,)
            request.errors.add("header", "Content-Type", msg)
            request.errors.status = 415
            return
    except ValueError:
        request.errors.add("body", "bsos", "Invalid JSON in request body")
        return

    if not isinstance(bso_datas, (tuple, list)):
        request.errors.add("body", "bsos", "Input data was not a list")
        return

    BATCH_MAX_COUNT = get_limit_config(request, "max_post_records")
    BATCH_MAX_BYTES = get_limit_config(request, "max_post_bytes")

    valid_bsos = {}
    invalid_bsos = {}

    total_bytes = 0
    for count, bso_data in enumerate(bso_datas):
        try:
            bso = BSO(bso_data)
        except ValueError:
            msg = "Input data was not a list of BSOs"
            request.errors.add("body", "bsos", msg)
            return

        try:
            id = bso["id"]
        except KeyError:
            request.errors.add("body", "bsos", "Input BSO has no ID")
            return

        if id in valid_bsos:
            request.errors.add("body", "bsos", "Input BSO has duplicate ID")
            return

        consistent, msg = bso.validate()
        if not consistent:
            invalid_bsos[id] = msg
            # Log status on how many invalid BSOs we get, and why.
            logmsg = "Invalid BSO %s/%s/%s (%s): %s"
            userid = request.matchdict["userid"]
            collection = request.matchdict.get("collection")
            logger.info(logmsg, userid, collection, id, msg, bso)
            continue

        if count >= BATCH_MAX_COUNT:
            invalid_bsos[id] = "retry bso"
            continue

        total_bytes += len(bso.get("payload", ""))
        if total_bytes >= BATCH_MAX_BYTES:
            invalid_bsos[id] = "retry bytes"
            continue

        valid_bsos[id] = bso

    request.validated["bsos"] = valid_bsos.values()
    request.validated["invalid_bsos"] = invalid_bsos


def parse_single_bso(request):
    """Validator to parse a single BSO from the request body.

    This validator accepts a single BSO in application/json format, parses
    and validates it, and places it under the key "bso".
    """
    content_type = request.content_type
    try:
        if content_type in ("application/json", "text/plain", None):
            bso_data = json_loads(request.body)
        else:
            msg = "Unsupported Media Type: %s" % (content_type,)
            request.errors.add("header", "Content-Type", msg)
            request.errors.status = 415
            return
    except ValueError:
        request.errors.add("body", "bso", "Invalid JSON in request body")
        return

    try:
        bso = BSO(bso_data)
    except ValueError:
        request.errors.add("body", "bso", "Invalid BSO data")
        return

    consistent, msg = bso.validate()
    if not consistent:
        request.errors.add("body", "bso", "Invalid BSO: " + msg)
        return

    request.validated["bso"] = bso


class KnownBadPayloadError(Exception):
    pass


def check_for_known_bad_payloads(request):
    """Reject specific payloads known to indicate client issues."""
    try:
        # Turns out some clients are not as good at crypto as we'd like.
        # Look for any signals that they might have messed it up and
        # reject attempts to set /crypto/keys in that case.
        if request.validated.get("collection") == "crypto":
            incoming_bsos = []
            if "bsos" in request.validated:
                incoming_bsos.extend(request.validated["bsos"])
            if "bso" in request.validated:
                incoming_bsos.append(request.validated["bso"])
            for bso in incoming_bsos:
                payload = bso.get("payload")
                if payload and KNOWN_BAD_PAYLOAD_REGEX.search(payload):
                    raise KnownBadPayloadError
    except KnownBadPayloadError:
        annotate_request(request, __name__ + ".known_bad_payload", 1)
        request.errors.add("body", "bso", "Known-bad BSO payload")
