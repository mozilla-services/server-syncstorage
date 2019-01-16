# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import logging
import re
import time

import tokenlib
import tokenlib.errors
from mozsvc.user import TokenServerAuthenticationPolicy
from pyramid.interfaces import IAuthenticationPolicy
from zope.interface import implements

logger = logging.getLogger(__name__)


DEFAULT_EXPIRED_TOKEN_TIMEOUT = 60 * 60 * 2  # 2 hours, in seconds

# Coarse validation of FxA userid, device ids, and key ids.
# This is not supposed to catch all invalid cases, but to act as a backstop
# that the ids are safe to use and store internally.
VALID_FXA_ID_REGEX = re.compile("^[A-Za-z0-9=-]{1,64}$")


class SyncStorageAuthenticationPolicy(TokenServerAuthenticationPolicy):
    """Pyramid authentication policy with special handling of expired tokens.

    This class extends the standard mozsvc TokenServerAuthenticationPolicy
    to (carefully) allow some access by holders of expired tokens.  Presenting
    an expired token will result in a principal of "expired:<uid>" rather than
    just "<uid>", allowing this case to be specially detected and handled for
    some resources without interfering with the usual authentication rules.
    """

    implements(IAuthenticationPolicy)

    def __init__(self, secrets=None, **kwds):
        self.expired_token_timeout = kwds.pop("expired_token_timeout", None)
        if self.expired_token_timeout is None:
            self.expired_token_timeout = DEFAULT_EXPIRED_TOKEN_TIMEOUT
        super(SyncStorageAuthenticationPolicy, self).__init__(secrets, **kwds)

    @classmethod
    def _parse_settings(cls, settings):
        """Parse settings for an instance of this class."""
        supercls = super(SyncStorageAuthenticationPolicy, cls)
        kwds = supercls._parse_settings(settings)
        expired_token_timeout = settings.pop("expired_token_timeout", None)
        if expired_token_timeout is not None:
            kwds["expired_token_timeout"] = int(expired_token_timeout)
        return kwds

    def decode_hawk_id(self, request, tokenid):
        """Decode a Hawk token id into its userid and secret key.

        This method determines the appropriate secrets to use for the given
        request, then passes them on to tokenlib to handle the given Hawk
        token.  If the id is invalid then ValueError will be raised.

        Unlike the superclass method, this implementation allows expired
        tokens to be used up to a configurable timeout.  The effective userid
        for expired tokens is changed to be "expired:<uid>".
        """
        now = time.time()
        node_name = self._get_node_name(request)
        # There might be multiple secrets in use,
        # so try each until we find one that works.
        secrets = self._get_token_secrets(node_name)
        for secret in secrets:
            try:
                tm = tokenlib.TokenManager(secret=secret)
                # Check for a proper valid signature first.
                # If that failed because of an expired token, check if
                # it falls within the allowable expired-token window.
                try:
                    data = self._parse_token(tm, tokenid, now)
                    userid = data["uid"]
                except tokenlib.errors.ExpiredTokenError:
                    recently = now - self.expired_token_timeout
                    data = self._parse_token(tm, tokenid, recently)
                    # We replace the uid with a special string to ensure that
                    # calling code doesn't accidentally treat the token as
                    # valid. If it wants to use the expired uid, it will have
                    # to explicitly dig it back out from `request.user`.
                    data["expired_uid"] = data["uid"]
                    userid = data["uid"] = "expired:%d" % (data["uid"],)
            except tokenlib.errors.InvalidSignatureError, e:
                # Token signature check failed, try the next secret.
                continue
            except TypeError, e:
                # Something went wrong when validating the contained data.
                raise ValueError(str(e))
            else:
                # Token signature check succeeded, quit the loop.
                break
        else:
            # The token failed to validate using any secret.
            logger.warn("Authentication Failed: invalid hawk id")
            raise ValueError("invalid Hawk id")

        # Let the app access all user data from the token.
        request.user.update(data)
        request.metrics["metrics_uid"] = data.get("hashed_fxa_uid")
        request.metrics["metrics_device_id"] = data.get("hashed_device_id")

        # Sanity-check that we're on the right node.
        if data["node"] != node_name:
            msg = "incorrect node for this token: %s"
            raise ValueError(msg % (data["node"],))

        # Calculate the matching request-signing secret.
        key = tokenlib.get_derived_secret(tokenid, secret=secret)

        return userid, key

    def encode_hawk_id(self, request, userid, extra=None):
        """Encode the given userid into a Hawk id and secret key.

        This method is essentially the reverse of decode_hawk_id.  It is
        not needed for consuming authentication tokens, but is very useful
        when building them for testing purposes.

        Unlike its superclass method, this one allows the caller to specify
        a dict of additional user data to include in the auth token.
        """
        node_name = self._get_node_name(request)
        secret = self._get_token_secrets(node_name)[-1]
        data = {"uid": userid, "node": node_name}
        if extra is not None:
            data.update(extra)
        tokenid = tokenlib.make_token(data, secret=secret)
        key = tokenlib.get_derived_secret(tokenid, secret=secret)
        return tokenid, key

    def _parse_token(self, tokenmanager, tokenid, now):
        """Parse, validate and normalize user data from a tokenserver token.

        This is a thin wrapper around tokenmanager.parse_token to apply
        some extra validation to the contained user data.  The data is
        signed and trusted, but it's still coming from outside the system
        so it's good defense-in-depth to validate it at our app boundary.

        We also deal with some historical baggage by renaming fields
        as needed.
        """
        data = tokenmanager.parse_token(tokenid, now=now)
        user = {}

        # It should always contain an integer userid.
        try:
            user["uid"] = data["uid"]
        except KeyError:
            raise ValueError("missing uid in token data")
        else:
            if not isinstance(user["uid"], int) or user["uid"] < 0:
                raise ValueError("invalid uid in token data")

        # It should always contain a string node name.
        try:
            user["node"] = data["node"]
        except KeyError:
            raise ValueError("missing node in token data")
        else:
            if not isinstance(user["node"], basestring):
                raise ValueError("invalid node in token data")

        # It might contain additional user identifiers for
        # storage and metrics purposes.
        #
        # There's some historical baggage here.
        #
        # Old versions of tokenserver would send a hashed "metrics uid" as the
        # "fxa_uid" key, attempting a small amount of anonymization.  Newer
        # versions of tokenserver send the raw uid as "fxa_uid" and the hashed
        # version as "hashed_fxa_uid".  The raw version may be used associating
        # stored data with a specific user, but the hashed version is the one
        # that we want for metrics.

        if "hashed_fxa_uid" in data:
            user["hashed_fxa_uid"] = data["hashed_fxa_uid"]
            if not VALID_FXA_ID_REGEX.match(user["hashed_fxa_uid"]):
                raise ValueError("invalid hashed_fxa_uid in token data")
            try:
                user["fxa_uid"] = data["fxa_uid"]
            except KeyError:
                raise ValueError("missing fxa_uid in token data")
            else:
                if not VALID_FXA_ID_REGEX.match(user["fxa_uid"]):
                    raise ValueError("invalid fxa_uid in token data")
            try:
                user["fxa_kid"] = data["fxa_kid"]
            except KeyError:
                raise ValueError("missing fxa_kid in token data")
            else:
                if not VALID_FXA_ID_REGEX.match(user["fxa_kid"]):
                    raise ValueError("invalid fxa_kid in token data")
        elif "fxa_uid" in data:
            user["hashed_fxa_uid"] = data["fxa_uid"]
            if not VALID_FXA_ID_REGEX.match(user["hashed_fxa_uid"]):
                raise ValueError("invalid fxa_uid in token data")

        if "hashed_device_id" in data:
            user["hashed_device_id"] = data["hashed_device_id"]
            if not VALID_FXA_ID_REGEX.match(user["hashed_device_id"]):
                raise ValueError("invalid hashed_device_id in token data")
        elif "device_id" in data:
            user["hashed_device_id"] = data.get("device_id")
            if not VALID_FXA_ID_REGEX.match(user["hashed_device_id"]):
                raise ValueError("invalid device_id in token data")
        return user


def includeme(config):
    """Include syncstorage-specific authentication into a pyramid config."""
    # Build a SyncStorageAuthenticationPolicy from the deployment settings.
    settings = config.get_settings()
    authn_policy = SyncStorageAuthenticationPolicy.from_settings(settings)
    config.set_authentication_policy(authn_policy)

    # Set the forbidden view to use the challenge() method from the policy.
    config.add_forbidden_view(authn_policy.challenge)
