# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import time
import logging

import tokenlib
import tokenlib.errors

from zope.interface import implements
from pyramid.interfaces import IAuthenticationPolicy
from mozsvc.user import TokenServerAuthenticationPolicy


logger = logging.getLogger("syncstorage")


DEFAULT_EXPIRED_TOKEN_TIMEOUT = 60 * 60 * 2  # 2 hours, in seconds


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
                    data = tm.parse_token(tokenid, now=now)
                except tokenlib.errors.ExpiredTokenError:
                    recently = now - self.expired_token_timeout
                    data = tm.parse_token(tokenid, now=recently)
                    data["uid"] = "expired:%d" % (data["uid"],)
            except ValueError:
                # Token validation failed, move on to the next secret.
                continue
            else:
                # Token validation succeeded, quit the loop.
                break
        else:
            # The token failed to validate using any secret.
            logger.warn("Authentication Failed: invalid hawk id")
            raise ValueError("invalid Hawk id")
        # Sanity-check the contained data.
        # Any errors raise ValueError, triggering auth failure.
        try:
            userid = data["uid"]
            token_node_name = data["node"]
        except KeyError, e:
            msg = "missing value in token data: %s"
            raise ValueError(msg % (e,))
        if token_node_name != node_name:
            msg = "incorrect node for this token: %s"
            raise ValueError(msg % (token_node_name,))
        # Calculate the matching request-signing secret.
        key = tokenlib.get_derived_secret(tokenid, secret=secret)

        request.metrics["fxa_uid"] = data.get("fxa_uid")
        request.metrics["device_id"] = data.get("device_id")

        return userid, key


def includeme(config):
    """Include syncstorage-specific authentication into a pyramid config."""
    # Build a SyncStorageAuthenticationPolicy from the deployment settings.
    settings = config.get_settings()
    authn_policy = SyncStorageAuthenticationPolicy.from_settings(settings)
    config.set_authentication_policy(authn_policy)

    # Set the forbidden view to use the challenge() method from the policy.
    config.add_forbidden_view(authn_policy.challenge)
