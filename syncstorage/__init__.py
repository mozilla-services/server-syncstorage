# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import logging
logger = logging.getLogger('syncserver')

from mozsvc.config import get_configurator

from syncstorage.controller import StorageController


def includeme(config):
    # Add config settings to use vepauth and basicauth.
    # This is a temporary hack.
    # It will go away once mozsvc gets built-in support for it, but is here
    # for now so that we can test the new auth flow.
    settings = config.registry.settings
    VEPAUTH_DEFAULTS = {
        "use": "repoze.who.plugins.vepauth:make_plugin",
        "audiences": "",
        "token_url": "/2.0/token",
        "token_manager": "syncstorage.tokens:ServicesTokenManager",
    }
    for key, value in VEPAUTH_DEFAULTS.iteritems():
        settings.setdefault("who.plugin.vepauth." + key, value)
    BASICAUTH_DEFAULTS = {
        "use": "repoze.who.plugins.basicauth:make_plugin",
        "realm": "Sync",
    }
    for key, value in BASICAUTH_DEFAULTS.iteritems():
        settings.setdefault("who.plugin.basicauth." + key, value)
    # Make sure there's a usable config for the "testauth" plugin.
    TESTAUTH_DEFAULTS = {
        "use": "syncstorage.tokens:TestingAuthenticator",
    }
    for key, value in TESTAUTH_DEFAULTS.iteritems():
        settings.setdefault("who.plugin.testauth." + key, value)
    # Set vepauth + basicauth as the default identifier, authenticator
    # and challenger combo.
    settings.setdefault("who.identifiers.plugins", "vepauth basicauth")
    settings.setdefault("who.authenticators.plugins", "vepauth testauth")
    settings.setdefault("who.challengers.plugins", "vepauth basicauth")
    # Include dependencies from other packages.
    config.include("cornice")
    config.include("mozsvc")
    config.include("mozsvc.user.whoauth")
    # Add in the stuff we define ourselves.
    config.add_renderer("whoisi", "syncstorage.views:WhoisiRenderer")
    config.add_renderer("newlines", "syncstorage.views:NewlinesRenderer")
    config.include("syncstorage.tweens")
    config.include("syncstorage.storage")
    config.scan("syncstorage.views")
    try:
        config.scan("syncstorage.storage")
    except ImportError:
        pass
    # Create a "controller" object.  This is a vestiage of the
    # pre-pyramid codebase and will probably go away in the future.
    config.registry["syncstorage.controller"] = StorageController(config)


def main(global_config, **settings):
    config = get_configurator(global_config, **settings)
    config.include(includeme)
    return config.make_wsgi_app()
