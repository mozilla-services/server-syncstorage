# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

from mozsvc.config import get_configurator
from mozsvc.plugin import load_from_settings
from syncstorage.controller import StorageController


def includeme(config):
    # Include dependencies from other packages.
    config.include("cornice")
    config.include("mozsvc")
    config.include("mozsvc.user.whoauth")
    # Add in the stuff we define ourselves.
    config.add_renderer("newlines", "syncstorage.views:NewlinesRenderer")
    config.include("syncstorage.tweens")
    config.include("syncstorage.storage")
    config.scan("syncstorage.views")
    # Create a "controller" object.  This is a vestiage of the
    # pre-pyramid codebase and will probably go away in the future.
    config.registry["syncstorage.controller"] = StorageController(config)


def main(global_config, **settings):
    config = get_configurator(global_config, **settings)
    metlog_wrapper = load_from_settings('metlog', config.registry.settings)
    config.registry['metlog'] = metlog_wrapper.client
    config.include(includeme)
    return config.make_wsgi_app()
