# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import mozsvc.config
import mozsvc.metrics


def includeme(config):
    """Install SyncStorage application into the given Pyramid configurator."""
    # Ensure that we have metlog loaded and ready for use as early as possible.
    mozsvc.metrics.load_metlog_client(config)
    # Disable cornice default exception-handling views.
    config.registry.settings.setdefault("handle_exceptions", False)
    # Include dependencies from other packages.
    config.include("cornice")
    config.include("mozsvc")
    config.include("mozsvc.user")
    # Add in the stuff we define ourselves.
    config.include("syncstorage.tweens")
    config.include("syncstorage.storage")
    config.include("syncstorage.views")


def get_configurator(global_config, **settings):
    """Load a SyncStorge configurator object from deployment settings."""
    config = mozsvc.config.get_configurator(global_config, **settings)
    # Ensure that we have metlog loaded and ready for use as early as possible.
    mozsvc.metrics.load_metlog_client(config)
    config.begin()
    try:
        config.include(includeme)
    finally:
        config.end()
    return config


def main(global_config, **settings):
    """Load a SyncStorage WSGI app from deployment settings."""
    config = get_configurator(global_config, **settings)
    return config.make_wsgi_app()
