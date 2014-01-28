# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""

Script to purge BSOs with expired TTLs.

This script takes a syncstorage config file and loops through each storage
backend therein, deleting any BSOs that have an expired TTL.  Note that this
is a purely optional administrative task, since expired items are not actually
returned in queries.  But it should help reduce overheads, improve performance
etc if run regularly.

"""

import os
import time
import logging
import optparse

import syncstorage.scripts
from syncstorage.storage import get_all_storages


logger = logging.getLogger("syncstorage.scripts.prunettl")


def purge_expired_items(config_file, grace_period=0, backend_interval=0):
    """Purge expired BSOs from all storage backends in the given config file.

    This function iterates through each storage backend in the given config
    file and calls its purge_expired_items() method.  The result is a
    gradual pruning of expired items from each database.
    """
    logger.info("Purging expired items")
    logger.debug("Using config file %r", config_file)
    config = syncstorage.scripts.load_configurator(config_file)

    for hostname, backend in get_all_storages(config):
        logger.debug("Purging backend for %s", hostname)
        config.begin()
        try:
            backend.purge_expired_items(grace_period)
        except Exception:
            logger.exception("Error while purging backend for %s", hostname)
        else:
            logger.debug("Purged backend for %s", hostname)
        finally:
            config.end()
        logger.debug("Sleeping for %d seconds", backend_interval)
        time.sleep(backend_interval)

    logger.info("Finished purging expired items")


def main(args=None):
    """Main entry-point for running this script.

    This function parses command-line arguments and passes them on
    to the purge_expired_items() function.
    """
    usage = "usage: %prog [options] config_file"
    parser = optparse.OptionParser(usage=usage)
    parser.add_option("", "--purge-interval", type="int", default=3600,
                      help="Interval to sleep between purging runs")
    parser.add_option("", "--backend-interval", type="int", default=360,
                      help="Interval to sleep between purging each backend")
    parser.add_option("", "--grace-period", type="int", default=86400,
                      help="Number of seconds grace to allow after expiry")
    parser.add_option("", "--oneshot", action="store_true",
                      help="Do a single purge run and then exit")
    parser.add_option("-v", "--verbose", action="count", dest="verbosity",
                      help="Control verbosity of log messages")

    opts, args = parser.parse_args(args)
    if len(args) != 1:
        parser.print_usage()
        return 1

    syncstorage.scripts.configure_script_logging(opts)

    config_file = os.path.abspath(args[0])

    purge_expired_items(config_file,
                        grace_period=opts.grace_period,
                        backend_interval=opts.backend_interval)
    if not opts.oneshot:
        while True:
            logger.debug("Sleeping for %d seconds", opts.purge_interval)
            time.sleep(opts.purge_interval)
            purge_expired_items(config_file,
                                grace_period=opts.grace_period,
                                backend_interval=opts.backend_interval)
    return 0


if __name__ == "__main__":
    syncstorage.scripts.run_script(main)
