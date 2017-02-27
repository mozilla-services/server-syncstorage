#!/usr/bin/env python
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""

Memcache data clearing script for SyncStorage.

This script takes a syncstorage config file, and reads a list of userids
from STDIN.  The memcache data for each user is wiped.

"""

import os
import sys
import logging
import optparse

import syncstorage.scripts
from syncstorage.storage import get_all_storages
from syncstorage.storage.memcached import MemcachedStorage

from syncstorage.scripts.mcread import maybe_open


logger = logging.getLogger(__name__)


def clear_memcache_data(config_file, input_file):
    """Clear memcache data for all userids listed in the given input file."""
    logger.info("Clearing data for uids in %s", input_file)
    logger.debug("Using config file %r", config_file)
    config = syncstorage.get_configurator({"__file__": config_file})

    # Search all configured storages to find one that uses memcached.
    # We assume that all storages share a single memcached server, and
    # so we can use this single instance as a representative.  This is
    # how things are deployed at Mozilla, but is not guaranteed by the code.
    for _, backend in get_all_storages(config):
        if isinstance(backend, MemcachedStorage):
            break
    else:
        raise RuntimeError("No memcached storage backends found.")
    logger.debug("Using memcache server at %r", backend.cache.pool.server)

    with maybe_open(input_file, "rt") as input_fileobj:
        for uid in input_fileobj:
            uid = uid.strip()
            if uid:
                logger.info("Clearing data for %s", uid)
                for key in backend.iter_cache_keys(uid):
                    backend.cache.delete(key)
                logger.debug("Cleared data for %s", uid)

    logger.info("Finished clearing memcache data")


def main(args=None):
    """Main entry-point for running this script.

    This function parses command-line arguments and passes them on
    to the clear_memcache_data() function.
    """
    usage = "usage: %prog [options] config_file"
    parser = optparse.OptionParser(usage=usage)
    parser.add_option("-f", "--input-file", default="-",
                      help="The file from which to read userids")
    parser.add_option("-v", "--verbose", action="count", dest="verbosity",
                      help="Control verbosity of log messages")

    opts, args = parser.parse_args(args)
    if len(args) != 1:
        parser.print_usage()
        return 1

    syncstorage.scripts.configure_script_logging(opts)

    config_file = os.path.abspath(args[0])

    if opts.input_file == "-":
        opts.input_file = sys.stdin
    clear_memcache_data(config_file, opts.input_file)
    return 0


if __name__ == "__main__":
    syncstorage.scripts.run_script(main)
