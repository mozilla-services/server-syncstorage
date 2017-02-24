#!/usr/bin/env python
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""

Memcache data reading script for SyncStorage.

This script takes a syncstorage config file, and reads a list of userids
from STDIN.  The memcache data for each user is printed to stdout.

"""

import os
import sys
import logging
import optparse
import contextlib

import syncstorage.scripts
from syncstorage.storage import get_all_storages
from syncstorage.storage.memcached import MemcachedStorage


logger = logging.getLogger(__name__)


def read_memcache_data(config_file, input_file, output_file):
    """Read memcache data for all userids listed in the given input file."""
    logger.info("Reading data for uids in %s", input_file)
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
        with maybe_open(output_file, "wt") as output_fileobj:

            for uid in input_fileobj:
                uid = uid.strip()
                if uid:
                    logger.info("Reading data for %s", uid)
                    for key in backend.iter_cache_keys(uid):
                        value = backend.cache.get(key)
                        if value is not None:
                            output_fileobj.write("%s %s\n" % (key, value))
                    logger.debug("Read data for %s", uid)

    logger.info("Finished reading memcache data")


@contextlib.contextmanager
def maybe_open(name_or_fileobj, mode):
    """Context-manager to open a file, unless it's already open.

    This context-manager accepts either a filename or filelike object as
    its first argument.  If the former, the file is automatically opened
    and closed like the standard open() function.  If the latter, the object
    is not automatically closed.

    This is very useful if the caller can specify a file by giving either
    its path or an existing filelike object.
    """
    if not isinstance(name_or_fileobj, basestring):
        yield name_or_fileobj
    else:
        with open(name_or_fileobj, mode) as fileobj:
            yield fileobj


def main(args=None):
    """Main entry-point for running this script.

    This function parses command-line arguments and passes them on
    to the clear_memcache_data() function.
    """
    usage = "usage: %prog [options] config_file"
    parser = optparse.OptionParser(usage=usage)
    parser.add_option("-f", "--input-file", default="-",
                      help="The file from which to read userids")
    parser.add_option("-o", "--output-file", default="-",
                      help="The file to which to write memcache data")
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
    if opts.output_file == "-":
        opts.output_file = sys.stdout
    read_memcache_data(config_file, opts.input_file, opts.output_file)
    return 0


if __name__ == "__main__":
    syncstorage.scripts.run_script(main)
