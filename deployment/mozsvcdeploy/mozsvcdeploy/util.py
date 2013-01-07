# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""

mozsvcdeploy.util:  misc utility functions for operating in a cluster
---------------------------------------------------------------------

Here be miscellaneous utility functions.  I expect some of them to be
factored out into their own modules in short order.

"""

import os
import sys
import stat
import time
import errno
import socket
import logging
import tempfile
import contextlib

import fabric.api
import fabric.context_managers

import boto.ec2


logger = logging.getLogger("mozsvcdeploy.util")

MOZSVC_INFO_DIR = "/etc/mozilla-services"


def read_info_file(name):
    """Read contents of a file from the mozsvc info directory.

    This function reads the contents of a file from the mozsvc info directory
    and returns them as a string.  If the file does not exist then it returns
    an emptry string.
    """
    try:
        with open(os.path.join(MOZSVC_INFO_DIR, name), "r") as f:
            return f.read()
    except EnvironmentError, e:
        if e.errno != errno.ENOENT:
            raise
        return ""


def write_info_file(name, contents):
    """Atomically replace the contents of a file in the mozsvc info directory.

    This function replaces the contents of a file in the mozsvc info directory
    with the given string, using an atomic rename operation to ensure that
    the file is always in a consistent state.
    """
    # Create a tempfile in the mozsvc info directory.
    # If the info directory turns out not to exist, we create it.
    try:
        tmpfd, tmpname = tempfile.mkstemp(prefix=name, dir=MOZSVC_INFO_DIR)
    except EnvironmentError, e:
        if e.errno != errno.ENOENT:
            raise
        try:
            os.makedirs(MOZSVC_INFO_DIR)
        except EnvironmentError, e:
            if e.errno != errno.EEXIST:
                raise
        tmpfd, tmpname = tempfile.mkstemp(prefix=name, dir=MOZSVC_INFO_DIR)
    # Write the new contents into the tempfile, then rename over the top.
    # We try to ensure the tempfile is cleaned up on error.
    try:
        os.write(tmpfd, contents)
        os.close(tmpfd)
        # If the target file exists, copy permissions from it.
        # Otherwise just set them to something sensible.
        try:
            st = os.stat(os.path.join(MOZSVC_INFO_DIR, name))
        except EnvironmentError:
            os.chmod(tmpname, 0644)
        else:
            os.chown(tmpname, st.st_uid, st.st_gid)
            os.chmod(tmpname, stat.S_IMODE(st.st_mode))
        os.rename(tmpname, os.path.join(MOZSVC_INFO_DIR, name))
    except Exception:
        exc_val, exc_typ, exc_tb = sys.exc_info()
        try:
            os.unlink(tmpname)
        except EnvironmentError, e:
            pass
        raise exc_val, exc_typ, exc_tb


@contextlib.contextmanager
def ssh_to_instance(instance):
    """Context-manager to run commands on the given instance.

    Use this context-manager to easily run commands against a specified EC2
    instance, like this:

        with ssh_to_instance(instance) as shell:
            shell.run("touch HELLO-WORLD")
            shell.run("/bin/true")

    It transparently uses fabric to ssh into the instance, and returns the
    fabric API object as context.  You can then call methods such as "run",
    "sudo", etc to execute commands.

    XXX TODO: this likely won't work very well in a multi-threaded environment,
    since IIRC the fabric settings context is not thread-local.
    """
    settings = {}
    settings["user"] = "ec2-user"
    settings["host_string"] = instance.public_dns_name
    with fabric.context_managers.settings(**settings):
        # For convenience, return the fabric api object as the context
        yield fabric.api


def wait_for_status(obj, status, via=None, timeout=5*60, sleep_time=5):
    """Poll the given object until it reaches a certain status.

    This function can be used on any boto API object with an update() method,
    to wait until it reaches a specific status.  It will periodically call
    the update() method to check on the status, existing either when the
    desired status is reached or when the specified timeout has elapsed.

    If the optional argument "via" is given, it must be a set of acceptable
    status strings that the object can pass through in its transition to the
    target status.  If it enters a status that is not in this list, an error
    will be raised.
    """
    logger.debug("waiting for %s to reach status %r", obj, status)
    if timeout is not None:
        max_time = time.time() + timeout
    while True:
        # Request the updated status.
        # Some object types have a "status" attribute that gets written
        # when update() is called, others only return it from update().
        # We support both types.
        cur_status = getattr(obj, "status", obj.update())
        logger.debug("...%s is currently %r", obj, cur_status)
        if cur_status == status:
            return True
        if via is not None and cur_status not in via:
            logger.warn("...%s has unexpected status %r", obj, status)
            raise RuntimeError("Unexpected status: %s" % (status,))
        if timeout is not None and time.time() >= max_time:
            logger.warn("...timeout waiting for %s status %r", obj, status)
            raise RuntimeError("Operation timed out")
        time.sleep(sleep_time)


def wait_for_instance_ready(instance, timeout=5*60, sleep_time=5):
    """Poll the instance until it appears to be ready, or timeout passes.

    This function periodically polls the given instance, waiting for the
    following conditions to be true:

        * the management API reports its status as "ready"
        * we can successfully connect on port 22

    """
    logger.debug("waiting for %s to be ready", instance)
    if timeout is not None:
        max_time = time.time() + timeout

    # Wait for it to be in "ready" state.
    wait_for_status(instance, "running", ["pending"], timeout, sleep_time)

    # Wait for ssh to be available.
    while True:
        logger.debug("...waiting for ssh on %s", instance.public_dns_name)
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(sleep_time)
            s.connect((instance.public_dns_name, 22))
            s.close()
            break
        except socket.error:
            if timeout is not None and time.time() >= max_time:
                logger.warn("...timeout waiting for %s to be ready", instance)
                raise RuntimeError("Operation timed out")
            time.sleep(sleep_time)


class cleanup_context(object):
    """Utility class for cleaning up transient AWS resources.

    This class can be used as a context manager to automate the cleanup of
    transient AWS resources, such a instances or volume snapshots.  Simply
    add resources to be cleaned up via the add() method, like so:

        with cleanup_context() as cleanup:
            instance = spawn_a_transient_instance()
            cleanup.add(instance)
            # When you exit the context, the instance will be terminated

    """

    def __init__(self):
        self.resources = []

    def add(self, resource, keep_on_success=False, keep_on_error=False):
        self.resources.append((resource, keep_on_success, keep_on_error))

    def __enter__(self):
        return self

    def __exit__(self, exc_typ=None, exc_val=None, exc_tb=None):
        # Find all the resources to delete, respecting keep_on_success/error.
        if exc_typ is None and exc_val is None and exc_tb is None:
            to_cleanup = [r[0] for r in self.resources if not r[1]]
        else:
            to_cleanup = [r[0] for r in self.resources if not r[2]]
        # We cleanup things in a specific order, to try to free resources
        # by e.g. tearning down instances before their attached volumes.
        to_cleanup.sort(key=self._sortkey_for_cleanup_ordering)
        for r in to_cleanup:
            try:
                self._cleanup_resource(r)
            except Exception:
                logger.exception("failed to cleanup %s", r)

    def _cleanup_resource(self, resource):
        """Tear down and delete the given EC2 resource."""
        logger.debug("cleaning up %s", resource)
        if hasattr(resource, "terminate"):
            resource.terminate()
            wait_for_status(resource, "terminated")
        if hasattr(resource, "delete"):
            resource.delete()

    def _sortkey_for_cleanup_ordering(self, resource):
        """Get a comparison key to sort resources into cleanup order.

        This class tries to clean up resources in a sensible order that will
        automatically break references between different types of resource.
        For example, it deletes instances before volumes so that any volumes
        mounted on an instance will be released and available for cleanup.

        This method makes all the ordering decisions, by return a "sort key"
        by which the resource objects can be ordered.  This is essentially
        an integer corresponding to the relative importance of the resource's
        class, such that they'll all sort correctly.

        Using sort(cmp=) rather than sort(key=) might be easier to understand
        but I find the code to be rather more complicated in this case.
        """
        ordered_classes = (
            boto.ec2.instance.Instance,
            boto.ec2.volume.Volume,
            boto.ec2.snapshot.Snapshot,
        )
        for i, cls in enumerate(ordered_classes):
            if isinstance(resource, cls):
                return i
        return i + 1
