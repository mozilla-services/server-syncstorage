# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""

mozsvcdeploy.whoami: determine information about the current cluster
--------------------------------------------------------------------

This module provides functions for determining information about the cluster
in which the current machine is participating.  Clearly they will only work
if run from a member of a cluster!

"""

import json
import logging

import requests

import boto
import boto.ec2.instance

import mozsvcdeploy.util


logger = logging.getLogger("mozsvcdeploy.whoami")

METADATA_URL = "http://169.254.169.254/latest/meta-data/"

CLUSTER_INFO = None
CLUSTER_INFO_FILE = "cluster.json"


def get_cluster_info():
    """Get cluster information for the current machine.

    This method looks for cluster information about the current machine,
    in a number of places:

        * a local in-process cache
        * the file /etc/mozilla-services/cluster.json
        * by interrogating the EC2 data api

    """
    global CLUSTER_INFO
    # If not cached in-process, try to read from file.
    # The file may not exist yet.
    if CLUSTER_INFO is None:
        data = mozsvcdeploy.util.read_info_file(CLUSTER_INFO_FILE)
        if data:
            CLUSTER_INFO = json.loads(data)
    # If not available in file, refresh it from the environment.
    # Try to write it back to the file for later use, but if we can't
    # (e.g. due to permissions) then it's not a fatal error.
    if CLUSTER_INFO is None:
        CLUSTER_INFO = find_cluster_info()
        try:
            data = json.dumps(CLUSTER_INFO)
            mozsvcdeploy.util.write_info_file(CLUSTER_INFO_FILE, data)
        except EnvironmentError:
            raise
            logger.exception("Could not write cluster info cache file")
    return CLUSTER_INFO


def find_cluster_info():
    """Interrogate the environment to find canonical cluster information.

    This function uses the EC2 managment API to figure out what cluster the
    current instance is part of, and then find its relevant peers.  This
    requires the instance to have delegated authority to call the EC2 API,
    and will fail if it does not.
    """
    cluster_info = {}
    con = boto.connect_ec2()
    # Find out what cluster I'm part of by looking at the current instance.
    myinst = boto.ec2.instance.Instance(con)
    myinst.id = requests.get(METADATA_URL + "instance-id").content
    myinst.update()
    cluster_info["product"] = myinst.tags["product"]
    cluster_info["cluster"] = myinst.tags["cluster"]
    cluster_info["zknodes"] = []
    # Now find the members of that cluster that provide the "zknode" service.
    filters = {}
    filters["tag:cluster"] = cluster_info["cluster"]
    filters["tag:service"] = "zknode"
    for reservation in con.get_all_instances(filters=filters):
        for instance in reservation.instances:
            cluster_info["zknodes"].append(instance.private_ip_address)
    return cluster_info
