# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""

Script to create a new mozsvcdeploy deployment cluster.

"""

import sys
import logging
import optparse

import boto
import boto.exceptions
import boto.ec2.elb


logger = logging.getLogger("mozsvcdeploy.scripts.create_cluster")


def ensure_load_balancer(product_name, cluster_name):
    """Ensure that the cluster has a load balancer up and running."""
    conn = boto.connect_elb()
    # Check if there is already an appropriately-named load balancer.
    lb_name = "%s-%s-lb" % (product_name, cluster_name,)
    try:
        for lb in conn.get_all_load_balancers(lb_name):
            logger.info("%s already exists", lb_name)
            return
    except boto.exceptions.BotoServerError, e:
        if "LoadBalancerNotFound" not in str(e):
            raise
    # There is not, we'll have to create one.
    logger.info("%s does not exist, creating...", lb_name)
    regions = ["us-east-1a", "us-east-1b"]
    ports = [(80, 80, "http")]
    lb = conn.create_load_balancer(lb_name, regions, ports)
    lb.configure_health_check(boto.ec2.elb.HealthCheck(
        interval=10,
        healthy_threshold=3,
        unhealthy_threshold=3,
        target="HTTP:80/__heartbeat__",
    ))
    logger.info("%s created", lb_name)
    # XXX TODO: SSL and SSL certificates


def create_cluster(product_name, cluster_name):
    ensure_load_balancer(product_name, cluster_name)


def main(args=None):
    """Main entry-point for running this script."""
    usage = "usage: %prog [options]"
    parser = optparse.OptionParser(usage=usage)
    parser.add_option("-v", "--verbose", action="count", dest="verbosity",
                      help="Control verbosity of log messages")

    opts, args = parser.parse_args(args)
    if len(args) != 1:
        parser.print_usage()
        return 1

    raise RuntimeError("NOT FINISHED YET")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(1)
