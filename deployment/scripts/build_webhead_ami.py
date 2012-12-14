#!/bin/env python
#
#  Script to re-master a webead AMI for a particular version of syncstorage.
#
#  This script creates a master machine image for a syncstorage webhead at
#  a given version number.  Run it like so:
#
#     python scripts/build_webhead_ami.py rpm-X.Y-Z
#
#  It will spin up a new EC2 instance from an appropriate base image, install
#  and configure the necessary packages, and save its state into a new AMI
#  that can be used to spin up individual webhead instances.
#  

import boto


# The base AMI to use for building the system.
# This is Amazon Linux, Instance-Storage, US-East-1.
# We will eventually need to tweak this for multiple availability zones.
BASE_IMAGE = "ami-e8249881"


# Launch the image in EC2.
#   * put it in appropriate security groups, etc?
#   * use cloud-init to run a script that installs everything:
#      * install and configure puppet
#      * let puppet suck down the necessary configs etc
#      * install the necessary dependencies
#      * checkout and build the syncstorage rpms
#      * remove unneeded dependencies
# Reboot the instance, and check that it comes back up
# Shut it down, snapshot into a new AMI
# Dispose of the instance

TAGS = {
  "Name": "sync2 webhead",
  "Product": "sync2",
  "Version": "dev",
  "Author": "rfkelly@mozilla.com"
}

