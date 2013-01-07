# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""

Script to build and register a new machine image from puppet templates.

This script can be used to build and register a new machine image from puppet
templates.  The resulting image is an instance-store-backed AMI derivced from
Amazon Linux.  We build it in a rather complicated way with the hope of doing
it as safely and repeatably as possible:

   * Spin up an EBS-backed instance with additional instance storage attached,
     and build it out to the desired configuration.
        * It must be an EBS-backed instance so that we can halt the machine
          without losing the root device.
        * It must have additional instance storage attached so that we've got
          enough space to build the AMI.

   * Stop the machine, and take a snapshot of its root device.
        * Stopping the machine ensures the disk is in a consistent state.

   * Clone a new EBS device from the snapshot, attach it to the instance, and
     bring the instance back up.

   * Copy the cloned EBS device into a local disk image, using the instance
     storage as scratch space.  Bundle this up using the standard command-line
     tools, and we're done!

(It would be much simpler to just build an EBS-backed AMI, but then all of our
machines would be dependant on EBS, the most notoriously unreliable of all the
components in AWS.  No thanks.)

"""

import sys
import logging
import optparse

import boto
import boto.ec2
from boto.ec2.blockdevicemapping import BlockDeviceType, BlockDeviceMapping

from mozsvcdeploy.util import (ssh_to_instance,
                               wait_for_status,
                               wait_for_instance_ready,
                               cleanup_context)


logger = logging.getLogger("mozsvcdeploy.scripts.build_ami")


REGION = "us-east-1"
AVAILABILITY_ZONE = "us-east-1a"
BASE_AMI = "ami-1624987f"  # Amazon Linux, EBS-Backed, 64-bit
INSTANCE_TYPE = "m1.small"
KEYPAIR_NAME = "rfkelly"
VOLUME_SIZE = 8  # XXX TODO: read this from the instance somehow


def launch_builder_instance(conn):
    logger.info("launching builder instance")
    # Launch an EBS-backed instance with addition instance-local storage.
    # We'll use the EBS root device as the base for the AMI, and the instance
    # storage for scratch space while building it.
    mapping = BlockDeviceMapping()
    mapping["/dev/sdb"] = BlockDeviceType()
    mapping["/dev/sdb"].ephemeral_name = "ephemeral0"
    reservation = conn.run_instances(BASE_AMI, instance_type=INSTANCE_TYPE,
                                     key_name=KEYPAIR_NAME,
                                     placement=AVAILABILITY_ZONE,
                                     block_device_map=mapping)
    instance = reservation.instances[0]
    return instance


def configure_script_logging(opts=None):
    """Configure stdlib logging to produce output from the script.

    This basically configures logging to send messages to stderr, with
    formatting that's more for human readability than machine parsing.
    It also takes care of the --verbosity command-line option.
    """
    if not opts or not opts.verbosity:
        loglevel = logging.WARNING
    elif opts.verbosity == 1:
        loglevel = logging.INFO
    else:
        loglevel = logging.DEBUG

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    handler.setLevel(loglevel)
    logger = logging.getLogger("mozsvcdeploy")
    logger.addHandler(handler)
    logger.setLevel(loglevel)


def main(args=None):
    """Main entry-point for running this script."""
    usage = "usage: %prog [options]"
    parser = optparse.OptionParser(usage=usage)
    parser.add_option("-v", "--verbose", action="count", dest="verbosity",
                      help="Control verbosity of log messages")

    opts, args = parser.parse_args(args)
    if len(args) != 0:
        parser.print_usage()
        return 1

    configure_script_logging(opts)

    conn = boto.connect_ec2()

    with cleanup_context() as cleanup:

        # build a running instance to the desired configuration
        instance = launch_builder_instance(conn)
        cleanup.add(instance)
        wait_for_instance_ready(instance)
        with ssh_to_instance(instance) as s:
            s.sudo("touch /TESTFILE")

        # stop the instance
        logger.info("stopping the builder instance")
        instance.stop()
        wait_for_status(instance, "stopped")

        # snapshot the root device
        logger.info("taking a snapshot of the root device")
        root_dev = instance.block_device_mapping[instance.root_device_name]
        snapshot = conn.create_snapshot(root_dev.volume_id)
        cleanup.add(snapshot)
        wait_for_status(snapshot, "completed")

        # clone it to a new volume
        logger.info("cloning the root device from its snapshot")
        vol = snapshot.create_volume(AVAILABILITY_ZONE, VOLUME_SIZE)
        cleanup.add(vol)
        wait_for_status(vol, "available")

        # Attach it to the instance
        logger.info("attaching the clone to the instance")
        vol.attach(instance.id, "/dev/sdh")
        wait_for_status(vol, "in-use")

        # Start the instance back up
        logger.info("restarting the builder instance")
        instance.start()
        wait_for_instance_ready(instance)

        # Figure out our numerical account ID.  This is a pretty stupid
        # hack, but there doesn't seem to be an API for it...
        for sg in conn.get_all_security_groups():
            account_id = sg.owner_id
            break
        else:
            raise RuntimeError("Could not find AWS account id")

        # OK, here we go...
        with ssh_to_instance(instance) as s:
            logger.info("preparing the disk image")
            # Mount the disk and clean up any transient state.
            s.sudo("mkdir -p /mnt/rootdev")
            s.sudo("mount /dev/sdh /mnt/rootdev")
            s.sudo("rm /mnt/rootdev/etc/ssh/ssh_host_*")
            s.sudo("umount /mnt/rootdev")
            # Clone the disk image into a local image file.
            s.sudo("mkdir -p /mnt/scratch")
            s.sudo("mount /dev/sdb /mnt/scratch")
            s.sudo("dd if=/dev/sdh of=/mnt/scratch/root.img")
            # Generate a random X.509 certificate for signing the image.
            # This seems stupid, but works, because it's not used for integrity
            # or permission checking - all the bundling process does it encrypt
            # the AMI to this private key, so as long as we don't need to unbundle
            # the AMI at a later date, it doesn't matter if we have the key.
            s.sudo("openssl genrsa 1024 > /mnt/scratch/pk.pem")
            s.sudo("yes '' | openssl req -new -x509 -nodes -sha1 -days 365 -key /mnt/scratch/pk.pem -outform PEM > /mnt/scratch/cert.pem")
            # Bundle it into an image
            # XXX TODO: can we add the s3 resources to the cleanup context?
            logger.info("bundling the AMI")
            s.sudo("mkdir -p /mnt/scratch/ami")
            s.sudo("EC2_HOME=/opt/aws /opt/aws/bin/ec2-bundle-image --cert /mnt/scratch/cert.pem --privatekey /mnt/scratch/pk.pem --image /mnt/scratch/root.img --prefix mozsvc-test --user " + str(account_id) + " --destination /mnt/scratch/ami --arch " + instance.architecture + " --kernel " + instance.kernel)
            s.sudo("EC2_HOME=/opt/aws /opt/aws/bin/ec2-upload-bundle --bucket mozsvc-test-amis-rfkelly --manifest /mnt/scratch/ami/mozsvc-test.manifest.xml --access-key " + conn.aws_access_key_id + " --secret-key " + conn.aws_secret_access_key)

    # Register the AMI
    logger.info("registering the AMI")
    ami_id = conn.register_image(name="mozsc-test-ami",
                                 description="rfkelly mozsvc test ami",
                                 image_location="mozsvc-test-amis-rfkelly/mozsvc-test.manifest.xml",
                                 architecture=instance.architecture,
                                 kernel_id=instance.kernel)
    logger.info("registered the AMI: %s", ami_id)

    # AND THERE WE HAVE IT FOLKS!
    print ami_id

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(1)
