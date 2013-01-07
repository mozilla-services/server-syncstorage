MozSvc Cluster Management Thingo
================================

This is an experiment in automating deployment of a syncstorage cluster in AWS,
in such as way that we might be able to re-purpose it for both other products
(e.g. campaginmanager, aitc) as well as for other deployment environments
(e.g. openstack).

IT IS NOT COMPLETE AND DOESNT WORK YET.

You'll need the "boto" library and your AWS access credentials set up as
environment variables.

The idea is to automatically set up and manage a bunch of related machines
within a single AWS region, spread across multiple availability zones, and
all coordinating with each other automatically via a shared ZooKeeper instance.

(I have no particular love for ZooKeeper, but alternatives seem few)


Concepts
--------

A **cluster** is a load balancer plus a set of machine instances that all work
together to operate a particular web product.  Each instance provides a one
particular **service** within the cluster, and the kinds of service involved
will depend on the particular product being deployed.

A cluster always contains at least three machines running the **zknode**
service, which provide a ZooKeeper ensemble for the cluster.  This ensemble
is used to store all runtime configuration data for the cluster (think
passwords, server lists, etc) and can also be used to provide coordination
facilities for other services.

Via the magic of the mozsvcdeploy module, a machine instance is always able
to find the IPs of its associated zookeeper ensemble.  This allows the machine
to bootstrap itself into the proper configuration when it first joins.

Each machine running in a cluster is tagged with the following metadata:

  * product:  the product being deployed (e.g. "syncstorage")
  * cluster:  the name of the cluster product cluster (e.g. "prod", "stage")
  * service:  the name of the particular service it provides (e.g. "dbnode")

The machine image for each service is built from a set of puppet scripts,
which can take data from the zookeeper ensemble at runtime.  There will be
some helper scripts in the mozsvcdeploy module to make this work properly.


Initializing a Cluster
~~~~~~~~~~~~~~~~~~~~~~

There will be a command "create_cluster" that can be used to initialize a new
cluster in AWS.  It will bring up the following resources if they are not
already present:

   * an elastic load balancer instance
   * a zknode service image
   * three zknode service instances

Use it like this::

  $> python mozsvcdeploy/scripts/create_cluster.py <product> <cluster-name>



Configuring a Cluster
~~~~~~~~~~~~~~~~~~~~~

I'll provide some means to read, set and delete configuration items in the
zookeeper store.  Not exactly sure what this will look like yet.  Maybe
a nice web interface.  Maybe this already exists!  We'll see...


Building a Service Image
~~~~~~~~~~~~~~~~~~~~~~~~

I'll provide a function build_service_image() and a corresponding command-line
script.  You provide the product name, cluster name, service name, version
number and the puppet scripts to use.  It will build the image, give it
appropriate tags, and store it off for later use.

You can also customize some stuff e.g. the security group to use.  We'll see
how this plays out.


Launching a Service Instance
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You provide the product name, cluster name, service name, version number.
The script will find the appropriate service image, possibly security group.
Then launch the image.

It will use cloud-init to write some basic information about the cluster
into the filesystem of the machine, so that it can easily bootstrap itself
into full membership.


Thought Experiments
-------------------

These are tasks we might like to perform within the cluster.  It should be
able to do them in a nice way, preferably with no downtime.  If it can't then
there's something wrong with the architecture.


Roll out a new version of the web app
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We can:

  * Build a new "webhead" service image with the updated code.
  * Spin up a single instance and insert it into the loadbalancer.
  * Watch the instance and make sure it's functioning properly.
  * Spin up however many more instances we need, and insert into loadbalance.
  * Remove the old instances from the load balancer.
  * If it all seems to be working, terminate the old instances.
  * If it hits the fan, re-insert the old instances and terminate the new ones.

If all goes smoothly then hooray, no downtime!


Add a new dbnode
~~~~~~~~~~~~~~~~

Steps:

  * launch a new instance from the existing "dbnode" service image
  * setup DNS entries, token secrets, etc
  * push updated webapp config into zookeeper
  * all the webheads detect the change and reload themselves
  * add it to the tokenserver database

If all goes smoothly then hooray, no downtime!


Change the MySQL password
~~~~~~~~~~~~~~~~~~~~~~~~~

Can we do this without downtime?

Steps I see so far, which have some downtime:

  * write the new password into zookeeper
  * puppet-apply on the dbnodes, which get the new password set
  * puppet-apply on the webheads, which get the new password and restart

The problem is, there's a window here where the dbnodes and the webheads may
have a different password.  We could give the webapp some ability to detect
this situation and recover, but it coud get fiddly...


Disorganised Thoughts, Misleading Notes, and Other Randomness
-------------------------------------------------------------

Basic idea:

  * A cluster has a product name, and a cluster name, e.g. "aws-syncstorage"
  * To initialize a cluster, we create 3 zookeeper nodes and set them up
    as a little ensemble.  Everything else in the cluster coorinates through
    them.
  * Haven't yet figured out a good story for replacing zookeeper servers, it
    would be better if they could come and go dynamically ala doozer or riak.
  * You can then start up cluster "members" usng puppet, and they can
    interrogate their environment to figure out who they are, find their
    zookeeper nodes, etc.  We will have some utilities for building an
    managine machine images of these members.

Basic things we need:

  * a Provider is a thin abstraction for the deployment environment; in the
    first instance a layer over EC2.  We could possibly use libcloud for this
    but I don't want to lock in a lowest-common-denominator solution just yet.
  * a Service is a particular component of the deployment, e.g. a webhead or
    a database node.
      * One service == one VM.  This is probably simplest approach.
      * How should be manage them - subclassing perhaps?  Or just a name and
        a pointer to some puppet recipes?
      * Each instance of a Service has a particular version number associated
        with it.
  * a Cluster represents an entire deployment of a product, which can contain
    multiple type of Service running on multiple instances.


We can use IAM to let the instances interrogate the cluster and find their
peers, which will allow them to bootstrap themselves into the cluster by
auto-discovering the zookeeper servers.  Obviously this could be a little
inefficient, but would substantially ease maintenance burden.

