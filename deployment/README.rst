
AWS Deployment Tools
====================

The initial deployment of the SyncStorage 2.0 service will be hosted with
Amazon Web Services.  This directory contains tooling and documentation for
working with that deployment.

Overview
--------

The deployment consists of the following key components:

 * An **Elastic Load Balancer** instance, which is the entry-point for all
   traffic hitting the service.
 * A cluster of **webhead** machines, which are EC2 instances running the
   server-syncstorage WSGI application and receiving traffic from the load
   balancer.
 * An **ElastiCache** cluster instance, used as a shared cache by the webheads.
   This may eventually be replaced by a cluster of custom EC2 images running
   couchbase.
 * Multiple **dbnode** machines, which are MySQL database hosts responsible for
   the actual storage.  Currently these are Amazon RDS instances, but they
   will be replaced by custom EC2 instances running MySQL.

In addition, access to SyncStorage is mediated by the **tokenserver** service,
which is hosted internally by Mozilla.

Visually::

                                                      +-------------+
                                                   +->| elasticache |
                 +-------------+                   |  +-------------+
             +-->| tokenserver |             +--x--+
             |   +-------------+             |  |  |  +-------------+
             |                               |  |  +->| elasticache |
 +--------+  |                  +---------+  |  |     +-------------+
 | client |--+-+             +->| webhead |--+--[---+
 +--------+    |    +-----+  |  +---------+     |   |      +--------+
               +--> | ELB |--+                  |   |   +->| dbnode |
                    +-----+  |  +---------+     |   |   |  +--------+
                             +->| webhead |-----+---x---+
                                +---------+             |  +--------+
                                                        +->| dbnode |
                                                           +--------+

Setup Notes
-----------

Security Groups
~~~~~~~~~~~~~~~

AWS uses security groups to control access between cloud resources.  For
syncstorage we need independent security groups for the webheads, the dbnodes,
and the elasticache cluster:
  
  * A "syncstorage-sg-web" EC2 security group for the webheads, allowing
    inbound traffic on ports 22 and 80.  Don't forget to "apply changes"
    after adding the rules!
  * A "syncstorage-sg-db" RDS security group to contain the dbnodes, allowing 
    access from machines in the "syncstorage-sg-web" security group.
  * A "syncstorage-sg-cache" ElastiCache security group for the elasticache
    instance, allowing access from machines in the "syncstorage-sg-web"
    security group.


Load Balancer
~~~~~~~~~~~~~

Create a "syncstorage-lb" instance of Elastic Load Balancer.  Configure it to
accept HTTP traffic on port 80 and forward it as HTTP traffic on port 80.

(XXX TODO: confer with JR and Ops on how to use HTTPS rather than HTTP)

Configure the "health check" ping path to be /__heartbeat__, which is a special
URL used by mozsvc applications to report on their health.

Don't add any instances to it yet, we don't have any running.



DBNode RDS Instance
~~~~~~~~~~~~~~~~~~~

Create a new RDS DB instance using MySQL, size db.m1.small, with 100 GB of
storage.  Create a default user of "syncstorage" and grab the corresponding
password from ./scripts/setup_webhead.sh.  Place the instance in the
"syncstorage-sg-db" security group.

Each dbnode manages 10 virtual hostnames.  Assign 10 new DNS hostnames for this
node (XXX TODO: how exactly?) and set them up as CNAME records pointing to the
load balancer.  Then create 10 corresponding databases on the instance named
"syncstorage0" through "syncstorage9":

    CREATE DATABASE syncstorageX;

Grab the internal DNS name for the new RDS instance, and edit the "webhead"
puppet recipe to add them to the db_nodes hash, like this::

    $db_nodes = {
      "external-hostname-0" => "internal-rds-hostname/syncstorage0",
      "external-hostname-1" => "internal-rds-hostname/syncstorage1",
      ...
      "external-hostname-9" => "internal-rds-hostname/syncstorage9",
    }
 

Use puppet to push this update out to any running webheads, so they can connect
to the new instance.

Finally, add the newly-assigned hostnames into the tokenserver nodes database.

(XXX TODO: get MySQL puppet recipes from Ops, convert this to use a custom
cluster of EC2 instances rather than RDS instances)


ElastiCache
~~~~~~~~~~~

Create a new Elasticache cluster, size cache.m1.small.  Use only a single node,
because the syncstorage code can't currently handle more than 1 node.  Place it
in the "syncstorage-sg-cache" security group.

Grab the DNS name for the new instance, and put it in the files under ./puppet/
at the appropriate location.  Then push the config change out to all the
webheads.

(XXX TODO: elasticache has an "autodiscovery" thing that can avoid having
to adjust the config as nodes are added or removed - we should look into that.
http://docs.amazonwebservices.com/AmazonElastiCache/latest/UserGuide/AutoDiscovery.ConfigCommand.html)


Webhead Instance
~~~~~~~~~~~~~~~~

Launch an m1.small instance using image "ami-e8249881".  This is an Amazon
Linux image using instance storage for its root partition.

Place it in the "syncstorage-sg-web" security group, and use the contents of
"./scripts/setup_webhead.sh" as the "user data" during startup.  This is
a cloud-init script that will build and install all the necessary packages to
stand up a fully-functioning webhead.

Ensure you assign an appropriate ssh key, so you can shell into it once it's
up and running.

Now wait.  A lot.

The machine will spend quite some time building syncstorage and all the
necessary dependencies, so ssh into it and watch "top" until there's no
more activity.  Check /var/log/cloud-init.log for any errors.

Associate the instance with the Load Balancer, and it should start accepting
traffic as soon as it's ready.

(XXX TODO: save it as an image for easy deployment of additional webheads;
this can't be dont through the web console because it's not using EBS for
its root device)


TODO: Integration with Tokenserver
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The tokenserver needs to know what hostnames are available, and the master
secret to use for each hostname.  The webheads need to know what hostnames map
to what DBNode, and the master secret to use for each hostname.

To add a new DBNode we need to do the following, in order:

   * Bring up the instance, configure MySQL, create tables, etc.
   * Assign a set of new hostnames to it in syncstorage::webhead manifest
   * Re-generate the secrets file from the puppet template, so that it includes
     the new hostnames
   * Push the new puppet config to the syncstorage webheads
   * Push the updated secrets file to the tokenserver
   * Add each new hostname into the nodes database on tokenserver

To change the master secret for an existing node we need to do the following,
in order:

   * Add a new secret for that node in puppet://syncstorage/secrets, leaving
     the current secret in place.
   * Push the new puppet config to the syncstorage webheads; it is now able
     to accept tokens signed with either new or old secret.
   * Push the updated secrets file to the tokenserver; it will now generate
     tokens with the new secret.
   * After some time, remove the old secret for the node from
     puppet://syncstorage/secrets and push to syncstorage and tokenserver.


TODO: Metrics and Logging
~~~~~~~~~~~~~~~~~~~~~~~~~

I need to talk to whd about this, and look at the existing puppet code.

TODO: Monitoring
~~~~~~~~~~~~~~~~

I need to look at the existing puppet code.


TODO: Misc
~~~~~~~~~~

nginx should be more generic, make a conf.d and allow other recipes
to insert configuration into it.
