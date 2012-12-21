
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
and the elasticache cluster.
  
  * A "sync2-sg-web" EC2 security group for the webheads, allowing inbound
    traffic on ports 22 and 80.
  * A "sync2-sg-db" RDS security group to contain the dbnodes, allowing 
    access from machines in the "sync2-sg-web" security group.
  * A "sync2-sg-cache" ElastiCache security group for the elasticache instance,
    allowing access from machines in the "sync2-sg-web" security group.


Load Balancer
~~~~~~~~~~~~~

Create a "sync2-lb" instance of Elastic Load Balancer.  Configure it to
accept HTTP traffic only, and forward it as HTTP traffic.

(XXX TODO: confer with JR and Ops on how to use HTTPS rather than HTTP)

Configure the "health check" to ping /__heartbeat__, which is a special
URL used by mozsvc applications to report on their health.

File a bug to get a <blah>.services.mozilla.com DNS alias setup as a CNAME
for the loadbalancer.


Webhead Instance
~~~~~~~~~~~~~~~~

Launch an m1.small instance using image "ami-e8249881".  This is an Amazon
Linux image using instance storage for its root partition.

Place it in the "sync2-sg-web" security group, and use the contents of
"./scripts/setup_webhead.sh" as the "user data" during startup.  This is
a cloud-init script that will build and install all the necessary packages to
stand up a fully-functioning webhead.

Associate the instance with the Load Balancer, and it should start accepting
traffic as soon as it's ready.


DBNode RDS Instance
~~~~~~~~~~~~~~~~~~~

Create a new RDS DB instance with appropriate storage.  Create a default user
of "sync2" and see ./scripts/setup_webhead.sh for the corresponding password.
Place the instance in the "sync2-sg-db" security group.

Grab the DNS name for the new instance, and put it in the files under ./puppet/
at the appropriate location.  Then push the config change out to all the
webheads.

(XXX TODO: the puppet files don't yet work with multiple dbnodes)

(XXX TODO: get MySQL puppet recipes from Ops, convert this to use a custom
cluter of EC2 instances)


ElastiCache
~~~~~~~~~~~

Create a new Elasticache cluster, appropriately sized.  Place it in the
"sync2-sg-cache" security group.

Grab the DNS name for the new instance, and put it in the files under ./puppet/
at the appropriate location.  Then push the config change out to all the
webheads.

(XXX TODO: elasticache has an "autodiscovery" thing that can avoid having
to adjust the config as nodes are added or removed - we should look into that.
http://docs.amazonwebservices.com/AmazonElastiCache/latest/UserGuide/AutoDiscovery.ConfigCommand.html)


Things To Do
~~~~~~~~~~~~

Things we haven't completely figured out yet:

* Metrics and logging setup
* Monitoring

