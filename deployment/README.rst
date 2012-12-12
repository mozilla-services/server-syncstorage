
AWS Deployment Notes
====================

Create WebHead AMI
-------------------

* Launch m1.small with Amazon Linux AMI


Create Storage Node AMI
-----------------------


Create Load Balancer
--------------------

Create a loadbalancer to send traffic through to the webheads.
* Configure it to accept HTTP traffic only, and forward it as HTTP
  * eventually we'll need to SSL this up somehow, maybe by uploading
    a chained certificate for AWS to use in the load balancer..?
* Add /__heartbeat__ as the health check URL


Create Security Group
---------------------

Needs ssh and http, nothing else.


Scripty Things To Write
-----------------------

Build and push a new version
  * based on bobm's syncpush script
  * with rollback etc?
  * both for storage nodes and webheads?

Provision a new storage node
  * spin it up from the AMI
  * get IP address, assign DNS name, etc?
  * update configuration on all webheads
      * puppet?
  * test its reachability somehow
  * update WIMMS database


When storage node boots up, it needs to:
  * initialize the database

When a webhead boots up, it needs to:
  * figure out what storage backends it can talk to
  * connect to memcache somehow
    * use the AWS hosted memcache?
  * 


Open Questions
--------------

We're doing some sort of puppet-based thing with RPMs, need to figure that out.

instance-store based Amz Linux AMI:
  ami-e8249881    
