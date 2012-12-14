
AWS Deployment Notes
====================

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


Create WebHead AMI
-------------------

* Launch m1.small with Amazon Linux AMI
* Use setup_webhead.sh as the user-data
* ...NOW WHAT?


Create Storage Node AMI
-----------------------

* Launch m1.small with Amazon Linux AMI
* Use setup_dbnode.sh as the user-data
* ...NOW WHAT?


Notes
-----

instance-store based Amz Linux AMI: ami-e8249881

