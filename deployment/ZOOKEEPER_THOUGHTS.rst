Crazy ZooKeeper Idea
====================

Build our own WSGI server container, in the spirit of gunicorn, but taking
its configuration data from a Zookeeper (or Doozer, or whatever) cluster
instead of from a file.

At startup, it produces the config dict and spawns worker processes just like
gunicorn.

Whenever a change is detected for the configuration, it does the equivalent
of a gunicorn SIGHUP - block incomming connections, spin up new workers, let 
old workers compelte and then tear them down.

When it detects that it's lost connectivity from the cluster, it murders all
its workers and hence takes itself out of the loadbalancer.  Maybe even the
individual worker processes can suicide if they lose connection to the cluster.

The idea being that as soon as a change comes in, we get workers ready to
operate according to that change.  using a prefork worker model means the
application code doesn't have to worry about reconfiguring itself, we just
sping up a new one with the changes.

Question:  how many webheads per cluster?


Crazy Bucketing Idea
====================

Instead of node-assignment, just do bucketing.

Fix a number of buckets, e.g. 4096.  Each user consistently and permantently
maps to a single bucket:

  hash(userid) => bucketid

Each bucket is owned by a particular dbnode.

Keep the node/bucket ownership data in zookeeper, so that everyone has a
consistent view of it.  It's then trivial to route requests to the right place.

When a node goes down, re-assign its buckets randomly between the other nodes.

When a new node comes up, re-assign some buckets to it.  Don't bother about
migrating the data, at least for now, because this is just like "blowing up"
the bucket like we currently do to a node.

The nodes whose buckets have been stolen will have to garbage-collect the data
that they no longer own.  Or maybe we could do it for them periodically from
the webheads.


But Ryan, What If We Need More Buckets??!!?!
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Restrict the number of buckets N to always be a power of two, N=2^n.

Instead of a modulo hash, use a "keystream" kind of hash where each userid
is mapped to an infinitely long string of bits.  Kindof like HMAC, which can
generate arbitrarily long string output.  But it doesn't need to be crypto
strong.

To figure out the bucket for a particular use, take the first n bits from
this string and interpret it as a big-endian integer.

To increase the number of buckets, n += 1.  Everyone gets a new bucket, and
every node gets a new set of buckets.  But each node can calculate the new set
of buckets that it owns, given just the old set of buckets.  And nobody has to
be moved to a different node!  This essentially just split each bucket in half,
leaving both halves on the same node.

So  B_n+1(uid) == 2 * B_n(uid)  or  2 * B_n(uid) + 1
