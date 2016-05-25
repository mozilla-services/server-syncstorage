[![CircleCI](https://circleci.com/gh/mozilla-services/server-syncstorage.svg?style=svg)](https://circleci.com/gh/mozilla-services/server-syncstorage)

# Storage Engine for Firefox Sync Server, version 1.5

This is the storage engine for version 1.5 of the Firefox Sync Server.
It implements the API defined at:

   * https://docs.services.mozilla.com/storage/apis-1.5.html

This code is only one part of the full Sync Server stack and is unlikely
to be useful in isolation.  It also supports a version of the Sync protocol
that is not yet used in production.

If you want to run a self-hosted version of the Sync Server for use with
existing versions of Firefox, you should start from here:

   * https://docs.services.mozilla.com/howtos/run-sync.html

If you want to run a self-hosted version of the new protocol for testing
in-development clients, you should start from here:

   * https://github.com/mozilla-services/server-full2

More general information can be found at the following links:

   * https://docs.services.mozilla.com/storage/
   * https://wiki.mozilla.org/Services/Sync
