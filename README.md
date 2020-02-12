[![CircleCI](https://circleci.com/gh/mozilla-services/server-syncstorage.svg?style=svg)](https://circleci.com/gh/mozilla-services/server-syncstorage)

# Storage Engine for Firefox Sync Server, version 1.5

This is the storage engine for version 1.5 of the Firefox Sync Server.
It implements the API defined at:

   * https://mozilla-services.readthedocs.io/en/latest/storage/apis-1.5.html

This code is only one part of the full Sync Server stack and is unlikely
to be useful in isolation.

If you want to run a self-hosted version of the Sync Server,
you should start from here:

   * https://mozilla-services.readthedocs.io/en/latest/howtos/run-sync-1.5.html

More general information can be found at the following links:

   * https://mozilla-services.readthedocs.io/en/latest/storage
   * https://wiki.mozilla.org/Services/Sync


## Release Procedure

To tag a new release of this server for deployment, you'll need to:

* Create and push a new git tag with the version number, e.g. `1.7.1`.
* File [a new deploy bug](https://bugzilla.mozilla.org/enter_bug.cgi?product=Cloud%20Services&component=Operations:%20Deployment%20Requests)
  specifying the version to be deployed.
  * CC the Sync Operations staff.
  * List any imporant new functionality or config changes in the bug description.
  * You might enjoy these [previous](https://bugzilla.mozilla.org/show_bug.cgi?id=1476181)
    [bugs](https://bugzilla.mozilla.org/show_bug.cgi?id=1317532) as examples.
* Coordinate with QA to run the [loadtest suite](https://github.com/mozilla-services/syncstorage-loadtest/).
* Be available for any questions from Ops as they process the deployment.

