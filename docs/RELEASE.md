# Release Procedure

These steps outline what is required for a new production release.

## Requirements

* All releases must be from the `master` branch.
* All releases must pass CI testing.
* All releases must be accompanied by a CHANGELOG.md that indicates bug fixes, new features and breaking changes. (This may be auto-generated by the [clog-cli](https://github.com/clog-tool/clog-cli) tool)

## Versions

Versioning should use a `{major}.{minor}.{patch}` version scheme. New `{major}` versions are only issued if backwards compatibility is impacted. `{ minor}` involves introduction of new features. `{patch}` versions involve bug fixes only.

## Release Process

1. switch to `master` branch
1. `git pull` to ensure local copy is completely up-to-date
1. `git diff origin/master` to ensure there are no local staged or uncommited changes.
1. run local testing to ensure no artifacts or other local changes that might break tests have been introduced.
1. change the release branch: `git checkout -b release/{major}.{minor}` (note `{version}` will be used as short-hand for `{major}.{minor}`)
1. edit `version` in `setup.py` to reflect current release version.
1. run `clog --setversion {version}`, verify changes were properly accounted for in `CHANGELOG.md`.
1. verify that edited files appear in `git status`
1. `git commit -m "chore: tag {version}` to commit the new version and record of changes.
1. `git tag -s -m "chore: tag {version}" {version}` to create a signed tag of the current HEAD commit for release
1. `git push --set-upstream origin release/{version}` to push the tags to the release branch
1. submit a github Pull Request to merge the release branch to master
1. Verify that the new tag, with no release info, appears in the github releases page.
1. Click the `Draft a new release` button
1. Enter the `{version}` for `Tag version`
1. Copy the relevant version data from `CHANGELOG.md` into the release description.
1. Once the rlease branch PR is approved and merged, click `Publish Release`.
1. File a bug for stage depolyment in Bugzilla under `Cloud Services` product, `Operations: Depolyment Requests` component. It should be titled `Please deploy server-syncstorage {version} to STAGE` and include the relevant version data from `CHANGELOG.md`


At this point, QA should take over, verify Stage and create the production deployment ticket.