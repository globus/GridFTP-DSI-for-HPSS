## About
GridFTP-DSI-for-HPSS is a Globus Connect Server Connector which allows HPSS administrators to create Globus Endpoints for HPSS installations. 

## Release Notes
None at this time. New issues will appear here before being incorporated into the official documentation.

## Verified HPSS Versions
Although this connector supports all HPSS versions 7.3 and higher, I can only
verify functionality for HPSS distributions on OS releases that I have access
to.

## Installation
If you are an admin planning to install the HPSS connector for production use,
go to the
[official docs](https://docs.globus.org/premium-storage-connectors/hpss/)
for the most recent installation instructions. 

RPMs are provided for HPSS/OS versions for sites that have graciously provided
me with development access. Check the
[release page](https://github.com/JasonAlt/GridFTP-DSI-for-HPSS/releases)
for RPMs for your HPSS/OS versions. If no RPM is avialable for your
configuration, you'll need to build the RPM as described below. Please consider
making development resources available for your configuration so that I can
provide RPMs as part of the release cycle.

## Building RPMs for your OS and HPSS Versions

RPM building and development require these packages:
* make
* gcc
* autoconf
* automake
* libtool
* rpm-build
* openssl
* globus-gridftp-server-devel
* libtirpc-devel
* hpss-lib-devel

I highly recommend building the RPM from source distributions available on the [release page](https://github.com/JasonAlt/GridFTP-DSI-for-HPSS/releases). Download the source for the latest release, extracts its contents then from within the source directory build the RPM:
```shell
$ make -f Makefile.bootstrap release
```

## Development

```shell
$ git clean -f -d -x
$ make -f Makefile.bootstrap develop
```

To see recent changes to the code base, see the [ChangeLog](ChangeLog).

### Tracking Issues
All features and fixes to this repository require a tracking GitHub issue *if* the change is worthy of discussion, *if* it is a notable or sizable change or *if* the change requires external tracking (ex. tracked in GitHub projects or for communication with collaborators or external users). 

### Submitting Changes
Fork the repository to your private GitHub account, make changes there and then require a pull request against the appropriate prelease branch. The commit message should include the issue number and summary, the `closes` or `fixes` keywords, and briefly describe any notable changes not worth of discussion in the tracking GitHub issue. Do not be overly verbose; `git diff` will give future devs the details, no need to be redundant. Each pull request should include only a single large change. Multiple minor changes may be grouped into a single pull request. Consider breaking large feature updates into multiple pull requests submitted over time during development.

All notable changes should be recorded in the ChangeLog.

### Branching Strategy
The `master` branch contains the latest stable version and is always releasable code. Features and fixes should follow the naming scheme `issue/<id>` where `<id>` is the tracking GitHub issue number. If the issue does not warrent a tracking GitHub issue, you can name the branch however you like _within your own forked copy of the repository_. New branches should fork the master or prelease branch as appropriate. Pull requests should be submitted against the target prelease branch. The master branch will be fast-forward-merged once the prelease is known to be stable and ready for release.

### Tagging Strategy
All stable releases are tagged `Version_<major>_<minor>_<release>` on master. Prereleases use `Prerelease_<major>_<minor>_<release>` as the release tag. Historically, this repository did not use `<release>` versioning; just assume that to have a value of `1` when no release value is given.

### Release Process
* Accept pull requests *only* against a prelease branch.
* Merge pull request onto prelease branch.
* Tag prerelease branch with next release version. Edit the GitHub release and mark it as a pre-release.
* Attach a source tar to the release, produce it from a clean build of 'make dist'.
* Coordinate with outside collaborators to verify release candidate.
* When ready, fast-forward merge prelease to master. 
* Tag master with latest release version.
* Attach latest source package to official release.
* Delete pre release tags and prerelease branch.
