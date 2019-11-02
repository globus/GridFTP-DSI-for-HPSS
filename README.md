## Release Notes
GridFTP-DSI-for-HPSS is a Globus Connect Server Connector which allows HPSS administrators to create Globus Endpoints for HPSS installations. For the most recent installation instructions, go to [https://docs.globus.org/premium-storage-connectors/hpss/](https://docs.globus.org/premium-storage-connectors/hpss/). To see recent changes to the code base, see the [ChangeLog](ChangeLog).

### Important Status Details 

##### BZ7883 prevents transfers larger than 4GiB on 7.5.2
HPSS bug BZ7883 prevents successful transfers of files over 4GiB on HPSS versions 7.5.2+. Due to what appears to be a transfer length calculation error, transfer of files larger than 4GiB generate an EIO error at the 4GiB mark and the transfer terminates. This bug impacts all HPSS clients using the HPSS PIO interface. Upgrade to HPSS 7.5.2u5 / HPSS 7.5.3u1 to resolve this issue.

##### Workaround for [issue35](https://github.com/JasonAlt/GridFTP-DSI-for-HPSS/issues/35) "Async stage requests cause red-ball-of-doom"
Recent changes to make use of the async stage request API for HPSS in order to avoid inundating the core server with duplicate stage requests has exposed a deficiency for the DSI use case of HPSS. The HPSS async stage API expects the call to be available long term in order to receive stage completion messages. However, the GridFTP/DSI use case is a short-lived transient environment; the GridFTP process can not wait minutes/hours/days for stage completion messages. Users of DSI versions 2.6+ will see the impact as a 'red-ball-of-doom' indicator in the HPSS GUI console. The warning is innocuous and can be ignored. IBM is aware of this issue and a CR has been created. 

As a work around, users of 2.6 should update to 2.7 and all users of 2.7+ can use the 'blackhole sync' method. This configures `nc` (netcat) to listen for stage completion messages intended for the DSI and discard whatever it receives. `nc` should be launched on a highly-available server reachable by the HPSS core servers (preferably run it directly on the core servers). Choose a port to use for receiving callback notifications on and run this command:
```shell
host> nc -v -v -k -l <port>
```
Once `nc` is running, add this to /etc/gridftp.d/hpss on the GridFTP nodes running the HPSS DSI:
```shell
$ASYNC_CALLBACK_ADDR <host>:<port>
```

### Recommended HPSS Patches
These HPSS issues severely impact performance so the patches are highly recommended.

**BZ2819** - PIO 60 second delay impacts small file performance. There is a small percentage chance that, after a transfer completes, HPSS PIO will wait 60 seconds before informing the client that the transfer has completed. This fix has been implemented in 7.3.3p9, 7.3.4, 7.4.1p1 and 7.4.2.

**BZ2856** - Enabling HPSS_API_REUSE_CONNECTIONS returns address already in use. This one sets a limit on how many active connections we can have. GridFTP and HPSS make considerable use of ephemeral TCP ports. Quick, successive file transfers can lead the system to run out of available ports. There is no fix for this HPSS issue at this time. The number of ephemeral ports can be increased and the amount of time a socket spends in timed wait can be decreased to help avoid this issue.

## Contributor Guidelines

### Tracking Issues
All features and fixes to this repository require a tracking GitHub issue *if* the change is worthy of discussion, *if* it is a notable or sizable change or *if* the change requires external tracking (ex. tracked in GitHub projects or for communication with collaborators or external users). 

### Submitting Changes
Fork the repository to your private GitHub account, make changes there and then require a pull request against the appropriate prelease branch. The commit message should include the issue number and summary, the `closes` or `fixes` keywords, and briefly describe any notable changes not worth of discussion in the tracking GitHub issue. Do not be overly verbose; `git diff` will give future devs the details, no need to be redundant. Each pull request should include only a single large change. Multiple minor changes may be grouped into a single pull request. Consider breaking large feature updates into multiple pull requests submitted over time during development.

All notable changes should be recorded in the ChangeLog.

### Branching Strategy
The `master` branch contains the latest stable version and is always releasable code. Features and fixes should follow the naming scheme `issue/<id>` where `<id>` is the tracking GitHub issue number. If the issue does not warrent a tracking GitHub issue, you can name the branch however you like _within your own forked copy of the repository_. New branches should fork the master or prelease branch as appropriate. Pull requests should be submitted against the target prelease branch. The master branch will be fast-forward-merged once the prelease is known to be stable and ready for release.

### Tagging Strategy
All stable releases are tagged `Version_<major>_<minor>_<release>` on master. Preleases use `Prerelease_<major>_<minor>_<release>` to the release tag. Historically, this repository did not use `<release>` versioning; just assume that to have a value of `1` when no release value is given.

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
