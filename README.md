## Release Notes
GridFTP-DSI-for-HPSS is a Globus Connect Server Connector which allows HPSS administrators to create Globus Endpoints for HPSS installations. For the most recent installation instructions, go to [https://docs.globus.org/premium-storage-connectors/hpss/](https://docs.globus.org/premium-storage-connectors/hpss/). To see recent changes to the code base, see the [ChangeLog](ChangeLog).

### Important Status Details 

##### BZ7883 prevents transfers larger than 4GiB on 7.5.2
HPSS bug BZ7883 prevents successful transfers of files over 4GiB on HPSS versions 7.5.2+. Due to what appears to be a transfer length calculation error, transfer of files larger than 4GiB generate an EIO error at the 4GiB mark and the transfer terminates. This bug impacts all HPSS clients using the HPSS PIO interface. It is recommended to not use the DSI with HPSS versions 7.5.2+ until a fix is provided.

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

