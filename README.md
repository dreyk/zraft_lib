# zraft_lib

Erlang [raft consensus protocol](https://raftconsensus.github.io) implementation .

Supported features:
- Runtime membership reconfiguration.
- Log truncation via snapshotting.
- Peer asynchronous RPC.
- Pluggable state machine.
- Optimistic log replication.
- Snapshot transfer via kernel sendfile command.
- Client sessions.
- Temporary data (like ephemeral nodes)
- Data change triggers.

## Erlang Architecture
![schema](docs/img/schema.png?raw=true)

## General Configuration
Example app configuration file.
```
[{zraft_lib,
     [{snapshot_listener_port,0},
      {election_timeout,500},
      {request_timeout,1000},
      {snapshot_listener_addr,"0,0,0,0"},
      {snapshot_backup,false},
      {log_dir,"./data"},
      {snapshot_dir,"./data"},
      {max_segment_size,10485760},
      {max_log_count,1000}]},
 {lager,
     [{error_logger_hwm,100},
      {error_logger_redirect,true},
      {crash_log_date,"$D0"},
      {crash_log_size,10485760},
      {crash_log_msg_size,65536},
      {handlers,
          [{lager_file_backend,
               [{file,"./log/console.log"},
                {level,info},
                {size,10485760},
                {date,"$D0"},
                {count,5}]},
           {lager_file_backend,
               [{file,"./log/error.log"},
                {level,error},
                {size,10485760},
                {date,"$D0"},
                {count,5}]}]},
      {crash_log,"./log/crash.log"},
      {crash_log_count,5}]},
 {sasl,[{sasl_error_logger,false}]}].
 ```
- "election_timeout" - Timeout(in ms) used by Follower to start new election process (default 500).
- "request_timeout" - Timeout(in ms) used by Leader to wait replication RPC reply from Follower (default 2*election_timeout).
- "snapshot_listener_port" - Default port used for transfer snapshot.(0 - any free port).
- "snapshot_listener_addr" - Bind Address for accept snapshot transfer connections.
- "snapshot_backup" - If it turn on all snapshot will be archived.
- "log_dir" - Directory to store RAFT logs and metadata.
- "snapshot_dir" - Directory to store snapshots.
- "max_segment_size" - Maximum size in bytes log segment.(New segment will be created after reach that threshold.)
- "max_log_count" - Snapshot/LogTruncation process will be started after every "max_log_count" applied entries.

## Create and Config RAFT Cluster.

```
zraft_client:create(Peers,BackEnd).
```
Parameters:
- `Peers` - lists of cluster peers, e.g. `[{test1,'test1@host1'},{test1,'test2@host2'},{other_test,'test3@host3'}]`.
- `BackEnd` - module name used for apply user requests.

Possible return values:
 - `{ok,Peers}` - cluster has created.
 - `{error,{PeerID,Error}}` - can't create PeerID, error is Error.
 - `{error,[{PeerID,Error}]}` - can't create peers.
 - `{error,Reason}` - cluster has created, but applying new configuration has failed with reason Reason.


## Basic operations.

#### Light Session Object.
Light session object used to track current raft cluster state, e.g. leader,failed peers, etc...

Create session object by PeerID:

```
zraft_client:light_session(PeerID,FailTimeout,ElectionTimeout).
```
Parameters:
- `PeerID` - ID of peer from luster.
- `FailTimeout` - If we detect that peer has failed,then we will not send any request to this peer during this Interval.
- `ElectionTimeout` - If we detect that peer isn't leader,then wee not send any request to this peer during this Interval.

Possible return values:
- `LightSession` - Light Session object.
- `{error,Reason}` - Can't read cluster configuration.

Create session by list PeerID:
```
zraft_client:light_session(PeersList,FailTimeout,ElectionTimeout).
```
This function will not try read configuration from cluster.

#### Write operation.

```
zraft_client:write(PeerID,Data,Timeout).

```

Parameters:
- `PeerID` - PeerID.
- `Data` - Request Data specific for BackEndModule.

Return:
- `{Result,LeaderPeerID}` - Result is result of applying Data to BackEndModule. LeaderPeerID is current leader ID.
- `{error,Error}` - Operation has failed. Typical reason is timeout,noproc.

Write using session object.
```
zraft_client:write(LaghtSessionObj,Data,Timeout).

```

Parameters:
- `LightSessionObj` - Light Sesssion Object.
- `Data` - Request Data specific for BackEndModule.

Return:
- `{Result,LightSessionObj}` - Result is result of applying Data to BackEndModule. LightSessionObj is update session object.
- `{error,Error}` - Operation has failed. Typical reason is timeout,all_failed. `all_failed` means,there are not alive peers. 

```
WARNING: during this request Data may be applyed to backend module twice.
```

#### Read request:

```
zraft_client:query(PeerID,Query,Timeout).

```
Parameters:
- `PeerID` - PeerID.
- `Query` - Request Data specific for backend module.

Return:
- `{Result,LeaderPeerID}` - Result is result of query. LeaderPeerID is current leader ID.
- `{error,Error}` - Operation has failed. Typical reason is timeout,noproc.

Or read data using light session object:

```
zraft_client:query(LaghtSessionObj,Query,Timeout).

```

Return:
- `{Result,LightSessionObj}` - Result is result of query. LightSessionObj is update session object.
- `{error,Error}` - Operation has failed. Typical reason is timeout,all_failed. `all_failed` means,there are not alive peers. 


#### Change Configuration:

```
zraft_client:set_new_conf(Peer,NewPeers,OldPeers,Timeout).
```

## Use Session:

You can create long lived session to RAFT cluster. It can be used triggers and temporary datas.

```
zraft_session:start_link(PeerOrPeers,SessionTimeout)->{ok,Session}.
```

If first parameter is PeerID other available Peer will be readed from that Peer.

#### Write Data and Ephemeral data.

```
zraft_session:write(Session,Data, Temporary, Timeout).
```
If Temporary is true then data will be deleted after session wil be expired.

#### Read Data and Set watchers

```
zraft_session:query(Session,Query,Watch,Timeout).
```

Watch is trigger reference that will be triggered after future changes.Trigger will be triggered only once, if you need new trigger you must data again.

Example:
```
zraft_session:query(S1,1,my_watcher,1000).
%%Result = not_found.
zraft_session:write(S2,{1,2},1000).
receive
     {swatch_trigger,my_watcher,Reason}->
          %%Data changed. Change Reason is data_chaged or leader chaged.
          ok
end.
zraft_session:query(S1,1,my_watcher,1000). %%watch again
```






##Standalone Server.

You can use it for tests from erlang console.

https://github.com/dreyk/zraft


## TODO:
- Write External API documentation.
- Add backend based on ets table
- Add "watcher" support (notify client about backend state changes).


