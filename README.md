# zraft_lib

Erlang [raft consensus protocol](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf) implementation .

Supported features:
- Runtime membership reconfiguration.
- Log truncation via snapshotting.
- Peer asynchronous RPC.
- Pluggable state machine.
- Optimistic log replication.
- Snapshot transfer via kernel sendfile command.

## Peer processes and message passing schema.
![schema](docs/img/schema.png?raw=true)

##Configuration
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
- "request_timeout" - Timeout(in ms) used by Leader to wait replicate RPC reply from Follower (default 2*election_timeout).
- "snapshot_listener_port" - Default port used for transfer snapshot.(0 - any free port).
- "snapshot_listener_addr" - Bind Address for accept snapshot transfer connections.
- "snapshot_backup" - If it's turn on all snapshot will be archived.
- "log_dir" - Directory to store RAFT logs and metadata.
- "snapshot_dir" - Directory to store snapshots.
- "max_segment_size" - Maximum size in bytes opened log file.(New file will be opened.)
- "max_log_count" - Snapshot/LogTruncation process will be started after every "max_log_count" applied entries.

## Cient API.

Create new quorum:

```

create(Peers,BackEnd)->{ok,ResultPeers}|{error,term()} when
    Peers::list(zraft_consensus:peer_id()),
    BackEnd::module(),
    ResultPeers::list(zraft_consensus:peer_id()).

```

Write request:

```

write(Raft,Data,Timeout)->{ok,Result,Result,NewRaftConf}|{error,timeout} when
    Raft::light_session()|zraft_consensus:peer_id(),
    Data::term(),
    Timeout::timeout(),
    Result::term(),
    NewRaftConf::light_session()|zraft_consensus:peer_id()

```

Read request:

```

query(Raft,Query,Timeout)->{ok,Result,NewRaftConf}|{error,timeout} when
    Raft::light_session()|zraft_consensus:peer_id(),
    Query::term(),
    Timeout::timeout(),
    Result::term(),
    NewRaftConf::light_session()|zraft_consensus:peer_id().

```

Change Configuration:

```

set_new_conf(Peer,NewPeers,OldPeers,Timeout)->Result when
    Peer::zraft_consensus:peer_id(),
    NewPeers::list(zraft_consensus:peer_id()),
    OldPeers::list(zraft_consensus:peer_id()),
    Timeout::timeout(),
    Result::{ok,list(zraft_consensus:peer_id())}|{error,peers_changed}|{error,leader_changed}|RaftError,
    RaftError::not_stable|newer_exists|process_prev_change.

```

Please look at [zraft_client](http://github.com/dreyk/zraft_lib/blob/master/src/zraft_client.erl) for more details.

Or just generate docs.

```
./rebar doc

```


##Standalone Server.

You can use it for tests from erlang console.

https://github.com/dreyk/zraft


## TODO:
- Write External API documentation.
- Add backend based on ets table
- Add "watcher" support (notify client about backend state changes).


