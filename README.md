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
[![schema](docs/img/schema.png?raw=true)]

## TODO:
- Write External API documentation.
- Add backend based on ets table
- Add "watcher" support (notify client about backend state changes).


