# zraft_lib

Erlang [raft consensus protocol](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf) implementation .

Supported features:
1. Runtime membership reconfiguration.
2. Log truncation via snapshotting (snapshot copied via kernel sendfile command).
3. Peer asynchronous rpc.
4. Pluggable state machine.


