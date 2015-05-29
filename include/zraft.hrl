
-record(vote_request,{from,term,epoch,last_index,last_term}).
-record(vote_reply,{from_peer,epoch,request_term,peer_term,granted,commit}).

-record(append_entries, {
    term =0,
    epoch=0,
    from,
    request_ref,
    prev_log_index=0,
    prev_log_term=0,
    entries :: term(),
    commit_index=0}).
-record(append_reply, {
    epoch,
    request_ref,
    term = 0,
    from_peer,
    last_index=0,
    success=false,
    agree_index=0
    }).

-record(install_snapshot,{from,request_ref,term,epoch,index,data}).
-record(install_snapshot_reply,{epoch,request_ref,term,from_peer,addr,port,result,index}).

-define(UPDATE_CMD,update).
-define(BECOME_LEADER_CMD,become_leader).
-define(LOST_LEADERSHIP_CMD,lost_leadership).
-define(OPTIMISTIC_REPLICATE_CMD,optimistic_replicate).
-define(VOTE_CMD,vote).


-define(OP_CONFIG,1).
-define(OP_DATA,2).
-define(OP_NOOP,3).

-define(BLANK_CONF,blank).
-define(STABLE_CONF,stable).
-define(STAGING_CONF,staging).
-define(TRANSITIONAL_CONF,transactional).

-define(ELECTION_TIMEOUT_PARAM,election_timeout).
-define(ELECTION_TIMEOUT,500).

-record(snapshot_info,{index=0,term=0,conf_index=0,conf=?BLANK_CONF}).

-record(log_op_result,{log_state,last_conf,result}).

-record(enrty,{index,term,type,data}).

-record(pconf,{old_peers=[],new_peers=[]}).

-record(raft_meta,{voted_for,current_term=0,back_end}).

-record(peer,{id,next_index=1,has_vote=false,last_agree_index=0,epoch=0}).

-record(log_descr,{first_index,last_index,last_term,commit_index}).

-record(leader_read_request,{from,request}).