%% -------------------------------------------------------------------
%% @author Gunin Alexander <guninalexander@gmail.com>
%% Copyright (c) 2015 Gunin Alexander.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(zraft_consensus).
-author("dreyk").

-behaviour(gen_fsm).

-include_lib("zraft_lib/include/zraft.hrl").

%% gen_fsm callbacks
-export([init/1,
    load/2,
    load/3,
    follower/2,
    follower/3,
    candidate/2,
    candidate/3,
    leader/2,
    leader/3,
    handle_event/3,
    handle_sync_event/4,
    handle_info/3,
    terminate/3,
    code_change/4]).

-export_type([
    peer_id/0,
    peer_name/0,
    rpc_cmd/0,
    from_peer_addr/0,
    snapshot_request/0,
    append_request/0,
    vote_request/0,
    index/0,
    raft_meta/0,
    raft_term/0,
    install_snapshot_request/0,
    install_snapshot_reply/0,
    snapshot_info/0
]).

-export([
    start_link/2,
    replicate_log/3,
    sync_peer/2,
    maybe_step_down/2,
    initial_bootstrap/1,
    stop/1,
    get_election_timeout/0,
    make_snapshot_info/3,
    need_snapshot/2,
    read_request/3,
    get_conf_request/2,
    query/3,
    get_conf/2,
    write/3,
    truncate_log/2,
    set_new_configuration/4,
    stat/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(INFO(S, As), ?debugFmt("[INFO] " ++ S, As)).
-define(INFO(S), ?debugMsg("[INFO] " ++ S)).
-define(WARNING(S, As), ?debugFmt("[WARNING] " ++ S, As)).
-define(WARNING(S), ?debugMsg("[WARNING] " ++ S)).
-define(ERROR(S, As), ?debugFmt("[ERROR] " ++ S, As)).
-define(ERROR(S), ?debugMsg("[ERROR] " ++ S)).
-define(DEBUG(S, As), ?debugFmt("[DEBUG] " ++ S, As)).
-define(DEBUH(S), ?debugMsg("[DEBUG] " ++ S)).
-else.
-define(INFO(S, As), lager:info(S, As)).
-define(INFO(S), lager:info(S)).
-define(WARNING(S, As), lager:warning(S, As)).
-define(WARNING(S), lager:warning(S)).
-define(ERROR(S, As), lager:error(S, As)).
-define(ERROR(S), lager:error(S)).
-define(ERROR(S, As), lager:debug(S, As)).
-define(ERROR(S), lager:debug(S)).
-endif.

-define(ETIMEOUT, zraft_util:get_env(?ELECTION_TIMEOUT_PARAM, ?ELECTION_TIMEOUT)).

-record(init_state, {log, fsm, id, back_end, snapshot_info, async, bootstrap = false}).
-record(sessions, {read = [], write = [], conf}).
-record(state, {
    log,
    config,
    id,
    timer,
    current_term = 0,
    snapshot_info,
    allow_commit = false,
    epoch = 1,
    leader,
    voted_for,
    back_end,
    peers = [],
    log_state,
    last_hearbeat,
    election_timeout,
    state_fsm,
    sessions = #sessions{}}).

-record(config, {id, state, conf, old_peers, new_peers}).
-record(read_request, {function, args, start_time, timeout}).
-record(conf_change_requet, {prev_id, start_time, timeout, from, peers, index}).
-record(write_request, {data, start_time, timeout, from}).
-type raft_term() :: non_neg_integer().
-type index() :: non_neg_integer().
-type peer_name() :: atom().
-type peer_id() :: {peer_name(), node()}.
-type append_request() :: #append_entries{}.
-type vote_request() :: #vote_request{}.
-type snapshot_request() :: term().
-type rpc_cmd() :: append_request()|vote_request()|snapshot_request().
-type from_peer_addr() :: {peer_id(), pid()}.
-type raft_meta() :: #raft_meta{}.
-type install_snapshot_request() :: #install_snapshot{}.
-type install_snapshot_reply() :: #install_snapshot_reply{}.
-type snapshot_info() :: #snapshot_info{}.


%%%===================================================================
%%% Client API
%%%===================================================================

%% @doc Query data form user backend.
-spec query(peer_id(), term(), timeout()) -> {ok, term()}|retry|{error, not_leader}|{error, term()}.
query(PeerID, Query, Timeout) ->
    send_leader_request(PeerID, read_request, [Query], Timeout).

%% @doc Read last stable quorum configuration
-spec get_conf(peer_id(), timeout()) -> {ok, term()}|retry|{error, not_leader}|{error, term()}.
get_conf(PeerID, Timeout) ->
    send_leader_request(PeerID, get_conf_request, [], Timeout).

%% @doc Write data to user backend
-spec write(peer_id(), term(), timeout()) -> ok|{leader, peer_id()}|{error, term()}.
write(PeerID, Data, Timeout) ->
    Now = os:timestamp(),
    Req = #write_request{timeout = zraft_util:miscrosec_timeout(Timeout), data = Data, start_time = Now},
    gen_fsm:sync_send_all_state_event(PeerID, Req, Timeout).

%% @doc Write data to user backend
-spec set_new_configuration(peer_id(), index(), list(peer_id()), timeout()) -> ok|{leader, peer_id()}|{error, term()}.
set_new_configuration(PeerID, PrevID, Peers, Timeout) ->
    Now = os:timestamp(),
    Req = #conf_change_requet{
        prev_id = PrevID,
        timeout = zraft_util:miscrosec_timeout(Timeout),
        peers = Peers,
        start_time = Now},
    gen_fsm:sync_send_all_state_event(PeerID, Req, Timeout).

stat(Peer) ->
    gen_fsm:sync_send_all_state_event(Peer, stat).
%%%===================================================================
%%% Internal Server API
%%%===================================================================
-spec get_election_timeout() -> timeout().
get_election_timeout() ->
    ?ETIMEOUT.

-spec start_link(peer_id(), module()) -> {ok, pid()} | {error, {already_started, pid()}} | {error, term()}.
start_link({Name, _} = PeerID, BackEnd) ->
    gen_fsm:start_link({local, Name}, ?MODULE, [PeerID, BackEnd], []).

-spec stop(peer_id()) -> ok.
stop(Peer) ->
    gen_fsm:sync_send_all_state_event(Peer, stop).

-spec replicate_log(Raft, ToPeer, AppendReq) -> ok when
    Raft :: from_peer_addr()|pid(),
    ToPeer :: peer_id(),
    AppendReq :: #append_entries{}.
replicate_log(P, ToPeer, AppendReq) ->
    send_event(P, {replicate_log, ToPeer, AppendReq}).

%% @doc Generate initial peer state.
-spec initial_bootstrap(peer_id()) -> ok.
initial_bootstrap(P) ->
    gen_fsm:sync_send_event(P, bootstrap).


-spec sync_peer(from_peer_addr(), true|false) -> ok.
sync_peer(P, MayBeCommit) ->
    send_all_state_event(P, {sync_peer, MayBeCommit}).

-spec maybe_step_down(from_peer_addr(), raft_term()) -> ok.
maybe_step_down(P, Term) ->
    send_event(P, {maybe_step_down, Term}).

-spec need_snapshot(peer_id(), install_snapshot_request()) -> ok.
need_snapshot(Peer, NewReq) ->
    zraft_peer_route:cmd(Peer, NewReq).

-spec make_snapshot_info(from_peer_addr(), pid(), index()) -> ok.
make_snapshot_info(Peer, From, Index) ->
    send_event(Peer, {make_snapshot_info, From, Index}).

-spec truncate_log(from_peer_addr(), #snapshot_info{}) -> ok.
truncate_log(Raft, SnapshotInfo) ->
    send_all_state_event(Raft, SnapshotInfo).

-spec send_leader_request(peer_id(), atom(), list(), timeout()) -> {ok, term()}|retry|{error, not_leader}|{error, term()}.
send_leader_request(PeerID, Function, Args, Timeout) ->
    Now = os:timestamp(),
    Req = #read_request{timeout = zraft_util:miscrosec_timeout(Timeout), function = Function, args = Args, start_time = Now},
    gen_fsm:sync_send_all_state_event(PeerID, Req, Timeout).

%%%===================================================================
%%% Peer lifecycle
%%%===================================================================
init([PeerID, BackEnd]) ->
    {ok, FSM} = zraft_fsm:start_link(peer(PeerID), BackEnd),
    {ok, Log} = zraft_fs_log:start_link(PeerID),
    {ok, load, #init_state{fsm = FSM, log = Log, back_end = BackEnd, id = PeerID}}.

init_state(InitState) ->
    #init_state{
        id = PeerID,
        fsm = FSM,
        log = Log,
        back_end = BackEnd,
        snapshot_info = Info
    } = InitState,
    ?INFO("~p: Init state",[PeerID]),
    Meta = zraft_fs_log:get_raft_meta(Log),
    case maybe_set_back_end(BackEnd, Meta, Log) of
        {error, Error} ->
            {stop, {error, Error}, InitState};
        {ok, Meta1} ->
            #raft_meta{current_term = CurrentTerm, back_end = BackEnd, voted_for = VotedFor} = Meta1,
            ConfDescr = zraft_fs_log:get_last_conf(Log),
            LogDescr = zraft_fs_log:get_log_descr(Log),
            State1 = #state{log = Log,
                current_term = CurrentTerm,
                voted_for = VotedFor,
                back_end = BackEnd,
                log_state = LogDescr,
                id = PeerID,
                state_fsm = FSM,
                snapshot_info = Info,
                election_timeout = ?ETIMEOUT
            },
            State2 = set_config(follower, ConfDescr, State1),
            State3 = start_timer(State2),
            case InitState#init_state.bootstrap of
                false ->
                    {next_state, follower, State3};
                From ->
                    bootstrap(From, State3)
            end
    end.

maybe_set_back_end(NewBackEnd, Meta = #raft_meta{back_end = undefined}, Log) ->
    Meta1 = Meta#raft_meta{back_end = NewBackEnd},
    zraft_fs_log:update_raft_meta(Log, Meta1),
    {ok, Meta1};
maybe_set_back_end(NewBackEnd, Meta = #raft_meta{back_end = OldBackEnd}, _Log) when NewBackEnd == OldBackEnd ->
    {ok, Meta};
maybe_set_back_end(_NewBackEnd, _Meta, _Log) ->
    {error, backend_already_exists}.

%%%===================================================================
%%% Load state
%%%===================================================================
load(_, State) ->
    {next_state, load, State}.

load(bootstrap, From, State) ->
    case State#init_state.bootstrap of
        false ->
            {next_state, load, State#init_state{bootstrap = From}};
        _ ->
            {reply, ok, load, State}
    end;
load(_, _, State) ->
    {next_state, load, State}.
%%%===================================================================
%%% Follower state
%%%===================================================================
follower(timeout, State = #state{config = ?BLANK_CONF}) ->%% don't set new timer
    {next_state, follower, State};
follower(timeout, State) ->%%start new election
    start_election(State);
follower({make_snapshot_info, From, Index}, State) ->
    make_snapshot_info(follower, From, Index, State);
follower(Req = #install_snapshot{data = Type}, State) when Type /= prepare ->
    handle_install_snapshot(follower, Req, State);
follower(Req = #append_entries{}, State) ->
    handle_append_entries(follower, Req, State);
follower(Req = #vote_request{}, State) ->
    handle_vote_reuqest(follower, Req, State);
follower(Req, State = #state{}) ->
    drop_request(follower, Req, State).

follower(bootstrap, From, State) ->
    bootstrap(From, State);
follower(_Event, _From, State) ->
    {reply, {error, follower_not_supported}, follower, State}.

bootstrap(From, State = #state{config = ?BLANK_CONF, id = PeerID}) ->
    case check_blank_state(State) of
        true ->
            gen_fsm:reply(From, ok),
            NewTerm = 1,
            Entry = #enrty{index = 1, type = ?OP_CONFIG, term = NewTerm, data = #pconf{old_peers = [PeerID]}},
            State1 = append([Entry], State),
            step_down(blank, NewTerm, State1);
        _ ->
            gen_fsm:reply(From, {error, invalid_blank_state}),
            {stop, invalid_blank_state, State}
    end;
bootstrap(From, State) ->
    gen_fsm:reply(From, {error, invalid_blank_state}),
    {next_state, follower, State}.

%%%===================================================================
%%% Candidate State
%%%===================================================================
candidate(timeout, State = #state{current_term = Term}) ->
    ?INFO("~p: Candidate start new election. Prev term ~p timed out", [State#state.id, Term]),
    start_election(State);
candidate(#vote_reply{request_term = Term1}, State = #state{current_term = Term2}) when Term1 /= Term2 ->
    %%ignore result. May be expired
    {next_state, candidate, State};
candidate(R = #vote_reply{peer_term = Term1, commit = CommitIndex},
    State = #state{current_term = Term2}) when Term1 > Term2 ->
    ?INFO("~p: Received vote response from ~p. PeerTerm:~p > SelfTerm:~p",
        [State#state.id, R#vote_reply.from_peer, Term1, Term2]),
    case maybe_shutdown(CommitIndex, State) of
        true ->
            {stop, nornal, State};
        _ ->
            step_down(candidate, Term1, State)
    end;
candidate(R = #vote_reply{from_peer = PeerID, granted = Granted, epoch = Epoch}, State) ->
    ?INFO("~p: Vote ~p response from ~p.", [State#state.id, Granted, PeerID]),
    update_peer(PeerID, fun(P) -> P#peer{has_vote = Granted, epoch = Epoch} end, State),
    if
        Granted ->
            maybe_become_leader(candidate, State);
        true ->
            case maybe_shutdown(R#vote_reply.commit, State) of
                true ->
                    {stop, normal, State};
                _ ->
                    {next_state, candidate, State}
            end
    end;
candidate(Req = #install_snapshot{data = Type}, State) when Type /= prepare ->
    handle_install_snapshot(candidate, Req, State);
candidate(Req = #append_entries{}, State) ->
    handle_append_entries(candidate, Req, State);
candidate(Req = #vote_request{}, State) ->
    handle_vote_reuqest(candidate, Req, State);
candidate({make_snapshot_info, From, Index}, State) ->
    make_snapshot_info(candidate, From, Index, State);
candidate(Req, State = #state{}) ->
    drop_request(candidate, Req, State).

candidate(_Event, _From, State) ->
    {reply, {error, candidate_not_supported}, candidate, State}.

%%%===================================================================
%%% Leader State
%%%===================================================================
leader(Req = #install_snapshot{data = prepare},
    State = #state{current_term = Term, epoch = Epoch, state_fsm = FSM}) ->
    Req1 = Req#install_snapshot{term = Term, epoch = Epoch},
    zraft_fsm:cmd(FSM, Req1),
    {next_state, leader, State};
leader(Req = #install_snapshot{}, State) ->
    handle_install_snapshot(leader, Req, State);
leader(Req = #append_entries{}, State) ->
    handle_append_entries(leader, Req, State);
leader(Req = #vote_request{}, State) ->
    reject_vote(leader, Req, State);%%always reject. Else staled peer may distrut quorum.
leader({make_snapshot_info, From, Index}, State) ->
    make_snapshot_info(leader, From, Index, State);
leader({maybe_step_down, Term}, State) ->
    %%Peer proxy process has received newer term in response
    if
        Term > State#state.current_term ->
            step_down(leader, Term, State);
        true ->
            {next_state, leader, State}
    end;
leader({replicate_log, ToPeer, AppendReq}, State = #state{log = Log, current_term = Term, epoch = Epoch}) ->
    zraft_fs_log:replicate_log(Log, ToPeer, AppendReq#append_entries{term = Term, epoch = Epoch}),
    {next_state, leader, State};
leader(Req, State = #state{}) ->
    drop_request(leader, Req, State).

%%Only for test
leader({append_test, Es}, _From, State = #state{log_state = #log_descr{last_index = Index}}) ->
    {A, _} = lists:foldr(fun(E, {Acc, I}) ->
        {[E#enrty{index = I} | Acc], I + 1} end, {[], Index + 1}, Es),
    State1 = append(A, State),
    {reply, ok, leader, State1};
leader(_Event, _From, State) ->
    {reply, {error, leader_not_supported}, leader, State}.

%%drop all unknown requests
drop_request(StateName, Req, State = #state{id = PeerID}) ->
    lager:warning("~p: In state ~s drop reuqest ~p", [PeerID, Req]),
    {next_state, StateName, State}.

%%%===================================================================
%%% FSM genaral
%%%===================================================================
handle_event(Info = #snapshot_info{}, load, InitialState = #init_state{log = Log}) ->
    Async = zraft_fs_log:truncate_before(Log, Info),
    {next_state, load, InitialState#init_state{snapshot_info = Info, async = Async}};
handle_event(_, load, InitialState) ->
    %%drop all in load state
    {next_state, load, InitialState};
handle_event(Info = #snapshot_info{}, StateName, State = #state{log = Log}) ->
    Async = zraft_fs_log:truncate_before(Log, Info),
    case zraft_fs_log:sync_fs(Async) of
        #log_op_result{result = ok, last_conf = NewConf, log_state = LogState} ->
            State1 = State#state{log_state = LogState, snapshot_info = Info},
            State2 = set_config(StateName, NewConf, State1),
            {next_state, StateName, State2};
        #log_op_result{result = Error} ->
            ?ERROR("~p: Fail truncate log ~p", [State#state.id, Error]),
            {stop, Error, State};
        Error ->
            ?ERROR("~p: Fail truncate log ~p", [State#state.id, Error]),
            {stop, {error, snapshot_failed}, State}
    end;
handle_event({sync_peer, true}, leader, State) ->
    %%log has replicated by peer proxy
    maybe_commit_quorum(State);
handle_event({sync_peer, _}, leader, State) ->
    %%Peer proxy process has received failed response
    State1 = apply_read_requests(State),
    {next_state, leader, State1};
handle_event({sync_peer, _MayBeCommit}, StateName, State) ->
    %%We already lost leadership
    {next_state, StateName, State};
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(stat, _From, load, State) ->
    S1 = #peer_start{
        state_name = load,
        agree_index = 0,
        allow_commit = false,
        conf = ?BLANK_CONF,
        conf_state = ?STABLE_CONF,
        epoch = 0,
        proxy_peer_stats = [],
        snapshot_info = #snapshot_info{},
        log_state = #log_descr{}
    },
    {reply,S1,load,State};
handle_sync_event(stat, _From, StateName, State) ->
    #state{
        epoch = E,
        allow_commit = AC,
        back_end = BackEnd,
        config = Config,
        current_term = T,
        leader = Leader
    } = State,
    case Config of
        ?BLANK_CONF ->
            Conf = ?BLANK_CONF,
            ConfState = ?STABLE_CONF;
        _ ->
            #config{conf = Conf, state = ConfState} = Config
    end,
    AgreeIndex = quorumMin(State, #peer.last_agree_index),
    Stat = #peer_start{
        agree_index = AgreeIndex,
        epoch = E,
        term = T,
        allow_commit = AC,
        leader = Leader,
        back_end = BackEnd,
        conf = Conf,
        conf_state = ConfState,
        state_name = StateName,
        log_state = State#state.log_state,
        snapshot_info = State#state.snapshot_info,
        proxy_peer_stats = proxy_stats(State)
    },
    {reply, Stat, StateName, State};
handle_sync_event(stop, _From, _StateName, State) ->
    ok = reset_subprocess(State),
    {stop, normal, ok, ok};
handle_sync_event(_, _From, load, InitialState) ->
    %%drop all in load state
    {reply, {error, loading}, load, InitialState};
handle_sync_event(Req = #read_request{args = Args}, From, leader,
    State = #state{sessions = Sessions, epoch = Epoch}) ->
    #sessions{read = Requests} = Sessions,
    %%First we must ensure that we are leader.
    %%Change epoch and send hearbeat
    %%Reqeust will be processed than quorum will agree that we are leader
    Epoch1 = Epoch + 1,
    Req1 = Req#read_request{args = [From | Args]},
    Requests1 = [{Epoch1, Req1} | Requests],
    State1 = State#state{epoch = Epoch1, sessions = Sessions#sessions{read = Requests1}},
    ok = update_peer_last_index(State1),
    replicate_peer_request(?OPTIMISTIC_REPLICATE_CMD, State1, []),
    gen_fsm:send_all_state_event(self(), {sync_peer, false}),
    {next_state, leader, State1};
handle_sync_event(#read_request{}, _From, StateName, State) ->
    %%Lost lidership
    %%Hint new leader in respose
    {reply, {leader, State#state.leader}, StateName, State};
handle_sync_event(Req = #write_request{}, From, leader,
    State = #state{sessions = Sessions, log_state = LogState, current_term = Term}) ->
    #sessions{write = Requests} = Sessions,
    %%Try replicate new entry.
    %%Response will be sended after entry will be ready to commit
    #log_descr{last_index = I} = LogState,
    NextIndex = I + 1,
    Entry = #enrty{term = Term, index = NextIndex, type = ?OP_DATA, data = Req#write_request.data},
    State1 = #state{log_state = LogState1} = append([Entry], State),
    if
        LogState1#log_descr.commit_index >= NextIndex ->
            %%TODO: is't possible?
            {reply, ok, leader, State1};
        true ->
            Req1 = Req#write_request{data = <<>>, from = From},
            Requests1 = [{NextIndex, Req1} | Requests],
            State2 = State1#state{sessions = Sessions#sessions{write = Requests1}},
            {next_state, leader, State2}
    end;
handle_sync_event(#write_request{}, _From, StateName, State) ->
    %%Lost lidership
    %%Hint new leader in respose
    {reply, {leader, State#state.leader}, StateName, State};
handle_sync_event(Req = #conf_change_requet{}, From, leader, State) ->
    change_configuration(Req#conf_change_requet{from = From}, State);
handle_sync_event(#conf_change_requet{}, _From, StateName, State) ->
    %%Lost lidership
    %%Hint new leader in respose
    {reply, {leader, State#state.leader}, StateName, State};
%%%===================================================================
%%% JUST FOR TESTS
%%%===================================================================
handle_sync_event(force_timeout, From, StateName, State) ->
    if
        State#state.timer == undefined ->
            {reply, false, StateName, State};
        true ->
            gen_fsm:cancel_timer(State#state.timer),
            gen_fsm:reply(From, true),
            ?MODULE:StateName(timeout, State#state{timer = undefined})
    end;

%% drop unknown
handle_sync_event(_Event, _From, StateName, State) ->
    Error = list_to_atom(atom_to_list(StateName) ++ "_not_supported"),
    {reply, {error, Error}, StateName, State}.

handle_info({Async, Res}, load, InitState = #init_state{async = Async}) ->
    case Res of
        #log_op_result{result = ok} ->
            init_state(InitState#init_state{async = undefined});
        #log_op_result{result = Error} ->
            ?ERROR("~p: Fail truncate log ~p", [InitState#init_state.id, Error]),
            {stop, Error, InitState};
        Error ->
            ?ERROR("~p: Fail truncate log ~p", [InitState#init_state.id, Error]),
            {stop, Error, InitState}
    end;
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _, ok) ->
    ok;
terminate(_Reason, load, #init_state{log = Log, fsm = FSM}) ->
    ok = zraft_fsm:stop(FSM),
    ok = zraft_fs_log:stop(Log),
    ok;
terminate(_Reason, _StateName, State) ->
    %%stop all subprocess
    reset_subprocess(State).

reset_subprocess(State = #state{log = Log, state_fsm = FSM}) ->
    stop_all_peer(State),
    ok = zraft_fsm:stop(FSM),
    ok = zraft_fs_log:stop(Log),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%Read info for snapshot
make_snapshot_info(StateName, From, Index, State = #state{log = Log}) ->
    zraft_fs_log:make_snapshot_info(Log, From, Index),
    {next_state, StateName, State}.

%%Try commit
maybe_commit_quorum(State) ->
    #state{log_state = LogDescr, log = Log, current_term = CurrentTerm, epoch = Epoch} = State,
    AgreeIndex = quorumMin(State, #peer.last_agree_index),
    if
        AgreeIndex =< LogDescr#log_descr.commit_index ->
            %%may be need send reply for read requests.
            State1 = apply_read_requests(State),
            %%already commited
            {next_state, leader, State1};
        AgreeIndex < LogDescr#log_descr.first_index ->
            {stop, {error, commit_collision}, State};
        true ->
            case commit_allowed(AgreeIndex, State) of
                {false, State1} ->
                    %%Dont' commit other(old) term
                    {next_state, leader, State1};
                {true, State1} when AgreeIndex > LogDescr#log_descr.last_index ->
                    %%We have not data entries for commit
                    {stop, {error, commit_collision}, State1};
                {true, State1} ->
                    %%Update log
                    #log_op_result{result = ToCommit, log_state = LogDescr1} = zraft_fs_log:update_commit_index(Log, AgreeIndex),
                    #log_descr{last_index = PrevIndex, last_term = PrevTerm} = LogDescr1,
                    %%Apply commited entry to user state
                    zraft_fsm:apply_commit(State#state.state_fsm, ToCommit),
                    %%may be need send reply for read requests.
                    State2 = apply_read_requests(State1#state{log_state = LogDescr1}),
                    %%may be send reply to write requests
                    State3 = accept_write_requests(State2),
                    State4 = accept_conf_request(State3),
                    %%Commit followers
                    OptimisticReplication =
                        zraft_log_util:append_request(Epoch, CurrentTerm, AgreeIndex, PrevIndex, PrevTerm, []),
                    to_all_follower_peer({?OPTIMISTIC_REPLICATE_CMD, OptimisticReplication}, State4),
                    %%Check if new config has been commited
                    maybe_change_config(State4)
            end
    end.
commit_allowed(_Index, State = #state{allow_commit = true}) ->
    {true, State};
commit_allowed(Index, State = #state{log = Log, current_term = CurrentTerm}) ->
    case zraft_fs_log:get_term(Log, Index) of
        T when CurrentTerm /= T ->
            %%Dont' commit other(old) term
            {false, State};
        _ ->
            %%allow_commit will be set to false then we lost lidership
            {true, State#state{allow_commit = true}}
    end.

maybe_change_config(State = #state{config = Config, log_state = LogState}) ->
    #config{id = ConfID} = Config,
    #log_descr{commit_index = Commit} = LogState,
    if
        Commit >= ConfID ->%%configuration has been commited
            case hasVote(State) of
                true when Config#config.state == ?TRANSITIONAL_CONF ->
                    %%apply new configuration
                    NewConf = #pconf{new_peers = [], old_peers = Config#config.new_peers},
                    Entry = #enrty{
                        index = LogState#log_descr.last_index + 1,
                        type = ?OP_CONFIG,
                        term = State#state.current_term,
                        data = NewConf},
                    State1 = append([Entry], State),
                    {next_state, leader, State1};
                true ->
                    %%Configuration is updatodate now
                    {next_state, leader, State};
                _ ->
                    %%We are note in new configuration
                    step_down(leader, State#state.current_term + 1, State)
            end;
        true ->
            {next_state, leader, State}
    end.

replicate_peer_request(Type, State, Entries) ->
    #state{current_term = Term, log_state = LogDescr, epoch = Epoch} = State,
    #log_descr{last_index = LastIndex, last_term = LastTerm, commit_index = Commit} = LogDescr,
    HearBeat = zraft_log_util:append_request(Epoch, Term, Commit, LastIndex, LastTerm, Entries),
    to_all_follower_peer({Type, HearBeat}, State),
    State.

%%StateName==candidate or follower(leader are rejecting all vote request)
handle_vote_reuqest(StateName, Req, State) ->
    case is_time_to_elect(State) of
        true ->
            #vote_request{from = From, last_term = ReqLastTerm, last_index = ReqLastIndex, term = NewTerm} = Req,
            {FromPeer, _} = From,
            #state{log_state = LogState, current_term = CurrentTerm, voted_for = OldVote} = State,
            #log_descr{last_index = LastIndex, last_term = LastTerm} = LogState,
            IsLogUpToDate =
                ReqLastTerm > LastTerm orelse
                    (ReqLastTerm == LastTerm andalso ReqLastIndex >= LastIndex),
            MustVotFor = if
                             IsLogUpToDate ->
                                 FromPeer;
                             true ->
                                 undefined
                         end,
            if
                CurrentTerm < NewTerm ->
                    reject_vote(StateName, Req, State);
                CurrentTerm == NewTerm andalso OldVote =/= undefined ->
                    %%We have voted for this term or we are candidate for it(have self vote).
                    if
                        OldVote == MustVotFor ->
                            %%TODO: Is't posible?
                            accept_vote(Req, State);
                        true ->
                            reject_vote(StateName, Req, State)
                    end;
                true ->
                    %%if we candidate then NewTerm>CurrentTerm(else see clause before)
                    %%if we follower  then NewTerm>CurrentTerm or we have't voted for this term yeat.
                    State1 = step_down(undefined, MustVotFor, StateName, NewTerm, State),
                    if
                        IsLogUpToDate ->
                            %%accept vote
                            accept_vote(Req, State1);
                        NewTerm > CurrentTerm ->
                            %%Voter has out of date log and newer term. Step to follower
                            reject_vote(follower, Req, State1);
                        true ->%%NewTerm==CurrentTerm and voter has out of date log
                            reject_vote(StateName, Req, State1)
                    end
            end;
        _ ->
            ?WARNING("Rejecting RequestVote from ~p, since we recently heard from a "
            "leader ~p. Should server ~p be shut down?",
                [Req#vote_request.from, State#state.leader, Req#vote_request.from]),
            reject_vote(StateName, Req, State),
            {next_state, StateName, State}
    end.

reject_vote(NextStateName, Req, State) ->
    #vote_request{from = From, term = Term, epoch = Epoch} = Req,
    #state{id = PeerID, log_state = LogState, current_term = CurrentTerm} = State,
    #log_descr{commit_index = Commit} = LogState,
    Reply = #vote_reply{
        commit = Commit,
        from_peer = PeerID,
        granted = false,
        peer_term = CurrentTerm,
        request_term = Term,
        epoch = Epoch
    },
    zraft_peer_route:reply_consensus(From, Reply),
    {next_state, NextStateName, State}.
accept_vote(Req, State) ->
    State1 = start_timer(State),%%restart election timer
    #vote_request{from = From, term = Term, epoch = Epoch} = Req,
    #state{id = PeerID, log_state = LogState, current_term = CurrentTerm} = State,
    #log_descr{commit_index = Commit} = LogState,
    Reply = #vote_reply{
        commit = Commit,
        from_peer = PeerID,
        granted = true,
        peer_term = CurrentTerm,
        request_term = Term,
        epoch = Epoch
    },
    zraft_peer_route:reply_consensus(From, Reply),
    {next_state, follower, State1}.

is_time_to_elect(#state{last_hearbeat = max}) ->
    false;
is_time_to_elect(#state{last_hearbeat = undefined}) ->
    true;
is_time_to_elect(#state{last_hearbeat = LastHearbeat, election_timeout = Timeout}) ->
    timer:now_diff(os:timestamp(), LastHearbeat) >= Timeout.

handle_install_snapshot(StateName, Req = #install_snapshot{term = T1, from = From},
    State = #state{current_term = T2, id = PeerID}) when T1 < T2 ->
    ?WARNING("~p: Caller ~p is out of date.", [PeerID, From]),
    reject_install_snapshot(Req, State),
    {next_state, StateName, State};
handle_install_snapshot(StateName, Req = #install_snapshot{term = T1, from = From},
    State = #state{current_term = T2, id = PeerID, leader = Leader}) when T1 == T2 ->
    {NewLeader, _} = From,
    if
        (Leader /= undefined andalso NewLeader /= Leader) orelse StateName == leader ->
            ?ERROR("~p: Received install snapshot from unknown leader ~p current leader is ~p",
                [PeerID, NewLeader, Leader]),
            {stop, {error, invalid_new_leader}, State};
        NewLeader == Leader ->
            install_snapshot(Req, State);
        true ->
            State1 = State#state{leader = NewLeader},%%only leader change
            install_snapshot(Req, State1)
    end;
handle_install_snapshot(StateName, Req = #install_snapshot{term = T1, from = From}, State) -> %%T1>T2->
    {NewLeader, _} = From,
    %%reset our vote, set new leader
    State1 = step_down(NewLeader, undefined, StateName, T1, State),
    install_snapshot(Req, State1).

reject_install_snapshot(Req, State) ->
    #install_snapshot{from = From, request_ref = Ref, epoch = Epoch, index = I} = Req,
    #state{id = PeerID, current_term = CurrentTerm} = State,
    Reply = #install_snapshot_reply{
        epoch = Epoch,
        result = failed,
        from_peer = PeerID,
        request_ref = Ref,
        term = CurrentTerm,
        index = I
    },
    zraft_peer_route:reply_proxy(From, Reply).

install_snapshot(Req, State) ->
    State1 = start_timer(State),
    CurrentTime = os:timestamp(),
    State2 = State1#state{last_hearbeat = CurrentTime},
    zraft_fsm:cmd(State2#state.state_fsm, Req),
    {next_state, follower, State2}.

handle_append_entries(StateName, Req = #append_entries{term = T1, from = From},
    State = #state{current_term = T2, id = PeerID}) when T1 < T2 ->
    ?WARNING("~p: Caller ~p is out of date.", [PeerID, From]),
    reject_append(Req, State),
    {next_state, StateName, State};
handle_append_entries(StateName, Req = #append_entries{term = T1, from = From},
    State = #state{current_term = T2, id = PeerID, leader = Leader}) when T1 == T2 ->
    {NewLeader, _} = From,
    if
        (Leader /= undefined andalso NewLeader /= Leader) orelse StateName == leader ->
            ?ERROR("~p: Received apped entry from unknown leader ~p current leader is ~p",
                [PeerID, NewLeader, Leader]),
            {stop, {error, invalid_new_leader}, State};
        NewLeader == Leader ->
            append_entries(Req, State);
        true ->
            State1 = State#state{leader = NewLeader},%%only leader change
            append_entries(Req, State1)
    end;
handle_append_entries(StateName, Req = #append_entries{term = T1, from = From},
    State) -> %%T1>T2->
    {NewLeader, _} = From,
    %%reset our vote, set new leade
    State1 = step_down(NewLeader, undefined, StateName, T1, State),
    append_entries(Req, State1).

reject_append(Req, State) ->
    #append_entries{from = From, request_ref = Ref, epoch = Epoch} = Req,
    #state{id = PeerID, log_state = LogState, current_term = CurrentTerm} = State,
    Reply = #append_reply{
        epoch = Epoch,
        success = false,
        from_peer = PeerID,
        request_ref = Ref,
        last_index = LogState#log_descr.last_index,
        term = CurrentTerm
    },
    zraft_peer_route:reply_proxy(From, Reply).

append_entries(Req, State = #state{log = Log, current_term = Term, id = PeerID}) ->
    State1 = start_timer(State),
    CurrentTime = os:timestamp(),
    #append_entries{
        epoch = Epoch,
        from = From,
        prev_log_index = PrevIndex,
        prev_log_term = PrevTerm,
        commit_index = Commit,
        entries = Entries,
        request_ref = Ref
    } = Req,
    Async = zraft_fs_log:append(Log, PrevIndex, PrevTerm, Commit, Entries),
    #log_op_result{result = Result, log_state = LogState, last_conf = NewConf} = zraft_fs_log:sync_fs(Async),
    Reply = #append_reply{
        agree_index = PrevIndex + length(Entries),
        last_index = LogState#log_descr.last_index,
        term = Term,
        from_peer = PeerID,
        request_ref = Ref,
        epoch = Epoch
    },
    State2 = State1#state{log_state = LogState, last_hearbeat = CurrentTime},
    case Result of
        {true, _LastIndex, ToCommit} ->
            zraft_fsm:apply_commit(State2#state.state_fsm, ToCommit),
            State3 = set_config(follower, NewConf, State2),
            Reply1 = Reply#append_reply{success = true};
        {false, _} ->
            State3 = State2,
            Reply1 = Reply
    end,
    zraft_peer_route:reply_proxy(From, Reply1),
    {next_state, follower, State3}.

start_election(State =
    #state{leader = Leader, epoch = Epoch, current_term = Term, id = PeerID, log = Log, back_end = BackEnd, log_state = LogState}) ->
    NextTerm = Term + 1,
    Leader /= undefined andalso
        ?INFO("Start for election in term ~p old leader was ~p", [NextTerm, Leader]),
    ok = zraft_fs_log:update_raft_meta(
        Log,
        #raft_meta{voted_for = PeerID, back_end = BackEnd, current_term = NextTerm}
    ),
    #log_descr{last_index = LastIndex, last_term = LastTerm} = LogState,
    VoteRequest = #vote_request{epoch = Epoch, term = NextTerm, from = peer(PeerID), last_index = LastIndex, last_term = LastTerm},
    update_all_peer(
        fun
            (P = #peer{id = ProxyPeer}) when ProxyPeer == PeerID ->
                P#peer{has_vote = true, epoch = Epoch};
            (P) ->
                P#peer{has_vote = false}
        end, State),

    to_all_peer_direct(VoteRequest, State),
    State1 = start_timer(State),
    maybe_become_leader(
        candidate,
        State1#state{current_term = NextTerm, leader = undefined, voted_for = PeerID}
    ).

%%Check quorum vote
maybe_become_leader(FallBackStateName, State) ->
    case quorumAll(State, #peer.has_vote) of
        true ->
            #state{id = MyID, current_term = Term, log_state = LogDescr} = State,
            #log_descr{last_index = LastIndex} = LogDescr,
            State1 = State#state{leader = MyID, voted_for = MyID, last_hearbeat = max},
            %%reset election timer
            State2 = cancel_timer(State1),
            %%force hearbeat from new leader. We don't count new commit index.
            replicate_peer_request(?BECOME_LEADER_CMD, State2, []),
            %%Add noop entry,it's needed for commit progress
            Noop = #enrty{type = ?OP_NOOP, data = <<>>, term = Term, index = LastIndex + 1},
            State3 = append([Noop], State2),
            {next_state, leader, State3};
        _ ->
            {next_state, FallBackStateName, State}
    end.

%%lost lidership. Step down to follower state
step_down(PrevState, NewTerm, State) ->
    State1 = step_down(undefined, undefined, PrevState, NewTerm, State),
    if
        PrevState == leader orelse PrevState == blank ->
            State2 = start_timer(State1);
        true ->
            State2 = State1
    end,
    {next_state, follower, State2}.

step_down(NewLeader, SetVoteTo, PrevState, NewTerm,
    State = #state{current_term = OldTerm}) when NewTerm > OldTerm ->
    lost_leadership(PrevState, State),
    State1 = State#state{allow_commit = false, current_term = NewTerm, leader = NewLeader, last_hearbeat = undefined},
    State2 = meybe_reset_last_hearbeat(PrevState, State1),
    State3 = reset_requests(State2),
    %%reset our vote
    update_vote(SetVoteTo, State3);
step_down(NewLeader, SetVoteTo, PrevState, _NewTerm, State) ->
    lost_leadership(PrevState, State),
    State1 = maybe_update_vote(SetVoteTo, State),
    State2 = meybe_reset_last_hearbeat(PrevState, State1),
    State3 = reset_requests(State2),
    State3#state{leader = NewLeader, allow_commit = false}.

meybe_reset_last_hearbeat(leader, State) ->
    State#state{last_hearbeat = undefined};
meybe_reset_last_hearbeat(_, State) ->
    State.
maybe_update_vote(undefined, State) ->
    State;
maybe_update_vote(VoteFor, State) ->
    update_vote(VoteFor, State).


%%update our vote
update_vote(VoteFor, State) ->
    #state{back_end = BackEnd, log = Log, current_term = Term} = State,
    State1 = State#state{voted_for = VoteFor},
    ok = zraft_fs_log:update_raft_meta(
        Log,
        #raft_meta{voted_for = VoteFor, back_end = BackEnd, current_term = Term}
    ),
    State1.



maybe_shutdown(CommitIndex, #state{id = PeerID, config = Config}) ->
    #config{id = ID, state = ConfState, old_peers = OldPeers} = Config,
    if
        CommitIndex >= ID andalso ConfState == ?STABLE_CONF ->
            %% if current configuration was commited and we are not member of it. We can stop.
            not ordsets:is_element(PeerID, OldPeers);
        true ->
            false
    end.

change_configuration(Req = #conf_change_requet{peers = Peers}, State) ->
    #state{sessions = Sessions, config = Config, current_term = Term, log_state = LogState} = State,
    if
        Config#config.state /= ?STABLE_CONF ->
            {reply, not_stable, leader, State};
        Config#config.id /= Req#conf_change_requet.prev_id ->
            {reply, newer_exists, leader, State};
        Sessions#sessions.conf /= undefined ->
            {reply, process_prev_change, leader, State};
        true ->
            #log_descr{last_index = Index} = LogState,
            NextIndex = Index + 1,
            NewConfEntry = #enrty{
                term = Term,
                index = NextIndex,
                type = ?OP_CONFIG,
                data = #pconf{old_peers = Config#config.old_peers, new_peers = ordsets:from_list(Peers)}
            },
            State1 = State#state{sessions = Sessions#sessions{conf = Req#conf_change_requet{index = NextIndex}}},
            State2 = append([NewConfEntry], State1),
            {next_state, leader, State2}
    end.

set_config(_StateName, ?BLANK_CONF, State) ->
    State#state{config = ?BLANK_CONF};
set_config(_StateName, PConf, State = #state{config = #config{conf = PConf}}) ->
    State;
set_config(StateName, {ConfID, #pconf{old_peers = Old, new_peers = New}} = PConf,
    State = #state{log_state = LogState, id = PeerID, back_end = BackEnd, peers = OldPeers}) ->
    NewPeersSet = ordsets:union([[PeerID], Old, New]),
    #log_descr{last_index = LastIndex, last_term = LastTerm, commit_index = Commit} = LogState,
    HearBeat = if
                   StateName == leader ->
                       #state{epoch = Epoch, current_term = Term} = State,
                       zraft_log_util:append_request(Epoch, Term, Commit, LastIndex, LastTerm, []);
                   true ->
                       undefined
               end,
    NewPeers = join_peers({peer(PeerID), BackEnd, HearBeat}, NewPeersSet, OldPeers, []),
    ConfState = case New of
                    [] ->
                        ?STABLE_CONF;
                    _ ->
                        ?TRANSITIONAL_CONF
                end,
    NewConf = #config{id = ConfID, old_peers = Old, new_peers = New, conf = PConf, state = ConfState},
    State#state{config = NewConf, peers = NewPeers}.

join_peers(_PeerParam, [], Peers, Acc) ->
    %%remove other peers
    lists:foreach(fun({_, P}) ->
        zraft_peer_proxy:stop(P) end, Peers),
    lists:reverse(Acc);
join_peers(PeerParam, [ID1 | T1], [], Acc) ->
    %%new peer
    NewPeer = make_peer(PeerParam, ID1),
    join_peers(PeerParam, T1, [], [NewPeer | Acc]);
join_peers(PeerParam, [ID1 | T1], [{ID2, P} | T2], Acc) when ID1 > ID2 ->
    %%remove peer
    zraft_peer_proxy:stop(P),
    join_peers(PeerParam, [ID1 | T1], T2, Acc);
join_peers(PeerParam, [ID1 | T1], [{ID2, P2} | T2], Acc) when ID1 < ID2 ->
    %%new peer
    NewPeer = make_peer(PeerParam, ID1),
    join_peers(PeerParam, T1, [{ID2, P2} | T2], [NewPeer | Acc]);
join_peers(PeerParam, [_ID1 | T1], [{ID2, P2} | T2], Acc) -> %%ID1==ID2
    join_peers(PeerParam, T1, T2, [{ID2, P2} | Acc]).

make_peer({Self, BackEnd, HearBeat}, PeerID) ->
    {MyID, _} = Self,
    {ok, PeerPID} = zraft_peer_proxy:start_link(Self, PeerID, BackEnd),
    if
        HearBeat == undefined orelse MyID == PeerID ->
            ok;
        true ->
            zraft_peer_proxy:cmd(PeerPID, {?BECOME_LEADER_CMD, HearBeat})
    end,
    {PeerID, PeerPID}.

hasVote(#state{config = Conf, id = ID}) ->
    #config{state = ConfState, old_peers = OldPeers} = Conf,
    case ConfState of
        ?TRANSITIONAL_CONF ->
            ordsets:is_element(ID, OldPeers) orelse ordsets:is_element(ID, ConfState#config.new_peers);
        _ ->
            ordsets:is_element(ID, OldPeers)
    end.

proxy_stats(#state{peers = Peers}) ->
    Ref = make_ref(),
    From = {Ref, self()},
    Count = lists:foldl(fun({_, P}, Acc) ->
        zraft_peer_proxy:stat(P, From), Acc + 1 end, 0, Peers),
    Res = collect_results(Count, Ref, []),
    lists:ukeysort(1, Res).


quorumMin(#state{config = ?BLANK_CONF}, _GetIndex) ->
    0;
quorumMin(#state{config = Conf, peers = Peers}, GetIndex) ->
    #config{state = ConfState, old_peers = OldPeers} = Conf,
    case ConfState of
        ?TRANSITIONAL_CONF ->
            erlang:min(
                quorumMin(OldPeers, Peers, GetIndex),
                quorumMin(Conf#config.new_peers, Peers, GetIndex)
            );
        _ ->
            quorumMin(OldPeers, Peers, GetIndex)
    end.

quorumAll(#state{config = Conf, peers = Peers}, GetIndex) ->
    #config{state = ConfState, old_peers = OldPeers} = Conf,
    case ConfState of
        ?TRANSITIONAL_CONF ->
            quorumAll(OldPeers, Peers, GetIndex) andalso quorumAll(Conf#config.new_peers, Peers, GetIndex);
        _ ->
            quorumAll(OldPeers, Peers, GetIndex)
    end.

quorumMin([], _AllPeers, _GetIndex) ->
    0;
quorumMin(Peers, AllPeers, GetIndex) ->
    Ref = make_ref(),
    {Vals, Count} = quorumMin(Peers, AllPeers, {Ref, self()}, GetIndex, 0),
    Vals1 = lists:sort(Vals),
    At = erlang:trunc((Count - 1) / 2),
    lists:nth(At + 1, Vals1).

quorumMin([], _, {Ref, _}, _GetIndex, Count) ->
    Vals = collect_results(Count, Ref, []),
    {Vals, Count};
quorumMin([ID1 | T1], [{ID2, _P2} | T2], Ref, GetIndex, Count) when ID1 > ID2 ->
    quorumMin([ID1 | T1], T2, Ref, GetIndex, Count);
quorumMin([ID1 | _T1], [{ID2, _P2} | _T2], _Ref, _GetIndex, _Count) when ID1 < ID2 ->
    ?ERROR("Configuration contains peer ~p that not included to all peers", [ID1]),
    exit({error, iconfig});
quorumMin([_ID1 | T1], [{_ID2, P2} | T2], Ref, GetIndex, Count) ->%%ID1==ID2
    zraft_peer_proxy:value(P2, Ref, GetIndex),
    quorumMin(T1, T2, Ref, GetIndex, Count + 1).

collect_results(0, _Ref, Acc) ->
    Acc;
collect_results(Count, Ref, Acc) ->
    receive
        {Ref, V} ->
            collect_results(Count - 1, Ref, [V | Acc])
    end.

quorumAll([], _AllPeers, _GetIndex) ->
    true;
quorumAll(Peers, AllPeers, GetIndex) ->
    Ref = make_ref(),
    quorumAll(Peers, AllPeers, {Ref, self()}, GetIndex, 0).

quorumAll([], _, {Ref, _}, _GetIndex, Count) ->
    TrueCount = collect_all(Count, Ref, 0),
    TrueCount >= (erlang:trunc(Count / 2) + 1);
quorumAll([ID1 | T1], [{ID2, _P2} | T2], Ref, GetIndex, Count) when ID1 > ID2 ->
    quorumAll([ID1 | T1], T2, Ref, GetIndex, Count);
quorumAll([ID1 | _T1], [{ID2, _P2} | _T2], _Ref, _GetIndex, _Count) when ID1 < ID2 ->
    ?ERROR("Configuration contains peer ~p that not included to all peers", [ID1]),
    exit({error, iconfig});
quorumAll([_ID1 | T1], [{_ID2, P2} | T2], Ref, GetIndex, Count) ->%%ID1==ID2
    zraft_peer_proxy:value(P2, Ref, GetIndex),
    quorumAll(T1, T2, Ref, GetIndex, Count + 1).

collect_all(0, _Ref, Acc) ->
    Acc;
collect_all(Count, Ref, Acc) ->
    receive
        {Ref, true} ->
            collect_all(Count - 1, Ref, Acc + 1);
        {Ref, _} ->
            collect_all(Count - 1, Ref, Acc)
    end.

append(Entries, State = #state{log = Log}) ->
    Async = zraft_fs_log:append_leader(Log, Entries),
    #log_op_result{log_state = LogState1, last_conf = NewConf, result = ok} = zraft_fs_log:sync_fs(Async),
    State1 = State#state{log_state = LogState1},
    State2 = set_config(leader, NewConf, State1),
    ok = update_peer_last_index(State2),
    replicate_peer_request(?OPTIMISTIC_REPLICATE_CMD, State2, Entries),
    gen_fsm:send_all_state_event(self(), {sync_peer, true}),
    State2.



check_blank_state(#state{current_term = T, snapshot_info = #snapshot_info{index = S}, log_state = LogState}) ->
    #log_descr{first_index = F, last_index = L} = LogState,
    (T == 0) and (L == 0) and (F == 1) and (S == 0).

start_timer(State = #state{timer = Timer, election_timeout = Timeout}) ->
    if
        Timer == undefined ->
            ok;
        true ->
            gen_fsm:cancel_timer(Timer)
    end,
    Timeout1 = zraft_util:random(Timeout) + Timeout,
    NewTimer = gen_fsm:send_event_after(Timeout1, timeout),
    State#state{timer = NewTimer}.

cancel_timer(State = #state{timer = Timer}) ->
    if
        Timer == undefined ->
            ok;
        true ->
            gen_fsm:cancel_timer(Timer)
    end,
    State#state{timer = undefined}.

update_peer_last_index(State = #state{epoch = Epoch, log_state = #log_descr{last_index = I}}) ->
    update_peer(
        fun(P) ->
            P#peer{last_agree_index = I, next_index = I + 1, epoch = Epoch} end,
        State),
    ok.

update_all_peer(Fun, State) ->
    to_all_peer({?UPDATE_CMD, Fun}, State).
update_peer(Fun, State = #state{id = PeerID}) ->
    update_peer(PeerID, Fun, State).
update_peer(PeerID, Fun, State) ->
    to_peer(PeerID, {?UPDATE_CMD, Fun}, State).

lost_leadership(StateName, State) when StateName == leader orelse StateName == candidate ->
    to_all_peer(?LOST_LEADERSHIP_CMD, State);
lost_leadership(_, _State) ->
    ok.


to_all_peer_direct(Cmd, #state{peers = Peers, id = MyID}) ->
    lists:foreach(
        fun
            ({PeerID, _}) when MyID == PeerID ->
                ok;
            ({PeerID, _}) ->
                zraft_peer_route:cmd(PeerID, Cmd) end, Peers).

to_all_follower_peer(Cmd, #state{peers = Peers, id = MyID}) ->
    lists:foreach(
        fun
            ({PeerID, _}) when PeerID == MyID ->
                ok;
            ({_PeerID, P}) ->
                zraft_peer_proxy:cmd(P, Cmd) end, Peers).

to_all_peer(Cmd, #state{peers = Peers}) ->
    lists:foreach(fun({_, P}) ->
        zraft_peer_proxy:cmd(P, Cmd) end, Peers).

to_peer(_PeerID, _Cmd, #state{peers = []}) ->
    ok;
to_peer(PeerID, Cmd, #state{peers = Peers}) ->
    case lists:keyfind(PeerID, 1, Peers) of
        false ->
            ?WARNING("Try update unknown peer ~p ~p", [PeerID, Peers]);
        {_, P} ->
            zraft_peer_proxy:cmd(P, Cmd)
    end,
    ok.

reset_requests(State) ->
    State1 = reset_read_requests(State),
    State2 = reset_write_requests(State1),
    reset_conf_request(State2).

reset_conf_request(State = #state{sessions = #sessions{conf = undefined}}) ->
    State;
reset_conf_request(State = #state{sessions = Sessions, leader = Leader}) ->
    reset_request(Sessions#sessions.conf, {leader, Leader}),
    State#state{sessions = Sessions#sessions{conf = undefined}}.
reset_read_requests(State = #state{sessions = #sessions{read = []}}) ->
    State;
reset_read_requests(State = #state{sessions = Sessions, leader = Leader}) ->
    #sessions{read = Requests} = Sessions,
    lists:foreach(fun({_, Req}) ->
        reset_request(Req, {leader, Leader}) end, Requests),
    State#state{sessions = Sessions#sessions{read = []}}.

reset_write_requests(State = #state{sessions = #sessions{write = []}}) ->
    State;
reset_write_requests(State = #state{sessions = Sessions}) ->
    #sessions{write = Requests} = Sessions,
    lists:foreach(fun({_, Req}) ->
        reset_request(Req, {error, not_leader}) end, Requests),
    State#state{sessions = Sessions#sessions{write = []}}.

apply_read_requests(State = #state{allow_commit = false}) ->
    State;
apply_read_requests(State = #state{sessions = Sessions}) ->
    #sessions{read = Requests} = Sessions,
    Epoch = quorumMin(State, #peer.epoch),
    Requests1 = apply_read_requests(Epoch, Requests, State),
    State#state{sessions = Sessions#sessions{read = Requests1}}.

apply_read_requests(_E1, [], _State) ->
    [];
apply_read_requests(E1, [{E2, Req} | T], State) when E2 > E1 ->
    case check_request_timeout(Req) of
        true ->
            [{E2, Req} | apply_read_requests(E1, T, State)];
        _ ->
            apply_read_requests(E1, T, State)
    end;
apply_read_requests(_E1, Requests, State) ->
    lists:foreach(fun({_, Req}) ->
        case check_request_timeout(Req) of
            true ->
                #read_request{function = Function, args = Args} = Req,
                erlang:apply(?MODULE, Function, [State | Args]);
            _ ->
                ok
        end end, Requests),
    [].

check_request_timeout(Req = #conf_change_requet{start_time = Start, timeout = Timeout}) ->
    check_request_timeout(Req, Start, Timeout);
check_request_timeout(Req = #read_request{start_time = Start, timeout = Timeout}) ->
    check_request_timeout(Req, Start, Timeout);
check_request_timeout(Req = #write_request{start_time = Start, timeout = Timeout}) ->
    check_request_timeout(Req, Start, Timeout).
check_request_timeout(Req, Start, Timeout) ->
    if
        Timeout == infinity ->
            true;
        true ->
            Delta = timer:now_diff(os:timestamp(), Start),
            if
                Delta < Timeout ->
                    true;
                true ->
                    reset_request(Req, {error, timeout}),
                    false
            end
    end.
reset_request(#conf_change_requet{from = From}, Reason) ->
    gen_fsm:reply(From, Reason);
reset_request(#write_request{from = From}, Reason) ->
    gen_fsm:reply(From, Reason);
reset_request(#read_request{args = [From | _]}, Reason) ->
    gen_fsm:reply(From, Reason).

%%Query data from state FSM.
read_request(#state{state_fsm = FSM}, From, Request) ->
    zraft_fsm:cmd(FSM, #leader_read_request{from = From, request = Request}).

%%Get last stable configuration
get_conf_request(#state{log_state = LogState, config = Conf}, From) ->
    #log_descr{commit_index = Commit} = LogState,
    #config{conf = ConfData, id = ConfIndex, state = ConfState} = Conf,
    if
        ConfState == ?STABLE_CONF andalso ConfIndex =< Commit ->
            {_, #pconf{old_peers = Peers}} = ConfData,
            gen_fsm:reply(From, {ok, {ConfIndex, Peers}});
        true ->
            gen_fsm:reply(From, retry)
    end.

accept_conf_request(State = #state{sessions = #sessions{conf = undefined}}) ->
    State;
accept_conf_request(State = #state{sessions = Sessions, log_state = LogState, config = Config}) ->
    #sessions{conf = Req} = Sessions,
    #log_descr{commit_index = Commit} = LogState,
    case check_request_timeout(Req) of
        true ->
            if
                Config#config.id > Req#conf_change_requet.index andalso Commit >= Config#config.id ->
                    gen_fsm:reply(Req#conf_change_requet.from, ok),
                    State#state{sessions = Sessions#sessions{conf = undefined}};
                true ->
                    State
            end;
        _ ->
            State#state{sessions = Sessions#sessions{conf = undefined}}
    end.

accept_write_requests(State = #state{sessions = Sessions, log_state = LogState}) ->
    #sessions{write = Requests} = Sessions,
    #log_descr{commit_index = I} = LogState,
    Requests1 = accept_write_requests(I, Requests),
    State#state{sessions = Sessions#sessions{write = Requests1}}.

accept_write_requests(_I1, []) ->
    [];
accept_write_requests(I1, [{I2, Req} | T]) when I2 > I1 ->
    case check_request_timeout(Req) of
        true ->
            [{I2, Req} | accept_write_requests(I1, T)];
        _ ->
            accept_write_requests(I1, T)
    end;
accept_write_requests(_E1, Requests) ->
    lists:foreach(fun({_, Req}) ->
        case check_request_timeout(Req) of
            true ->
                gen_fsm:reply(Req#write_request.from, ok);
            _ ->
                ok
        end end, Requests),
    [].


stop_all_peer(#state{peers = Peers}) ->
    lists:foreach(fun({_, P}) ->
        ok = zraft_peer_proxy:stop_sync(P) end, Peers).

-spec peer(peer_id()) -> from_peer_addr().
peer(ID) ->
    {ID, self()}.

send_event(P, Event) when is_pid(P) ->
    gen_fsm:send_event(P, Event);
send_event({_, P}, Event) ->
    gen_fsm:send_event(P, Event).

send_all_state_event(P, Event) when is_pid(P) ->
    gen_fsm:send_all_state_event(P, Event);
send_all_state_event({_, P}, Event) ->
    gen_fsm:send_all_state_event(P, Event).

-ifdef(TEST).
setup_node() ->
    zraft_util:set_test_dir("test-data"),
    net_kernel:start(['zraft_test@localhost', shortnames]),
    ok.
stop_node(_) ->
    net_kernel:stop(),
    zraft_util:clear_test_dir("test-data"),
    ok.

bootstrap_test_() ->
    {
        setup,
        fun setup_node/0,
        fun stop_node/1,
        fun(_X) ->
            [
                bootstrap()
            ]
        end
    }.

bootstrap() ->
    {"bootstrap", fun() ->
        Peer = {test, node()},
        {ok, load, InitState} = init([Peer, zraft_dict_backend]),
        {next_state, follower, State} = init_state(InitState#init_state{snapshot_info = #snapshot_info{}}),
        {next_state, follower, State1} = follower(bootstrap, {self(), make_ref()}, State),
        ?assertEqual(1, State1#state.current_term),
        Entries = zraft_fs_log:get_entries(State1#state.log, 1, 1),
        ?assertMatch([#enrty{term = 1, index = 1, type = ?OP_CONFIG, data = #pconf{old_peers = [Peer]}}],
            Entries),
        cancel_timer(State1),
        {next_state, leader, State2} = follower(timeout, State1),
        ?assertEqual(2, State2#state.current_term),
        ?assertEqual(undefined, State2#state.timer),
        ?assertMatch(#log_descr{commit_index = 0, first_index = 1, last_index = 2, last_term = 2},
            State2#state.log_state),
        Entries1 = zraft_fs_log:get_entries(State1#state.log, 2, 2),
        ?assertMatch([#enrty{term = 2, index = 2, type = ?OP_NOOP}], Entries1)
    end}.

-endif.