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
-module(zraft_peer_proxy).
-author("dreyk").

-include_lib("zraft_lib/include/zraft.hrl").

-define(INFO(State,S, As),?MINFO("~p: "++S,[print_id(State)|As])).
-define(INFO(State,S), ?MINFO("~p: "++S,[print_id(State)])).
-define(ERROR(State,S, As),?MERROR("~p: "++S,[print_id(State)|As])).
-define(ERROR(State,S), ?MERROR("~p: "++S,[print_id(State)])).
-define(DEBUG(State,S, As),?MDEBUG("~p: "++S,[print_id(State)|As])).
-define(DEBUG(State,S), ?MDEBUG("~p: "++S,[print_id(State)])).
-define(WARNING(State,S, As),?MWARNING("~p: "++S,[print_id(State)|As])).
-define(WARNING(State,S), ?MWARNING("~p: "++S,[print_id(State)])).

-behaviour(gen_server).

-export([start_link/3]).

-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3,
    value/3,
    stop/1,
    cmd/2,
    stat/2,
    stop_sync/1]).

-record(snapshot_progress, {snapshot_dir, process, mref, index}).
-record(state, {
    peer,
    raft,
    request_timer,
    hearbeat_timer,
    request_ref,
    force_hearbeat = false,
    force_request = false,
    current_term = 0,
    current_epoch = 0,
    back_end, request_timeout, snapshot_progres}).


stat(Peer,From)->
    gen_server:cast(Peer,{stat,From}).

value(Peer, From, GetIndex) ->
    gen_server:cast(Peer, {get, From, GetIndex}).

stop(Peer) ->
    gen_server:cast(Peer, stop).

stop_sync(Peer) ->
    gen_server:call(Peer, stop).

cmd(Peer, Cmd) ->
    gen_server:cast(Peer, Cmd).

start_link(Raft, PeerID, BackEnd) ->
    gen_server:start_link(?MODULE, [Raft, PeerID, BackEnd], []).

init([Raft, PeerID, BackEnd]) ->
    gen_server:cast(self(), start_peer),
    ReqTimeout = zraft_consensus:get_election_timeout()*2,
    {ok, #state{raft = Raft, back_end = BackEnd, peer = #peer{id = PeerID}, request_timeout = ReqTimeout}}.

handle_call(force_hearbeat_timeout, _, State = #state{hearbeat_timer = Timer}) ->
    if
        Timer == undefined ->
            {reply, no_timer, State};
        true ->
            zraft_util:gen_server_cancel_timer(Timer),
            Timer1 = zraft_util:gen_server_cast_after(0, hearbeat_timeout),
            {reply, ok, State#state{hearbeat_timer = Timer1}}
    end;
handle_call(force_request_timeout, _, State = #state{request_timer = Timer}) ->
    if
        Timer == undefined ->
            {reply, no_timer, State};
        true ->
            zraft_util:gen_server_cancel_timer(Timer),
            Timer1 = zraft_util:gen_server_cast_after(1, request_timeout),
            {reply, ok, State#state{request_timer = Timer1}}
    end;
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(stop, State) ->
    {stop, normal, State};

handle_cast(start_peer, State) ->
    maybe_start_remote(State),
    {noreply, State};

handle_cast(?LOST_LEADERSHIP_CMD,
    State = #state{peer = Peer}) ->
    State1 = reset_timers(true, State),
    State2 = reset_snapshot(State1),
    State3 = State2#state{peer = Peer#peer{has_vote = false, epoch = 0}, current_term = 0},
    {noreply, State3};
handle_cast(hearbeat_timeout, State = #state{request_ref = Ref}) when Ref /= undefined ->
    ?WARNING(State,"There is active request"),
    {noreply, State};
handle_cast(hearbeat_timeout, State) ->%%send new hearbeat
    State1 = reset_timers(false, State),
    case State#state.snapshot_progres of
        undefined ->
            State2 = start_replication(State1),
            {noreply, State2};
        _ ->
            %%Send herabeat to check update peer state
            State2 = install_snapshot_hearbeat(hearbeat, State1),
            {noreply, State2}
    end;
handle_cast(request_timeout, State = #state{request_ref = undefined}) ->
    ?WARNING(State,"There is't active request"),
    {noreply, State};
handle_cast(request_timeout, State) ->%%send new request
    State1 = reset_timers(false, State),
    State2 = reset_snapshot(State1),%%May be snapshot is being copied
    State3 = start_replication(State2),%%Start new attempt
    {noreply, State3};

handle_cast({?BECOME_LEADER_CMD, HearBeat},
    State = #state{peer = Peer}) ->%%peer has elected
    #append_entries{term = CurrentTerm, epoch = Epoch, prev_log_index = LastLogIndex} = HearBeat,
    State1 = reset_timers(true, State),%%discard all active requets
    State2 = reset_snapshot(State1),%%stop copy snaphsot
    State3 = State2#state{
        force_hearbeat = true,%% We must match followers log before start replication
        current_term = CurrentTerm,
        current_epoch = Epoch,
        peer = Peer#peer{last_agree_index = 0, next_index = LastLogIndex + 1}
    },
    State4 = replicate(HearBeat, State3),
    {noreply, State4};

handle_cast({?OPTIMISTIC_REPLICATE_CMD, Req},
    State = #state{request_ref = Ref}) when Ref /= undefined ->
    %%prev request has't finished yet
    #append_entries{term = Term, epoch = Epoch} = Req,
    {noreply, State#state{force_request = true, current_term = Term, current_epoch = Epoch}};

handle_cast({?OPTIMISTIC_REPLICATE_CMD, Req},
    State = #state{peer = Peer, snapshot_progres = undefined}) ->
    #peer{next_index = NextIndex} = Peer,
    PrevIndex = NextIndex - 1,
    #append_entries{prev_log_index = ReqPrevIndex, term = Term, epoch = Epoch} = Req,
    State1 = State#state{current_term = Term, current_epoch = Epoch},
    State2 = if
                 PrevIndex == ReqPrevIndex ->
                     %%peer in sync.Just add all entries
                     replicate(Req, State1);
                 true ->
                     %%need more log to replcate
                     start_replication(State1)
             end,
    {noreply, State2};

handle_cast({?OPTIMISTIC_REPLICATE_CMD, Req}, State) ->%%snapshot are being copied
    #append_entries{term = Term, epoch = Epoch} = Req,
    State1 = State#state{current_term = Term, current_epoch = Epoch},
    %%Just send hearbeat
    State2 = install_snapshot_hearbeat(hearbeat, State1),
    {noreply, State2};

handle_cast(#append_reply{epoch = Epoch, success = true, agree_index = Index, request_ref = RF},
    State = #state{force_request = FR, request_ref = RF}) ->
    State1 = update_peer(true, Index, Index + 1, Epoch, State),
    State2 = reset_timers(true, State1),
    State3 = if
                 FR ->
                     %%We have new entries to replicate
                     start_replication(State2);
                 true ->
                     start_hearbeat_timer(State2)
             end,
    {noreply, State3};

handle_cast(#append_reply{term = PeerTerm},
    State = #state{current_term = CurrentTerm,raft = Raft}) when PeerTerm > CurrentTerm ->
    %%Actualy CurrentTerm maybe out of date now, but it's not problem. We will receive new term or shutdown soon.
    ?WARNING(State,"Peer has new term(leader)"),
    zraft_consensus:maybe_step_down(Raft, PeerTerm),
    State1 = reset_timers(true, State),
    {noreply, State1};

handle_cast(#append_reply{request_ref = RF, last_index = LastIndex, epoch = Epoch},
    State = #state{peer = Peer, request_ref = RF}) ->
    ?WARNING(State,"Peer out of date"),
    DecNext = Peer#peer.next_index - 1,
    NextIndex = max(1, min(LastIndex, DecNext)),
    State1 = update_peer(NextIndex, Epoch, State),
    progress(State1);

handle_cast(#append_reply{}, State) ->%%Out of date responce
    {noreply, State};


handle_cast(Req = #install_snapshot{request_ref = RF, term = Term, epoch = Epoch},
    State = #state{request_ref = RF}) ->
    State1 = reset_timers(false, State),
    ?INFO(State,"Need Install snaphsot"),
    %%try start snapshot copy process
    State2 = install_snapshot(Req, State1#state{current_epoch = Epoch, current_term = Term}),
    {noreply, State2};

handle_cast(#install_snapshot{}, State) ->%%Out of date responce
    %%close all opened files
    {noreply, State};

handle_cast(Resp = #install_snapshot_reply{result = start, request_ref = RF, epoch = Epoch},
    State = #state{request_ref = RF, force_request = FR}) ->
    State1 = update_peer(Epoch, State),
    State2 = reset_timers(true, State1),
    State3 = start_copy_snapshot(Resp, State2),
    State4 = if
                 FR ->
                     install_snapshot_hearbeat(hearbeat, State3);
                 true ->
                     start_hearbeat_timer(State3)
             end,
    {noreply, State4};
handle_cast(#install_snapshot_reply{result = hearbeat, request_ref = RF, epoch = Epoch},
    State = #state{request_ref = RF, force_request = FR}) ->
    State1 = update_peer(Epoch, State),
    State2 = reset_timers(true, State1),
    State3 = if
                 FR ->
                     install_snapshot_hearbeat(hearbeat, State2);
                 true ->
                     start_hearbeat_timer(State2)
             end,
    {noreply, State3};
handle_cast(#install_snapshot_reply{index = Index, result = finish, request_ref = RF, epoch = Epoch},
    State = #state{request_ref = RF}) ->
    State1 = update_peer(true, Index, Index + 1, Epoch, State),
    State2 = reset_snapshot(State1),
    progress(State2#state{force_request = true});

%%Snapshot RPC failed
handle_cast(#install_snapshot_reply{term = PeerTerm},
    State = #state{current_term = CurrentTerm,raft = Raft}) when PeerTerm > CurrentTerm ->
    %%Actualy CurrentTerm maybe out of date now but is's not problem. We will receive new term or shutdown soon.
    ?WARNING(State,"Peer has new term(leader)"),
    zraft_consensus:maybe_step_down(Raft, PeerTerm),
    State1 = reset_timers(true, State),
    State2 = reset_snapshot(State1),
    {noreply, State2};
handle_cast(#install_snapshot_reply{request_ref = RF, epoch = Epoch},
    State = #state{request_ref = RF}) ->
    ?WARNING(State,"Copy snapshot failed"),
    State1 = update_peer(Epoch, State),
    State2 = reset_snapshot(State1),
    progress(State2#state{force_request = true});

handle_cast(#install_snapshot_reply{}, State) ->%%Out of date responce
    {noreply, State};

handle_cast({?UPDATE_CMD, Fun}, State = #state{peer = Peer}) ->
    Peer1 = Fun(Peer),
    {noreply, State#state{peer = Peer1}};
handle_cast({get, From, GetIndex}, State = #state{peer = Peer}) ->
    reply(From, erlang:element(GetIndex, Peer)),
    {noreply, State};
handle_cast({stat, From}, State = #state{peer = Peer,snapshot_progres = Progress}) ->
    IsSnapshoting = (Progress /= undefined),
    Stat = #proxy_peer_stat{peer_state = Peer,is_snapshoting = IsSnapshoting},
    reply(From, {Peer#peer.id,Stat}),
    {noreply, State};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({'DOWN', Ref, process, _, normal},
    State = #state{snapshot_progres = #snapshot_progress{mref = Ref}}) ->
    ?INFO(State,"Snapshot has transfered."),
    State1 = reset_timers(false, State),
    State2 = install_snapshot_hearbeat(finish, State1),
    {noreply, State2#state{snapshot_progres = undefined, force_request = true}};
handle_info({'DOWN', Ref, process, _, Reason},
    State = #state{snapshot_progres = #snapshot_progress{mref = Ref}}) ->
    ?ERROR(State,"Snapshot transfer failed ~p",[Reason]),
    progress(State#state{force_request = true, snapshot_progres = undefined});
handle_info(_, State) ->
    {noreply, State}.

terminate(Reason, State=#state{snapshot_progres = Progress}) ->
    Reason==normal orelse ?WARNING(State,"Proxy is being stoped ~p",[Reason]),
    if
        Progress == undefined ->
            ok;
        true ->
            if
                is_pid(Progress#snapshot_progress.process) ->
                    exit(Progress#snapshot_progress.process, kill);
                true ->
                    ok
            end
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

start_replication(State) ->
    #state{peer = Peer, raft = Raft, force_hearbeat = FH, request_timeout = Timeout} = State,
    #peer{next_index = NextIndex, id = PeerID} = Peer,
    PrevIndex = NextIndex - 1,
    RequestRef = erlang:make_ref(),
    Req = #append_entries{
        prev_log_index = PrevIndex,
        request_ref = RequestRef,
        entries = not FH,
        from = from_addr(State)
    },
    zraft_consensus:replicate_log(Raft, PeerID, Req),
    Timer = zraft_util:gen_server_cast_after(Timeout, request_timeout),
    State#state{request_ref = RequestRef, request_timer = Timer}.
replicate(Req, State) ->
    #state{peer = Peer,request_timeout = Timeout} = State,
    #peer{id = PeerID} = Peer,
    RequestRef = erlang:make_ref(),
    #append_entries{commit_index = Commit,prev_log_index = Prev,entries = Entries}=Req,
    NewCommitIndex = min(Commit,Prev+length(Entries)),
    zraft_peer_route:cmd(
        PeerID,
        Req#append_entries{commit_index = NewCommitIndex,request_ref = RequestRef, from = from_addr(State)}
    ),
    Timer = zraft_util:gen_server_cast_after(Timeout, request_timeout),
    State#state{request_ref = RequestRef, request_timer = Timer}.

install_snapshot(Req, State) ->
    #state{peer = Peer,request_timeout = Timeout} = State,
    #peer{id = PeerID} = Peer,
    SnapsotProgress = #snapshot_progress{snapshot_dir = Req#install_snapshot.data, index = Req#install_snapshot.index},
    RequestRef = erlang:make_ref(),
    NewReq = Req#install_snapshot{data = start, request_ref = RequestRef, from = from_addr(State)},
    zraft_peer_route:cmd(PeerID, NewReq),
    Timer = zraft_util:gen_server_cast_after(Timeout, request_timeout),
    State#state{request_ref = RequestRef, request_timer = Timer, snapshot_progres = SnapsotProgress}.
install_snapshot_hearbeat(Type, State) ->
    #state{
        peer = Peer,
        request_timeout = Timeout,
        snapshot_progres = Progress,
        current_term = Term,
        current_epoch = Epoch
    } = State,
    #peer{id = PeerID} = Peer,
    #snapshot_progress{index = Index} = Progress,
    RequestRef = erlang:make_ref(),
    NewReq = #install_snapshot{
        data = Type,
        request_ref = RequestRef,
        from = from_addr(State),
        index = Index,
        term = Term,
        epoch = Epoch
    },
    zraft_peer_route:cmd(PeerID, NewReq),
    Timer = zraft_util:gen_server_cast_after(Timeout, request_timeout),
    State#state{request_ref = RequestRef, request_timer = Timer}.

reset_snapshot(State = #state{snapshot_progres = undefined}) ->
    State;
reset_snapshot(State = #state{snapshot_progres = #snapshot_progress{mref = Ref, process = P}}) ->
    ?INFO(State,"Reseting snapshot transfer"),
    if
        Ref /= undefined ->
            erlang:demonitor(Ref, [flush]),
            erlang:exit(P, kill);
        true ->
            ok
    end,
    State#state{snapshot_progres = undefined}.

reset_timers(Result, State = #state{request_timer = RT, hearbeat_timer = HT}) ->
    cancel_timer(RT),
    cancel_timer(HT),
    State1 = State#state{
        request_ref = undefined,
        request_timer = undefined,
        hearbeat_timer = undefined},
    if
        Result ->
            State1#state{force_hearbeat = false, force_request = false};
        true ->
            State1
    end.

progress(State = #state{force_hearbeat = FH, force_request = FR}) ->
    State1 = reset_timers(false, State),
    State2 = if
                 FH ->
%%Attemt new heabeat scince last one failed
%%if hearbeat accpetd we must start replicate log immediatly
                     start_replication(State1#state{force_request = true});
                 FR ->
%%Prev Hearbeat or replication failed
                     start_replication(State1);
                 true ->
%%Start hearbeat timer
                     start_hearbeat_timer(State1)
             end,
    {noreply, State2}.

start_hearbeat_timer(State) ->
    Ref = zraft_util:gen_server_cast_after(zraft_consensus:get_election_timeout(), hearbeat_timeout),
    State#state{hearbeat_timer = Ref}.

reply({Ref, Pid}, Msg) ->
    Pid ! {Ref, Msg};
reply(_, _) ->
    ok.

cancel_timer(undefined) ->
    0;
cancel_timer(Ref) ->
    zraft_util:gen_server_cancel_timer(Ref).

maybe_start_remote(#state{back_end = BackEnd, peer = Peer,raft = Raft}) ->
    case Raft of
        {ID,_} when ID==Peer#peer.id ->
            ok;
        _ ->
            zraft_peer_route:start_peer(Peer#peer.id, BackEnd),
            ok
    end.

start_copy_snapshot(#install_snapshot_reply{port = Port, addr = Addr},
    State = #state{snapshot_progres = P}) ->
    ?INFO(State,"Starting transfer snapshot via tcp ~p:~p",[Addr,Port]),
    Fun = fun() ->
        FilesToCopy  = zraft_snapshot_receiver:copy_info(P#snapshot_progress.snapshot_dir),
        case catch zraft_snapshot_receiver:copy_files(print_id(State),FilesToCopy, Addr, Port) of
            ok->
                zraft_snapshot_receiver:discard_files_info(FilesToCopy),
                ok;
            Else->
                zraft_snapshot_receiver:discard_files_info(FilesToCopy),
                exit(Else)
        end
    end,
    {PID, MRef} = spawn_monitor(Fun),
    State#state{snapshot_progres = P#snapshot_progress{mref = MRef, process = PID}}.

update_peer(Epoch, State = #state{peer = Peer, raft = Raft}) ->
    Peer1 = Peer#peer{epoch = Epoch},
    zraft_consensus:sync_peer(Raft, flase),
    State#state{peer = Peer1}.
update_peer(NextIndex, Epoch, State = #state{peer = Peer, raft = Raft}) ->
    Peer1 = Peer#peer{epoch = Epoch, next_index = NextIndex},
    zraft_consensus:sync_peer(Raft, false),
    State#state{peer = Peer1}.
update_peer(MayBeCommit, LastIndex, NextIndex, Epoch, State = #state{peer = Peer, raft = Raft}) ->
    Peer1 = Peer#peer{epoch = Epoch, last_agree_index = LastIndex, next_index = NextIndex},
    zraft_consensus:sync_peer(Raft, MayBeCommit),
    State#state{peer = Peer1}.

print_id(#state{raft = Raft,peer = #peer{id = ProxyID}})->
    PeerID = zraft_util:peer_id(Raft),
    {PeerID,'->',ProxyID}.

from_addr(#state{raft = Raft})->
    ID = zraft_util:peer_id(Raft),
    {ID,self()}.

-ifdef(TEST).
setup_peer() ->
    meck:new(zraft_peer_route, [passthrough]),
    meck:new(zraft_consensus),
    meck:new(zraft_snapshot_receiver,[passthrough]),
    meck:expect(zraft_consensus, get_election_timeout, fun() -> 1000 end),
    meck:expect(zraft_peer_route, start_peer, fun(PeerToStart, BackEnd) ->
        ?debugFmt("Starting ~p:~s", [PeerToStart, BackEnd]), ok end),
    meck:expect(zraft_snapshot_receiver,copy_info,fun(_)-> [] end),
    meck:expect(zraft_snapshot_receiver,copy_files,fun(A1,A2,A3,A4)->
        ?debugFmt("copy snapshot: ~p",[{A1,A2,A3,A4}]) end),
    ok.
stop_peer(_) ->
    meck:unload(zraft_peer_route),
    meck:unload(zraft_consensus),
    meck:unload(zraft_snapshot_receiver),
    ok.

proxy_test_() ->
    {
        setup,
        fun setup_peer/0,
        fun stop_peer/1,
        fun(_X) ->
            [
                commands()
            ]
        end
    }.

commands() ->
    {"test communication", fun() ->
        Me = self(),
        Raft = {{test, node()}, Me},
        PeerID = {test1, node()},
        {ok, Proxy} = start_link(Raft, PeerID, zraft_dict_backend),
        meck:expect(zraft_consensus, replicate_log,
            fun(_, _, Req) ->
                Me ! {replicate_log, Req}
            end),
        meck:expect(zraft_consensus, sync_peer,
            fun(_, MayBeCommit) ->
                Me ! {sync, MayBeCommit}
            end),
        meck:expect(zraft_peer_route, cmd, fun(_, Req) ->
            Me ! {cmd, Req}
        end),
        cmd(Proxy, {?BECOME_LEADER_CMD,
            #append_entries{
                commit_index = 0,
                entries = [],
                epoch = 3,
                prev_log_index = 5,
                prev_log_term = 5,
                term = 5}}),
        R1 = gen_server:call(Proxy, force_hearbeat_timeout),
        ?assertEqual(no_timer, R1),
        R2 = wait_request(),
        ?assertMatch(
            {cmd, #append_entries{entries = [], prev_log_term = 5, prev_log_index = 5, epoch = 3, term = 5, commit_index = 0}},
            R2
        ),
        R3 = gen_server:call(Proxy, force_request_timeout),
        ?assertEqual(ok, R3),
        R4 = wait_request(),
        ?assertMatch(
            {replicate_log, #append_entries{entries = false, prev_log_index = 5, prev_log_term = 0, term = 0, epoch = 0}},
            R4
        ),
        fake_append_reply(Proxy, R4, #append_reply{term = 5, agree_index = 0, last_index = 7, success = false}),
        R5 = wait_request(),
        ?assertMatch({sync, false}, R5),
        R6 = wait_request(),
        ?assertMatch(
            {replicate_log, #append_entries{entries = false, prev_log_index = 4, prev_log_term = 0, term = 0, epoch = 0}},
            R6
        ),
        fake_append_reply(Proxy, R6, #append_reply{term = 5, agree_index = 4, last_index = 4, success = true}),
        R7 = wait_request(),
        ?assertMatch({sync, true}, R7),
        %%start replicaterion
        R8 = wait_request(),
        ?assertMatch(
            {replicate_log, #append_entries{entries = true, prev_log_index = 4, prev_log_term = 0, term = 0, epoch = 0}},
            R8
        ),
        fake_append_reply(Proxy, R8, #append_reply{term = 5, last_index = 4, success = false}),
        R9 = wait_request(),
        ?assertMatch({sync, false}, R9),
        R10 = wait_request(),
        ?assertMatch(
            {replicate_log, #append_entries{entries = true, prev_log_index = 3, prev_log_term = 0, term = 0, epoch = 0}},
            R10
        ),
        fake_append_reply(Proxy, R10, #append_reply{term = 5, last_index = 5, agree_index = 5, success = true}),
        R11 = wait_request(),
        ?assertMatch({sync, true}, R11),
        R12 = gen_server:call(Proxy, force_request_timeout),
        ?assertEqual(no_timer, R12),
        R13 = gen_server:call(Proxy, force_hearbeat_timeout),
        ?assertEqual(ok, R13),
        R14 = wait_request(),
        ?assertMatch(
            {replicate_log, #append_entries{entries = true, prev_log_index = 5, prev_log_term = 0, term = 0, epoch = 0}},
            R14
        ),
        fake_append_reply(Proxy, R14, #append_reply{term = 5, last_index = 5, agree_index = 5, success = true}),
        R15 = wait_request(),
        ?assertMatch({sync, true}, R15),
        R16 = gen_server:call(Proxy, force_request_timeout),
        ?assertEqual(no_timer, R16),
        cmd(Proxy, {?OPTIMISTIC_REPLICATE_CMD,
            #append_entries{
                commit_index = 0,
                entries = [1],
                epoch = 4,
                prev_log_index = 5,
                prev_log_term = 5,
                term = 6}}),
        R17 = wait_request(),
        ?assertMatch(
            {cmd, #append_entries{entries = [1], prev_log_index = 5, prev_log_term = 5, term = 6, epoch = 4}},
            R17
        ),
        fake_append_reply(Proxy, R17, #append_reply{term = 6, last_index = 6, agree_index = 6, success = true}),
        R18 = wait_request(),
        ?assertMatch({sync, true}, R18),
        S1 = sys:get_state(Proxy),
        ?assertMatch(
            #state{
                current_epoch = 4,
                current_term = 6,
                force_hearbeat = false,
                force_request = false,
                request_ref = undefined,
                request_timer = undefined,
                snapshot_progres = undefined,
                peer = #peer{epoch = 4, has_vote = false, id = PeerID, last_agree_index = 6, next_index = 7}
            },
            S1
        ),
        cmd(Proxy, {?OPTIMISTIC_REPLICATE_CMD,
            #append_entries{
                commit_index = 0,
                entries = [1],
                epoch = 4,
                prev_log_index = 7,
                prev_log_term = 7,
                term = 6}}),
        R19 = wait_request(),
        ?assertMatch(
            {replicate_log, #append_entries{entries = true, prev_log_index = 6}},
            R19
        ),
        fake_need_snapshot(R19),
        R20 = wait_request(),
        ?assertMatch(
            {
                cmd,
                #install_snapshot{index = 3, term = 6, epoch = 7, data = start}
            },
            R20
        ),
        fake_snapshot_reply(PeerID,R20,#install_snapshot_reply{result=start,index = 3,addr = 1,port = 1,term = 6}),
        R21 = wait_request(),
        ?assertMatch({sync,flase}, R21),
        R22 = wait_request(),
        ?assertMatch(
            {
                cmd,
                #install_snapshot{index = 3, term = 6, epoch = 7, data = finish}
            },
            R22
        ),
        fake_snapshot_reply(PeerID,R22,#install_snapshot_reply{result=finish,index = 3,term = 6}),
        R23 = wait_request(),
        ?assertMatch({sync,true}, R23),
        R24 = wait_request(),
        ?assertMatch(
            {replicate_log, #append_entries{entries = true, prev_log_index = 3, prev_log_term = 0, term = 0, epoch = 0}},
            R24
        ),
        ok = stop_sync(Proxy)
    end}.

wait_request() ->
    receive
        Req ->
            Req
    after 2000 ->
        ?assert(fase)
    end.

fake_append_reply(PeerID, {_, #append_entries{request_ref = Ref, from = From, epoch = Epoch}}, Reply) ->
    zraft_peer_route:reply_proxy(From, Reply#append_reply{from_peer = PeerID, request_ref = Ref, epoch = Epoch}).
fake_need_snapshot({_, #append_entries{request_ref = Ref, from = From}}) ->
    Snapshot = #install_snapshot{from = From, request_ref = Ref, data = [], epoch = 7, index = 3, term = 6},
    zraft_peer_route:reply_proxy(From, Snapshot).
fake_snapshot_reply(PeerID, {_, #install_snapshot{request_ref = Ref, from = From, epoch = Epoch}}, Reply) ->
    Reply1 = Reply#install_snapshot_reply{
        from_peer = PeerID,
        epoch = Epoch,
        request_ref = Ref
    },
    zraft_peer_route:reply_proxy(From, Reply1).

-endif.