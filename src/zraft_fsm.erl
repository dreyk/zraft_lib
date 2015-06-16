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
-module(zraft_fsm).
-author("dreyk").

-behaviour(gen_server).

%% API
-export([start_link/2]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-export([
    cmd/2,
    apply_commit/2,
    stop/1,
    set_state/2,
    stat/1
]).

-include_lib("zraft_lib/include/zraft.hrl").

-define(SNAPSHOT_HEADER_VERIOSN, 1).
-define(DATA_DIR, zraft_util:get_env(snapshot_dir, "data")).

-define(MAX_COUNT, zraft_util:get_env(max_log_count, 1000)).
-define(SNAPSHOT_BACKUP, zraft_util:get_env(snapshot_backup, false)).

-define(INFO(State, S, As), ?MINFO("~p: " ++ S, [print_id(State) | As])).
-define(INFO(State, S), ?MINFO("~p: " ++ S, [print_id(State)])).
-define(ERROR(State, S, As), ?MERROR("~p: " ++ S, [print_id(State) | As])).
-define(ERROR(State, S), ?MERROR("~p: " ++ S, [print_id(State)])).
-define(DEBUG(State, S, As), ?MDEBUG("~p: " ++ S, [print_id(State) | As])).
-define(DEBUG(State, S), ?MDEBUG("~p: " ++ S, [print_id(State)])).
-define(WARNING(State, S, As), ?MWARNING("~p: " ++ S, [print_id(State) | As])).
-define(WARNING(State, S), ?MWARNING("~p: " ++ S, [print_id(State)])).


-record(state, {
    raft,
    raft_state = follower,
    sessions,
    watchers,
    monitors,
    ustate,
    back_end,
    last_index = 0,
    last_snapshot_index = 0,
    log_count = 0,
    max_count,
    active_snapshot,
    dir,
    snapshot_count = 0,
    last = 0,
    last_dir = []}).
-record(snapshoter, {pid, seq, last_index, dir, mref, log_count, file, type, from}).

-spec start_link(zraft_consensus:from_peer_addr(), module()) -> {ok, pid()} | {error, term()}.

start_link(Raft, BackEnd) ->
    gen_server:start_link(?MODULE, [Raft, BackEnd], []).

-spec set_state(pid(), atom()) -> ok.
set_state(P, StateName) ->
    gen_server:cast(P, {set_state, StateName}).

-spec apply_commit(pid(), list(#entry{})) -> ok.
apply_commit(P, Entries) ->
    gen_server:cast(P, {append, Entries}).

-spec stop(pid()) -> ok.
stop(P) ->
    gen_server:call(P, stop).

-spec cmd(pid(), term()) -> ok.
cmd(FSM, Request) ->
    gen_server:cast(FSM, Request).

-spec stat(pid())->#fsm_stat{}.
stat(P)->
    gen_server:call(P,stat).

init([Raft, BackEnd]) ->
    gen_server:cast(self(), init),
    State = #state{
        back_end = BackEnd,
        raft = Raft,
        max_count = ?MAX_COUNT
    },
    {ok, State}.

delayed_init(State = #state{raft = Raft}) ->
    PeerID = zraft_util:peer_id(Raft),
    Dir = filename:join([?DATA_DIR, zraft_util:peer_name(PeerID), "snapshots"]),
    ok = zraft_util:make_dir(Dir),
    Seq = clean_dir(Dir),
    WatchersTable = ets:new(watcher_table_name(State), [bag, {write_concurrency, false}, {read_concurrency, false}]),
    State1 = install_snapshot(State#state{
        snapshot_count = Seq + 1,
        last = Seq,
        dir = Dir,
        watchers = WatchersTable
    }),
    {noreply, State1}.

handle_call(stat,_From,State=#state{sessions = S,monitors = M,watchers = W})->
    S1 = ets:info(S,size),
    S2 = ets:info(M,size),
    S3 = ets:info(W,size),
    {reply,#fsm_stat{session_number = S2,tmp_count = S1,watcher_number = S3},State};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(init, State) ->
    delayed_init(State);
handle_cast({set_state, StateName}, State) ->
    change_raft_state(StateName, State);
handle_cast({copy_timeout, From, Index},
    State = #state{active_snapshot = #snapshoter{from = From, last_index = Index}}) ->
    State1 = discard_snapshot({error, hearbeat_fail}, State),
    {noreply, State1};
handle_cast({append, Entries}, State) ->
    append(Entries, State);
handle_cast(Req = #install_snapshot{data = prepare}, State) ->
    case read_last_snapshot_info(State) of
        {ok, #snapshot_info{index = Index}} ->
            Req1 = Req#install_snapshot{index = Index, data = State#state.last_dir},
            zraft_peer_route:reply_proxy(Req1#install_snapshot.from, Req1),
            {noreply, State};
        Else ->
            {stop, Else, State}
    end;
handle_cast(Req = #install_snapshot{data = start}, State) ->
    prepare_install_snapshot(Req, State);
handle_cast(Req = #install_snapshot{data = hearbeat}, State) ->
    hearbeat_install_snapshot(Req, State);
handle_cast(Req = #install_snapshot{data = finish}, State) ->
    finish_install_snapshot(Req, State);
handle_cast(Req = #read{}, State) ->
    read(Req, State);
handle_cast(_, State) ->
    {noreply, State}.

handle_info({timeout, _, {'$zraft_timeout', Event}}, State) ->
    handle_cast(Event, State);
handle_info({'DOWN', Ref, process, _, normal},
    State = #state{active_snapshot = #snapshoter{mref = Ref, type = copy}}) ->
    ?INFO(State, "Snapshot transfer has finished"),
    #state{active_snapshot = Snapshot} = State,
    zraft_util:gen_server_cast_after(
        zraft_consensus:get_election_timeout(),
        {copy_timeout, Snapshot#snapshoter.from, Snapshot#snapshoter.last_index}
    ),
    Snapshot1 = Snapshot#snapshoter{pid = undefined, mref = undefined},
    {noreply, State#state{active_snapshot = Snapshot1}};
handle_info({'DOWN', Ref, process, _, normal},
    State = #state{active_snapshot = #snapshoter{mref = Ref}}) ->
    ?INFO(State, "Snapshot has finished"),
    finish_snapshot(State);
handle_info({'DOWN', Ref, process, _, Error},
    State = #state{active_snapshot = #snapshoter{mref = Ref}}) ->
    ?ERROR(State, "Snapshot make/transfer has failed"),
    #snapshoter{type = Type} = State#state.active_snapshot,
    State1 = discard_snapshot(Error, State),
    if
        Type == copy ->
            {noreply, State1};
        true ->
            {stop, Error, State1}
    end;

handle_info({'DOWN', CRef, process, From, _}, State = #state{monitors = Monitors, raft_state = leader}) ->
    case ets:lookup(Monitors, From) of
        [{From, CRef, _, _}] ->
            zraft_consensus:send_swrite(State#state.raft, #swrite{data = CRef, from = From, message_id = ?EXPIRE_SESSION});
        [] ->
            ok
    end,
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, State) ->
    discard_snapshot(Reason, State),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

safe_rename(undefined, _) ->
    ok;
safe_rename(File, Dest) ->
    zraft_util:del_dir(Dest),
    ok = file:rename(File, Dest).


read(#read{from = From, request = Query, watch = false}, State = #state{back_end = BackEnd, ustate = UState}) ->
    case BackEnd:query(Query, UState) of
        {ok, Res} ->
            reply_caller(From, {ok, Res});
        {ok, _, Res} ->
            reply_caller(From, {ok, Res});
        {error, Err} ->
            reply_caller(From, {ok, {error, Err}})
    end,
    {noreply, State};
read(#read{from = From, request = Query, watch = WatchRef, global_time = Time},
    State = #state{back_end = BackEnd, ustate = UState}) ->
    Caller = case From of
                 {To, _} ->
                     To;
                 _ when is_pid(From) ->
                     From
             end,
    case is_in_session(false, Caller, Time, State) of
        true ->
            case BackEnd:query(Query, UState) of
                {ok, WatchInfo, Res} ->
                    reply_caller(State, From, #sread_reply{data = Res, ref = WatchRef}),
                    register_watchers(WatchInfo, Caller, WatchRef, State);
                {ok, _Res} ->
                    reply_caller(State, From, #sread_reply{data = {error, watch_not_supported}, ref = WatchRef});
                {error, Err} ->
                    reply_caller(State, From, #sread_reply{data = {error, Err}, ref = WatchRef})
            end;
        false ->
            reply_caller(State, Caller, ?DISCONNECT_MSG)
    end,
    {noreply, State}.

register_watchers(WatchInfo, Caller, WatchRef, #state{watchers = WatchersTab}) ->
    Add = lists:foldl(fun(WatchKey, Acc) ->
        R = {{watch, WatchKey}, Caller, WatchRef},
        case ets:match(WatchersTab, R) of
            [] ->
                [R | Acc];
            _ ->
                Acc
        end end, [], WatchInfo),
    ets:insert(WatchersTab, Add).

trigger_watchers([], _) ->
    ok;
trigger_watchers(Updates, #state{watchers = WatchersTab}) ->
    L1 = lists:foldl(fun(K, Acc) ->
        case ets:lookup(WatchersTab, {watch, K}) of
            [] ->
                Acc;
            Watchers ->
                ets:delete(WatchersTab, {watch, K}),
                lists:foldl(fun({_, Caller, WatchRef}, Acc1) ->
                    [{Caller, WatchRef} | Acc1] end, Acc, Watchers)
        end end, [], Updates),
    L2 = lists:usort(L1),
    lists:foreach(fun({Caller, WatchRef}) ->
        Caller ! #swatch_trigger{ref = WatchRef, reason = change} end, L2).

trigger_all_watchers(#state{watchers = WatchersTab}) ->
    L1 = ets:foldl(fun({_, Caller, WatchRef}, Acc) ->
        [{Caller, WatchRef} | Acc] end, [], WatchersTab),
    L2 = lists:usort(L1),
    lists:foreach(fun({Caller, WatchRef}) ->
        Caller ! #swatch_trigger{ref = WatchRef, reason = change_leader} end, L2).

append([], State) ->
    {noreply, State};
append(Entries, State) ->
    #state{
        last_index = LastIndex,
        back_end = BackEnd,
        ustate = UState,
        log_count = Count
    } = State,
    {UState1, Count1, LastIndex1, GlobalTime1} = lists:foldl(fun(E, {UStateAcc, CountAcc, IndexAcc, TimeAcc}) ->
        #entry{index = EI, type = Type, global_time = Time} = E,
        if
            EI =< LastIndex ->
                ?WARNING(State, "Try apply old entry ~p then last applied~p", [E#entry.index, LastIndex]),
                exit({error, old_entry}),
                {UStateAcc, CountAcc, IndexAcc, TimeAcc};
            true ->
                if
                    Type == ?OP_DATA ->
                        NewUState = apply_data(Time, E#entry.data, BackEnd, UStateAcc, State),
                        {NewUState, CountAcc + 1, EI, Time};
                    true ->
                        {UStateAcc, CountAcc + 1, EI, Time}
                end
        end end, {UState, Count, LastIndex, 0}, Entries),
    State1 = State#state{last_index = LastIndex1, ustate = UState1},
    State2 =
        if
            GlobalTime1 > 0 ->
                expire_sessions(GlobalTime1, State1);
            true ->
                State1
        end,
    maybe_take_snapshost(Count1, State2).

%%no session. It's not nedeed to
apply_data(_GlabalTime, Req = #write{}, BackEnd, UState, State = #state{raft_state = RaftState}) ->
    #write{data = Data, from = From} = Req,
    {Result, UState1} = apply_data(false, From, Data, BackEnd, UState, State),
    reply_caller(RaftState, From, {ok, Result}),
    UState1;
apply_data(GlobalTime, Req = #swrite{}, BackEnd, UState, State) ->
    #swrite{
        from = From,
        message_id = MsgID,
        data = Data,
        acc_upto = AccUpTo,
        temporary = Temporary
    } = Req,
    #state{monitors = Monitors, raft_state = RaftState, sessions = Sessions} = State,
    if
        MsgID == ?EXPIRE_SESSION ->
            case ets:lookup(Monitors, From) of
                [{_, _, _, _}] ->
                    expire_session(From, BackEnd, UState, State);
                _ ->
                    %%already expired
                    UState
            end;
        MsgID == ?CLIENT_CONNECT ->
            case ets:lookup(Monitors, From) of
                [{_, _, ExpiredAt, _}] when ExpiredAt > GlobalTime ->
                    reply_caller(RaftState, From, #swrite_error{sequence = MsgID, error = exists}),
                    UState;
                [{_, _, ExpiredAt, _}] when ExpiredAt =< GlobalTime ->
                    %%expire session and create new
                    UState1 = expire_session_data(From, BackEnd, UState, State),
                    reply_caller(RaftState, From, #swrite_reply{sequence = MsgID, data = ok}),
                    create_new_session(From, GlobalTime, Data, State),
                    UState1;
                [] ->
                    reply_caller(RaftState, From, #swrite_reply{sequence = MsgID, data = ok}),
                    create_new_session(From, GlobalTime, Data, State),
                    UState
            end;
        MsgID == ?CLIENT_CLOSE ->
            expire_session(From, BackEnd, UState, State);
        MsgID == ?CLIENT_PING ->
            case is_in_session(true, From, GlobalTime, State) of
                true ->
                    acc_session(AccUpTo, From, State),
                    reply_caller(State, From, #swrite_reply{data = Data, sequence = MsgID}),
                    UState;
                false ->
                    expire_session(From, BackEnd, UState, State)
            end;
        is_integer(MsgID) ->
            case is_in_session(true, From, GlobalTime, State) of
                true ->
                    case ets:match(Sessions, {{reply, From}, MsgID, '$1'}) of
                        [] ->
                            {Result, UState1} = apply_data(Temporary, From, Data, BackEnd, UState, State),
                            ets:insert(Sessions, {{reply, From}, MsgID, Result});
                        [[PrevResult]] ->
                            Result = PrevResult,
                            UState1 = UState
                    end,
                    reply_caller(State#state.raft_state, From, #swrite_reply{data = Result, sequence = MsgID}),
                    UState1;
                false ->
                    expire_session(From, BackEnd, UState, State)
            end;
        true ->
            ?WARNING(State, "Invalid messageid ~p", [MsgID]),
            UState
    end;
apply_data(_GlobalTime, Else, _BackEnd, UState, State) ->
    ?WARNING(State, "Unknow data value ~p", [Else]),
    UState.

is_in_session(UpdateSession, From, Time,#state{monitors = Monitors}) ->
    case ets:lookup(Monitors, From) of
        [{_, MRef, ExpiredAt, Timeout}] when Time < ExpiredAt andalso UpdateSession ->
            ets:insert(Monitors, {From, MRef, Time + Timeout, Timeout}),
            true;
        [{_, _, ExpiredAt, _Timeout}] when Time < ExpiredAt ->
            true;
        _ ->
            false
    end.

create_new_session(From, GlobalTime, Timeout, State) ->
    MRef = if
               State#state.raft_state == leader ->
                   erlang:monitor(process, From);
               true ->
                   undefined
           end,
    %%register client process local
    true = ets:insert_new(State#state.monitors, {From, MRef, GlobalTime + Timeout, Timeout}).

apply_data(Temporary, From, Data, BackEnd, UState, State) ->
    Appl = if
               Temporary ->
                   BackEnd:apply_data(Data, From, UState);
               true ->
                   BackEnd:apply_data(Data, UState)
           end,
    case Appl of
        {R, U} ->
            {R, U};
        {R, UpdatedKeys, U} ->
            trigger_watchers(UpdatedKeys, State),
            {R, U}
    end.


acc_session(0, _From, _) ->
    ok;
acc_session(UpTo, From, #state{sessions = Sessions}) ->
    Match = [{{{reply, From}, '$1', '_'}, [{'=<', '$1', {const, UpTo}}], [true]}],
    ets:select_delete(Sessions, Match).

expire_sessions(GlobalTime, State = #state{monitors = Monitors, back_end = BackEnd, ustate = UState}) ->
    Match = [{{'$1', '_', '$2', '_'}, [{'=<', '$2', {const, GlobalTime}}], [['$1','$2']]}],
    Del = ets:select(Monitors, Match),
    UState1 = lists:foldl(fun([From,ExpireAt], UStateAcc) ->
        ?INFO(State,"Expire session ~p at ~p",[{From,ExpireAt},GlobalTime]),
        expire_session(From, BackEnd, UStateAcc, State)
    end, UState, Del),
    State#state{ustate = UState1}.

expire_session(From, BackEnd, Ustate, State = #state{raft_state = RaftState}) ->
    UState1 = expire_session_data(From, BackEnd, Ustate, State),
    reply_caller(RaftState, From, ?DISCONNECT_MSG),
    UState1.
expire_session_data(From, BackEnd, Ustate, State = #state{sessions = Sessions, monitors = Monitors, watchers = Watchers}) ->
    {ok, ExpiredKeys, UState1} = BackEnd:expire_session(From, Ustate),
    ets:delete(Sessions, {reply, From}),
    ets:match_delete(Watchers, {{watch, '_'}, From, '_'}),
    case ets:lookup(Monitors, From) of
        [{_, MRef, _, _}] ->
            demonitor_session(MRef),
            ets:delete(Monitors, From);
        [] ->
            ok
    end,
    trigger_watchers(ExpiredKeys, State),
    UState1.

clean_dir(Dir) ->
    {ok, Files} = file:list_dir(Dir),
    Last = lists:foldl(fun(F, Acc) ->
        FName = filename:join(Dir, F),
        case F of
            "snapshot.tmp-" ++ N ->
                zraft_util:del_dir(FName),
                last_snapshot(get_index(N), Acc);
            "archive.tmp-" ++ N ->
                file:delete(FName),
                last_snapshot(get_index(N), Acc);
            "snapshot-" ++ N ->
                last_snapshot(get_index(N), Acc);
            "archive-" ++ N ->
                last_snapshot(get_index(N), Acc);
            _ ->
                zraft_util:del_dir(FName),
                Acc
        end end, 0, Files),
    delelte_old(Last, Dir),
    Last.

delelte_old(0, Dir) ->
    {ok, Files} = file:list_dir(Dir),
    lists:foreach(fun(F) ->
        zraft_util:del_dir(filename:join(Dir, F)) end, Files);
delelte_old(Seq, Dir) ->
    {ok, Files} = file:list_dir(Dir),
    ArchiveName = "archive-" ++ integer_to_list(Seq),
    SnaphotName = "snapshot-" ++ integer_to_list(Seq),
    lists:foreach(fun(F) ->
        if
            F == ArchiveName ->
                ok;
            F == SnaphotName ->
                ok;
            true ->
                zraft_util:del_dir(filename:join(Dir, F))
        end end, Files).

last_snapshot(Seq1, Seq2) ->
    max(Seq1, Seq2).

get_index(T) ->
    case catch list_to_integer(T) of
        I when is_integer(I) ->
            I;
        _ ->
            0
    end.

maybe_take_snapshost(NewCount,
    State = #state{max_count = MaxCount,
        active_snapshot = Active, last_index = Index}) when NewCount >= MaxCount andalso Active == undefined andalso Index > 0 ->
    take_snapshost(NewCount, State#state{log_count = 0});
maybe_take_snapshost(NewCount, State) ->
    State1 = State#state{log_count = NewCount},
    {noreply, State1}.

finish_install_snapshot(Req = #install_snapshot{from = From, index = I}, State) ->
    Reply = install_answer(State#state.raft, Req),
    case State#state.active_snapshot of
        #snapshoter{from = From, last_index = I, type = copy} ->
            Return = finish_snapshot(State),
            zraft_peer_route:reply_proxy(From, Reply#install_snapshot_reply{result = finish}),
            Return;
        _ ->
            zraft_peer_route:reply_proxy(From, Reply),
            {noreply, State}
    end.

hearbeat_install_snapshot(Req = #install_snapshot{from = From, index = I}, State) ->
    Reply = install_answer(State#state.raft, Req),
    Reply1 = case State#state.active_snapshot of
                 #snapshoter{from = From, last_index = I, type = copy} ->
                     Reply#install_snapshot_reply{result = hearbeat};
                 _ ->
                     Reply
             end,
    zraft_peer_route:reply_proxy(From, Reply1),
    {noreply, State}.

prepare_install_snapshot(Req = #install_snapshot{index = Index},
    State = #state{last_snapshot_index = Last, raft = Raft}) when Index < Last ->
    %%snapshot is stale
    Reply = install_answer(Raft, Req),
    zraft_peer_route:reply_proxy(Req#install_snapshot.from, Reply),
    {noreply, State};
prepare_install_snapshot(Req, State = #state{dir = Dir, snapshot_count = SN, raft = Raft}) ->
    State1 = stop_snaphsoter(State),
    SnapshotDir = filename:join(Dir, "snapshot.tmp-" ++ integer_to_list(SN)),
    ok = zraft_util:make_dir(SnapshotDir),
    {ok, {{Addr, Port}, Writer}} = zraft_snapshot_receiver:start(print_id(State), SnapshotDir),
    Reply = install_answer(Raft, Req),
    Reply1 = Reply#install_snapshot_reply{
        addr = Addr,
        port = Port,
        result = start
    },
    zraft_peer_route:reply_proxy(Req#install_snapshot.from, Reply1),
    Mref = erlang:monitor(process, Writer),
    Snapshoter = #snapshoter{
        pid = Writer,
        dir = SnapshotDir,
        last_index = Req#install_snapshot.index,
        seq = SN,
        mref = Mref,
        from = Req#install_snapshot.from,
        type = copy},
    {noreply, State1#state{snapshot_count = SN + 1, active_snapshot = Snapshoter}}.

install_answer(Raft, #install_snapshot{epoch = E, term = T, request_ref = Ref, index = I}) ->
    #install_snapshot_reply{
        epoch = E,
        request_ref = Ref,
        from_peer = Raft,
        term = T,
        index = I,
        result = failed
    }.

take_snapshost(Count,
    State) ->
    #state{
        raft = Raft,
        back_end = BackEnd,
        ustate = UState,
        last_index = LastIndex,
        dir = Dir,
        snapshot_count = SN,
        sessions = Sessions,
        monitors = Monitors
    } = State,
    ResultFile = case ?SNAPSHOT_BACKUP of
                     true ->
                         filename:join(Dir, "archive.tmp-" ++ integer_to_list(SN));
                     _ ->
                         undefined
                 end,
    SnapshotDir = filename:join(Dir, "snapshot.tmp-" ++ integer_to_list(SN)),
    ok = zraft_util:make_dir(SnapshotDir),
    DataDir = filename:join(SnapshotDir, "data"),
    SessionsFile = filename:join(SnapshotDir, "sessions"),
    MonitorsFile = filename:join(SnapshotDir, "monitors"),
    ok = ets:tab2file(Monitors, MonitorsFile),
    ok = ets:tab2file(Sessions, SessionsFile),
    ok = zraft_util:make_dir(DataDir),
    {ok, Writer} = zraft_snapshot_writer:start(Raft, self(), LastIndex, ResultFile, SnapshotDir),
    Mref = erlang:monitor(process, Writer),
    UState1 = case BackEnd:snapshot(UState) of
                  {sync, Fun, UTmp} ->
                      ok = Fun(DataDir),
                      zraft_snapshot_writer:data_done(Writer),
                      UTmp;
                  {async, Fun, UTmp} ->
                      zraft_snapshot_writer:data(Writer, DataDir, Fun),
                      UTmp
              end,
    Snapshoter = #snapshoter{
        type = dump,
        pid = Writer,
        dir = SnapshotDir,
        last_index = LastIndex,
        seq = SN,
        mref = Mref,
        log_count = Count,
        from = self(),
        file = ResultFile},
    {noreply, State#state{snapshot_count = SN + 1, ustate = UState1, active_snapshot = Snapshoter}}.

read_last_snapshot_info(#state{last = 0}) ->
    {error, snapshot_empty};
read_last_snapshot_info(#state{last_dir = SnapshotDir}) ->
    zraft_snapshot_writer:read_snapshot_info(SnapshotDir);
read_last_snapshot_info(SnapshotDir) ->
    zraft_snapshot_writer:read_snapshot_info(SnapshotDir).

discard_snapshot(_, State = #state{active_snapshot = undefined}) ->
    State;
discard_snapshot(Reason, State = #state{back_end = BackEnd, ustate = UState}) ->
    #snapshoter{type = Type} = State#state.active_snapshot,
    ?ERROR(State, "Snapshot make/transfer has failed: ~p", [Reason]),
    State1 = stop_snaphsoter(State),
    if
        Type == copy ->
            State1;
        true ->
            {ok, UState1} = BackEnd:snapshot_failed(Reason, UState),
            State1#state{ustate = UState1}
    end.

stop_snaphsoter(State = #state{active_snapshot = undefined}) ->
    State;
stop_snaphsoter(State = #state{active_snapshot = S}) ->
    #snapshoter{pid = P, type = Type} = S,
    if
        is_pid(P) ->
            StopFun = if
                          Type == copy ->
                              fun() -> zraft_snapshot_receiver:stop(P) end;
                          true ->
                              fun() -> zraft_snapshot_writer:stop(P) end
                      end,
            stop_safe(P, StopFun, S);
        true ->
            stop_safe(P, fun() -> ok end, S)
    end,
    State#state{active_snapshot = undefined}.


stop_safe(P, Func, #snapshoter{dir = Dir, file = File}) ->
    case catch Func() of
        ok ->
            ok;
        _ ->
            exit(P, kill)
    end,
    zraft_util:del_dir(Dir),
    zraft_util:del_dir(File).

finish_snapshot(State) ->
    #state{active_snapshot = Snapshot, dir = Dir, back_end = BackEnd, ustate = UState} = State,
    #snapshoter{
        type = Type,
        last_index = LastSnapshotIndex,
        file = TmpBackUpName,
        dir = TmpSnapshotName,
        seq = Seq} = Snapshot,
    SSeq = integer_to_list(Seq),
    FinalBackupName = filename:join(Dir, "archive-" ++ SSeq),
    FinalSnapshotName = filename:join(Dir, "snapshot-" ++ SSeq),
    safe_rename(TmpBackUpName, FinalBackupName),
    safe_rename(TmpSnapshotName, FinalSnapshotName),
    State1 = State#state{
        active_snapshot = undefined,
        last_dir = FinalSnapshotName,
        last = Seq,
        last_snapshot_index = LastSnapshotIndex
    },
    if
        Type == copy ->
            State2 = install_snapshot(State1),
            {noreply, State2};
        true ->
            {ok, UState1} = BackEnd:snapshot_done(UState),
            State2 = State1#state{ustate = UState1},
            truncate_log(State2),
            {noreply, State2}
    end.

install_snapshot(State = #state{last = 0, back_end = BackEnd, raft = Raft}) ->
    Sessions = ets:new(session_table_name(State), [bag, {write_concurrency, false}, {read_concurrency, false}]),
    Monitors = ets:new(monitor_table_name(State), [ordered_set, {write_concurrency, false}, {read_concurrency, false}]),
    {ok, UState} = BackEnd:init(zraft_util:peer_id(Raft)),
    truncate_log(Raft, #snapshot_info{}),
    State#state{ustate = UState, sessions = Sessions, monitors = Monitors};
install_snapshot(State = #state{raft = Raft, ustate = Ustate, back_end = BackEnd, dir = Dir, last = Last}) ->
    SnapshotDir = filename:join(Dir, "snapshot-" ++ integer_to_list(Last)),
    DataDir = filename:join(SnapshotDir, "data"),
    {ok, Ustate1} = BackEnd:install_snapshot(DataDir, Ustate),
    {ok, SnaphotInfo} = read_last_snapshot_info(SnapshotDir),
    #snapshot_info{index = Index} = SnaphotInfo,
    State1 = State#state{
        ustate = Ustate1,
        last_index = Index,
        log_count = 0,
        last_dir = SnapshotDir,
        last_snapshot_index = Index
    },
    State2 = restore_sessions(State1),
    truncate_log(Raft, SnaphotInfo),
    State2.

restore_sessions(State = #state{last_dir = SnapshotDir}) ->
    MonitorFile = filename:join(SnapshotDir, "monitors"),
    SessionsFile = filename:join(SnapshotDir, "sessions"),
    R1 = ets:file2tab(MonitorFile),
    R2 = ets:file2tab(SessionsFile),
    case {R1, R2} of
        {{ok, T1}, {ok, T2}} ->
            if
                State#state.raft_state == leader ->
                    upgrade_monitors(T1);
                true ->
                    ok
            end,
            State#state{monitors = T1, sessions = T2};
        _ ->
            read_table_error(R1, MonitorFile, State),
            read_table_error(R2, SessionsFile, State),
            Sessions = ets:new(session_table_name(State), [bag, {write_concurrency, false}, {read_concurrency, false}]),
            Monitors = ets:new(monitor_table_name(State), [ordered_set, {write_concurrency, false}, {read_concurrency, false}]),
            State#state{monitors = Monitors, sessions = Sessions}
    end.

upgrade_monitors(Tab) ->
    ets:foldl(fun({From, _OldRef, _E, _T}, Acc) ->
        MRef = erlang:monitor(process, From),
        ets:update_element(Tab, From, {2, MRef}),
        Acc
    end, 0, Tab).

read_table_error({ok, _}, _File, _State) ->
    ok;
read_table_error(Error, File, State) ->
    ?ERROR(
        State,
        "Can't load sessions table from snapshot file ~p. Continue with empty sessions. Reason is ~p",
        [File, Error]
    ).

monitor_table_name(State) ->
    state_table_name("monitors", State).
watcher_table_name(State) ->
    state_table_name("watchers", State).
session_table_name(State) ->
    state_table_name("sessions", State).
state_table_name(Suffix, #state{raft = Raft}) ->
    {{Name, _}, _} = Raft,
    list_to_atom(atom_to_list(Name) ++ "_" ++ Suffix).

truncate_log(State = #state{raft = Raft, last_dir = SnapshotDir}) ->
    case read_last_snapshot_info(SnapshotDir) of
        {ok, Info} ->
            truncate_log(Raft, Info);
        _ ->
            ?ERROR(State, "Can't read last snapshot ~s", [SnapshotDir]),
            ok
    end.

truncate_log(Raft, SnapshotInfo) ->
    zraft_consensus:truncate_log(Raft, SnapshotInfo).

print_id(#state{raft = undefined}) ->
    test;
print_id(#state{raft = Raft}) ->
    zraft_util:peer_id(Raft).


reply_caller(#state{raft_state = RaftState}, From, Msg) ->
    reply_caller(RaftState, From, Msg);
reply_caller(RaftState, From, _) when RaftState /= leader orelse From == undefined ->
    ok;
reply_caller(leader, From, Msg) ->
    reply_caller(From, Msg).

reply_caller(undeined, _) ->
    ok;
reply_caller(From, Msg) ->
    zraft_consensus:reply_caller(From, Msg).

change_raft_state(RaftState, State = #state{raft_state = RaftState}) ->
    {noreply, State};
change_raft_state(NewRaftState, State = #state{raft_state = leader, watchers = W, monitors = M})
    when NewRaftState /= leader ->
    trigger_all_watchers(State),
    ets:delete_all_objects(W),
    ets:foldl(fun(O, Acc) ->
        case O of
            {_, MRef, _, _} ->
                demonitor_session(MRef);
            _ ->
                ok
        end, Acc end, 0, M),
    {noreply, State#state{raft_state = NewRaftState}};
change_raft_state(leader, State = #state{monitors = M, raft = Raft}) ->
    NewLeader = zraft_util:peer_id(Raft),
    upgrade_monitors(State#state.monitors),
    ets:foldl(fun(O, Acc) ->
        case O of
            {Caller, _, _, _} ->
                reply_caller(Caller, {leader, NewLeader});
            _ ->
                ok
        end, Acc end, 0, M),
    {noreply, State#state{raft_state = leader}};
change_raft_state(NewRaftState, State) ->
    {noreply, State#state{raft_state = NewRaftState}}.

demonitor_session(undefined)->
    ok;
demonitor_session(MRef)->
    erlang:demonitor(MRef).


-ifdef(TEST).

setup() ->
    application:set_env(zraft_lib, max_log_count, 10),
    application:set_env(zraft_lib, snapshot_backup, true),
    zraft_util:set_test_dir("test-snapshot"),
    ok.
clear_setup(_) ->
    application:unset_env(zraft_lib, max_log_count),
    application:unset_env(zraft_lib, snapshot_backup),
    zraft_util:clear_test_dir("test-snapshot"),
    ok.

backend_test_() ->
    {
        setup,
        fun setup/0,
        fun clear_setup/1,
        fun(_X) ->
            [
                read_write(),
                snapshot()
            ]
        end
    }.

read_write() ->
    {"read_write_test", fun() ->
        Raft = {{test, node()}, self()},
        {ok, P} = start_link(Raft, zraft_dict_backend),
        set_state(P, leader),
        receive
            {'$gen_all_state_event', SnapshotInfo} ->
                ?assertMatch(#snapshot_info{index = 0, conf = ?BLANK_CONF, conf_index = 0, term = 0}, SnapshotInfo);
            Else ->
                ?assertMatch(result, Else)
        after 2000 ->
            ?assert(false)
        end,
        apply_commit(P, [#entry{index = 1, data = #write{data = {1, "1"}}, term = 1, type = ?OP_DATA}]),
        Stat = sys:get_state(P),
        ?assertMatch(#state{last_index = 1, last_snapshot_index = 0, log_count = 1}, Stat),
        Ref = make_ref(),
        Me = {self(), Ref},
        cmd(P, #read{from = Me, request = 1}),
        receive
            {Ref, Res} ->
                ?assertMatch({ok, {ok, "1"}}, Res);
            Else1 ->
                ?assertMatch(result, Else1)
        after 1000 ->
            ?assert(false)
        end,
        cmd(P, #read{from = Me, request = 2}),
        receive
            {Ref, Res1} ->
                ?assertMatch({ok, not_found}, Res1);
            Else2 ->
                ?assertMatch(result, Else2)
        after 1000 ->
            ?assert(false)
        end,
        ok = stop(P)
    end}.

snapshot() ->
    {"snapshot", fun() ->
        Raft = {{test, node()}, self()},
        {ok, P} = start_link(Raft, zraft_dict_backend),
        set_state(P, leader),
        receive
            {'$gen_all_state_event', SnapshotInfo} ->
                ?assertMatch(#snapshot_info{index = 0, conf = ?BLANK_CONF, conf_index = 0, term = 0}, SnapshotInfo);
            Else ->
                ?assertMatch(result, Else)
        after 2000 ->
            ?assert(false)
        end,
        Ref = make_ref(),
        Me = {self(), Ref},
        cmd(P, #read{from = Me, request = 1}),
        receive
            {Ref, Res} ->
                ?assertMatch({ok, not_found}, Res);
            Else1 ->
                ?assertMatch(result, Else1)
        after 1000 ->
            ?assert(false)
        end,
        apply_commit(P, [#entry{index = I, data = #write{data = {I, integer_to_list(I)}}, term = 1, type = ?OP_DATA} || I <- lists:seq(1, 10)]),
        receive
            {'$gen_event', {make_snapshot_info, {ReqRef1, From}, Index}} ->
                ?assertEqual(10, Index),
                From ! {ReqRef1, #snapshot_info{index = Index, conf = [], term = 1, conf_index = 1}};
            Else2 ->
                ?assertMatch(result, Else2)
        after 2000 ->
            ?assert(false)
        end,
        cmd(P, #read{from = Me, request = 1}),
        receive
            {Ref, Res1} ->
                ?assertMatch({ok, {ok, "1"}}, Res1);
            Else3 ->
                ?assertMatch(result, Else3)
        after 1000 ->
            ?assert(false)
        end,
        receive
            {'$gen_all_state_event', SnapshotInfo1} ->
                ?assertMatch(#snapshot_info{index = 10, conf = [], conf_index = 1, term = 1}, SnapshotInfo1);
            Else4 ->
                ?assertMatch(result, Else4)
        after 3000 ->
            ?assert(false)
        end,
        ok = stop(P),
        {ok, P1} = start_link(Raft, zraft_dict_backend),
        set_state(P1, leader),
        receive
            {'$gen_all_state_event', SnapshotInfo2} ->
                ?assertMatch(#snapshot_info{index = 10, conf = [], conf_index = 1, term = 1}, SnapshotInfo2);
            Else5 ->
                ?assertMatch(result, Else5)
        after 2000 ->
            ?assert(false)
        end,
        cmd(P1, #read{from = Me, request = 10}),
        receive
            {Ref, Res3} ->
                ?assertMatch({ok, {ok, "10"}}, Res3);
            Else6 ->
                ?assertMatch(result, Else6)
        after 1000 ->
            ?assert(false)
        end,
        Stat1 = sys:get_state(P1),
        ?assertMatch(#state{last_index = 10, last_snapshot_index = 10, log_count = 0}, Stat1),
        ok = stop(P1)
    end}.

setup_sessions() ->
    meck:new(zraft_dict_backend, [passthrough]),
    #state{back_end = zraft_dict_backend, raft_state = leader, ustate = dict:new()}.
stop_sessions(_) ->
    meck:unload(zraft_dict_backend),
    ok.

sessions_test_() ->
    {
        setup,
        fun setup_sessions/0,
        fun stop_sessions/1,
        fun(X) ->
            [
                session_write(X),
                session_read_watch(X),
                session_fail(X)
            ]
        end
    }.

create_test_tables(State) ->
    Sessions = ets:new(sessions, [bag, {write_concurrency, false}, {read_concurrency, false}]),
    Watchers = ets:new(watchers, [bag, {write_concurrency, false}, {read_concurrency, false}]),
    Monitor = ets:new(monitor, [ordered_set, {write_concurrency, false}, {read_concurrency, false}]),
    State#state{sessions = Sessions, monitors = Monitor, watchers = Watchers}.

session_write(State0) ->
    {"test session write", fun() ->
        State = #state{sessions = Sessions, monitors = Monitor} = create_test_tables(State0),
        Me = self(),
        OpenReq = #swrite{data = 10, from = Me, message_id = ?CLIENT_CONNECT},
        meck:expect(zraft_dict_backend, apply_data, fun(_, St1) -> {new_result1, St1} end),
        apply_data(0, OpenReq, zraft_dict_backend, [], State),
        OpenRes1 = wait_result(),
        ?assertMatch(#swrite_reply{sequence = ?CLIENT_CONNECT, data = ok}, OpenRes1),
        S1 = ets:lookup(Monitor, Me),
        ?assertMatch([{Me, _, 10, 10}], S1),
        W1 = #swrite{data = 1, acc_upto = 0, from = Me, message_id = 1},
        apply_data(1, W1, zraft_dict_backend, [], State),
        R1 = wait_result(),
        ?assertMatch(#swrite_reply{sequence = 1, data = new_result1}, R1),
        S2 = ets:lookup(Monitor, Me),
        ?assertMatch([{Me, _, 11, 10}], S2),
        meck:expect(zraft_dict_backend, apply_data, fun(_, St2) -> {new_result2, St2} end),
        apply_data(2, W1, zraft_dict_backend, [], State),
        R2 = wait_result(),
        ?assertMatch(#swrite_reply{sequence = 1, data = new_result1}, R2),
        W2 = #swrite{data = 1, acc_upto = 0, from = Me, message_id = 2},
        apply_data(3, W2, zraft_dict_backend, [], State),
        R3 = wait_result(),
        ?assertMatch(#swrite_reply{sequence = 2, data = new_result2}, R3),
        S3 = ets:lookup(Monitor, Me),
        ?assertMatch([{Me, _, 13, 10}], S3),
        Vals1 = lists:keysort(2, ets:tab2list(Sessions)),
        ?assertMatch([{{reply, _}, 1, new_result1}, {{reply, _}, 2, new_result2}], Vals1),
        P1 = #swrite{data = ping, acc_upto = 2, from = Me, message_id = ?CLIENT_PING},
        apply_data(4, P1, zraft_dict_backend, [], State),
        R4 = wait_result(),
        ?assertMatch(#swrite_reply{data = ping, sequence = ?CLIENT_PING}, R4),
        Vals2 = lists:keysort(2, ets:tab2list(Sessions)),
        ?assertMatch([], Vals2),

        apply_data(3, W2, zraft_dict_backend, [], State),
        R5 = wait_result(),
        ?assertMatch(#swrite_reply{sequence = 2, data = new_result2}, R5),
        Vals3 = lists:keysort(2, ets:tab2list(Sessions)),
        ?assertMatch([{{reply, _}, 2, new_result2}], Vals3),
        expire_sessions(15, State),
        R6 = wait_result(),
        ?assertEqual(?DISCONNECT_MSG, R6),
        Vals4 = lists:keysort(2, ets:tab2list(Sessions)),
        ?assertMatch([], Vals4),
        Vals5 = ets:tab2list(Monitor),
        ?assertMatch([], Vals5)
    end}.

session_read_watch(State0) ->
    {"test session read_watch", fun() ->
        State = #state{monitors = Monitor, watchers = Watchers} = create_test_tables(State0),
        Me = self(),
        Read = #read{from = Me, global_time = 1, watch = ref1},
        read(Read, State),
        RRes1 = wait_result(),
        ?assertEqual(?DISCONNECT_MSG, RRes1),
        OpenReq = #swrite{data = 10, from = Me, message_id = ?CLIENT_CONNECT},
        apply_data(0, OpenReq, zraft_dict_backend, dict:new(), State),
        OpenRes1 = wait_result(),
        ?assertMatch(#swrite_reply{sequence = ?CLIENT_CONNECT, data = ok}, OpenRes1),
        S1 = ets:lookup(Monitor, Me),
        ?assertMatch([{Me, _, 10, 10}], S1),
        meck:expect(zraft_dict_backend, query, fun(_, _) ->
            {ok, [trigger1], ok} end),
        meck:expect(zraft_dict_backend, apply_data, fun(_, St1) -> {new_result1, [trigger1], St1} end),
        read(Read, State),
        RRes2 = wait_result(),
        ?assertEqual(#sread_reply{data = ok, ref = ref1}, RRes2),
        Vals1 = ets:tab2list(Watchers),
        ?assertMatch([{{watch, trigger1}, _, ref1}], Vals1),
        W1 = #swrite{data = 1, acc_upto = 0, from = Me, message_id = 1},
        apply_data(1, W1, zraft_dict_backend, dict:new(), State),
        Trigger1 = wait_result(),
        ?assertMatch(#swatch_trigger{ref = ref1}, Trigger1),
        WRes1 = wait_result(),
        ?assertMatch(#swrite_reply{sequence = 1, data = new_result1}, WRes1),
        Vals2 = ets:tab2list(Watchers),
        ?assertEqual([], Vals2)
    end}.

session_fail(State0) ->
    {"test session fail", fun() ->
        Me = self(),
        State = #state{monitors = Monitor} = create_test_tables(State0#state{raft = Me}),
        OpenReq = #swrite{data = 10, from = Me, message_id = ?CLIENT_CONNECT},
        apply_data(0, OpenReq, zraft_dict_backend, [], State),
        OpenRes1 = wait_result(),
        ?assertMatch(#swrite_reply{sequence = ?CLIENT_CONNECT, data = ok}, OpenRes1),
        [{Me, MRef, 10, 10}] = ets:lookup(Monitor, Me),
        W1 = #swrite{data = {1, 1}, acc_upto = 0, from = Me, message_id = 1, temporary = true},
        apply_data(1, W1, zraft_dict_backend, dict:new(), State),
        WRes1 = wait_result(),
        ?assertMatch(#swrite_reply{sequence = 1, data = ok}, WRes1),
        handle_info({'DOWN', MRef, process, Me, test}, State),
        RepCmd = wait_result(),
        ExpMsg = #swrite{message_id = ?EXPIRE_SESSION, data = MRef, from = Me},
        ?assertEqual({'$gen_all_state_event', ExpMsg}, RepCmd),
        meck:expect(zraft_dict_backend, expire_session, fun(From, St1) -> From ! expired, {ok, [], St1} end),
        apply_data(2, ExpMsg, zraft_dict_backend, [], State),
        R1 = wait_result(),
        ?assertEqual(expired, R1),
        R2 = wait_result(),
        ?assertEqual(?DISCONNECT_MSG, R2)

    end}.

wait_result() ->
    receive
        A ->
            A
    end.

-endif.