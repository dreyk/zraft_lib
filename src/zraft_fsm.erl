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
    stop/1
]).

-include_lib("zraft_lib/include/zraft.hrl").

-define(SNAPSHOT_HEADER_VERIOSN, 1).
-define(DATA_DIR, zraft_util:get_env(snapshot_dir, "data")).

-define(MAX_COUNT, zraft_util:get_env(max_log_count, 1000)).
-define(SNAPSHOT_BACKUP, zraft_util:get_env(snapshot_backup, false)).

-define(INFO(State,S, As),?MINFO("~p: "++S,[print_id(State)|As])).
-define(INFO(State,S), ?MINFO("~p: "++S,[print_id(State)])).
-define(ERROR(State,S, As),?MERROR("~p: "++S,[print_id(State)|As])).
-define(ERROR(State,S), ?MERROR("~p: "++S,[print_id(State)])).
-define(DEBUG(State,S, As),?MDEBUG("~p: "++S,[print_id(State)|As])).
-define(DEBUG(State,S), ?MDEBUG("~p: "++S,[print_id(State)])).
-define(WARNING(State,S, As),?MWARNING("~p: "++S,[print_id(State)|As])).
-define(WARNING(State,S), ?MWARNING("~p: "++S,[print_id(State)])).

-record(state, {
    raft,
    sessions,
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

-spec start_link(zraft_concensus:from_peer_addr(), module()) -> {ok, pid()} | {error, term()}.

start_link(Raft, BackEnd) ->
    gen_server:start_link(?MODULE, [Raft, BackEnd], []).

-spec apply_commit(pid(), list(#entry{})) -> ok.
apply_commit(P, Entries) ->
    gen_server:cast(P, {append, Entries}).

-spec stop(pid()) -> ok.
stop(P) ->
    gen_server:call(P, stop).

-spec cmd(pid(), term()) -> ok.
cmd(FSM, Request) ->
    gen_server:cast(FSM, Request).

init([Raft, BackEnd]) ->
    gen_server:cast(self(),init),
    State = #state{
        back_end = BackEnd,
        raft = Raft,
        max_count = ?MAX_COUNT
    },
    {ok, State}.

delayed_init(State=#state{raft = Raft})->
    PeerID = zraft_util:peer_id(Raft),
    Dir = filename:join([?DATA_DIR, zraft_util:peer_name(PeerID), "snapshots"]),
    ok = zraft_util:make_dir(Dir),
    Seq = clean_dir(Dir),
    State1 = install_snapshot(State#state{
        snapshot_count = Seq + 1,
        last = Seq,
        dir = Dir
    }),
    {noreply, State1}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(init,State)->
    delayed_init(State);
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
            {noreply,State};
        Else ->
            {stop, Else, State}
    end;
handle_cast(Req = #install_snapshot{data = start}, State) ->
    prepare_install_snapshot(Req, State);
handle_cast(Req = #install_snapshot{data = hearbeat}, State) ->
    hearbeat_install_snapshot(Req, State);
handle_cast(Req = #install_snapshot{data = finish}, State) ->
    finish_install_snapshot(Req, State);
handle_cast(#leader_read_request{from = From, request = Query}, State = #state{back_end = BackEnd, ustate = UState}) ->
    case BackEnd:query(Query, UState) of
        {ok, Res} ->
            reply_caller(From, Res);
        {error, Err} ->
            reply_caller(From, {error, Err})
    end,
    {noreply, State};
handle_cast(_, State) ->
    {noreply, State}.

handle_info({timeout,_,{'$zraft_timeout', Event}},State)->
    handle_cast(Event,State);
handle_info({'DOWN', Ref, process, _, normal},
    State = #state{active_snapshot = #snapshoter{mref = Ref, type = copy}}) ->
    ?INFO(State,"Snapshot transfer has finished"),
    #state{active_snapshot = Snapshot} = State,
    zraft_util:gen_server_cast_after(
        zraft_consensus:get_election_timeout(),
        {copy_timeout, Snapshot#snapshoter.from, Snapshot#snapshoter.last_index}
    ),
    Snapshot1 = Snapshot#snapshoter{pid = undefined, mref = undefined},
    {noreply, State#state{active_snapshot = Snapshot1}};
handle_info({'DOWN', Ref, process, _, normal},
    State = #state{active_snapshot = #snapshoter{mref = Ref}}) ->
    ?INFO(State,"Snapshot has finished"),
    finish_snapshot(State);
handle_info({'DOWN', Ref, process, _, Error},
    State = #state{active_snapshot = #snapshoter{mref = Ref}}) ->
    ?ERROR(State,"Snapshot make/transfer has failed"),
    #snapshoter{type = Type} = State#state.active_snapshot,
    State1 = discard_snapshot(Error, State),
    if
        Type == copy ->
            {noreply, State1};
        true ->
            {stop, Error, State1}
    end;
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


append([], State) ->
    {noreply, State};
append(Entries, State) ->
    #state{last_index = LastIndex, back_end = BackEnd, ustate = UState, log_count = Count} = State,
    {UState1,Count1, LastIndex1} = lists:foldl(fun(E, {UStateAcc,CountAcc,IndexAcc}) ->
        #entry{index = EI,type = Type} = E,
        if
            EI =< LastIndex->
                ?WARNING(State,"Try applt old entry ~p then last applied~p",[E#entry.index,LastIndex]),
                exit({error,old_entry}),
                {UStateAcc,CountAcc,IndexAcc};
            true->
                if
                    Type==?OP_DATA->
                        case E#entry.data of
                            #write{data = Data,from = From}->
                                {Result,NewUState}=BackEnd:apply_data(Data,UStateAcc),
                                reply_caller(From,Result),
                                {NewUState,CountAcc+1,EI};
                            Else->
                                ?WARNING(State,"Unknow data value ~p",[Else]),
                                {UStateAcc,CountAcc+1,EI}
                        end;
                    true->
                        {UStateAcc,CountAcc+1,EI}
                end
        end end,{UState, Count, LastIndex}, Entries),
    maybe_take_snapshost(Count1, State#state{last_index = LastIndex1, ustate = UState1}).


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
        active_snapshot = Active,last_index = Index}) when NewCount >= MaxCount andalso Active == undefined andalso Index>0 ->
    take_snapshost(NewCount, State#state{log_count = 0});
maybe_take_snapshost(NewCount, State) ->
    State1 = State#state{log_count = NewCount},
    {noreply, State1}.

finish_install_snapshot(Req = #install_snapshot{from = From, index = I}, State) ->
    Reply = install_answer(State#state.raft, Req),
    case State#state.active_snapshot of
        #snapshoter{from = From, last_index = I, type = copy} ->
            Return  = finish_snapshot(State),
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
    {ok, {{Addr, Port}, Writer}} = zraft_snapshot_receiver:start(print_id(State),SnapshotDir),
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

install_answer(Raft,#install_snapshot{epoch = E, term = T, request_ref = Ref, index = I}) ->
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
        sessions = Sessions
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
    SessionsFile = filename:join(SnapshotDir,"sessions"),
    ok = ets:tab2file(Sessions,SessionsFile),
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
discard_snapshot(Reason, State = #state{ back_end = BackEnd, ustate = UState}) ->
    #snapshoter{type = Type} = State#state.active_snapshot,
    ?ERROR(State,"Snapshot make/transfer has failed: ~p", [Reason]),
    State1 = stop_snaphsoter(State),
    if
        Type == copy ->
            State1;
        true ->
            {ok, UState1} = BackEnd:snapshot_failed(Reason,UState),
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
            State2 = State1#state{ustate  = UState1},
            truncate_log(State2),
            {noreply, State2}
    end.

install_snapshot(State = #state{last = 0, back_end = BackEnd, raft = Raft}) ->
    Sessions = ets:new(session_table_name(State),[ordered_set,{write_concurrency,false},{read_concurrency,false}]),
    {ok, UState} = BackEnd:init(zraft_util:peer_id(Raft)),
    truncate_log(Raft,#snapshot_info{}),
    State#state{ustate = UState,sessions = Sessions};
install_snapshot(State = #state{raft = Raft,ustate = Ustate, back_end = BackEnd, dir = Dir, last = Last}) ->
    SnapshotDir = filename:join(Dir, "snapshot-" ++ integer_to_list(Last)),
    DataDir = filename:join(SnapshotDir, "data"),
    SessionsFile = filename:join(SnapshotDir, "sessions"),
    {ok, Ustate1} = BackEnd:install_snapshot(DataDir, Ustate),
    {ok, SnaphotInfo} = read_last_snapshot_info(SnapshotDir),
    #snapshot_info{index = Index} = SnaphotInfo,
    truncate_log(Raft,SnaphotInfo),
    Sessions = case ets:file2tab(SessionsFile) of
                   {ok,Tab}->
                       %%expire_sessions(Tab),
                       Tab;
                   Error->
                       ?ERROR(
                           State,
                           "Can't load sessions from snapshot ~p. Continue with empty sessions. Reason is ~p",
                           [SessionsFile,Error]
                       ),
                       ets:new(session_table_name(State),[ordered_set,{write_concurrency,false},{read_concurrency,false}])
    end,
    State#state{
        ustate = Ustate1,
        last_index = Index,
        log_count = 0,
        last_dir = SnapshotDir,
        last_snapshot_index = Index,
        sessions = Sessions
    }.

session_table_name(#state{raft = Raft})->
    {{Name,_},_}=Raft,
    list_to_atom(atom_to_list(Name)++"_sessions").

truncate_log(State=#state{raft = Raft,last_dir = SnapshotDir})->
    case read_last_snapshot_info(SnapshotDir) of
        {ok,Info}->
            truncate_log(Raft,Info);
        _->
            ?ERROR(State,"Can't read last snapshot ~s",[SnapshotDir]),
            ok
    end.

truncate_log(Raft,SnapshotInfo)->
    zraft_consensus:truncate_log(Raft,SnapshotInfo).

print_id(#state{raft = Raft})->
    zraft_util:peer_id(Raft).

%% expire_sessions(Sessions)->
%%     Now = zraft_util:now_millisec()+1,
%%     Match = [{{{session,'$1'},'$2','$3'},[{'<','$3',{const,Now}}],['$1','$2']}],
%%     case ets:select(Sessions,Match) of
%%         []->
%%             ok;
%%         Expire->
%%             ets:delete(Sessions,{})
%%     end.
%%
%% expire_session(MRef,Sessions)->
%%     L = ets:match(Sessions,{{MRef,'$1'},'$2','_'}).

reply_caller(undefined,_)->
    ok;
reply_caller(From,Msg)->
    gen_fsm:reply(From,Msg).


-ifdef(TEST).

setup() ->
    application:set_env(zraft_lib,max_log_count,10),
    application:set_env(zraft_lib,snapshot_backup,true),
    zraft_util:set_test_dir("test-snapshot"),
    ok.
clear_setup(_) ->
    application:unset_env(zraft_lib,max_log_count),
    application:unset_env(zraft_lib,snapshot_backup),
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
        receive
            {'$gen_all_state_event',SnapshotInfo}->
                ?assertMatch(#snapshot_info{index = 0,conf = ?BLANK_CONF,conf_index = 0,term = 0},SnapshotInfo);
            Else->
                ?assertMatch(result,Else)
        after 2000->
            ?assert(false)
        end,
        apply_commit(P, [#entry{index = 1, data = #write{data = {1, "1"}}, term = 1, type = ?OP_DATA}]),
        Stat = sys:get_state(P),
        ?assertMatch(#state{last_index = 1,last_snapshot_index = 0,log_count = 1},Stat),
        Ref = make_ref(),
        Me = {self(), Ref},
        cmd(P, #leader_read_request{from = Me, request = 1}),
        receive
            {Ref, Res} ->
                ?assertMatch({ok, "1"}, Res);
            Else1 ->
                ?assertMatch(result, Else1)
        after 1000 ->
            ?assert(false)
        end,
        cmd(P, #leader_read_request{from = Me, request = 2}),
        receive
            {Ref, Res1} ->
                ?assertMatch({ok,not_found}, Res1);
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
        receive
            {'$gen_all_state_event',SnapshotInfo}->
                ?assertMatch(#snapshot_info{index = 0,conf = ?BLANK_CONF,conf_index = 0,term = 0},SnapshotInfo);
            Else->
                ?assertMatch(result,Else)
        after 2000->
            ?assert(false)
        end,
        Ref = make_ref(),
        Me = {self(), Ref},
        cmd(P, #leader_read_request{from = Me, request = 1}),
        receive
            {Ref, Res} ->
                ?assertMatch({ok,not_found}, Res);
            Else1 ->
                ?assertMatch(result, Else1)
        after 1000 ->
            ?assert(false)
        end,
        apply_commit(P, [#entry{index = I, data = #write{data = {I, integer_to_list(I)}}, term = 1, type = ?OP_DATA} || I <- lists:seq(1, 10)]),
        receive
            {'$gen_event',{make_snapshot_info,{ReqRef1,From},Index}}->
                ?assertEqual(10,Index),
                From ! {ReqRef1,#snapshot_info{index = Index,conf = [],term = 1,conf_index = 1}};
            Else2->
                ?assertMatch(result,Else2)
        after 2000->
            ?assert(false)
        end,
        cmd(P, #leader_read_request{from = Me, request = 1}),
        receive
            {Ref, Res1} ->
                ?assertMatch({ok, "1"}, Res1);
            Else3 ->
                ?assertMatch(result, Else3)
        after 1000 ->
            ?assert(false)
        end,
        receive
            {'$gen_all_state_event',SnapshotInfo1}->
                ?assertMatch(#snapshot_info{index = 10,conf = [],conf_index = 1,term = 1},SnapshotInfo1);
            Else4->
                ?assertMatch(result,Else4)
        after 3000->
            ?assert(false)
        end,
        ok = stop(P),
        {ok, P1} = start_link(Raft, zraft_dict_backend),
        receive
            {'$gen_all_state_event',SnapshotInfo2}->
                ?assertMatch(#snapshot_info{index = 10,conf = [],conf_index = 1,term = 1},SnapshotInfo2);
            Else5->
                ?assertMatch(result,Else5)
        after 2000->
            ?assert(false)
        end,
        cmd(P1, #leader_read_request{from = Me, request = 10}),
        receive
            {Ref, Res3} ->
                ?assertMatch({ok, "10"}, Res3);
            Else6 ->
                ?assertMatch(result, Else6)
        after 1000 ->
            ?assert(false)
        end,
        Stat1 = sys:get_state(P1),
        ?assertMatch(#state{last_index = 10,last_snapshot_index = 10,log_count = 0},Stat1),
        ok = stop(P1)
    end}.

- endif.