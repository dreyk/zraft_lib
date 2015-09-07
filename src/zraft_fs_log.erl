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
-module(zraft_fs_log).
-author("dreyk").

-behaviour(gen_server).

%% API
-export([start_link/1]).

-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-export([
    truncate_before/2,
    sync_fs/1,
    append/5,
    max_segment_size/0,
    load_fs/1,
    get_raft_meta/1,
    update_raft_meta/2,
    get_last_conf/1,
    stop/1,
    append_leader/2,
    get_log_descr/1,
    get_term/2,
    get_entries/3,
    replicate_log/3,
    update_commit_index/2,
    make_snapshot_info/3,
    load_raft_meta/1,
    sync/1,
    test_append/1,
    remove_data/1
]).

-export_type([
    logger/0
]).

-include("zraft.hrl").

-define(SEGMENT_VERSION, 1).
-define(RECORD_START, 0).

-define(DATA_DIR, zraft_util:get_env(log_dir, "data")).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(SYNC_DEFAULT_MODE,default).
-define(SYNC_LEADER_DIRTY_MODE,leader_dirty).
-define(SYNC_DIRTY_MODE,dirty).

-define(MAX_SEGMENT_SIZE, zraft_util:get_env(max_segment_size, 10485760)).%%10 MB
-define(SYNC_MODE,zraft_util:get_env(log_sync_mode,?SYNC_DEFAULT_MODE)).%%avaible options: default,dirty,leader_dirty
-define(READ_BUFFER_SIZE, 1048576).%%1MB
-define(SERVER, ?MODULE).

-record(segment, {
    firt_index,
    last_index,
    fname,
    fd,
    size = 1,
    entries = []
}).
-record(meta, {
    version,
    first,
    raft_meta = #raft_meta{}
}).
-record(fs, {
    raft_meta,
    peer_id,
    open_segment,
    peer_dir,
    log_dir,
    closed_segments = [],
    next_file,
    fcounter = 1,
    meta_version = 1,
    first_index = 1,
    last_index = 0,
    last_term = 0,
    commit = 0,
    configs,
    last_conf,
    max_segment_size,
    snapshot_info,
    unsynced=false,
    sync_mode
}).

-type logger() :: pid().

max_segment_size() ->
    ?MAX_SEGMENT_SIZE.

get_raft_meta(FS) ->
    gen_server:call(FS, {get, #fs.raft_meta}).

-spec update_raft_meta(logger(), zraft_consensus:raft_meta()) -> ok.
update_raft_meta(FS, Meta) ->
    gen_server:call(FS, {raft_meta, Meta}).
update_commit_index(FS, Commit) ->
    async_work(FS,{update_commit_index, Commit}).

get_term(FS, Index) ->
    gen_server:call(FS, {get_term, Index}).
get_last_conf(FS) ->
    gen_server:call(FS, {get, #fs.last_conf}).
get_log_descr(FS) ->
    gen_server:call(FS, {get, log_descr}).
get_entries(FS, From, To) ->
    gen_server:call(FS, {get_entries, From, To}).
replicate_log(FS, ToPeer, Req) ->
    gen_server:cast(FS, {replicate_log, ToPeer, Req}).

stop(FS) ->
    gen_server:call(FS, stop).

make_snapshot_info(FS, From, Index) ->
    async_work(FS, From, {make_snapshot_info, Index}).

truncate_before(FS, SnapshotInfo) ->
    async_work(FS, {truncate_before, SnapshotInfo}).

append(FS, PrevIndex, PrevTerm, ToCommitIndex, Entries) ->
    async_work(FS, {append, PrevIndex, PrevTerm, ToCommitIndex, Entries}).

append_leader(FS, Entries) ->
    gen_server:cast(FS, {append_leader, Entries}).

sync(FS)->
    gen_server:cast(FS,sync).

async_work(FS, Command) ->
    Ref = make_ref(),
    async_work(FS, {Ref, self()}, Command),
    Ref.

async_work(FS, From, Command) ->
    gen_server:cast(FS, {command, From, Command}).

sync_fs(Sync) ->
    receive
        {Sync, Result} ->
            Result
    end.

-spec remove_data(zraft_consensus:peer_id())->ok|{error,term()}.
remove_data(PeerID)->
    PeerDirName = zraft_util:peer_name_to_dir_name(zraft_util:peer_name(PeerID)),
    PeerDir = filename:join([?DATA_DIR, PeerDirName]),
    zraft_util:del_dir(PeerDir).

start_link(PeerID) ->
    gen_server:start_link(?MODULE, [PeerID], []).

init([PeerID]) ->
    gen_server:cast(self(), {init, PeerID}),
    {ok, loading}.

load_fs(PeerID) ->
    PeerDirName = zraft_util:peer_name_to_dir_name(zraft_util:peer_name(PeerID)),
    PeerDir = filename:join([?DATA_DIR, PeerDirName]),
    LogDir = filename:join(PeerDir, "log"),
    ok = zraft_util:make_dir(PeerDir),
    ok = zraft_util:make_dir(LogDir),
    FS1 = #fs{
        snapshot_info = #snapshot_info{},
        log_dir = LogDir,
        peer_dir = PeerDir,
        peer_id = PeerID,
        max_segment_size = ?MAX_SEGMENT_SIZE,
        sync_mode = ?SYNC_MODE
    },
    FS2 = load_meta(FS1),
    FS3 = load_fs_log(FS2),
    FS4 = update_metadata(FS3),
    FS5 = update_metadata(FS4),
    FS5.

handle_call({raft_meta, Meta}, _From, State) ->
    State1 = update_metadata(State#fs{raft_meta = Meta}),
    State2 = update_metadata(State1),
    {reply, ok, State2,0};
handle_call({get, log_descr}, _From, State) ->
    Res = log_descr(State),
    {reply, Res, State,0};
handle_call({get_term, Index}, _From, State) ->
    Val = term_at(Index, State),
    {reply, Val, State,0};
handle_call({get_entries, From, To}, _From, State) ->
    Entries = entries(From, To, State),
    {reply, Entries, State,0};
handle_call({get, Index}, _From, State) ->
    Val = erlang:element(Index, State),
    {reply, Val, State,0};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State,0}.

handle_cast({init, PeerID}, loading) ->
    State = load_fs(PeerID),
    {noreply, State};

handle_cast({replicate_log, ToPeer, Req}, State) ->
    handle_replicate_log(ToPeer, Req, State),
    {noreply, State,0};
handle_cast( {append_leader, Entries}, State) ->
    State1 = append(Entries, State),
    State2 = State1#fs{unsynced = true},
    {noreply, State2,0};
handle_cast({command, From, Cmd}, State) ->
    {ok, Reply, State1} = handle_command(Cmd, State),
    send_reply(From, Reply),
    {noreply, State1,0};
handle_cast(sync,State)->
    State1 = sync_last(State),
    {noreply,State1};
handle_cast(_Request, State) ->
    {noreply, State,0}.


handle_info(timeout,State)->
    if
        State#fs.sync_mode == ?SYNC_DIRTY_MODE->
            {noreply,State};
        true->
            State1 = sync_last(State),
            {noreply,State1}
    end;
handle_info(_Info, State) ->
    {noreply, State,0}.

terminate(_Reason, #fs{open_segment = Open} = FS) ->
    close_segment(Open, FS),
    ok;
terminate(_Reason, _) ->
    ok.


sync_last(State=#fs{unsynced = true,open_segment = #segment{fd = ToSync}})->
    ok = file:datasync(ToSync),
    State#fs{unsynced = false};
sync_last(State)->
    %%already has been synced
    State.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

log_descr(#fs{last_index = L, first_index = F, last_term = T, commit = C}) ->
    #log_descr{last_index = L, first_index = F, last_term = T, commit_index = C}.

handle_command({make_snapshot_info, Index}, State) ->
    Res = make_snapshot_info(Index, State),
    {ok, Res, State};
handle_command({truncate_before, SnapshotInfo}, State) ->
    State1 = truncate_segment_before(SnapshotInfo, State),
    make_result(ok, State1);
handle_command({append, PrevIndex, PrevTerm, ToCommitIndex, Entries}, State) ->
    {ok, Result, State1} = append_entry(PrevIndex, PrevTerm, ToCommitIndex, Entries, State),
    make_result(Result, State1);
handle_command({update_commit_index, Commit},State) ->
    ToCommit = entries(State#fs.commit + 1, Commit, State),
    if
        State#fs.unsynced andalso State#fs.sync_mode == ?SYNC_DEFAULT_MODE->
            #fs{open_segment = #segment{fd = ToSync}} = State,
            ok = file:datasync(ToSync),
            State1 = State#fs{commit = Commit,unsynced = false},
            make_result(ToCommit, State1);
        true->
            State1 = State#fs{commit = Commit},
            make_result(ToCommit, State1)
    end.

make_result(Result, State = #fs{last_conf = Conf}) ->
    {ok, #log_op_result{last_conf = Conf, log_state = log_descr(State), result = Result}, State}.

append_entry(PrevIndex, _PrevTerm, _ToCommitIndex, _Entries, State = #fs{last_index = LastIndex})
    when PrevIndex > LastIndex ->
    lager:warning("Rejecting AppendEntries RPC: would leave gap ~p>~p", [PrevIndex, LastIndex]),
    {ok, {false, LastIndex}, State};
append_entry(PrevIndex, PrevTerm, ToCommitIndex, Entries0, State) ->
    #fs{first_index = FirstIndex,
        last_index = LastIndex,
        commit = Commited,
        open_segment = OpenSegment,
        closed_segments = ClosedSegment} = State,
    TermAgree = if
                    PrevIndex < FirstIndex ->
                        %%log trucated after snaphot. All data commited.
                        true;
                    true ->
                        term_at(PrevIndex, State) == PrevTerm
                end,
    case TermAgree of
        true ->
            Segments = [OpenSegment | ClosedSegment],
            %% Remove truncated entry.
            %% Quourum agree that is't already commited.
            Entries = [E || E <- Entries0, E#entry.index >= FirstIndex],
            {TruncIndex, NewLastIndex, NewEntries} =
                maybe_trunc(Commited, Entries, Segments, LastIndex),
            is_safe_trunctate(Commited, TruncIndex),
            NewCommit = max(ToCommitIndex, Commited),
            if
                NewLastIndex < NewCommit ->
                    lager:error("Not enough data to commit at ~p", [NewCommit]),
                    exit({error, commit_collision});
                true ->
                    ok
            end,
            State1 = truncate_segment_after(TruncIndex, State),
            State2 = append(NewEntries, State1),
            State3 = maybe_sync_follower(State2),
            ToCommit = entries(Commited + 1, NewCommit, State2),
            {ok, {true, NewLastIndex, ToCommit},
                State3#fs{commit = NewCommit}};
        _ ->
            lager:warning("Rejecting AppendEntries RPC: terms don't agree"),
            {ok, {false, LastIndex}, State}
    end.

maybe_sync_follower(State)->
    if
        State#fs.sync_mode /= ?SYNC_DIRTY_MODE->
            #fs{open_segment = #segment{fd = ToSync}} = State,
            ok = file:datasync(ToSync),
            State#fs{unsynced = false};
        true->
            State#fs{unsynced = true}
    end.
maybe_trunc(_CommitIndex, [], _Segments, LastIndex) ->
    {LastIndex + 1, LastIndex, []};
maybe_trunc(CommitIndex, [FirstToAdd | _] = Entries, Segments, _Lastindex) ->
    #entry{index = LastAddIndex} = lists:last(Entries),
    LogEnries = split_at_index(CommitIndex, FirstToAdd#entry.index, LastAddIndex, Segments, []),
    case preappend(CommitIndex, Entries, LogEnries) of
        false ->
            {LastAddIndex + 1, LastAddIndex, []};
        {TruncIndex, NewEntries} ->
            {TruncIndex, LastAddIndex, NewEntries}
    end.
%get log entries form Min to Max in asc order
split_at_index(Commited, Min, Max,
    [#segment{firt_index = First} | T], P1) when First > Max ->
    true = is_safe_trunctate(Commited, First),
    split_at_index(Commited, Min, Max, T, P1);
split_at_index(Commited, Min, Max,
    [#segment{last_index = Last,firt_index = First, entries = Entries} | T], P1)->
    case is_intersec(First,Last,Min,Max) of
        true->
            P2 = split_entry_at_index(Commited, Min, Max, Entries, P1),
            split_at_index(Commited, Min, Max, T, P2);
        _->
            P1
    end;
split_at_index(_Commited, _Min, _Max, _Segments, P1) ->
    P1.

is_intersec(L1,H1,L2,H2)->
    (L2 >= L1 andalso L2 =< H1) orelse (H2 >= L1 andalso H2 =< H1) orelse (L2<L1 andalso H2>H1).

split_entry_at_index(Commited, Min, Max, [#entry{index = I2} | T], P1) when I2 > Max ->
    true = is_safe_trunctate(Commited, I2),
    split_entry_at_index(Commited, Min, Max, T, P1);
split_entry_at_index(Commited, Min, Max, [#entry{index = I2} = E | T], P1) when I2 >= Min ->
    split_entry_at_index(Commited, Min, Max, T, [E | P1]);
split_entry_at_index(_Commited, _Min, _Max, _Log, P1) ->
    P1.

preappend(Commited, [#entry{index = I1, term = T1} | Tail1], [#entry{index = I1, term = T1} | Tail2]) ->
    preappend(Commited, Tail1, Tail2);
preappend(Commited, [#entry{index = I1} | _] = Entries, _LogTail) ->
    is_safe_trunctate(Commited, I1),
    {I1, Entries};
preappend(_Commited, [], []) ->
    %%all entry already exists in log.
    false.

truncate_segment_before(Snaphot = #snapshot_info{index = Commit},
    FS = #fs{open_segment = Open, closed_segments = Closed, configs = Conf1, commit = OldCommitIndex}) ->
    Last = Open#segment.last_index,
    FS2 = if
              Last =< Commit ->
                  %%remove all log files
                  lists:foreach(fun(S) ->
                      delete_segment(S) end, Closed),
                  delete_segment(Open),
                  {NewSegment, FS1} = new_segment(Commit + 1, FS),
                  LastTerm = if
                                 Last == Commit ->
                                     FS#fs.last_term;
                                 true ->
                                     0
                             end,
                  FS1#fs{open_segment = NewSegment, closed_segments = [], last_index = Commit, last_term = LastTerm};
              true ->
                  %%Last>Commit
                  truncate_segments_before(Commit, FS, [Open | Closed], [])
          end,
    Conf2 = truncate_conf_before(Commit, Conf1),
    LastConf = last_conf(Snaphot, Conf2),
    NewCommitIndex = max(OldCommitIndex, Commit),
    FS3 = FS2#fs{first_index = Commit + 1, commit = NewCommitIndex, configs = Conf2, last_conf = LastConf, snapshot_info = Snaphot},
    FS4 = update_metadata(FS3),
    update_metadata(FS4).

truncate_segments_before(Commit, FS,
    [#segment{last_index = Last, firt_index = First} = S | T], Acc) when Last > Commit ->
    if
        Commit >= First ->
            %%all prev segment will be deleted.
            Acc1 = if
                       S#segment.fd == undefined ->
                           %%if segment closed change it's bounds.
                           close_segment(set_segment_first_index(Commit + 1, S), Acc, FS);
                       true ->
                           %%if segment open just change first index
                           [set_segment_first_index(Commit + 1, S) | Acc]
                   end,
            truncate_segments_before(Commit, FS, T, Acc1);
        true ->
            %%First>Commit. Check prev segment
            truncate_segments_before(Commit, FS, T, [S | Acc])
    end;
truncate_segments_before(_Commit, FS, OldLog, Acc) ->
    [Open | Closed] = lists:reverse(Acc),
    lists:foreach(fun(S) ->
        delete_segment(S) end, OldLog),
    FS#fs{open_segment = Open, closed_segments = Closed}.


truncate_segment_after(Index, FS = #fs{open_segment = Open, closed_segments = Closed}) ->
    if
        Open#segment.last_index < Index ->
            FS;
        true ->
            %%else open segment will be split or deleted
            FS1 = truncate_segments_after(FS, Index, [Open | Closed]),
            NewConfigs = truncate_conf_after(Index, FS#fs.configs),
            FS1#fs{configs = NewConfigs, last_conf = last_conf(FS#fs.snapshot_info, NewConfigs)}
    end.
truncate_segments_after(FS, Index, [#segment{firt_index = First} = S | T]) when First >= Index ->
    %%delte segment
    delete_segment(S),
    truncate_segments_after(FS, Index, T);
truncate_segments_after(FS, Index, [#segment{last_index = Last} = S | T]) ->%%Index>First
    Closed = if
                 Last < Index ->
                     close_segment(S, T, FS);
                 true ->
                     %%set new last index
                     close_segment(S#segment{last_index = Index - 1}, T, FS)
             end,
    {NewSegment, FS1} = new_segment(Index, FS),
    NewLast = Index - 1,
    FS2 = FS1#fs{open_segment = NewSegment, closed_segments = Closed, last_index = NewLast},
    Term = term_at(NewLast, FS2),
    FS2#fs{last_term = Term};
truncate_segments_after(FS, Index, []) ->
    {NewSegment, FS1} = new_segment(Index, FS),
    FS1#fs{open_segment = NewSegment, closed_segments = [], last_index = Index - 1, last_term = 0}.


delete_segment(#segment{fname = Name, fd = Fd}) ->
    if
        Fd =/= undefined ->
            ok = file:close(Fd);
        true ->
            ok
    end,
    ok = file:delete(Name).

set_segment_first_index(Index, #segment{last_index = L} = S) when Index > L ->
    S#segment{entries = [], firt_index = Index};
set_segment_first_index(Index, #segment{entries = Log} = S) ->
    Log1 = truncate_log_before(Index - 1, Log),
    S#segment{entries = Log1, firt_index = Index}.

truncate_log_before(C, [#entry{index = I} = E | T]) when I > C ->
    [E | truncate_log_before(C, T)];
truncate_log_before(_, _) ->
    [].
truncate_conf_before(C, [{I, _} = E | T]) when I > C ->
    [E | truncate_conf_before(C, T)];
truncate_conf_before(_, _) ->
    [].

close_segment(S, Closed, FS) ->
    case close_segment(S, FS) of
        deleted ->
            Closed;
        S1 ->
            [S1 | Closed]
    end.
close_segment(#segment{firt_index = First, last_index = Last} = S, FS) ->
    if
        First > Last ->
            delete_segment(S),
            deleted;
        true ->
            #segment{fname = PrevName, fd = Fd, entries = Log} = S,
            if
                Fd =/= undefined ->
                    ok = file:datasync(Fd),
                    ok = file:close(Fd);
                true ->
                    ok
            end,
            Log1 = truncate_log_after(Last, Log),
            NewName = filename:join(FS#fs.log_dir,
                integer_to_list(First) ++
                    "-" ++
                    integer_to_list(Last) ++
                    ".rlog"),
            case NewName of
                PrevName ->
                    S#segment{fd = undefined, entries = Log1};
                _ ->
                    ok = file:rename(PrevName, NewName),
                    S#segment{fd = undefined, fname = NewName, entries = Log1}
            end
    end.

truncate_log_after(Index, [#entry{index = I2} | T]) when I2 > Index ->
    truncate_log_after(Index, T);
truncate_log_after(_Index, Log) ->
    Log.

%%in most of cases we need only one new segment. May be optimize it in future.
new_segment(Index, FS = #fs{next_file = {FName, FD}}) ->
    {
        #segment{firt_index = Index, last_index = Index - 1, fname = FName, fd = FD},
        FS#fs{next_file = undefined}
    };
new_segment(Index, FS = #fs{fcounter = N, log_dir = Dir, max_segment_size = Size}) ->
    {FName, FD} = new_file(Dir, N, Size),
    {
        #segment{firt_index = Index, last_index = Index - 1, fname = FName, fd = FD},
        FS#fs{fcounter = N + 1}
    }.


new_file(Dir, N, MaxSize) ->
    FName = filename:join(Dir, "open-" ++ integer_to_list(N) ++ ".rlog"),
    {ok, FD} = file:open(FName, [write, binary,raw]),
    file:allocate(FD, 0, MaxSize),
    ok = file:write(FD, <<?SEGMENT_VERSION:8>>),
    ok = file:datasync(FD),
    {FName, FD}.

send_reply({Ref, Pid}, Result) ->
    Pid ! {Ref, Result};
send_reply(_, _Result) ->
    ok.

append([], FS) ->
    FS;
append(Entries,
    FS = #fs{last_index = Index, open_segment = Open, closed_segments = Closed, configs = Conf}) ->
    append_entries(Index + 1, Entries, Open, FS, Closed, Open#segment.entries, Conf).

test_append(Max)->
    FS = load_fs({test,node()}),
    Start = os:timestamp(),
    test_append(FS,1,Max),
    {ok,round(Max*1000000/timer:now_diff(os:timestamp(),Start))}.
test_append(_FS,N,Max) when N>Max->
    ok;
test_append(FS,N,Max)->
    test_append(append([#entry{index = N,data = {add,{1,1}},global_time = 1,term = 1,type = ?OP_DATA}],FS),N+1,Max).

append_entries(_Index, [], Open, FS, Acc, LogAcc, ConfAcc) ->
    LastConf = last_conf(FS#fs.snapshot_info, ConfAcc),
    [#entry{index = LastIndex, term = LastTerm} | _] = LogAcc,
    FS#fs{
        last_index = LastIndex,
        last_term = LastTerm,
        open_segment = Open#segment{entries = LogAcc},
        closed_segments = Acc,
        configs = ConfAcc,
        last_conf = LastConf};
append_entries(Index, [#entry{index = Index0, data = Data, type = Type, term = Term,global_time = GTime} = E | T],
    Open = #segment{size = Size}, FS = #fs{max_segment_size = MaxSize}, Acc, LogAcc, ConfAcc) ->
    if
        Index0 == undefined orelse Index0 == Index ->
            ok;
        true ->
            exit({error, index_mismach, Index, Index0})
    end,
    DataBytes = <<Index:64, Term:64, Type:8,GTime:64,(term_to_binary(Data))/binary>>,
    Crs = erlang:crc32(DataBytes),
    DataSize = size(DataBytes),
    Buffer = <<?RECORD_START:8, Crs:32, DataSize:32, DataBytes/binary>>,
    BufferSize = 9 + DataSize,
    NewSize = BufferSize + Size,
    ConfAcc1 = case Type of
                   ?OP_CONFIG ->
                       [{Index, Data} | ConfAcc];
                   _ ->
                       ConfAcc
               end,
    if
        Size > 1 andalso NewSize > MaxSize ->
            {NewSegment, FS1} = new_segment(Index, FS),
            Acc1 = close_segment(Open#segment{entries = LogAcc}, Acc, FS1),
            NewSegment1 = write_to_file(Index, BufferSize, Buffer, NewSegment),
            append_entries(Index + 1, T, NewSegment1, FS1, Acc1, [E], ConfAcc1);
        true ->
            Open1 = write_to_file(Index, BufferSize, Buffer, Open),
            append_entries(Index + 1, T, Open1, FS, Acc, [E | LogAcc], ConfAcc1)
    end.

write_to_file(Index, BufferSize, Buffer, Segment = #segment{fd = FD, size = Size}) ->
    ok = file:write(FD, Buffer),
    Segment#segment{last_index = Index, size = Size + BufferSize}.


load_fs_log(FS = #fs{log_dir = Dir, first_index = Index}) ->
    {ok, Files} = file:list_dir(Dir),
    Segments = load_segments_file(Files, FS, []),
    load_segments_data(Index, Segments, FS, [], []).


load_segments_data(Index, [], FS, SegmentAcc, ConfAcc) ->
    {NewSegment, FS1} = new_segment(Index, FS),
    {LastIndex, LastTerm} = case SegmentAcc of
                                [] ->
                                    {Index - 1, 0};
                                [#segment{entries = Log} | _] ->
                                    [#entry{index = LI, term = LT} | _] = Log,
                                    {LI, LT}
                            end,
    LastConf = last_conf(FS#fs.snapshot_info, ConfAcc),
    FS1#fs{
        closed_segments = SegmentAcc,
        open_segment = NewSegment,
        last_index = LastIndex,
        last_term = LastTerm,
        configs = ConfAcc,
        last_conf = LastConf};
load_segments_data(Index, [Segment | Tail], FS, SegmentAcc, ConfAcc) ->
    case Segment#segment.firt_index of
        FirstIndex when FirstIndex == Index orelse is_tuple(FirstIndex) ->
            case load_file(Index, Segment#segment.last_index, Segment#segment.fname, [], ConfAcc) of
                {_Res, {{Index, EndIndex}, Log, ConfAcc1}} ->
                    SegmentAcc1 =
                        close_segment(
                            Segment#segment{firt_index = Index, last_index = EndIndex, entries = Log},
                            SegmentAcc,
                            FS),
                    load_segments_data(EndIndex + 1, Tail, FS, SegmentAcc1, ConfAcc1);
                {_Res, {{OtherIndex, _EndIndex}, _Log, _}} ->
                    lager:error("Rejecting Segment: would leave gap ~p!=~p", [Index, OtherIndex]),
                    SegmentAcc1 =
                        close_segment(Segment#segment{firt_index = Index, last_index = Index - 1}, SegmentAcc, FS),
                    load_segments_data(Index, Tail, FS, SegmentAcc1, ConfAcc)
            end;
        FirstIndex ->
            lager:error("Rejecting Segment: would leave gap ~p!=~p", [Index, FirstIndex]),
            %%drop segment
            SegmentAcc1 =
                close_segment(
                    Segment#segment{firt_index = FirstIndex, last_index = FirstIndex - 1},
                    SegmentAcc,
                    FS),
            load_segments_data(Index, Tail, FS, SegmentAcc1, ConfAcc)
    end.

load_segments_file([], _, Acc) ->
    lists:keysort(#segment.firt_index, Acc);
load_segments_file([SName | Tail], FS = #fs{log_dir = Dir}, Acc) ->
    FName = filename:join(Dir, SName),
    case is_open_filename(SName) of
        true ->
            case open_file_index(SName) of
                {error, _} ->
                    lager:warning("Log contains bad file ~s", [FName]),
                    load_segments_file(Tail, FS, Acc);
                I ->
                    load_segments_file(
                        Tail,
                        FS,
                        [#segment{firt_index = {open, I}, last_index = hi, fname = FName} | Acc]
                    )
            end;
        _ ->
            case segment_bound(SName) of
                {error, _} ->
                    lager:warning("Log contains bad file ~s", [FName]),
                    load_segments_file(Tail, FS, Acc);
                {Lo, Hi} ->
                    load_segments_file(Tail, FS, [#segment{firt_index = Lo, fname = FName, last_index = Hi} | Acc])
            end
    end.

is_open_filename([$o, $p, $e, $n, $- | _T]) ->
    true;
is_open_filename(_) ->
    false.

open_file_index(Name) ->
    Name2 = filename:rootname(Name, ".rlog"),
    case Name2 of
        [$o, $p, $e, $n, $- | T] ->
            case catch list_to_integer(T) of
                I when is_integer(I) ->
                    I;
                _ ->
                    {error, badname}
            end;
        _ ->
            {error, badname}
    end.
segment_bound(Name) ->
    Name2 = filename:rootname(Name, ".rlog"),
    case string:tokens(Name2, "-") of
        [SLO, SHI] ->
            case catch list_to_integer(SLO) of
                Lo when is_integer(Lo) ->
                    case catch list_to_integer(SHI) of
                        Hi when is_integer(Hi) ->
                            {Lo, Hi};
                        _ ->
                            {error, badname}
                    end;
                _ ->
                    {error, badname}
            end;
        _ ->
            {error, badname}
    end.


load_file(Lo, Hi, FName, LogAcc, ConfAcc) ->
    {ok, FD} = file:open(FName, [read, binary, {read_ahead, ?READ_BUFFER_SIZE}]),
    Result = case file:read(FD, 1) of
                 {ok, <<?SEGMENT_VERSION:8>>} ->
                     case read_entries(Lo, Lo, Hi, FName, FD, LogAcc, ConfAcc) of
                         {ok, {LastIndex, LogAcc1, ConfAcc1}} ->
                             {ok, {{Lo, LastIndex}, LogAcc1, ConfAcc1}};
                         {error, {LastIndex, LogAcc1, ConfAcc1}} ->
                             {error, {{Lo, LastIndex}, LogAcc1, ConfAcc1}}
                     end;
                 Else ->
                     lager:error("Bad segment file format ~s - ~p", [FName, Else]),
                     {error, {{Lo, Lo - 1}, LogAcc, ConfAcc}}
             end,
    file:close(FD),
    Result.

read_entries(PIndex, Lo, Hi, FName, FD, Acc, Conf) ->
    case file:read(FD, 9) of
        {ok, <<?RECORD_START:8, Crs:32, Size:32>>} ->
            case read_entry(PIndex, Lo, Hi, Crs, Size, FD) of
                next ->
                    read_entries(PIndex, Lo, Hi, FName, FD, Acc, Conf);
                {next, Entry} ->
                    read_entries(PIndex + 1, Lo, Hi, FName, FD, [Entry | Acc], maybe_add_conf(Entry, Conf));
                {stop, Entry} ->
                    {ok, {PIndex, [Entry | Acc], maybe_add_conf(Entry, Conf)}};
                {error, Error} ->
                    lager:error("Can't read entry from ~s at ~p: ~p", [FName, PIndex, Error]),
                    {error, {PIndex - 1, Acc, Conf}}
            end;
        eof ->
            {ok, {PIndex - 1, Acc, Conf}};
        {error, Error} ->
            lager:error("Can't read entry from ~s at ~p: ~p", [FName, PIndex, Error]),
            {error, {PIndex - 1, Acc, Conf}}
    end.

maybe_add_conf(#entry{type = ?OP_CONFIG, index = I, data = Data}, Acc) ->
    [{I, Data} | Acc];
maybe_add_conf(_, Acc) ->
    Acc.

read_entry(PIndex, Lo, Hi, Crs, Size, FD) ->
    case file:read(FD, Size) of
        {ok, Buffer} ->
            BufferCrs = erlang:crc32(Buffer),
            if
                BufferCrs == Crs ->
                    case Buffer of
                        <<Index:64, _Term:64, _Type:8,_GTime:64, _BData/binary>> when Index < Lo ->
                            next;
                        <<Index:64, Term:64, Type:8,GTime:64, BData/binary>> when Index == Hi andalso Index == PIndex ->
                            case catch binary_to_term(BData) of
                                {'EXIT', Error} ->
                                    {error, Error};
                                Data ->
                                    {
                                        stop,
                                        #entry{global_time = GTime,index = Index, type = Type, data = Data, term = Term}
                                    }
                            end;
                        <<Index:64, Term:64, Type:8,GTime:64, BData/binary>> when Index == PIndex ->
                            case catch binary_to_term(BData) of
                                {'EXIT', Error} ->
                                    {error, Error};
                                Data ->
                                    {next,
                                        #entry{global_time = GTime,index = Index, type = Type, data = Data, term = Term}}
                            end;
                        <<_Index:64, _/binary>> ->
                            {error, index_sequence_error};
                        _Else ->
                            {error, currupted_record}

                    end;
                true ->
                    {error, check_crs32_fail}
            end;
        eof ->
            {error, eof};
        Else ->
            Else
    end.

update_metadata(FS = #fs{meta_version = Version, first_index = Index, peer_dir = Dir, raft_meta = RaftMeta}) ->
    NewVersion = Version + 1,
    SName = if
                NewVersion rem 2 == 0 ->
                    "meta1.info";
                true ->
                    "meta2.info"
            end,
    FName = filename:join(Dir, SName),
    ok = file:write_file(FName, term_to_binary(#meta{version = NewVersion, first = Index, raft_meta = RaftMeta})),
    FS#fs{meta_version = NewVersion}.

load_meta(FS = #fs{peer_dir = Dir,peer_id = ID}) ->
    Meta2 = #meta{version = V2} = case read_meta_file(filename:join(Dir, "meta2.info")) of
                                      {error, _} ->
                                          #meta{version = 1, first = 1,raft_meta = #raft_meta{id = ID}};
                                      M ->
                                          M
                                  end,
    #meta{version = V, first = Index, raft_meta = RaftMeta} = case read_meta_file(filename:join(Dir, "meta1.info")) of
                                                                  {error, _} ->
                                                                      Meta2;
                                                                  #meta{version = V1} when V1 < V2 ->
                                                                      Meta2;
                                                                  Meta1 ->
                                                                      Meta1
                                                              end,
    FS#fs{meta_version = V, first_index = Index, raft_meta = RaftMeta}.

read_meta_file(FileName) ->
    case file:read_file(FileName) of
        {ok, Data} ->
            case catch binary_to_term(Data) of
                #meta{} = M ->
                    M;
                Else ->
                    lager:error("Can't parse erlang term from file ~s:~p", [FileName, Else]),
                    {error, badformat}
            end;
        Else ->
            lager:error("Can't read metafile ~s:~p", [FileName, Else]),
            Else
    end.

entries(Lo, Hi, _) when Lo > Hi ->
    [];
entries(Lo, Hi, #fs{open_segment = Open, closed_segments = Closed}) ->
    entries1(Lo, Hi, [Open | Closed], []).

entries1(_Lo, _Hi, [], Acc) ->
    Acc;
entries1(Lo, _Hi, [#segment{last_index = L} | _T], Acc) when L < Lo ->
    Acc;
entries1(Lo, Hi, [#segment{firt_index = F} | T], Acc) when F > Hi ->
    entries1(Lo, Hi, T, Acc);
entries1(Lo, Hi, [#segment{entries = Log} | T], Acc) ->
    Acc2 = entries2(Lo, Hi, Log, Acc),
    entries1(Lo, Hi, T, Acc2).

entries2(_Lo, _Hi, [], Acc) ->
    Acc;
entries2(Lo, Hi, [#entry{index = I} | T], Acc) when I > Hi ->
    entries2(Lo, Hi, T, Acc);
entries2(Lo, _Hi, [#entry{index = I} | _T], Acc) when I < Lo ->
    Acc;
entries2(Lo, Hi, [E | T], Acc) ->
    entries2(Lo, Hi, T, [E | Acc]).


term_at(Index, #fs{last_index = Index, last_term = Term}) ->
    Term;
term_at(Index, #fs{snapshot_info = #snapshot_info{index = Index, term = Term}}) ->
    Term;
term_at(Index, #fs{last_index = Hi, first_index = Lo}) when Index < Lo orelse Index > Hi ->
    0;
term_at(Index, State) ->
    case entries(Index, Index, State) of
        [#entry{term = Term}] ->
            Term;
        _ ->
            0
    end.

is_safe_trunctate(CommitIndex, Index) when CommitIndex < Index ->
    true;
is_safe_trunctate(CommitIndex, Index) ->
    lager:error("Should never truncate committed entries ~p >= ~p",[CommitIndex,Index]),
    exit({error, commit_collision}).

last_conf(#snapshot_info{conf = SConf, conf_index = SI}, Configs) ->
    case last_conf(Configs) of
        ?BLANK_CONF ->
            if
                SI == 0 ->
                    ?BLANK_CONF;
                true ->
                    {SI, SConf}
            end;
        Last ->
            Last
    end.
last_conf([E | _]) ->
    E;
last_conf([]) ->
    ?BLANK_CONF.

last_conf_before(Index, #snapshot_info{conf = SConf, conf_index = SI}, Configs) ->
    case last_conf_before(Index, Configs) of
        ?BLANK_CONF ->
            if
                SI > Index ->
                    ?BLANK_CONF;
                SI == 0 ->
                    ?BLANK_CONF;
                true ->
                    {SI, SConf}
            end;
        Last ->
            Last
    end.
last_conf_before(Index, [{I, _} | T]) when I > Index ->
    last_conf_before(Index, T);
last_conf_before(_Index, [E | _T]) ->
    E;
last_conf_before(_Index, []) ->
    ?BLANK_CONF.

truncate_conf_after(Index, [{ID, _} | T]) when ID >= Index ->
    truncate_conf_after(Index, T);
truncate_conf_after(_Index, Configs) ->
    Configs.

handle_replicate_log(ToPeer, Req, State) ->
    PrevIndex = Req#append_entries.prev_log_index,
    NextIndex = PrevIndex + 1,
    #fs{first_index = FirstIndex} = State,
    if
        FirstIndex > NextIndex ->
            need_snapshot(Req, State);
        true ->
            case term_at(PrevIndex, State) of
                0 when PrevIndex == 0 andalso FirstIndex == 1 ->
                    handle_replicate_log(ToPeer, NextIndex, 0, Req, State);
                0 ->
                    need_snapshot(Req, State);
                PrevTerm ->
                    handle_replicate_log(ToPeer, NextIndex, PrevTerm, Req, State)
            end
    end.

need_snapshot(Req, #fs{peer_id = Peer}) ->
    #append_entries{from = From, request_ref = Ref} = Req,
    NewReq = #install_snapshot{from = From, request_ref = Ref, data = prepare},
    zraft_consensus:need_snapshot(Peer, NewReq).

handle_replicate_log(ToPeer, NextIndex, PrevTerm, Req = #append_entries{entries = false}, State) ->
    Commit = min(State#fs.commit, NextIndex - 1),
    Req1 = Req#append_entries{commit_index = Commit, entries = [], prev_log_term = PrevTerm},
    zraft_peer_route:cmd(ToPeer, Req1);
handle_replicate_log(ToPeer, NextIndex, PrevTerm, Req, State) ->
    Entries = entries(NextIndex, State#fs.last_index, State),
    case Entries of
        [#entry{index = NextIndex} | _] ->
            ok;
        [#entry{index = NextIndex1} | _] ->
            exit({error, {NextIndex1, '=/=', NextIndex}});
        _ ->
            ok
    end,
    Commit = min(State#fs.commit, NextIndex - 1 + length(Entries)),
    Req1 = Req#append_entries{commit_index = Commit, entries = Entries, prev_log_term = PrevTerm},
    zraft_peer_route:cmd(ToPeer, Req1).

make_snapshot_info(Index, #fs{peer_id = ID, commit = Commit}) when Commit < Index ->
    lager:error("~p: Attempt to make snapshot uncommited index ~p", [ID, Index]),
    {error, invalid_index};
make_snapshot_info(Index,
    State = #fs{peer_id = ID, snapshot_info = LastSnapshot, configs = Configs}) ->
    if
        Index == 0 ->
            lager:warning("~p: Attempt to make snapshot for empty log", [ID]),
            Term = 0;
        Index == LastSnapshot#snapshot_info.index ->
            lager:warning("~p: Attempt to make snapshot where we already have one", [ID]),
            Term = LastSnapshot#snapshot_info.term;
        true ->
            case term_at(Index, State) of
                0 ->
                    lager:warning("~p: Attempt to make snapshot for already discarded log", [ID]),
                    Term = 0;
                Tmp1 ->
                    Term = Tmp1
            end
    end,
    case last_conf_before(Index, State#fs.snapshot_info, Configs) of
        ?BLANK_CONF ->
            ConfIndex = 0,
            Conf = ?BLANK_CONF;
        {I1, V1} ->
            ConfIndex = I1,
            Conf = V1
    end,
    #snapshot_info{term = Term, index = Index, conf = Conf, conf_index = ConfIndex}.

load_raft_meta(Dir) ->
    case read_meta_file(filename:join(Dir, "meta2.info")) of
        #meta{version = V1}=M1 ->
            case read_meta_file(filename:join(Dir, "meta1.info")) of
                #meta{version = V2}=M2 when V2>V1->
                    {ok,M2#meta.raft_meta};
                _->
                    {ok,M1#meta.raft_meta}
            end;
        Error->
            case read_meta_file(filename:join(Dir, "meta1.info")) of
                #meta{}=M2->
                    {ok,M2#meta.raft_meta};
                _->
                    Error
            end
    end.

-ifdef(TEST).
setup_log() ->
    zraft_util:set_test_dir("test-log"),
    application:set_env(zraft_lib, max_segment_size, 124),%%3 entry per log
    ok.
stop_log(_) ->
    zraft_util:clear_test_dir("test-log"),
    application:unset_env(zraft_lib, max_segment_size),
    ok.

-define(INITIAL_ENTRY, [
    #entry{index = 1, term = 1, data = <<"a">>, type = ?OP_CONFIG},
    #entry{index = 2, term = 1, data = <<"b">>, type = ?OP_CONFIG},
    #entry{index = 3, term = 1, data = <<"c">>, type = ?OP_CONFIG},
    #entry{index = 4, term = 2, data = <<"d">>, type = ?OP_CONFIG},
    #entry{index = 5, term = 2, data = <<"e">>, type = ?OP_CONFIG},
    #entry{index = 6, term = 2, data = <<"f">>, type = ?OP_CONFIG},
    #entry{index = 7, term = 8, data = <<"g">>, type = ?OP_CONFIG}
]).


fs_log_test_() ->
    {
        setup,
        fun setup_log/0,
        fun stop_log/1,
        fun(_X) ->
            [
                check_max_size(),
                new_append(),
                append_truncate1(),
                append_truncate2(),
                snaphost_on_commit(),
                snaphost_on_commit_all(),
                corrupted_log_file(),
                truncate_conflict(),
                server_log()
            ]
        end
    }.

check_max_size() ->
    {"max size", fun() ->
        Max = max_segment_size(),
        ?assertEqual(124, Max)
    end}.

new_append() ->
    {"new append", fun() ->
        zraft_util:del_dir(?DATA_DIR),
        Log = load_fs({test, node()}),
        LogEntriesTest = ?INITIAL_ENTRY,
        {ok, OpRes, Log1} = handle_command({append, 0, 0, 0, LogEntriesTest}, Log),
        check_op_result(1, 7, 8, 0, {7, <<"g">>}, OpRes),
        File1 = test_log_file_name("1-3.rlog"),
        File2 = test_log_file_name("4-6.rlog"),
        Configs = lists:reverse(
            [{I, D} || #entry{type = Type, index = I, data = D} <- ?INITIAL_ENTRY, Type == ?OP_CONFIG]
        ),
        ?assertMatch(
            #fs{
                fcounter = 4,
                first_index = 1,
                last_index = 7,
                last_conf = {7, <<"g">>},
                last_term = 8,
                configs = Configs,
                open_segment = #segment{firt_index = 7, last_index = 7},
                closed_segments = [
                    #segment{firt_index = 4, last_index = 6, fname = File2},
                    #segment{firt_index = 1, last_index = 3, fname = File1}
                ]
            },
            Log1
        ),
        Entries1 = entries(1, 7, Log1),
        ?assertEqual(LogEntriesTest, Entries1),
        Log2 = load_fs({test, node()}),
        File3 = test_log_file_name("7-7.rlog"),
        ?assertMatch(
            #fs{
                fcounter = 2,
                first_index = 1,
                last_index = 7,
                last_term = 8,
                last_conf = {7, <<"g">>},
                configs = Configs,
                open_segment = #segment{firt_index = 8, last_index = 7},
                closed_segments = [
                    #segment{firt_index = 7, last_index = 7, fname = File3},
                    #segment{firt_index = 4, last_index = 6, fname = File2},
                    #segment{firt_index = 1, last_index = 3, fname = File1}
                ]
            },
            Log2
        ),
        Entries2 = entries(1, 7, Log2),
        ?assertEqual(LogEntriesTest, Entries2),
        zraft_util:del_dir(?DATA_DIR)
    end}.

append_truncate1() ->
    {"append truncate", fun() ->
        zraft_util:del_dir(?DATA_DIR),
        Log = load_fs({test, node()}),
        {ok, _, Log1} = handle_command({append, 0, 0, 0, ?INITIAL_ENTRY}, Log),
        {ok, OpRes, Log2} = handle_command(
            {append, 7, 8, 0, [#entry{index = 5, term = 3, data = <<"e">>, type = ?OP_CONFIG}]},
            Log1
        ),
        check_op_result(1, 5, 3, 0, {5, <<"e">>}, OpRes),
        File1 = test_log_file_name("1-3.rlog"),
        File2 = test_log_file_name("4-4.rlog"),
        Configs = lists:reverse(
            [{I, D} || #entry{type = Type, index = I, data = D} <- ?INITIAL_ENTRY, Type == ?OP_CONFIG, I < 5] ++
            [{5, <<"e">>}]
        ),
        ?assertMatch(
            #fs{
                fcounter = 5,
                first_index = 1,
                last_index = 5,
                last_term = 3,
                last_conf = {5, <<"e">>},
                configs = Configs,
                open_segment = #segment{firt_index = 5, last_index = 5},
                closed_segments = [
                    #segment{firt_index = 4, last_index = 4, fname = File2},
                    #segment{firt_index = 1, last_index = 3, fname = File1}
                ]
            },
            Log2
        ),
        LogEntriesTest = [E || E <- ?INITIAL_ENTRY,
            E#entry.index < 5] ++ [#entry{index = 5, term = 3, data = <<"e">>, type = ?OP_CONFIG}],
        Entries1 = entries(1, 7, Log2),
        Log3 = load_fs({test, node()}),
        ?assertEqual(LogEntriesTest, Entries1),
        File3 = test_log_file_name("5-5.rlog"),
        ?assertMatch(
            #fs{
                fcounter = 2,
                first_index = 1,
                last_index = 5,
                last_term = 3,
                last_conf = {5, <<"e">>},
                configs = Configs,
                open_segment = #segment{firt_index = 6, last_index = 5},
                closed_segments = [
                    #segment{firt_index = 5, last_index = 5, fname = File3},
                    #segment{firt_index = 4, last_index = 4, fname = File2},
                    #segment{firt_index = 1, last_index = 3, fname = File1}
                ]
            },
            Log3
        ),
        Entries2 = entries(1, 7, Log3),
        ?assertEqual(LogEntriesTest, Entries2),
        ok = zraft_util:del_dir(?DATA_DIR)
    end}.

append_truncate2() ->
    {"append truncate last", fun() ->
        zraft_util:del_dir(?DATA_DIR),
        Log = load_fs({test, node()}),
        {ok, _, Log1} = handle_command({append, 0, 0, 0, ?INITIAL_ENTRY}, Log),
        {ok, OpRes, Log2} = handle_command(
            {append, 7, 8, 0, [#entry{index = 7, term = 3, data = <<"e">>, type = ?OP_CONFIG}]},
            Log1
        ),
        check_op_result(1, 7, 3, 0, {7, <<"e">>}, OpRes),
        File1 = test_log_file_name("1-3.rlog"),
        File2 = test_log_file_name("4-6.rlog"),
        ?assertMatch(
            #fs{
                fcounter = 5,
                first_index = 1,
                last_index = 7,
                last_term = 3,
                last_conf = {7, <<"e">>},
                open_segment = #segment{firt_index = 7, last_index = 7},
                closed_segments = [
                    #segment{firt_index = 4, last_index = 6, fname = File2},
                    #segment{firt_index = 1, last_index = 3, fname = File1}
                ]
            },
            Log2
        ),
        LogEntriesTest = [E || E <- ?INITIAL_ENTRY, E#entry.index < 7] ++
            [#entry{index = 7, term = 3, data = <<"e">>, type = ?OP_CONFIG}],
        Entries1 = entries(1, 7, Log2),
        ?assertEqual(LogEntriesTest, Entries1),
        ok = zraft_util:del_dir(?DATA_DIR)
    end}.

snaphost_on_commit() ->
    {"snaphost on commit", fun() ->
        zraft_util:del_dir(?DATA_DIR),
        Log = load_fs({test, node()}),
        {ok, _, Log1} = handle_command({append, 0, 0, 0, ?INITIAL_ENTRY}, Log),
        {ok, _, Log2} = handle_command(
            {append, 7, 8, 0, [#entry{index = 7, term = 3, data = <<"e">>, type = ?OP_CONFIG}]},
            Log1
        ),
        File1 = test_log_file_name("6-6.rlog"),
        {ok, OpRes, Log3} = handle_command({truncate_before, #snapshot_info{index = 5}}, Log2),
        check_snaphost_result(6, 7, 3, 5, {7, <<"e">>}, OpRes),
        ?assertMatch(
            #fs{
                fcounter = 5,
                first_index = 6,
                last_index = 7,
                last_term = 3,
                last_conf = {7, <<"e">>},
                open_segment = #segment{firt_index = 7, last_index = 7},
                closed_segments = [
                    #segment{firt_index = 6, last_index = 6, fname = File1}
                ]
            },
            Log3
        ),
        Log4 = load_fs({test, node()}),
        File2 = test_log_file_name("7-7.rlog"),
        ?assertMatch(
            #fs{
                fcounter = 2,
                first_index = 6,
                last_index = 7,
                last_term = 3,
                last_conf = {7, <<"e">>},
                open_segment = #segment{firt_index = 8, last_index = 7},
                closed_segments = [
                    #segment{firt_index = 7, last_index = 7, fname = File2},
                    #segment{firt_index = 6, last_index = 6, fname = File1}
                ]
            },
            Log4
        ),
        LogEntriesTest = [
            E
            ||
            E <- ?INITIAL_ENTRY,
            E#entry.index == 6] ++ [#entry{index = 7, term = 3, data = <<"e">>, type = ?OP_CONFIG}],
        Entries1 = entries(1, 7, Log4),
        ?assertEqual(LogEntriesTest, Entries1),
        ok = zraft_util:del_dir(?DATA_DIR)
    end}.


snaphost_on_commit_all() ->
    {"snaphost on commit clean all", fun() ->
        zraft_util:del_dir(?DATA_DIR),
        Log = load_fs({test, node()}),
        {ok, _, Log1} = handle_command({append, 0, 0, 0, ?INITIAL_ENTRY}, Log),
        {ok, OpRes, Log2} = handle_command({truncate_before, #snapshot_info{index = 7}}, Log1),
        check_snaphost_result(8, 7, 8, 7, ?BLANK_CONF, OpRes),
        ?assertMatch(
            #fs{
                fcounter = 5,
                first_index = 8,
                last_index = 7,
                last_term = 8,
                last_conf = ?BLANK_CONF,
                configs = [],
                open_segment = #segment{firt_index = 8, last_index = 7},
                closed_segments = []
            },
            Log2
        ),
        Entries1 = entries(0, 10, Log2),
        ?assertEqual([], Entries1),
        Log3 = load_fs({test, node()}),
        ?assertMatch(
            #fs{
                fcounter = 2,
                first_index = 8,
                last_index = 7,
                last_term = 0,
                last_conf = ?BLANK_CONF,
                configs = [],
                open_segment = #segment{firt_index = 8, last_index = 7},
                closed_segments = [
                ]
            },
            Log3
        ),
        Entries2 = entries(0, 10, Log3),
        ?assertEqual([], Entries2),
        ok = zraft_util:del_dir(?DATA_DIR)
    end}.

corrupted_log_file() ->
    {"corrupted file", fun() ->
        zraft_util:del_dir(?DATA_DIR),
        Log = load_fs({test, node()}),
        handle_command({append, 0, 0, 0, ?INITIAL_ENTRY}, Log),
        File1 = test_log_file_name("1-3.rlog"),
        File2 = test_log_file_name("4-6.rlog"),
        corrupt_file(File2),
        Log2 = load_fs({test, node()}),
        File3 = test_log_file_name("4-4.rlog"),
        ?assertMatch(
            #fs{
                fcounter = 2,
                first_index = 1,
                last_index = 4,
                last_term = 2,
                last_conf = {4, <<"d">>},
                open_segment = #segment{firt_index = 5, last_index = 4},
                closed_segments = [
                    #segment{firt_index = 4, last_index = 4, fname = File3},
                    #segment{firt_index = 1, last_index = 3, fname = File1}
                ]
            },
            Log2
        ),
        LogEntriesTest = [E || E <- ?INITIAL_ENTRY, E#entry.index < 5],
        Entries2 = entries(0, 10, Log2),
        ?assertEqual(LogEntriesTest, Entries2),
        zraft_util:del_dir(?DATA_DIR)
    end}.

corrupt_file(File) ->
    {ok, FD} = file:open(File, [read, write, binary, {read_ahead, ?READ_BUFFER_SIZE}]),
    {ok, _} = file:read(FD, 1),
    {ok, <<?RECORD_START:8, _:32, Size:32>>} = file:read(FD, 9),
    {ok, _} = file:read(FD, Size),
    {ok, <<?RECORD_START:8, _:32, _:32>>} = file:read(FD, 9),
    ok = file:write(FD, <<"sa">>),
    ok = file:close(FD).

truncate_conflict() ->
    {"truncate and don't change commited", fun() ->
        zraft_util:del_dir(?DATA_DIR),
        Log = load_fs({test, node()}),
        {ok, _, Log1} = handle_command({append, 0, 0, 0, ?INITIAL_ENTRY}, Log),
        {ok, _, Log2} = handle_command({append, 7, 8, 3, []}, Log1),
        FailEntries = [
            #entry{index = 1, term = 1, data = 1, type = ?OP_CONFIG},
            #entry{index = 2, term = 2, data = 2, type = ?OP_CONFIG},%%already commited
            #entry{index = 3, term = 2, data = 3, type = ?OP_CONFIG},
            #entry{index = 4, term = 2, data = 4, type = ?OP_CONFIG}
        ],
        {_, M} = spawn_monitor(fun() ->
            handle_command({append, 1, 1, 0, FailEntries}, Log2) end),
        receive
            {'DOWN', M, _Type, _, Reason} ->
                ?assertEqual({error, commit_collision}, Reason)
        end
    end}.

server_log() ->
    {"server functions", fun() ->
        zraft_util:del_dir(?DATA_DIR),
        {ok, FS} = start_link({test, node()}),
        Sync = append(FS, 0, 0, 0, ?INITIAL_ENTRY),
        OpRes = sync_fs(Sync),
        check_op_result(1, 7, 8, 0, {7, <<"g">>}, OpRes),
        R1 = update_raft_meta(FS, new_raft_meta),
        ?assertEqual(ok, R1),
        #log_descr{last_index = R2, first_index = R3} = get_log_descr(FS),
        ?assertEqual(7, R2),
        ?assertEqual(1, R3),
        R4 = get_last_conf(FS),
        ?assertEqual({7, <<"g">>}, R4),
        R5 = get_raft_meta(FS),
        ?assertEqual(new_raft_meta, R5),
        stop(FS),
        {ok, FS1} = start_link({test, node()}),
        #log_descr{last_index = R21, first_index = R31} = get_log_descr(FS1),
        ?assertEqual(7, R21),
        ?assertEqual(1, R31),
        R41 = get_last_conf(FS1),
        ?assertEqual({7, <<"g">>}, R41),
        R51 = get_raft_meta(FS1),
        ?assertEqual(new_raft_meta, R51),
        stop(FS1),
        zraft_util:del_dir(?DATA_DIR)
    end}.

check_op_result(
    FirstIndex,
    LastIndex,
    LastTerm,
    Commit,
    Conf,
    #log_op_result{
        log_state = LogState,
        result = Res,
        last_conf = LastConf
    }) ->
    ?assertMatch(
        {
            true,
            LastIndex,
            []
        },
        Res
    ),
    ?assertMatch(
        #log_descr{last_index = LastIndex, commit_index = Commit, first_index = FirstIndex, last_term = LastTerm},
        LogState
    ),
    ?assertMatch(
        Conf,
        LastConf
    ).
check_snaphost_result(
    FirstIndex,
    LastIndex,
    LastTerm,
    Commit,
    Conf,
    #log_op_result{
        log_state = LogState,
        result = Res,
        last_conf = LastConf
    }) ->
    ?assertMatch(
        ok,
        Res
    ),
    ?assertMatch(
        #log_descr{last_index = LastIndex, commit_index = Commit, first_index = FirstIndex, last_term = LastTerm},
        LogState
    ),
    ?assertMatch(
        Conf,
        LastConf
    ).

test_log_file_name(Name) ->
    PeerDirName = zraft_util:peer_name_to_dir_name(zraft_util:peer_name({test, node()})),
    filename:join([?DATA_DIR,PeerDirName,"log", Name]).

-endif.