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
-module(zraft_snapshot_writer).
-author("dreyk").

-behaviour(gen_server).

%% API
-export([start/5]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-export([
    data_done/1,
    data/3,
    read_snapshot_info/1,
    stop/1
]).

-include_lib("zraft_lib/include/zraft.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(DATA_DIR, "test-snaphot").
-endif.

-record(state, {raft,fsm,index,result_file,snap_dir,data_done=false,descr_done=false,descr_req}).

data_done(Writer)->
    gen_server:cast(Writer,data_done).

data(Writer,DirName,Fun)->
    gen_server:cast(Writer,{data,DirName,Fun}).

start(Raft,Fsm,LastIndex,ResultFile,SnapshotDir) ->
    gen_server:start(?MODULE, [Raft,Fsm,LastIndex,ResultFile,SnapshotDir], []).

stop(P)->
    gen_server:call(P,stop).

init([Raft,Fsm,LastIndex,ResultDir,SnapshotDir]) ->
    DescrRef = make_ref(),
    erlang:monitor(process,Fsm),
    zraft_consensus:make_snapshot_info(Raft,{DescrRef,self()},LastIndex),
    {ok, #state{
        raft = Raft,
        fsm = Fsm,
        index = LastIndex,
        result_file = ResultDir,
        snap_dir = SnapshotDir,
        descr_req = DescrRef
    }}.

handle_call(stop, _From, State) ->
    {stop,normal,ok,State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(data_done, State) ->
    State1 = State#state{data_done = true},
    maybe_finish(State1);
handle_cast({data,DirName,Fun}, State) ->
    ok = Fun(DirName),
    State1 = State#state{data_done = true},
    maybe_finish(State1);
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({'DOWN',_, process,_,_Reason},State)->
    {stop,{error,parent_exit},State};
handle_info({Ref,Res}, State=#state{descr_req = Ref}) ->
    State1 = State#state{descr_req = undefined},
    case Res of
        #snapshot_info{term = 0}->
            {stop,{error,null_term},State1};
        #snapshot_info{conf = ?BLANK_CONF}->
            {stop,{error,empty_conf},State1};
        Info=#snapshot_info{}->
            write_header(Info,State1);
        {error,Error}->
            {stop,{error,Error},State1}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(normal,_State) ->
    %%zraft_util:del_dir(State#state.snap_dir),
    ok;
terminate(Reason,State) ->
    lager:error("Snapshot ~s failed:~p",[State#state.snap_dir,Reason]),
    zraft_util:del_dir(State#state.snap_dir),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
maybe_finish(State=
    #state{descr_done = true,data_done = true,result_file = Res,snap_dir = Snapshot})->
    matbe_make_snapshot_backup(Res,Snapshot),
    {stop,normal,State};
maybe_finish(State)->
    {noreply,State}.

matbe_make_snapshot_backup(undefined,_)->
    ok;
matbe_make_snapshot_backup(Res,Snapshot)->
    {ok,_}=zip:zip(Res,[Snapshot]).

write_header(Info,State)->
    State1 = State#state{descr_done = true},
    HeaderFile = filename:join(State#state.snap_dir,"info"),
    ok = write_header_file(Info,HeaderFile),
    maybe_finish(State1).

write_header_file(#snapshot_info{}=Info,HeaderFile)->
    file:write_file(HeaderFile,term_to_binary(Info)).

read_snapshot_info(SnapshotDir)->
    HeaderFile = filename:join(SnapshotDir,"info"),
    case file:read_file(HeaderFile) of
        {ok,C}->
            case catch binary_to_term(C) of
                Info = #snapshot_info{}->
                    {ok,Info};
                _->
                    {error,invalid_snapshot_header}
            end;
        Else->
            lager:error("Can't read snapshot header ~s: ~p",[HeaderFile,Else]),
            {error,invalid_snapshot_header}
    end.


-ifdef(TEST).
setup() ->
    zraft_util:del_dir(?DATA_DIR),
    zraft_util:make_dir(?DATA_DIR),
    ok.
clear_setup(_) ->
    zraft_util:del_dir(?DATA_DIR),
    ok.

write_snapshot_test_() ->
    {
        setup,
        fun setup/0,
        fun clear_setup/1,
        fun(_X) ->
            [
                write()
            ]
        end
    }.

write() ->
    {"write snapshot", fun() ->
        ResultFile = filename:join(?DATA_DIR,"tmp-1"),
        SnapshotDir = filename:join(?DATA_DIR,"dump-1"),
        DataDir = filename:join(SnapshotDir,"data"),
        ok = zraft_util:make_dir(DataDir),
        {ok,Writer} = start(self(),self(),10,ResultFile,SnapshotDir),
        WriterRef = monitor(process,Writer),
        zraft_snapshot_writer:data(Writer,DataDir,fun(ToDir)->
            file:write_file(filename:join(ToDir,"dump"),<<"test">>) end),
        receive
            {'$gen_event',{make_snapshot_info,{ReqRef1,_},Index}}->
                ?assertEqual(10,Index),
                Writer ! {ReqRef1,#snapshot_info{index = Index,conf = [],term = 1,conf_index = 1}};
            Else->
                ?assertMatch(bad_result,Else)
            after 2000->
                ?assert(false)
        end,
        receive
            {'DOWN',WriterRef, process,_, Reason}->
                ?assertEqual(normal,Reason);
            Else1->
                ?assertMatch(bad_result,Else1)
            after 2000->
                ?assert(false)
        end,
        Res = zip:unzip(ResultFile),
        ?assertMatch({ok,["test-snaphot/dump-1/info",
            "test-snaphot/dump-1/data/dump"]},Res),
        {ok,SInfo} = read_snapshot_info("test-snaphot/dump-1"),
        ?assertMatch(#snapshot_info{index = 10,conf = [],term = 1,conf_index = 1},SInfo),
        {ok,C2} = file:read_file("test-snaphot/dump-1/data/dump"),
        ?assertEqual(<<"test">>,C2)
    end}.

-endif.

