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
-module(zraft_lib_sup).
-author("dreyk").

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1,start_consensus/2,start_consensus/1]).

-include("zraft.hrl").

-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec(init(Args :: term()) ->
    {ok, {SupFlags :: {RestartStrategy :: supervisor:strategy(),
        MaxR :: non_neg_integer(), MaxT :: non_neg_integer()},
        [ChildSpec :: supervisor:child_spec()]
    }}).
init([]) ->
    Timeout = max(1,round(zraft_consensus:get_election_timeout()*4/1000)),
    SupFlags = {one_for_one,2,Timeout},
    Peers  = read_peers(),
    {ok, {SupFlags, Peers}}.

-spec start_consensus(zraft_consensus:peer_id(),module()) -> supervisor:startchild_ret().
start_consensus(PeerID,BackEnd)->
    Spec = consensus_spec([PeerID,BackEnd]),
    start_result(supervisor:start_child(?MODULE, Spec)).

-spec start_consensus(zraft_consensus:peer_id()) -> supervisor:startchild_ret().
start_consensus(PeerID)->
    Spec = consensus_spec([PeerID]),
    start_result(supervisor:start_child(?MODULE, Spec)).

start_result({ok,P})->
    {ok,P};
start_result({error,{already_started,_}})->
    {error,already_created};
start_result({error,already_present})->
    {error,already_created};
start_result(Err)->
    Err.

%% @private
consensus_spec([{PeerName,_}|_]=Args) ->
    {
        PeerName,
        {zraft_consensus, start_link,Args},
        permanent,
        5000,
        worker,
        [zraft_consensus]
    }.

%%@private
read_peers()->
    DataDir = zraft_util:get_env(log_dir, "data"),
    case file:list_dir(DataDir) of
        {ok,Dirs}->
            read_peers(DataDir,Dirs,[]);
        _->
            []
    end.

read_peers(DataDir,[Dir|T],Acc)->
    RaftDir = filename:join(DataDir,Dir),
    case zraft_fs_log:load_raft_meta(RaftDir) of
        {ok,#raft_meta{id = Peer,back_end = BackEnd}}->
            Spec = consensus_spec([Peer,BackEnd]),
            read_peers(DataDir,T,[Spec|Acc]);
        _->
            lager:warning("~p does't contain peer meta",[RaftDir]),
            read_peers(DataDir,T,Acc)
    end;
read_peers(_DataDir,[],Acc)->
    Acc.
