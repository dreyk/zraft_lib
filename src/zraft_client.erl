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
-module(zraft_client).
-author("dreyk").

%% API
-export([create/2]).

-define(TIMEOUT,5000).
-define(CREATE_TIMEOUT,5000).

-spec create(list(zraft_consensus:peer_id()),module())->{ok,list(zraft_consensus:peer_id())}|{error,term()}.
create(Peers,UseBackend)->
    [FirstPeer|_]=Peers,
    case FirstPeer of
        {_,Node} when Node=:=node()->
            create(FirstPeer,Peers,UseBackend);
        {_,Node}->
            rpc:call(Node,?MODULE,create,[FirstPeer,Peers,UseBackend])
    end.
-spec create(zraft_consensus:peer_id(),list(zraft_consensus:peer_id()),module())->
    {ok,list(zraft_consensus:peer_id())}|{error,term()}.
create(FirstPeer,AllPeers,UseBackend)->
    case zraft_lib_sup:start_consensus(FirstPeer,UseBackend) of
        {ok,Pid}->
            case zraft_consensus:initial_bootstrap(Pid) of
                ok->
                    StartErrors = lists:foldl(
                        fun(P,Acc) when P == FirstPeer->
                            Acc;
                        (P,Acc)->
                        case zraft_lib_sup:start_consensus(P,UseBackend) of
                            {ok,_}->
                                Acc;
                            Else->
                                [{P,Else}|Acc]
                        end end,
                        [],
                        AllPeers
                    ),
                    case StartErrors of
                        []->
                            set_new_conf(FirstPeer,AllPeers,[FirstPeer],?CREATE_TIMEOUT);
                        _->
                            {error,{peers_start_error,StartErrors}}
                    end;
                Else->
                    Else
            end;
        {error,Err}->
            {error,Err}
    end.

-spec set_new_conf(Peer,NewPeers,OldPeers,Timeout)->Result when
    Peer::zraft_consensus:peer_id(),
    NewPeers::list(zraft_consensus:peer_id()),
    OldPeers::list(zraft_consensus:peer_id()),
    Timeout::timeout(),
    Result::{ok,list(zraft_consensus:peer_id())}|{error,peers_changed}|{error,leader_changed}|RaftError,
    RaftError::zraft_consensus:raft_runtime_error().
set_new_conf(PeerID,NewPeers,OldPeers,Timeout)->
    NewSorted = ordsets:from_list(NewPeers),
    case wait_stable_conf(PeerID,Timeout) of
        {ok,_Leader,_Index,NewSorted}->
            {ok,NewPeers};
        {ok,Leader,Index,HasPeers}->
            case ordsets:from_list(OldPeers) of
                HasPeers->
                    {error,peers_changed};
                _->
                    case zraft_consensus:set_new_configuration(Leader,Index,NewSorted,Timeout) of
                        ok->{ok,NewPeers};
                        {leader,_NewLeader}->
                            {error,leader_changed};
                        Else->
                            Else
                    end
            end
    end.

wait_stable_conf(Peer,Timeout)->
    wait_stable_conf(Peer,os:timestamp(),Timeout).

wait_stable_conf(Peer,Start,Timeout)->
    case zraft_util:is_expired(Start,Timeout) of
        true->
            {error,timeout};
        _->
            case zraft_consensus:get_conf(Peer,Timeout) of
                {leader,undefined}->
                    timer:sleep(zraft_consensus:get_election_timeout()),
                    wait_stable_conf(Peer,Start,Timeout);
                {leader,NewLeader}->
                    wait_stable_conf(NewLeader,Start,Timeout);
                {ok,{0,_}}->
                    timer:sleep(zraft_consensus:get_election_timeout()),
                    wait_stable_conf(Peer,Start,Timeout);
                {ok,{Index,Peers}}->
                    {ok,{Peer,Index,Peers}};
                retry->
                    timer:sleep(zraft_consensus:get_election_timeout()),
                    wait_stable_conf(Peer,Start,Timeout);
                {error,Error}->
                    {error,Error}
            end
    end.
