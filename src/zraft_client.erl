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
-export([
    query/3,
    write/3,
    write_once/3,
    get_conf/1,
    get_conf/2,
    light_session/1,
    create/2,
    create/3
]).

-include_lib("zraft_lib/include/zraft.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


-define(TIMEOUT, 5000).
-define(CREATE_TIMEOUT, 5000).

-record(light_session, {peers, leader, peers_tmp = []}).

-type light_session() :: #light_session{}.

%%%===================================================================
%%% Read/Write
%%%===================================================================

-spec light_session(Conf) -> light_session() | {error, Reason} when
    Conf :: list(zraft_consensus:peer_id())|zraft_consensus:peer_id(),
    Reason :: no_peers|term().
%% @doc Create light session for read/write operations
%%
%% Use it for create object that will be used to batch read/write operation.
%%
%% If Conf is single PeerID then quorum configuration will be read.
%% In that case it may return error.
%%
%% You must crate new object scine you know that quorum configuration has been changed.
%%
%% If Conf is empty list then  {error,no_peers} will be returned.
%% @end
light_session([F | _] = Peers) ->
    #light_session{peers = Peers, leader = F};
light_session([]) ->
    {error, no_peers};
light_session(PeerID) ->
    case get_conf(PeerID) of
        {ok, {Leader, Peers}} ->
            #light_session{peers = Peers, leader = Leader};
        Error ->
            Error
    end.

-spec query(Raft, Query, Timeout) -> {Result, NewRaftConf}|RuntimeError when
    Raft :: light_session()|zraft_consensus:peer_id(),
    Query :: term(),
    Timeout :: timeout(),
    Result :: term(),
    NewRaftConf :: light_session()|zraft_consensus:peer_id(),
    RuntimeError :: {error, timeout}|{error, noproc}.
%% @doc Read data from state machine.
%%
%% Query parameter value depends on backend type used for state machine.
%%
%% If Raft is single peer than it will try to read data from it. Request will be redirected to leader
%% if that peer is follower or canditate. If peer is unreachable or is going down request will fail with error {error,noproc}.
%%
%% If Raft is light session object and current leader is going down it will retry requet to other peer and so on
%% until receive respose or timeout.
%% @end
query(Raft, Query, Timeout) ->
    Fun = fun(ID) -> zraft_consensus:query(ID, Query, Timeout) end,
    peer_execute(Raft, Fun, Timeout).

-spec write(Raft, Data, Timeout) -> {Result, NewRaftConf}|RuntimeError when
    Raft :: light_session()|zraft_consensus:peer_id(),
    Data :: term(),
    Timeout :: timeout(),
    Result :: term(),
    NewRaftConf :: light_session()|zraft_consensus:peer_id(),
    RuntimeError :: {error, timeout}|{error, noproc}.
%% @doc Write data to state machine.
%%
%% Data parameter value depends on backend type used for state machine.
%%
%% If Raft is single peer than it will try to write data from it. Request will be redirected to leader
%% if that peer is follower or canditate. If peer is unreachable or is going down request will fail with error {error,noproc}.
%%
%% If Raft is light session object and current leader is going down it will retry requet to other peer and so on
%% until receive respose or timeout.
%% @end
write(Raft, Data, Timeout) ->
    Fun = fun(ID) -> zraft_consensus:write(ID, Data, Timeout) end,
    peer_execute(Raft, Fun, Timeout).

%% @doc Write data to state machine.
%%
%% It guarantees that data will be applied to state machine exacly once.
%% If Raft is single peer than it will try to write data from it. Request will be redirected to leader
%% if that peer is follower or canditate. If peer is unreachable or is going down request will fail with error {error,noproc}.
%%
%% If Raft is light session object and current leader is going down it will retry requet to other peer and so on
%% until receive respose or timeout.
-spec write_once(light_session(),term(),timeout())->{term(),light_session()}|{error,timeout()}.
write_once(Session,Data,Timeout)->
    execute_once(Session,Data,os:timestamp(),Timeout).


%%%===================================================================
%%% Configuration
%%%===================================================================
-spec get_conf(PeerID) -> {ok, {Leader, Peers}}|{error, term()} when
    PeerID :: zraft_consensus:peer_id(),
    Leader :: zraft_consensus:peer_id(),%%Current leader
    Peers :: list(zraft_consensus:peer_id()).
%% @doc Read raft consensus configuaration.
%% @equiv get_conf(PeerID,5000)
%% @end
get_conf(PeerID) ->
    get_conf(PeerID, ?TIMEOUT).

-spec get_conf(PeerID, Timeout) -> {ok, {Leader, Peers}}|{error, term()} when
    PeerID :: zraft_consensus:peer_id(),
    Timeout :: timeout(),
    Leader :: zraft_consensus:peer_id(),
    Peers :: list(zraft_consensus:peer_id()).
%% @doc Read raft consensus configuaration.
%%
%% PeerID may be any peer in quorum. If it's not a leader, request will be redirected to the current leader
%% If leader losts lidership or goes down during execution it may return runtime error or {error,timeout}.
%% In that case you may retry request.
%% @end
get_conf(PeerID, Timeout) ->
    case wait_stable_conf(PeerID, Timeout) of
        {ok, {Leader, _Index, Peers}} ->
            {ok, {Leader, Peers}};
        Error ->
            Error
    end.

%%
-spec set_new_conf(Peer, NewPeers, OldPeers, Timeout) -> Result when
    Peer :: zraft_consensus:peer_id(),
    NewPeers :: list(zraft_consensus:peer_id()),
    OldPeers :: list(zraft_consensus:peer_id()),
    Timeout :: timeout(),
    Result :: {ok, list(zraft_consensus:peer_id())}|{error, peers_changed}|{error, leader_changed}|RaftError,
    RaftError :: {error, not_stable|newer_exists|process_prev_change}.
set_new_conf(PeerID, NewPeers, OldPeers, Timeout) ->
    NewSorted = ordsets:from_list(NewPeers),
    case wait_stable_conf(PeerID, Timeout) of
        {ok, {_Leader, _Index, NewSorted}} ->
            {ok, NewPeers};
        {ok, {Leader, Index, HasPeers}} ->
            case ordsets:from_list(OldPeers) of
                HasPeers ->
                    case catch zraft_consensus:set_new_configuration(Leader, Index, NewSorted, Timeout) of
                        ok -> {ok, NewPeers};
                        {leader, _NewLeader} ->
                            {error, leader_changed};
                        Else ->
                            format_error(Else)
                    end;
                _ ->
                    {error, peers_changed}
            end;
        Else->
            Else
    end.
%%%===================================================================
%%% Create new quorum
%%%===================================================================
-spec create(Peers, BackEnd) -> {ok, ResultPeers}|{error, term()} when
    Peers :: list(zraft_consensus:peer_id()),
    BackEnd :: module(),
    ResultPeers :: list(zraft_consensus:peer_id()).
%% @doc Create new quorum.
%% @equiv create(lists:nth(1,Peers),Peers,UseBackend)
%% @end
create(Peers, UseBackend) ->
    [FirstPeer | _] = Peers,
    case FirstPeer of
        {_, Node} when Node =:= node() ->
            create(FirstPeer, Peers, UseBackend);
        {_, Node} ->
            rpc:call(Node, ?MODULE, create, [FirstPeer, Peers, UseBackend])
    end.
-spec create(FirstPeer, Peers, BackEnd) -> {ok, ResultPeers}|{error, term()} when
    FirstPeer :: zraft_consensus:peer_id(),
    Peers :: list(zraft_consensus:peer_id()),
    BackEnd :: module(),
    ResultPeers :: list(zraft_consensus:peer_id()).
%% @doc Create new quorum.
%%
%% First it will be initialized FirstPeer. After that all other peers will be started and new configuration
%% will be applied to the FirstPeer and replicated to other.
%%
%% It returns error:
%%
%% 1. Some peer has been alredy started.
%%
%% 2. 5sec timeout will be expired.
%% @end
create(FirstPeer, AllPeers, UseBackend) ->
    case zraft_lib_sup:start_consensus(FirstPeer, UseBackend) of
        {ok, Pid} ->
            case catch zraft_consensus:initial_bootstrap(Pid) of
                ok ->
                    StartErrors = lists:foldl(
                        fun(P, Acc) when P == FirstPeer ->
                            Acc;
                            (P, Acc) ->
                                case zraft_lib_sup:start_consensus(P, UseBackend) of
                                    {ok, _} ->
                                        Acc;
                                    Else ->
                                        [{P, Else} | Acc]
                                end end,
                        [],
                        AllPeers
                    ),
                    case StartErrors of
                        [] ->
                            set_new_conf(FirstPeer, AllPeers, [FirstPeer], ?CREATE_TIMEOUT);
                        _ ->
                            {error, {peers_start_error, StartErrors}}
                    end;
                Else ->
                    format_error(Else)
            end;
        {error, Err} ->
            {error, Err}
    end.


%%%===================================================================
%%% Private
%%%===================================================================

peer_execute(Raft, Fun, Timeout) ->
    Start = os:timestamp(),
    case Raft of
        #light_session{} ->
            peer_execute_sessions(Raft, Fun, Start, Timeout);
        _ ->
            peer_execute(Raft, Fun, Start, Timeout)
    end.
peer_execute(PeerID, Fun, Start, Timeout) ->
    case catch Fun(PeerID) of
        {ok, Result} ->
            {Result, PeerID};
        {leader, NewLeader} ->
            case zraft_util:is_expired(Start, Timeout) of
                true ->
                    {error, timeout};
                {false,_Timeout1} ->
                    peer_execute(NewLeader, Fun, os:timestamp(), Timeout)
            end;
        Else ->
            format_error(Else)
    end.
peer_execute_sessions(Session = #light_session{}, Fun, Start, Timeout) ->
    Leader = current_session_leader(Session),
    Next = case catch Fun(Leader) of
        {ok, Result} ->
            {Result, Session#light_session{leader = Leader}};
        {leader, NewLeader} when NewLeader /= undefined ->
            {continue,Session#light_session{leader = NewLeader}};
        _Else ->
            Session1 = next_leader(Session, Leader),
            {continue,Session1}
    end,
    case Next of
        {continue, NextSession} ->
            case zraft_util:is_expired(Start, Timeout) of
                true ->
                    {error, timeout};
                {false,_Timeout1} ->
                    peer_execute_sessions(NextSession, Fun,Start,Timeout)
            end;
        Else ->
            Else
    end.

execute_once(Session, Data, Start, Timeout) ->
    SWrite = #swrite{data = Data, acc_upto = 0, from = self(), message_id = 1},
    execute_once(Session,1, SWrite, Start, Timeout).

execute_once(Session, Seq, SWrite, Start, Timeout) ->
    Leader = current_session_leader(Session),
    MRef = zraft_consensus:send_swrite(Leader, SWrite#swrite{timeout = Timeout}),
    Next = receive
               #swrite_error{sequence = Seq, error = not_leader, leader = NewLeader} when NewLeader /= undefined ->
                   {continue, Session#light_session{leader = NewLeader}};
               #swrite_error{sequence = Seq}->
                   Session1 = next_leader(Session, Leader),
                   {continue, Session1};
               #swrite_reply{sequence = Seq, data = Result} ->
                   {Result, Session#light_session{leader = Leader}};
               {'DOWN', MRef, process, _, _Error} ->
                   Session1 = next_leader(Session, Leader),
                   {continue, Session1}
           after Timeout ->
               {error, timeout}
           end,
    erlang:demonitor(MRef,[flush]),
    case Next of
        {continue, NextSession} ->
            case zraft_util:is_expired(Start, Timeout) of
                true ->
                    {error, timeout};
                {false,_Timeout1} ->
                    execute_once(NextSession, Seq, SWrite,Start, Timeout)
            end;
        Else ->
            Else
    end.

current_session_leader(#light_session{leader = undefined, peers = [L | _]}) ->
    L;
current_session_leader(#light_session{leader = undefined, peers_tmp = Tmp}) ->
    [L | _] = lists:reverse(Tmp),
    L;
current_session_leader(#light_session{leader = L}) ->
    L.

next_leader(Session = #light_session{peers = Peers, peers_tmp = Tmp}, Except) ->
    {Next, Peers1, Tmp1} = next_leader1(Peers, Except, Tmp),
    Session#light_session{leader = Next, peers = Peers1, peers_tmp = Tmp1}.
next_leader1([P], _Expept, []) ->
    {P, [P], []};
next_leader1([P | T], Except, Back) when P == Except ->
    next_leader1(T, Except, [P | Back]);
next_leader1([P | T], _Exept, Back) ->
    {P, T, [P | Back]};
next_leader1([], Exept, Back) ->
    next_leader1(lists:reverse(Back), Exept, []).


%% @private
wait_stable_conf(Peer, Timeout) ->
    wait_stable_conf(Peer, os:timestamp(), Timeout).

%% @private
wait_stable_conf(Peer, Start, Timeout) ->
    case zraft_util:is_expired(Start, Timeout) of
        true ->
            {error, timeout};
        {false,_Timeout1} ->
            case catch zraft_consensus:get_conf(Peer, Timeout) of
                {leader, undefined} ->
                    timer:sleep(zraft_consensus:get_election_timeout()),
                    wait_stable_conf(Peer, os:timestamp(), Timeout);
                {leader, NewLeader} ->
                    wait_stable_conf(NewLeader, os:timestamp(), Timeout);
                {ok, {0, _}} ->
                    timer:sleep(zraft_consensus:get_election_timeout()),
                    wait_stable_conf(Peer,os:timestamp(), Timeout);
                {ok, {Index, Peers}} ->
                    {ok, {Peer, Index, Peers}};
                retry ->
                    timer:sleep(zraft_consensus:get_election_timeout()),
                    wait_stable_conf(Peer,os:timestamp(), Timeout);
                Error ->
                    format_error(Error)
            end
    end.

%% @private
format_error({'EXIT', _Reason}) ->
    {error, noproc};
format_error({error, _} = Error) ->
    Error;
format_error(Error) ->
    {error, Error}.

-ifdef(TEST).

next_leader_test() ->
    S1 = light_session([1, 2, 3]),
    S2 = next_leader(S1, 1),
    ?assertEqual(#light_session{peers = [3], leader = 2, peers_tmp = [2, 1]}, S2),
    S3 = next_leader(S2, 3),
    ?assertEqual(#light_session{peers = [2, 3], leader = 1, peers_tmp = [1]}, S3),
    S4 = next_leader(S3, 0),
    ?assertEqual(#light_session{peers = [3], leader = 2, peers_tmp = [2, 1]}, S4).

-endif.