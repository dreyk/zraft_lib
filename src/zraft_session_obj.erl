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
-module(zraft_session_obj).
-author("dreyk").

-export([
    create/3,
    set_leader/2,
    change_leader/2,
    next/1,
    fail/1,
    reset/1,
    leader/1,
    is_session/1
]).

-export_type([
    light_session/0
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(NORMAL, ok).

-record(light_session, {peers, leader, backoff,election_timeout}).

-type light_session() :: #light_session{}.

is_session(#light_session{}) ->
    true;
is_session(_) ->
    false.

-spec create(list(zraft_consensus:peer_id()), timeout(),timeout()) -> light_session().
create([], _BackOff,_ElectionTimeout) ->
    throw({error, no_peers});
create([F | _] = Peers, BackOff,ElectionTimeout) ->
    PeersStatus = [{P, {?NORMAL,?NORMAL}} || P <- Peers],
    #light_session{
        peers = orddict:from_list(PeersStatus),
        leader = F,
        backoff = BackOff * 1000,
        election_timeout = ElectionTimeout*1000
    }.

-spec set_leader(zraft_consensus:peer_id(), light_session())->light_session().
set_leader(NewLeader,SObj = #light_session{peers = Peers})->
    [_|T] = reorder(NewLeader,Peers,[]),
    Peers1 = [{NewLeader,{?NORMAL,?NORMAL}}|T],
    SObj#light_session{peers = Peers1,leader = NewLeader}.

-spec change_leader(zraft_consensus:peer_id(), light_session()) ->
    light_session()|{error, all_failed}|{error,etimeout}.
change_leader(NewLeader,SObj = #light_session{peers = Peers})->
    [{Old,{O1,_O2}}|T1] = Peers,
    Peers1 = [{Old,{O1,os:timestamp()}}|T1],
    Peers2 = reorder(NewLeader,Peers1,[]),
    find_leader(SObj#light_session{peers = Peers2}).

find_leader(SObj = #light_session{peers = Peers,backoff = BackOff,election_timeout = Election})->
    Check = fun({_,{N1,N2}})->
        case is_expired(BackOff,N1) of
            true->
                case is_expired(Election,N2) of
                    true->
                        true;
                    _->
                        false
                end;
            _->
                failed
        end
    end,
    case next_candidate(Check,Peers,[],failed) of
        {failed,_,_}->
            {error, all_failed};
        {false,_,_}->
            {error,etimeout};
        {true,Leader,Peer3}->
            SObj#light_session{leader = Leader,peers = Peer3}
    end.

next_candidate(_Check,[],Acc,Status)->
    {Status,undefined,lists:reverse(Acc)};
next_candidate(Check,[{P,_}=E|T],Acc,Status)->
    case Check(E) of
        failed->
            next_candidate(Check,T,[E|Acc],Status);
        true->
            {true,P,[{P,{?NORMAL,?NORMAL}}|T]++lists:reverse(Acc)};
        false->
            next_candidate(Check,T,[E|Acc],false)
    end.

reorder(P,[{P,_}|_]=H,Acc)->
    H++lists:reverse(Acc);
reorder(P,[E|T],Acc)->
    reorder(P,T,[E|Acc]);
reorder(P,[],Acc)->
    [{P,{?NORMAL,?NORMAL}}|lists:reverse(Acc)].

-spec next(light_session()) -> light_session()|{error, all_failed}|{error,etimeout}.
next(SObj = #light_session{peers = Peers}) ->
    [{Old,{O1,_O2}}|T1] = Peers,
    Peers1 = [{Old,{O1,os:timestamp()}}|T1],
    find_leader(SObj#light_session{peers = Peers1}).

-spec fail(light_session()) -> light_session()|{error, all_failed}|{error,etimeout}.
fail(SObj = #light_session{peers = Peers}) ->
    [{Old,{_O1,O2}}|T1] = Peers,
    Peers1 = [{Old,{os:timestamp(),O2}}|T1],
    find_leader(SObj#light_session{peers = Peers1}).

-spec reset(light_session()) -> light_session().
reset(SObj = #light_session{peers = Peers}) ->
    Peers1 = [{P,{?NORMAL,?NORMAL}}||{P,_}<-Peers],
    [{Leader,_}|_]=lists:keysort(1,Peers1),
    SObj#light_session{peers = Peers1,leader = Leader}.


-spec leader(light_session()) -> zraft_consensus:peer_id().
leader(#light_session{leader = Leader}) ->
    Leader.


is_expired(_Timeout,?NORMAL)->
    true;
is_expired(Timeout,V)->
    case timer:now_diff(os:timestamp(), V) of
        D when D >= Timeout ->
            true;
        _ ->
            false
     end.

-ifdef(TEST).

next_leader_test() ->
    S1 = create([1, 2, 3, 4, 5],100,100),
    L1 = leader(S1),
    ?assertEqual(1, L1),
    S2 = set_leader(3, S1),
    L2 = leader(S2),
    ?assertEqual(3, L2),
    S3 = fail(S2),
    L3 = leader(S3),
    ?assertEqual(4, L3),
    S4 = fail(S3),
    L4 = leader(S4),
    ?assertEqual(5, L4),
    S5 = fail(S4),
    L5 = leader(S5),
    ?assertEqual(1, L5),
    S6 = fail(S5),
    L6 = leader(S6),
    ?assertEqual(2, L6),
    Fail = fail(S6),
    ?assertEqual({error, all_failed}, Fail),
    S7 = reset(S6),
    L7 = leader(S7),
    ?assertEqual(1, L7).

-endif.