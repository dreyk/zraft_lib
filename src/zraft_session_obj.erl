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
    create/2,
    set_leader/2,
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

-record(light_session, {peers, leader, backoff}).

-type light_session() :: #light_session{}.

is_session(#light_session{}) ->
    true;
is_session(_) ->
    false.

-spec create(list(zraft_consensus:peer_id()), timeout()) -> light_session().
create([], _BackOff) ->
    throw({error, no_peers});
create([F | _] = Peers, BackOff) ->
    PeersStatus = [{P, ?NORMAL} || P <- Peers],
    #light_session{peers = orddict:from_list(PeersStatus), leader = F, backoff = BackOff * 1000}.

-spec set_leader(zraft_consensus:peer_id(), light_session()) -> light_session().
set_leader(NewLeader, SObj = #light_session{peers = Peers, leader = OldLeader}) ->
    Peers1 = orddict:store(OldLeader, ?NORMAL, Peers),
    Peers2 = orddict:store(NewLeader, ?NORMAL, Peers1),
    SObj#light_session{peers = Peers2, leader = NewLeader}.

-spec next(light_session()) -> light_session().
next(SObj = #light_session{peers = Peers, leader = Leader,backoff = BackOff}) ->
    Peers1 = backoff(BackOff,Peers),
    case next_candidate(Leader, Peers1, undefined) of
        not_found ->
            SObj;
        NewCandidate ->
            SObj#light_session{leader = NewCandidate, peers = Peers}
    end.

-spec fail(light_session()) -> light_session()|{error, all_failed}.
fail(SObj = #light_session{peers = Peers, leader = Leader,backoff = BackOff}) ->
    Peers1 = backoff(BackOff,Peers),
    Peers2 = orddict:store(Leader, os:timestamp(), Peers1),
    case next_candidate(Leader, Peers2, undefined) of
        not_found ->
            {error, all_failed};
        NewCandidate ->
            SObj#light_session{leader = NewCandidate, peers = Peers2}
    end.

-spec reset(light_session()) -> light_session().
reset(#light_session{peers = Peers,backoff = BackOff}) ->
    create([P || {P, _} <- Peers],BackOff).

-spec leader(light_session()) -> zraft_consensus:peer_id().
leader(#light_session{leader = Leader}) ->
    Leader.


next_candidate(PrevLeader, [{C, ?NORMAL} | T], undefined) when C < PrevLeader ->
    next_candidate(PrevLeader, T, C);
next_candidate(PrevLeader, [{C, ?NORMAL} | _T], _FirstCandidate) when C > PrevLeader ->
    C;
next_candidate(PrevLeader, [_ | T], FirstCandidate) ->
    next_candidate(PrevLeader, T, FirstCandidate);
next_candidate(_PrevLeader, [], undefined) ->
    not_found;
next_candidate(_PrevLeader, [], FirstCandidate) ->
    FirstCandidate.


backoff(BackOff, Peers) ->
    orddict:map(
        fun
            (_K, ?NORMAL) ->
                ?NORMAL;
            (_K, S1) ->
                case timer:now_diff(os:timestamp(), S1) of
                    D when D >= BackOff ->
                        ?NORMAL;
                    _ ->
                        S1
                end end, Peers).

-ifdef(TEST).

next_leader_test() ->
    S1 = create([1, 2, 3, 4, 5],100),
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