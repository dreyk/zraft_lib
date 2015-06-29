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
-module(session_zraft_client).
-author("dreyk").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("zraft.hrl").


-define(TIMEOUT, 10000).

force_timeout(P) ->
    gen_fsm:sync_send_all_state_event(P, force_timeout).

setup_node() ->
    zraft_util:set_test_dir("session-test-data"),
    application:set_env(zraft_lib, max_log_count, 10),
    net_kernel:start(['zraft_test@localhost', shortnames]),
    ok.
stop_node(_) ->
    net_kernel:stop(),
    application:unset_env(zraft_lib, max_log_count),
    zraft_util:clear_test_dir("session-test-data1"),
    ok.

session_test_() ->
    {
        setup,
        fun setup_node/0,
        fun stop_node/1,
        fun(_X) ->
            [
                sessions(),
                session_first()
            ]
        end
    }.
sessions() ->
    {timeout,30,fun() ->
        PeerID1 = {test1, node()},
        {ok, Peer1} = zraft_consensus:start_link(PeerID1, zraft_dict_backend),
        PeerID2 = {test2, node()},
        {ok, Peer2} = zraft_consensus:start_link(PeerID2, zraft_dict_backend),
        PeerID3 = {test3, node()},
        {ok, Peer3} = zraft_consensus:start_link(PeerID3, zraft_dict_backend),
        ok = zraft_consensus:initial_bootstrap(Peer1),
        true = force_timeout(Peer1),
        ok = wait_leadership(Peer1,2,1),
        C = zraft_consensus:set_new_configuration(PeerID1, 1, lists:usort([PeerID1, PeerID2, PeerID3]), ?TIMEOUT),
        ?assertMatch(ok, C),
        ok = wait_new_config(4, PeerID1, 1),
        {ok,S1} = zraft_session:start_link(PeerID1,10000),
        {ok,S2} = zraft_session:start_link(PeerID2,10000),
        W1 = zraft_session:write(S1,{1,v1},1000),
        ?assertEqual(ok,W1),
        R1 = zraft_session:query(S2,1,1000),
        ?assertEqual({ok,v1},R1),
        R2 = zraft_session:query(S2,2,waite1,1000),
        ?assertEqual(not_found,R2),
        W2 = zraft_session:write(S1,{2,v2},true,1000),
        ?assertEqual(ok,W2),
        receive
            T1->
                ?assertEqual(#swatch_trigger{ref = waite1,reason = change},T1),
                ?debugMsg("First trigger ok")
        end,
        R3 = zraft_session:query(S2,2,waite2,1000),
        ?assertEqual({ok,v2},R3),
        zraft_session:stop(S1),
        receive
            T2->
                ?assertEqual(#swatch_trigger{ref = waite2,reason = change},T2),
                ?debugMsg("Expire trigger ok")
        end,
        R4 = zraft_session:query(S2,2,waite3,1000),
        ?assertEqual(not_found,R4),
        Me = self(),
        spawn_link(fun()->
            Me ! {w5,(catch zraft_session:write(S2,{3,3},2000))} end),
        spawn_link(fun()->
            Me ! {r5,(catch zraft_session:query(S2,1,2000))} end),
        ok = zraft_consensus:stop(Peer1),
        receive
            T3->
                ?assertEqual(#swatch_trigger{ref = waite3,reason=change_leader},T3),
                ?debugMsg("Leader change ok")
        end,
        receive
            {w5,W5}->
                ?assertEqual(ok,W5)
        end,
        receive
            {r5,R5}->
                ?assertEqual({ok,v1},R5)
        end,
        zraft_session:stop(S2),
        ok = zraft_consensus:stop(Peer2),
        ok = zraft_consensus:stop(Peer3)
    end}.

session_first() ->
    {timeout,30,fun() ->
        PeerID1 = {test1, node()},
        PeerID2 = {test2, node()},
        PeerID3 = {test3, node()},
        {ok, Peer1} = zraft_consensus:start_link(PeerID1, zraft_dict_backend),
        wait_start(Peer1,1),
        {ok,S1} = zraft_session:start_link([PeerID1,PeerID2,PeerID3],10000),
        timer:sleep(1000),
        {ok, Peer2} = zraft_consensus:start_link(PeerID2, zraft_dict_backend),
        {ok, Peer3} = zraft_consensus:start_link(PeerID3, zraft_dict_backend),
        {L,_} = wait_leader(Peer1,1),
        ?debugFmt("Leader is ~p",[L]),
        R1 = zraft_session:query(S1,1,10000),
        ?assertEqual({ok,v1},R1),
        zraft_session:stop(S1),
        ok = zraft_consensus:stop(Peer1),
        ok = zraft_consensus:stop(Peer2),
        ok = zraft_consensus:stop(Peer3)
    end}.

down_attempt(Attempt,Max) when Attempt<Max->
    ok;
down_attempt(_Attempt,_Max)->
    exit({error,to_many_attempts}).
wait_leadership(Peer,Commit, Attempt) ->
    down_attempt(Attempt,20),
    case zraft_consensus:stat(Peer) of
        #peer_start{state_name = leader,log_state = #log_descr{commit_index = Commit}} ->
            ok;
        #peer_start{} ->
            ?debugFmt("Wait leadership attempt - ~p",[Attempt]),
            wait_leadership(Peer,Commit, Attempt + 1)
    end.
wait_start(Peer, Attempt) ->
    timer:sleep(500),
    down_attempt(Attempt,20),
    case zraft_consensus:stat(Peer) of
        #peer_start{state_name = load} ->
            wait_start(Peer, Attempt + 1);
        _->
            ok
    end.
wait_leader(Peer, Attempt) ->
    timer:sleep(500),
    down_attempt(Attempt,20),
    case zraft_consensus:stat(Peer) of
        #peer_start{state_name = leader, leader = L, term = T} ->
            {L, T};
        #peer_start{state_name = follower, leader = L, term = T} when L /= undefined ->
            {L, T};
        #peer_start{state_name = S, leader = L} ->
            ?debugFmt("Current state ~s,leader is ~p. Make leader attempt - ~p", [S, L, Attempt]),
            wait_leader(Peer, Attempt + 1)
    end.

wait_new_config(Index, PeerID, Attempt) ->
    down_attempt(Attempt,30),
    case zraft_consensus:get_conf(PeerID, ?TIMEOUT) of
        {ok, {Index, _}} ->
            ok;
        _ ->
            ?debugFmt("Wait config attempt - ~p", [Attempt]),
            wait_new_config(Index, PeerID, Attempt + 1)
    end.

-endif.

