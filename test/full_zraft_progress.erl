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
-module(full_zraft_progress).
-author("dreyk").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("zraft_lib/include/zraft.hrl").


-define(TIMEOUT,10000).

force_timeout(P) ->
    gen_fsm:sync_send_all_state_event(P, force_timeout).

setup_node() ->
    zraft_util:set_test_dir("test-data"),
    net_kernel:start(['zraft_test@localhost', shortnames]),
    ok.
stop_node(_) ->
    net_kernel:stop(),
    zraft_util:clear_test_dir("test-data"),
    ok.

progress_test_() ->
    {
        setup,
        fun setup_node/0,
        fun stop_node/1,
        fun(_X) ->
            [
                progress()
            ]
        end
    }.
progress() ->
    {"progress", fun() ->
        PeerID1 = {test1, node()},
        {ok, Peer1} = zraft_consensus:start_link(PeerID1, zraft_dict_backend),
        PeerID2 = {test2, node()},
        {ok, Peer2} = zraft_consensus:start_link(PeerID2, zraft_dict_backend),
        PeerID3 = {test3, node()},
        {ok, Peer3} = zraft_consensus:start_link(PeerID3, zraft_dict_backend),
        ok = zraft_consensus:initial_bootstrap(Peer1),
        true = force_timeout(Peer1),
        Res1 = zraft_consensus:stat(Peer1),
        ?assertMatch(
            #peer_start{
                term = 2,
                state_name = leader,
                log_state = #log_descr{commit_index = 2, first_index = 1, last_index = 2, last_term = 2},
                leader = {test1,_}
            },
            Res1
        ),
        Res2 = zraft_consensus:stat(Peer2),
        ?assertMatch(
            #peer_start{
                term = 0,
                state_name = follower,
                log_state = #log_descr{commit_index = 0, first_index = 1, last_index = 0, last_term = 0},
                leader = undefined,
                conf_state = ?STABLE_CONF,
                conf = ?BLANK_CONF,
                snapshot_info = #snapshot_info{conf_index = 0,conf = ?BLANK_CONF,index = 0,term = 0}
            },
            Res2
        ),
        Res3 = zraft_consensus:stat(Peer3),
        ?assertMatch(
            #peer_start{
                term = 0,
                state_name = follower,
                log_state = #log_descr{commit_index = 0, first_index = 1, last_index = 0, last_term = 0},
                leader = undefined,
                conf_state = ?STABLE_CONF,
                conf = ?BLANK_CONF,
                snapshot_info = #snapshot_info{conf_index = 0,conf = ?BLANK_CONF,index = 0,term = 0}
            },
            Res3
        ),
        C1 = zraft_consensus:get_conf(PeerID1,?TIMEOUT),
        ?assertMatch({ok,{1,[{test1,_}]}},C1),
        C2 = zraft_consensus:set_new_configuration(PeerID1,1,lists:usort([PeerID1,PeerID2,PeerID3]),?TIMEOUT),
        ?assertMatch(ok,C2),
        Res4 = zraft_consensus:stat(Peer1),
        ?assertMatch(
            #peer_start{
                term = 2,
                state_name = leader,
                log_state = #log_descr{commit_index = 4, first_index = 1, last_index = 4, last_term = 2},
                leader = {test1,_}
            },
            Res4
        ),
        Res5 = zraft_consensus:stat(Peer2),
        ?assertMatch(
            #peer_start{
                term = 2,
                state_name = follower,
                log_state = #log_descr{commit_index = 4, first_index = 1, last_index = 4, last_term = 2},
                leader = {test1,_}
            },
            Res5
        ),
        Res6 = zraft_consensus:stat(Peer3),
        ?assertMatch(
            #peer_start{
                term = 2,
                state_name = follower,
                log_state = #log_descr{commit_index = 4, first_index = 1, last_index = 4, last_term = 2},
                leader = {test1,_}
            },
            Res6
        ),
        ok = zraft_consensus:stop(Peer1),
        ok = zraft_consensus:stop(Peer2),
        ok = zraft_consensus:stop(Peer3)
    end}.


-endif.
