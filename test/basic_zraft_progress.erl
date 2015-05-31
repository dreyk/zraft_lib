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
-module(basic_zraft_progress).
-author("dreyk").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("zraft_lib/include/zraft.hrl").

append_test(P, E) ->
    gen_fsm:sync_send_event(P, {append_test, E}).
force_timeout(P) ->
    gen_fsm:sync_send_all_state_event(P, force_timeout).

setup_node() ->
    net_kernel:start(['zraft_test@localhost', shortnames]),
    zraft_util:set_test_dir("test-data"),
    ok.
stop_node(_) ->
    zraft_util:clear_test_dir("test-data"),
    net_kernel:stop(),
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
        PeerName = {test, node()},
        {ok, Peer} = zraft_consensus:start_link(PeerName, zraft_dict_backend),
        ok = zraft_consensus:initial_bootstrap(Peer),
        true = force_timeout(Peer),
        Res1 = zraft_consensus:stat(Peer),
        ?assertMatch(
            #peer_start{
            term = 2,
            state_name = leader,
            allow_commit = true,
            agree_index = 2,
            log_state = #log_descr{commit_index = 2,first_index = 1, last_index = 2, last_term = 2},
            leader = {test,_}
            },
            Res1
        ),
        Ref = make_ref(),
        Peer1 = {test1, {Ref, self()}},
        Peer2 = {test2, {Ref, self()}},
        %%set new config
        NewConfEntry = #enrty{term = 2, index = 3, type = ?OP_CONFIG, data = #pconf{old_peers = [PeerName],
            new_peers = ordsets:from_list([PeerName, Peer1, Peer2])}},
        ok = append_test(Peer, [NewConfEntry]),
        Res2 = zraft_consensus:stat(Peer),
        %%transitional state
        ?assertMatch(
            #peer_start{
                term = 2,
                state_name = leader,
                log_state = #log_descr{commit_index = 2, first_index = 1, last_index = 3, last_term = 2},
                leader = {test,_},
                conf_state = ?TRANSITIONAL_CONF,
                conf = {3,#pconf{old_peers = [{test,_}],new_peers = [{test,_},{test1,_},{test2,_}]}},
                back_end = zraft_dict_backend
            },
            Res2
        ),
        Up1 = check_progress(Peer1),
        ?assertMatch({peer_up,{{test,_},_}},Up1),
        Up2 = check_progress(Peer2),
        ?assertMatch({peer_up,{{test,_},_}},Up2),
        %%check hearbeat
        Command1_1 = check_progress(Peer1),
        ?assertMatch(
            #append_entries{commit_index = 2, term = 2, entries = [], prev_log_index = 3, prev_log_term = 2},
            Command1_1
        ),
        Command2_1 = check_progress(Peer2),
        ?assertMatch(
            #append_entries{commit_index = 2, term = 2, entries = [], prev_log_index = 3, prev_log_term = 2},
            Command2_1
        ),
        %%reject hearbeat, our log out of date
        Reply1 = #append_reply{success = false, term = 2, last_index = 0, agree_index = 0},
        fake_reply(Command1_1, Reply1, Peer1),
        fake_reply(Command2_1, Reply1, Peer2),
        Command1_2 = check_progress(Peer1),
        ?assertMatch(
            #append_entries{commit_index = 0, term = 2, entries = [], prev_log_index = 0, prev_log_term = 0},
            Command1_2
        ),
        Command2_2 = check_progress(Peer2),
        ?assertMatch(
            #append_entries{commit_index = 0, term = 2, entries = [], prev_log_index = 0, prev_log_term = 0},
            Command2_2
        ),
        %%accept new hearbeat
        Reply2 = #append_reply{success = true, term = 2, last_index = 0, agree_index = 0},
        fake_reply(Command1_2, Reply2, Peer1),
        fake_reply(Command2_2, Reply2, Peer2),
        Command1_3 = check_progress(Peer1),
        ?assertMatch(
            #append_entries{commit_index = 2, term = 2, prev_log_index = 0, prev_log_term = 0},
            Command1_3
        ),
        Command2_3 = check_progress(Peer2),
        ?assertMatch(
            #append_entries{commit_index = 2, term = 2, prev_log_index = 0, prev_log_term = 0},
            Command2_3
        ),
        %%must receive prev log
        ?assertMatch(
            [
                #enrty{index = 1, term = 1, type = ?OP_CONFIG, data = #pconf{}},
                #enrty{index = 2, term = 2, type = ?OP_NOOP},
                #enrty{index = 3, term = 2, type = ?OP_CONFIG, data = #pconf{}}
            ],
            Command1_3#append_entries.entries
        ),
        ?assertMatch(
            [
                #enrty{index = 1, term = 1, type = ?OP_CONFIG, data = #pconf{}},
                #enrty{index = 2, term = 2, type = ?OP_NOOP},
                #enrty{index = 3, term = 2, type = ?OP_CONFIG, data = #pconf{}}
            ],
            Command2_3#append_entries.entries
        ),
        %%replication ok
        Reply3 = #append_reply{success = true, term = 2, last_index = 3, agree_index = 3},
        fake_reply(Command1_3, Reply3, Peer1),
        fake_reply(Command2_3, Reply3, Peer2),
        Command1_4 = check_progress(Peer1),
        %%check commit message
        ?assertMatch(
            #append_entries{commit_index = 3, term = 2, prev_log_index = 3, prev_log_term = 2, entries = []},
            Command1_4
        ),
        Command2_4 = check_progress(Peer2),
        ?assertMatch(
            #append_entries{commit_index = 3, term = 2, prev_log_index = 3, prev_log_term = 2, entries = []},
            Command2_4
        ),
        Reply4 = #append_reply{success = true, term = 2, last_index = 3, agree_index = 3},
        fake_reply(Command1_4, Reply4, Peer1),
        fake_reply(Command2_4, Reply4, Peer2),
        Command1_5 = check_progress(Peer1),
        %%check new conf
        ?assertMatch(
            #append_entries{commit_index = 3, term = 2, prev_log_index = 3, prev_log_term = 2,
                entries = [#enrty{index = 4, type = ?OP_CONFIG, term = 2}]},
            Command1_5
        ),
        Command2_5 = check_progress(Peer2),
        ?assertMatch(
            #append_entries{commit_index = 3, term = 2, prev_log_index = 3, prev_log_term = 2,
                entries = [#enrty{index = 4, type = ?OP_CONFIG, term = 2}]},
            Command2_5
        ),
        Res3 = zraft_consensus:stat(Peer),
        %%stable state
        ?assertMatch(
            #peer_start{
                term = 2,
                state_name = leader,
                log_state = #log_descr{commit_index = 3, first_index = 1, last_index = 4, last_term = 2},
                leader = {test,_},
                conf_state = ?STABLE_CONF,
                conf = {4,#pconf{new_peers = [],old_peers = [{test,_},{test1,_},{test2,_}]}}
            },
            Res3
        ),
        Reply5 = #append_reply{success = true, term = 2, last_index = 4, agree_index = 4},
        fake_reply(Command1_5, Reply5, Peer1),
        fake_reply(Command2_5, Reply5, Peer2),
        Command1_6 = check_progress(Peer1),
        %%check new conf commit
        ?assertMatch(
            #append_entries{commit_index = 4, term = 2, prev_log_index = 4, prev_log_term = 2,
                entries = []},
            Command1_6
        ),
        Command2_6 = check_progress(Peer2),
        ?assertMatch(
            #append_entries{commit_index = 4, term = 2, prev_log_index = 4, prev_log_term = 2,
                entries = []},
            Command2_6
        ),
        %%accept commit new conf
        Reply6 = #append_reply{success = true, term = 2, last_index = 4, agree_index = 4},
        fake_reply(Command1_6, Reply6, Peer1),
        fake_reply(Command2_6, Reply6, Peer2),

        Res4 = zraft_consensus:stat(Peer),
        ?assertMatch(
            #peer_start{
                term = 2,
                state_name = leader,
                log_state = #log_descr{commit_index = 4, first_index = 1, last_index = 4, last_term = 2},
                leader = {test,_},
                conf_state = ?STABLE_CONF,
                conf = {4,#pconf{new_peers = [],old_peers = [{test,_},{test1,_},{test2,_}]}}
            },
            Res4
        ),
        %%peer1 new leader
        VoteReq = #vote_request{term = 3, from = Peer1, last_index = 3, last_term = 2},
        ok = zraft_peer_route:cmd(PeerName, VoteReq),
        Command1_7 = check_progress(Peer1),
        %%leader must reject hearbeat
        ?assertMatch(
            #vote_reply{commit = 4, granted = false, peer_term = 2, request_term = 3},
            Command1_7
        ),
        Ref1 = ref1,
        NewLeaderHearbeat =
            #append_entries{
                prev_log_term = 2,
                prev_log_index = 4,
                entries = [],
                commit_index = 4,
                request_ref = Ref1,
                from = Peer1,
                term = 3
            },
        ok = zraft_peer_route:cmd(PeerName, NewLeaderHearbeat),
        Command1_8 = check_progress(Peer1),
        ?assertMatch(
            #append_reply{last_index = 4, agree_index = 4, request_ref = ref1, success = true, term = 3},
            Command1_8
        ),
        %%start new election
        true = force_timeout(Peer),
        Command1_9 = check_progress(Peer1),
        ?assertMatch(
            #vote_request{last_term = 2, last_index = 4, term = 4},
            Command1_9
        ),
        Command2_9 = check_progress(Peer2),
        ?assertMatch(
            #vote_request{last_term = 2, last_index = 4, term = 4},
            Command2_9
        ),
        Res5 = zraft_consensus:stat(Peer),
        ?assertMatch(
            #peer_start{
                term = 4,
                state_name = candidate,
                log_state = #log_descr{commit_index = 4, first_index = 1, last_index = 4, last_term = 2},
                leader = undefined,
                conf_state = ?STABLE_CONF,
                conf = {4,#pconf{new_peers = [],old_peers = [{test,_},{test1,_},{test2,_}]}}
            },
            Res5
        ),
        %%reject and new election
        Reply7 = #vote_reply{request_term = 4, granted = false, peer_term = 4, commit = 4},
        fake_reply(Command1_9, Reply7, Peer1),
        Reply8 = #vote_reply{request_term = 4, granted = false, peer_term = 5, commit = 4},
        fake_reply(Command2_9, Reply8, Peer2),
        true = force_timeout(Peer),
        Command1_10 = check_progress(Peer1),
        ?assertMatch(
            #vote_request{last_term = 2, last_index = 4, term = 6},
            Command1_10
        ),
        Command2_10 = check_progress(Peer2),
        ?assertMatch(
            #vote_request{last_term = 2, last_index = 4, term = 6},
            Command2_10
        ),
        Reply9 = #vote_reply{request_term = 6, granted = true, peer_term = 5, commit = 4},
        fake_reply(Command1_10, Reply9, Peer1),
        fake_reply(Command2_10, Reply9, Peer2),
        Res6 = zraft_consensus:stat(Peer),
        ?assertMatch(
            #peer_start{
                term = 6,
                state_name = leader,
                log_state = #log_descr{commit_index = 4, first_index = 1, last_index = 5, last_term = 6},
                leader = {test,_},
                conf_state = ?STABLE_CONF,
                conf = {4,#pconf{new_peers = [],old_peers = [{test,_},{test1,_},{test2,_}]}}
            },
            Res6
        ),
        Command1_11 = check_progress(Peer1),
        ?assertMatch(
            #append_entries{commit_index = 4, prev_log_index = 4, prev_log_term = 2, entries = [], term = 6},
            Command1_11
        ),
        Command2_11 = check_progress(Peer2),
        ?assertMatch(
            #append_entries{commit_index = 4, prev_log_index = 4, prev_log_term = 2, entries = [], term = 6},
            Command2_11
        ),
        Reply10 = #append_reply{success = true, term = 6, last_index = 4, agree_index = 4},
        fake_reply(Command1_11, Reply10, Peer1),
        fake_reply(Command2_11, Reply10, Peer2),
        Command1_12 = check_progress(Peer1),
        ?assertMatch(
            #append_entries{commit_index = 4, prev_log_index = 4, prev_log_term = 2, term = 6,
                entries = [#enrty{index = 5, type = ?OP_NOOP, term = 6}]},
            Command1_12
        ),
        Command2_12 = check_progress(Peer2),
        ?assertMatch(
            #append_entries{commit_index = 4, prev_log_index = 4, prev_log_term = 2, term = 6,
                entries = [#enrty{index = 5, type = ?OP_NOOP, term = 6}]},
            Command2_12
        ),
        ok = zraft_consensus:stop(PeerName)
    end}.

check_progress(Peer) ->
    receive
        {Peer, V} ->
            V
    after 2000 ->
        {error, timeout}
    end.

fake_reply(Command = #append_entries{epoch = E}, Reply, Peer) ->
    zraft_peer_route:reply_proxy(
        Command#append_entries.from,
        Reply#append_reply{from_peer = {Peer,self()}, request_ref = Command#append_entries.request_ref,epoch = E}
    );
fake_reply(Command = #vote_request{epoch = E}, Reply, Peer) ->
    zraft_peer_route:reply_consensus(
        Command#vote_request.from,
        Reply#vote_reply{from_peer = Peer,epoch = E}
    ).

-endif.
