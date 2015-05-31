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
    zraft_util:set_test_dir("full-test-data"),
    application:set_env(zraft_lib,max_log_count,10),
    net_kernel:start(['zraft_test@localhost', shortnames]),
    ok.
stop_node(_) ->
    net_kernel:stop(),
    application:unset_env(zraft_lib,max_log_count),
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
        ok = wait_new_config(4,PeerID1,1),
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
        W1 = zraft_consensus:write(PeerID2,{1,"1"},?TIMEOUT),
        ?assertMatch({leader,{test1,_}},W1),
        W2 = zraft_consensus:write(PeerID1,{1,"1"},?TIMEOUT),
        ?assertMatch(ok,W2),
        ok = wait_success_read(1,PeerID1,1),

        %%drop test2 peer and all it's data
        ok = zraft_consensus:stop(Peer2),
        ok = zraft_util:del_dir("full-test-data/test2-zraft_test_localhost"),
        %%restart it this empty state
        {ok, Peer22} = zraft_consensus:start_link(PeerID2, zraft_dict_backend),
        %%wait log replicate
        ok = wait_follower_sync(5,5,2,PeerID2,Peer22,1),
        R1 = zraft_consensus:query_local(PeerID2,fun(Dict)->lists:ukeysort(1,dict:to_list(Dict)) end,?TIMEOUT),
        ?assertMatch([{1,"1"}],R1),


        [zraft_consensus:write(PeerID1,{I,integer_to_list(I)},?TIMEOUT)||I<-lists:seq(2,8)],
        Res7 = zraft_consensus:stat(Peer1),
        ?assertMatch(
            #peer_start{
                term = 2,
                state_name = leader,
                log_state = #log_descr{commit_index = 12, first_index = 11, last_index = 12, last_term = 2},
                leader = {test1,_},
                snapshot_info = #snapshot_info{index = 10,term = 2,conf_index = 4}
            },
            Res7
        ),

        ok = wait_snapshot_done(10,Peer1,1),
        ok = zraft_consensus:stop(Peer3),
        ok = zraft_util:del_dir("full-test-data/test3-zraft_test_localhost"),
        %%restart it this empty state
        {ok, Peer32} = zraft_consensus:start_link(PeerID3, zraft_dict_backend),
        %%wait log replicate and snapshot
        ok = wait_follower_sync(12,12,2,PeerID3,Peer32,1),

        R2 = zraft_consensus:query_local(PeerID3,fun(Dict)->lists:ukeysort(1,dict:to_list(Dict)) end,?TIMEOUT),
        ?assertMatch([{1,"1"},{2,"2"},{3,"3"},{4,"4"},{5,"5"},{6,"6"},{7,"7"},{8,"8"}],R2),

        ok = zraft_consensus:stop(Peer1),
        ok = zraft_consensus:stop(Peer22),
        ok = zraft_consensus:stop(Peer32)
    end}.

wait_new_config(Index,PeerID,Attempt)->
    case  zraft_consensus:get_conf(PeerID,?TIMEOUT) of
        {ok,{Index,_}}->
            ok;
        _->
            ?debugFmt("Wait config attempt - ~p",[Attempt]),
            wait_new_config(Index,PeerID,Attempt+1)
    end.

wait_success_read(Key,PeerID,Attempt)->
    case  zraft_consensus:query(PeerID,Key,?TIMEOUT) of
        {ok,_}->
            ok;
        _->
            ?debugFmt("Wait read attempt - ~p",[Attempt]),
            wait_success_read(Key,PeerID,Attempt+1)
    end.

wait_follower_sync(CommitIndex,LastIndex,Term,PeerID,Peer,Attempt)->
    case zraft_consensus:stat(Peer) of
        #peer_start{term = Term,state_name = follower,log_state = #log_descr{last_index = LastIndex,commit_index = CommitIndex}}->
            ?debugFmt("Wait start ~p current state[term:~p,last-index:~p,commit:~p,state:~p] attempt - ~p",
                [PeerID,Term,LastIndex,CommitIndex,foolower,finished]),
            ok;
        #peer_start{term = T1,state_name = StateName,log_state = #log_descr{last_index = L1,commit_index = C1}}->
            ?debugFmt("Wait start ~p current state[term:~p,last-index:~p,commit:~p,state:~p] attempt - ~p",
                [PeerID,T1,L1,C1,StateName,Attempt]),
            timer:sleep(100),
            wait_follower_sync(CommitIndex,LastIndex,Term,PeerID,Peer,Attempt+1);
        _->
            ?debugFmt("Wait start ~p attempt - ~p",[PeerID,Attempt]),
            wait_follower_sync(CommitIndex,LastIndex,Term,PeerID,Peer,Attempt+1)
    end.

wait_snapshot_done(CommitIndex,Peer,Attempt)->
    case zraft_consensus:stat(Peer) of
        #peer_start{snapshot_info = #snapshot_info{index = CommitIndex}}->
            ok;
        _->
            ?debugFmt("Wait snapshot attempt - ~p",[Attempt]),
            wait_snapshot_done(CommitIndex,Peer,Attempt+1)
    end.


-endif.
