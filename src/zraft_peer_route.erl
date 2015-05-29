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
-module(zraft_peer_route).
-author("dreyk").

%% API
-export([cmd/2,reply_consensus/2,reply_proxy/2,start_peer/2]).

-spec cmd(zraft_consensus:peer_id(),zraft_consensus:rpc_cmd())->ok.
cmd({Name,Node},Command) when is_atom(Name) andalso is_atom(Node)->
    gen_fsm:send_event({Name,Node},Command);
cmd({_Name,{_Ref,From}}=Peer,Command)->
    From ! {Peer,Command}.

-spec reply_consensus(zraft_consensus:from_peer_addr(),term())->ok.
reply_consensus({_,Pid},Reply) when is_pid(Pid)->
    gen_fsm:send_event(Pid,Reply);
reply_consensus({_,{_Ref,Pid}}=Peer,Reply) when is_pid(Pid)->
    Pid ! {Peer,Reply}.
-spec reply_proxy(zraft_consensus:from_peer_addr(),term())->ok.
reply_proxy({_,Pid},Reply) when is_pid(Pid)->
    gen_server:cast(Pid,Reply);
reply_proxy({_,{_Ref,Pid}}=Peer,Reply) when is_pid(Pid)->
    Pid ! {Peer,Reply}.

start_peer({Name,Node},BackEnd) when is_atom(Node)->
    spawn(fun()->
        Res = rpc:call(Node,zraft_lib_sup,start_consensus,[{Name,Node},BackEnd]),
        lager:info("Start remote ~p: ~p",[{Name,Node},Res])
    end);
start_peer(_,_)->
    ok.