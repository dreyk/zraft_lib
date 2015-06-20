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
-module(zraft_backend).
-author("dreyk").

%% API
-export([]).

-type state() :: term().
-type read_cmd()::term().
-type write_cmd()::term().
-type snapshot_fun()::fun().
-type keys()::list(term()).

%% init backend FSM
-callback init(PeerId :: zraft_consensus:peer_id()) -> state().

%% read/query data from FSM
-callback query(ReadCmd :: read_cmd(), State :: state()) -> {ok, Data :: term()} | {ok,WatchKeys :: keys(),Data::term()}.

%% write data to FSM
-callback apply_data(WriteCmd :: write_cmd(), State :: state()) ->
    {Result, State :: state()} | {Result,TriggerKesy::keys(),State::state()}
    when Result :: term().

%% write data to FSM
-callback apply_data(WriteCmd :: write_cmd(),Session :: zraft_consensus:csession(),State::state()) ->
    {Result, State :: state()} | {Result,TriggerKesy::keys(),State::state()}
    when Result :: term().

-callback expire_session(Session :: zraft_consensus:csession(), State :: state())->
    {ok,State::state()}|{ok,TriggerKesy::keys(),State::state()}.

%% Prepare FSM to take snapshot async if it's possible otherwice return function to take snapshot immediatly
-callback snapshot(State :: state())->{sync, snapshot_fun(), state()} | {async, snapshot_fun(), state()}.

%% Notify that snapshot has done.
-callback snapshot_done(State :: state())->{ok, NewState :: state()}.

%% Notify that snapshot has failed.
-callback snapshot_failed(Reason :: term(), State :: state())->{ok,state()}.

%% Read data from snapshot file or directiory.
-callback install_snapshot(FileName :: file:filename(), State :: state()|undefined)-> {ok,state()} | {error, Reason :: term}.
