%%-------------------------------------------------------------------
%% @author lol4t0
%% @copyright (C) 2015
%% @doc
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
-module(zraft_ets_backend).
-author("lol4t0").
-behaviour(zraft_backend).

%% API
-export([init/1, query/2, apply_data/2, snapshot/1, snapshot_done/1, snapshot_failed/2, install_snapshot/2]).

-record(state, {
    ets_ref :: ets:tab()
}).

-define(STATE_FILENAME, "state").

init(_PeerId) ->
    #state{
        ets_ref = ets:new(undefined, [set, protected])
    }.

query({get, Key}, #state{ets_ref = Tab}) ->
    Result = case ets:lookup(Tab, Key) of
        [Object] ->
            Object;
        [] ->
            {ok, not_found}
    end,
    {ok, Result};

%% Pattern is match spec
query({list, Pattern}, #state{ets_ref = Tab}) when is_list(Pattern)->
    {ok, ets:select(Tab, Pattern)};

%% Pattern is match pattern
query({list, Pattern}, #state{ets_ref = Tab}) when is_tuple(Pattern) orelse is_atom(Pattern) ->
    {ok, ets:match(Tab, Pattern)};

%% Pattern is function
query(SelectFun, #state{ets_ref = Tab}) when is_function(SelectFun) ->
    {ok, SelectFun(ets:tab2list(Tab))};

query(_, _State) ->
    {ok, {error, invalid_request}}.


apply_data({put, Data}, State = #state{ets_ref = Tab}) ->
    Result = ets:insert(Tab, Data),
    {Result, State};
apply_data({add, Data}, State = #state{ets_ref = Tab}) ->
    Result = ets:insert_new(Tab, Data),
    {Result, State};
apply_data(_, State) ->
    {{error, invalid_request}, State}.

snapshot(State = #state{ets_ref = Tab}) ->
    SaveFun = fun(Dest) ->
        File = filename:join(Dest, ?STATE_FILENAME),
        ets:tab2file(Tab, File, [{extended_info, [md5sum]}])
    end,
    {async, SaveFun, State}.

snapshot_done(State) ->
    {ok, State}.

snapshot_failed(_Reason, State) ->
    {ok, State}.

install_snapshot(Dir, State = #state{ets_ref = OldTab}) ->
    File = filename:join(Dir, ?STATE_FILENAME),
    case ets:file2tab(File, [{verify,true}]) of
        {ok, NewTab} ->
            ets:delete(OldTab),
            {ok, State#state{ets_ref = NewTab}};
        Else ->
            Else
    end.
