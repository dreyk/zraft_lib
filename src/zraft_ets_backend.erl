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
        ets_ref = ets:new(undefined, [set, public])
    }.

query({get, Key}, #state{ets_ref = Tab}) ->
    Result = case ets:lookup(Tab, Key) of
        [Object] ->
            Object;
        [] ->
            not_found
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
apply_data({append, Data}, State = #state{ets_ref = Tab}) ->
    Result = ets:insert_new(Tab, Data),
    {Result, State};
apply_data({delete, Keys}, State = #state{ets_ref = Tab}) when is_list(Keys) ->
    lists:foreach(fun(Key) -> ets:delete(Tab, Key) end, Keys),
    {true, State};
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

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-define(TEST_DIR, "test-ets-backend").

ets_backend_test_() ->
    {
        setup,
        fun setup_test/0,
        fun stop_test/1,
        fun(Ets) ->
            [
                test_empty_get(Ets),
                test_put(Ets),
                test_get(Ets),
                test_snapshot(Ets)
            ]
        end
    }.

test_empty_get(Ets) ->
    [
        ?_assertMatch({ok, not_found}, query({get, <<"1">>}, Ets)),
        ?_assertMatch({ok, []}, query({list, '$1'}, Ets)),
        ?_assertMatch({ok, []}, query({list, {"/", '$2'}}, Ets)),
        ?_assertMatch({ok, []}, query({list, [{'$1', [], ['$1']}]}, Ets)),
        fun() ->
            query(
                fun
                    ([]) -> ok;
                    (_) -> ?assert(false)
                end,
                Ets)
        end,
        ?_assertMatch({ok, {error, invalid_request}}, query({ls, "/"}, Ets))
    ].

test_put(Ets) ->
    fun() ->
        ?assertMatch({true, Ets}, apply_data({put, {["/", "1"], "v1"}}, Ets)),
        ?assertMatch({false, Ets}, apply_data({append, {["/", "1"], "v1"}}, Ets)),
        ?assertMatch({true, Ets}, apply_data({delete, [["/", "1"]]}, Ets)),
        ?assertMatch({true, Ets}, apply_data({append, {["/", "1"], "v1"}}, Ets)),
        ?assertMatch({true, Ets}, apply_data({put, [
            {["/", "1"], "v1"},
            {["/", "2", "3"], "v2"}
        ]}, Ets)),
        ?assertMatch({false, Ets}, apply_data({append, [
            {["/", "1"], "v1"},
            {["/", "3"], "v3"}
        ]}, Ets)),
        ?assertMatch({{error, invalid_request}, Ets}, apply_data({replace, {["/", "4"], "v4"}}, Ets))
    end.

test_get(Ets) ->
    [
        ?_assertMatch({ok, {["/", "1"], "v1"}}, query({get, ["/", "1"]}, Ets)),
        ?_assertMatch({ok, [
            [["1"], "v1"],
            [["2", "3"], "v2"]
        ]}, query({list, {["/" | '$1'], '$2'}}, Ets)),
        ?_assertMatch({ok, [
            {["/", "1"], "v1"},
            {["/", "2", "3"], "v2"}
        ]}, query({list, [{{['$1' | '$2'], '$3'}, [{'=:=', '$1', "/"}], [{{['$1' | '$2'], '$3'}}]}]}, Ets))
    ].

test_snapshot(Ets = #state{ets_ref = Tab}) ->
    fun() ->
        TabData = lists:usort(ets:tab2list(Tab)),
        Snapshot = snapshot(Ets),
        ?assertMatch({async, _, Ets}, Snapshot),
        {_, SnapshotFun, _} = Snapshot,
        ?assert(is_function(SnapshotFun, 1)),
        Me = self(),
        spawn(
            fun() ->
                R = SnapshotFun(?TEST_DIR),
                Me ! {test, R}
            end),
        R = receive
                {test, V}->
                    V
            end,
        ?assertEqual(ok, R),
        ?assert(filelib:is_file(filename:join(?TEST_DIR, ?STATE_FILENAME))),

        InstallRes = install_snapshot(?TEST_DIR, Ets),
        ?assertMatch({ok, #state{}}, InstallRes),
        {ok, #state{ets_ref = Tab2}} = InstallRes,
        ?assertNotEqual(Tab, Tab2),
        ?assertEqual(TabData, lists:usort(ets:tab2list(Tab2))),

        EtsNew = init(0),
        InstallResNew = install_snapshot(?TEST_DIR, EtsNew),
        ?assertMatch({ok, #state{}}, InstallResNew),
        {ok, #state{ets_ref = Tab3}} = InstallResNew,
        ?assertEqual(TabData, lists:usort(ets:tab2list(Tab3)))
    end.

stop_test(_Ets) ->
    zraft_util:clear_test_dir(?TEST_DIR).

setup_test() ->
    zraft_util:set_test_dir(?TEST_DIR),
    init(0).

-endif.
