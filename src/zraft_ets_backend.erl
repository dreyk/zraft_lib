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

-export_type([state/0]).

-record(state, {
    ets_ref :: ets:tab()
}).

-record(item, {
    key :: term(),
    value :: term(),
    counter = 0 :: non_neg_integer(),
    mode = object:: object | group
}).

-opaque state() :: #state{}.

-define(STATE_FILENAME, "state").

init(_PeerId) ->
    #state{
        ets_ref = ets:new(undefined, [set, public, {keypos, #item.key}])
    }.

-spec query
    ({get, Key :: term()}, state()) -> {ok, KeyList} | {ok, Object :: zraft_backend:kv()} | {ok, not_found} when
        KeyList :: {GroupKey :: term(), [Key :: term()]};
    ({list, Pattern :: ets:match_pattern()}, state()) -> {ok, [KeyList]} | {ok, [Object :: zraft_backend:kv()]} when
        KeyList :: {GroupKey :: term(), [Key :: term()]};
    ({fold, SelectFunction, Acc :: term()}, state()) -> {ok, term()} when
        SelectFunction :: fun((Key :: term(), Value :: term(), Acc :: term()) -> term()).

query({get, Key}, #state{ets_ref = Tab}) ->
    Result = case ets:lookup(Tab, Key) of
        [#item{key = Key, value = Value}] ->
            {Key, Value};
        [] ->
            not_found
    end,
    {ok, Result};

%% Pattern is match pattern
query({list, Pattern}, #state{ets_ref = Tab}) ->
    ActualPattern = #item{key = Pattern},
    {ok, lists:map(
        fun(#item{key = Key, value = Value}) ->
            {Key, Value}
        end, ets:match(Tab, ActualPattern))
    };

query({fold, SelectFun, Acc0}, #state{ets_ref = Tab}) when is_function(SelectFun) ->
    try
        {ok, ets:foldl(
            fun(#item{key = Key, value = Value}, Acc) ->
                SelectFun(Key, Value, Acc)
            end, Acc0, Tab)
        }
    catch
        error:badarg ->
            {error, invalid_request};
        A:B ->
            {error, {A, B}}

    end;
query(_, _State) ->
    {ok, {error, invalid_request}}.


-type apply_data_result() :: {{ok, [Key :: term()]}, state()} | {{error, Reason :: term()}, state()}.
-type increment_counter_result() ::
      {{ok, NewCounterValue :: integer(), [Key :: term()]}, state()}
    | {{error, Reason :: term()}, state()}.
-spec apply_data
    (WriteCommand :: {put | put_new | append, Data :: [zraft_backend:kv()]}, state()) -> apply_data_result();
    (WriteCommand :: {delete, Keys :: [term()]}, state()) -> apply_data_result();
    (WriteCommand :: {increment, CounterName :: term(), Increment :: integer()}, state()) -> increment_counter_result().

apply_data(Command, State) ->
    try
        apply_data_unsafe(Command, State)
    catch
        error:badarg ->
            {{error, invalid_request}, State}
    end.

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

%% ---------------------------------------------------------------------------------------------------------------------
%% PRIVATE FUNCTIONS
%% ---------------------------------------------------------------------------------------------------------------------

apply_data_unsafe({put, Data}, State = #state{ets_ref = Tab})->
    DataList = make_list(Data),
    OverwriteGroup = lists:any(
        fun({K, _V}) ->
            case ets:lookup(Tab, K) of
                [#item{mode = group}] -> true;
                _ -> false
            end
        end, DataList),
    Result = if
        OverwriteGroup ->
            {error, overwrite_group};
        true ->
            Items = make_items(DataList),
            true = ets:insert(Tab, Items),
            Keys = extract_keys(DataList),
            {ok, Keys}
    end,
    {Result, State};
apply_data_unsafe({put_new, Data}, State = #state{ets_ref = Tab}) ->
    DataList  = make_list(Data),
    Items = make_items(DataList),
    Result = case ets:insert_new(Tab, Items) of
        true ->
            {ok, extract_keys(DataList)};
        false ->
            {error, overwrite_request}
    end,
    {Result, State};
apply_data_unsafe({delete, Keys}, State = #state{ets_ref = Tab}) when is_list(Keys) ->
    AllKeys = find_keys_recursive(Tab, Keys, []),
    lists:foreach(fun(Key) -> ets:delete(Tab, Key) end, AllKeys),
    {{ok, AllKeys}, State};
apply_data_unsafe({increment, Key, Incr}, State = #state{ets_ref = Tab}) ->
    NewVal = ets:update_counter(Tab, Key, Incr),
    {NewVal, State};
apply_data_unsafe({append, GroupKVs}, State = #state{ets_ref = Tab}) ->
    DataList = make_list(GroupKVs),
    AllKeys = lists:flatmap(
        fun(GroupKey) ->
            append_one(GroupKey, Tab)
        end, DataList),
    {{ok, AllKeys}, State};
apply_data_unsafe(_, State) ->
    {{error, invalid_request}, State}.

append_one({GroupKey, Value}, Tab) ->
    GroupItem = #item{counter = NewCnt} = case ets:lookup(Tab, GroupKey) of
       [#item{counter = Cnt, mode = group} = I] ->
           I#item{counter = Cnt + 1};
       [#item{counter = Cnt, mode = object} = I] ->
           I#item{counter = Cnt + 1, value = [], mode = group};
       [] ->
           #item{mode = group, value = []}
    end,
    NewKey = {GroupKey, NewCnt},
    NewItem = #item{key = NewKey, value = Value},
    true = ets:insert(Tab, [GroupItem, NewItem]),
    [GroupKey, NewKey].

find_keys_recursive(Tab, Keys, Acc0) ->
    lists:foldl(
        fun(Key, List) ->
            case ets:lookup(Tab, Key) of
                [#item{key = Key, mode = object}] ->
                    [Key | List];
                [#item{key = Key, mode = group, value = KeyList}] ->
                    find_keys_recursive(Tab, KeyList, [Key | List]);
                [] ->
                    List
            end
        end, Acc0, Keys).

make_items(DataList) ->
    lists:map(fun({K, V}) -> #item{key = K, value = V} end, DataList).

make_list(Data) when is_list(Data) ->
    Data;
make_list(Data) ->
    [Data].

extract_keys(DataList) ->
    element(1, lists:unzip(DataList)).

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
    [
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
        end,
        fun() ->
            ?assertMatch({true, Ets}, apply_data({put, {"2", 1}}, Ets)),
            ?assertMatch({13, Ets}, apply_data({increment, "2", 12}, Ets)),
            ?assertMatch({-5, Ets}, apply_data({increment, "2", -18}, Ets)),
            ?assertMatch({{error, invalid_request}, Ets}, apply_data({increment, "21", -17}, Ets))
        end
    ].

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
    [
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
        end,
        fun() ->
            Ets2 = init(0),
            ?assertNotException(error, badarg, install_snapshot("invalid", Ets2)),
            ?assertMatch({error, _}, install_snapshot("invalid", Ets2))
        end
    ].

stop_test(_Ets) ->
    zraft_util:clear_test_dir(?TEST_DIR).

setup_test() ->
    zraft_util:set_test_dir(?TEST_DIR),
    init(0).

-endif.
