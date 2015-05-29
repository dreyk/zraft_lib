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
-module(zraft_util).
-author("dreyk").

%% API
-export([
    peer_name/1,
    escape_node/1,
    node_name/1,
    get_env/2,
    random/1
]).
-export([
    del_dir/1,
    make_dir/1,
    node_addr/1,
    miscrosec_timeout/1,
    gen_server_cancel_timer/1,
    gen_server_cast_after/2,
    peer_id/1,
    set_test_dir/1,
    clear_test_dir/1]).

peer_name({Name,Node}) when is_atom(Name)->
    atom_to_list(Name)++"-"++node_name(Node);
peer_name({Name,Node})->
    binary_to_list(base64:encode(term_to_binary(Name)))++"-"++node_name(Node).

node_name(Node)->
    escape_node(atom_to_list(Node)).

escape_node([])->
    [];
escape_node([$@|T])->
    [$_|escape_node(T)];
escape_node([$.|T])->
    [$_|escape_node(T)];
escape_node([E|T])->
    [E|escape_node(T)].


get_env(Key, Default) ->
    case application:get_env(zraft_lib, Key) of
        {ok, Value} ->
            Value;
        _ ->
            Default
    end.

%% @doc Generate "random" number X, such that `0 <= X < N'.
-spec random(pos_integer()) -> pos_integer().
random(N) ->
    erlang:phash2(erlang:statistics(io), N).

del_dir(Dir) ->
    case file:list_dir(Dir) of
        {ok, Files} ->
            lists:foreach(fun(F) ->
                del_dir(filename:join(Dir, F)) end, Files),
            file:del_dir(Dir);
        _ ->
            file:delete(Dir),
            file:del_dir(Dir)
    end.

make_dir(undefined)->
    exit({error,dir_undefined});
make_dir("undefined"++_)->
    exit({error,dir_undefined});
make_dir(Dir) ->
    case make_safe(Dir) of
        ok ->
            ok;
        {error, enoent} ->
            S1 = filename:split(Dir),
            S2 = lists:droplast(S1),
            case make_dir(filename:join(S2)) of
                ok ->
                    make_safe(Dir);
                Else ->
                    Else
            end;
        Else ->
            Else
    end.
make_safe(Dir)->
    case file:make_dir(Dir) of
        ok->
            ok;
        {error,eexist}->
            ok;
        Else->
            Else
    end.

node_addr(Node)->
    L = atom_to_list(Node),
    case string:tokens(L,"@") of
        [_,"nohost"]->
            "127.0.0.1";
        [_,Addr]->
            Addr;
        _->
            "127.0.0.1"
    end.

miscrosec_timeout(Timeout) when is_integer(Timeout)->
    Timeout*1000;
miscrosec_timeout(Timeout)->
    Timeout.

gen_server_cast_after(Time, Event) ->
    erlang:send_after(Time,self(),{'$gen_cast', Event}).
gen_server_cancel_timer(Ref)->
    case erlang:cancel_timer(Ref) of
        false ->
            receive {timeout, Ref, _} -> 0
            after 0 -> false
            end;
        RemainingTime ->
            RemainingTime
    end.

-spec peer_id(zraft_consensus:from_peer_addr())->zraft_consensus:peer_id().
peer_id({ID,_})->
    ID.

set_test_dir(Dir)->
    del_dir(Dir),
    ok = make_dir(Dir),
    application:set_env(zraft_lib,log_dir,Dir),
    application:set_env(zraft_lib,snapshot_dir,Dir).
clear_test_dir(Dir)->
    application:unset_env(zraft_lib,log_dir),
    application:unset_env(zraft_lib,snapshot_dir),
    del_dir(Dir).
