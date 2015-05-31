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
-module(zraft_dict_backend).
-author("dreyk").

-behaviour(zraft_backend).

-export([init/1,query/2,apply_data/2,snapshot/1,snapshot_done/1,snapshot_failed/2,install_snapshot/2]).


%% @doc init backend FSM
init(_) ->
    {ok,dict:new()}.

query(Fn,Dict) when is_function(Fn)->
    V = Fn(Dict),
    {ok,V};
query(Key,Dict) ->
    V = dict:find(Key,Dict),
    {ok,V}.

%% @doc write data to FSM
apply_data(List,Dict) ->
    Dict1 = lists:foldl(fun({K,V},Acc)->
        dict:store(K,V,Acc) end,Dict,List),
    {ok,Dict1}.

%% @doc Prepare FSM to take snapshot asycn if it's possible otherwice return function to take snapshot immediatly
snapshot(Dict)->
    Fun = fun(ToDir)->
        File = filename:join(ToDir,"state"),
        {ok,FD}=file:open(File,[write,raw,binary]),
        lists:foreach(fun(E)->
            V1 = term_to_binary(E),
            Size = size(V1),
            Row = <<0:8,Size:64,V1/binary>>,
            file:write(FD,Row)
            end,dict:to_list(Dict)),
        ok = file:close(FD),
        ok
    end,
    {async,Fun,Dict}.

%% @doc Notify that snapshot has done.
snapshot_done(Dict)->
    {ok,Dict}.

%% @doc Notify that snapshot has failed.
snapshot_failed(_Reason,Dict)->
    {ok,Dict}.

%% @doc Read data from snapshot file or directiory.
install_snapshot(Dir,_)->
    File = filename:join(Dir,"state"),
    {ok,FD}=file:open(File,[read,raw,binary]),
    Res = case read(FD,[]) of
        {ok,Data}->
            {ok,dict:from_list(Data)};
        Else->
            Else
    end,
    file:close(FD),
    Res.

read(FD,Acc)->
    case file:read(FD,9) of
        {ok,<<0:8,Size:64>>}->
            case file:read(FD,Size) of
                {ok,B}->
                    case catch binary_to_term(B) of
                        {'EXIT', _}->
                            {error,file_corrupted};
                        {K,V}->
                            read(FD,[{K,V}|Acc]);
                        _->
                            {error,file_corrupted}
                    end;
                _->
                    {error,file_corrupted}
            end;
        eof->
            {ok,Acc};
        _->
            {error,file_corrupted}
    end.