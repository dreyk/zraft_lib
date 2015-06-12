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
-module(zraft_session).
-author("dreyk").

-behaviour(gen_server).

%% API
-export([start_link/1]).

-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-include_lib("zraft_lib/include/zraft.hrl").

-record(state, {session,requests,message_id,acc_upto,leader_mref,epoch,timer,timeout}).

-spec start_link(zraft_consensus:peer_id()|list(zraft_consensus:peer_id()))->{ok, pid()} | {error, Reason::term()}.
start_link(Peer) ->
    gen_server:start_link(?MODULE, [Peer], []).

init([Peer]) ->
    Session = zraft_client:light_session(Peer),
    Requests = ets:new(client_session,[ordered_set]),
    {ok, #state{session = Session,requests = Requests,message_id = 1,epoch = 1}}.


handle_call({write,Req},From, State) ->
    handle_write(Req,From,State);
handle_call({query,Req},From, State) ->
    handle_query(Req,From,State).

handle_cast(_Request, State) ->
    {noreply, State}.


handle_info(#swrite_reply{sequence = ?CLIENT_PING,data = Timer},State=#state{timer = Timer})->
    State1 = ping(State),
    {noreply,State1};
handle_info(#swrite_reply{sequence = ?CLIENT_PING}, State)->
    {noreply,State};
handle_info(#swrite_reply{sequence = ?CLIENT_CONNECT,data = Result}, State=#state{})->
    case Result of
        ok->
            State1 = ping(State),
            {noreply,State1};
        Else->
            %%Session alredy opened
            {stop,Else,State}
    end;
handle_info(#swrite_reply{sequence = ID,data = Result}, State) when is_integer(ID)->
    write_reply(ID,Result,State);
handle_info(#swrite_error{sequence = ID,leader = NewLeader,error = not_leader}, State) when is_integer(ID)->
    %%repeat only this request. other requests will be restart on session change leader event
    State1 = change_leader(NewLeader,State),
    repeat_write(ID,State1);

handle_info({{read,ReadRef},#sread_reply{data = Data}}, State)->
    read_reply(ReadRef,Data,State);
handle_info({{read,ReadRef},{leader,NewLeader}}, State)->
    %%repeat only this request. other requests will be restart on session change leader event
    State1 = change_leader(NewLeader,State),
    repeat_read(ReadRef,State1);
handle_info({{read,ReadRef},Data}, State)->
    read_reply(ReadRef,Data,State);
handle_info(#swatch_trigger{ref = {Caller,Watch}},State=#state{requests = Requets})->
    Caller ! #swatch_trigger{ref=Watch},
    ets:delete(Requets,{Caller,Watch}),
    {noreply,State};

handle_info(?DISCONNECT_MSG,State)->
    {stop,?DISCONNECT_MSG,State};

handle_info({leader,NewLeader},State)->
    lager:warning("Leader changed to ~p",[NewLeader]),
    State1 = change_leader(NewLeader,State),
    %%Actualy no requests will be restart if leader has not changed,because epoch doesn't change
    restart_requets(State1),
    {noreply,State1};
handle_info({'DOWN', Ref, process, _,_},State = #state{leader_mref = Ref})->
    lager:warning("Current leader has failed"),
    State1 = change_leader(undefined,State),
    restart_requets(State1),
    {noreply,State1};

handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


ping(State=#state{acc_upto = To,timer = Timer,timeout = Timeout})->
    erlang:cancel_timer(Timer),
    NewTimer = erlang:start_timer(Timeout,self(),session_timeout),
    Req = #swrite{message_id = ?CLIENT_PING,acc_upto = To,from = self(),data = NewTimer},
    write_to_raft(Req,State),
    State#state{timer = NewTimer}.

handle_query({Query,Watch,Timeout},From, State=#state{requests = Requests,epoch = E}) ->
    {Caller,_}=From,
    Req = case Watch of
              false->
                  {Query,Watch,Timeout};
              _->
                  {Query,{Caller,Watch},Timeout}
          end,
    MRef = erlang:monitor(process,Caller),
    TRef = erlang:start_timer(Timeout,self(),{request,MRef}),
    read_from_raft(MRef,Req,State),
    ets:insert(Requests,{MRef,Req,From,TRef,E}),
    {noreply,State}.

repeat_read(Ref,State=#state{requests = Requests,epoch = E})->
    case ets:lookup(Requests,Ref) of
        [{Ref,Req,_From,_TRef,_E0}]->
            ets:update_element(Requests,Ref,{5,E}),
            read_from_raft(Ref,Req,State),
            {noreply,State};
        _->
            {noreply,State}
    end.

handle_write({Request,Temporary,Timeout},From, State=#state{message_id = ID,acc_upto = To,requests = Requests,epoch = E}) ->
    Req = #swrite{message_id = ID,acc_upto = To,from = self(),temporary = Temporary,data = Request},
    write_to_raft(Req,State),
    {Caller,_}=From,
    TRef = erlang:start_timer(Timeout,self(),{request,ID}),
    MRef = erlang:monitor(process,Caller),
    ets:insert(Requests,{ID,Req,From,TRef,MRef,E}),
    ets:insert(Requests,{MRef,ID}),
    {noreply,State#state{message_id = ID+1}}.
repeat_write(ID,State=#state{requests = Requests,acc_upto = To,epoch = E})->
    case ets:lookup(Requests,ID) of
        [{ID,Req,_From,_TRef,_MRef,_E0}]->
            ets:update_element(Requests,ID,{6,E}),
            Req1 = Req#swrite{acc_upto = To},
            write_to_raft(Req1,State),
            {noreply,State};
        _->
            {noreply,State}
    end.

write_to_raft(Req,#state{session = Session})->
    Leader  = zraft_session_obj:leader(Session),
    zraft_consensus:send_swrite(Leader,Req).
read_from_raft(Ref,{Query,Watch,Timeout},#state{session = Session})->
    Leader = zraft_session_obj:leader(Session),
    zraft_consensus:async_query(Leader,{self(),{read,Ref}},Watch,Query,Timeout).

read_reply(Ref,Result,State=#state{requests = Requests})->
    Reply = case Result of
                {ok,R1}->
                    R1;
                _->
                    Result
            end,
    case ets:lookup(Requests,Ref) of
        [{Ref,Req,From,TRef,_Epoch}]->
            register_watcher(Req,State),
            gen_server:reply(From,Reply),
            erlang:demonitor(Ref),
            timer:cancel(TRef),
            ets:delete(Requests,Ref),
            {noreply,State};
        _->
            {noreply,State}
    end.

register_watcher({_,false,_},_State)->
    ok;
register_watcher({_,Watch,_},#state{requests = Requests})->
    ets:insert(Requests,Watch).

write_reply(ID,Result,State=#state{requests = Requests})->
    case ets:lookup(Requests,ID) of
        [{ID,_Req,From,TRef,MRef,_Epoch}]->
            gen_server:reply(From,Result),
            erlang:demonitor(MRef),
            timer:cancel(TRef),
            ets:delete(Requests,ID),
            ets:delete(Requests,MRef),
            State1 = update_upto(State),
            {noreply,State1};
        _->
            {noreply,State}
    end.

update_upto(State=#state{requests = Requests,message_id = ID})->
    case ets:next(Requests,0) of
        K when is_integer(K)->
            State#state{acc_upto = K};
        _->
            State#state{acc_upto = ID-1}
    end.

change_leader(undefined,State=#state{session = Session})->
    case zraft_session_obj:fail(Session) of
        {error,all_failed}->
            exit({error,no_peer_available});
        NewSession->
            State1 = State#state{session = NewSession},
            install_leader(State1)
    end;
change_leader(NewLeader,State=#state{session = Session})->
    case zraft_session_obj:leader(Session) of
        NewLeader->
            State;
        _->
            NewSession = zraft_session_obj:set_leader(NewLeader,Session),
            State1 = State#state{session = NewSession},
            install_leader(State1)
    end.

install_leader(State=#state{session = Session,leader_mref = PrevRef,epoch = E})->
    trigger_all_watcher(State),
    case PrevRef of
        undefined->
            ok;
        _->
            erlang:demonitor(PrevRef)
    end,
    Leader = zraft_session_obj:leader(Session),
    Mref = erlang:monitor(process,Leader),
    State1 = State#state{leader_mref = Mref,epoch = E+1},
    ping(State1).

trigger_all_watcher(#state{requests = Requests})->
    All = ets:match(Requests,{'$1','$2'}),
    lists:foreach(fun([Caller,Watch])->
        Caller ! #swatch_trigger{ref = Watch} end,All),
    ets:match_delete(Requests,{'_','_'}).

restart_requets(State = #state{requests = Requests,epoch = E})->
    MatchWrite = [{{'$1','_','_','_','_','$2'}, [{'<', '$2', {const, E}}], ['$1']}],
    W = ets:select(Requests,MatchWrite),
    lists:foreach(fun(ID)->
        repeat_write(ID,State) end,W),
    MatchRead =  [{{'$1','_','_','_','$2'}, [{'<', '$2', {const, E}}], ['$1']}],
    R = ets:select(Requests,MatchRead),
    lists:foreach(fun(Ref)->
        repeat_read(Ref,State) end,R).
