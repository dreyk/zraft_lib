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
-export([start_link/2]).

-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-include_lib("zraft_lib/include/zraft.hrl").

-define(BACKOFF, 3000).

-record(state, {session, requests,watchers, message_id, acc_upto, leader_mref, epoch, timer, timeout, connected}).

-spec start_link(zraft_consensus:peer_id()|list(zraft_consensus:peer_id()), timeout()) ->
    {ok, pid()} | {error, Reason :: term()}.
start_link(Peer, Timeout) ->
    gen_server:start_link(?MODULE, [Peer, Timeout], []).

init([Peer, Timeout]) ->
    Session = zraft_client:light_session(Peer, ?BACKOFF),
    Requests = ets:new(client_session, [ordered_set, {write_concurrency, false}, {read_concurrency, false}]),
    Watchers = ets:new(client_watchers,[bag,{write_concurrency, false}, {read_concurrency, false}]),
    State = #state{
        session = Session,
        requests = Requests,
        watchers = Watchers,
        message_id = 1,
        epoch = 1,
        timeout = Timeout,
        connected = false
    },
    State1 = connect(State),
    {ok, State1}.


handle_call({write, Req}, From, State) ->
    handle_write(Req, From, State);
handle_call({query, Req}, From, State) ->
    handle_query(Req, From, State).

handle_cast(_Request, State) ->
    {noreply, State}.


handle_info(#swrite_reply{sequence = ?CLIENT_PING, data = Timer}, State = #state{timer = Timer}) ->
    State1 = update_upto(State),
    {noreply, State1};
handle_info(#swrite_reply{sequence = ?CLIENT_PING}, State) ->
    {noreply, State};
handle_info(#swrite_reply{sequence = ?CLIENT_CONNECT}, State = #state{}) ->
    State1 = ping(State#state{connected = true}),
    {noreply, State1};
handle_info(#swrite_reply{sequence = ID, data = Result}, State) when is_integer(ID) ->
    write_reply(ID, Result, State);
handle_info(#swrite_error{sequence = ID, leader = NewLeader, error = not_leader}, State) when is_integer(ID) ->
    %%repeat only this request. other requests will be restart on session change leader event
    State1 = change_leader(NewLeader, State),
    repeat_write(ID, State1);

handle_info({{read, ReadRef}, #sread_reply{data = Data}}, State) ->
    read_reply(ReadRef, Data, State);
handle_info({{read, ReadRef}, {leader, NewLeader}}, State) ->
    %%repeat only this request. other requests will be restart on session change leader event
    State1 = change_leader(NewLeader, State),
    repeat_read(ReadRef, State1);
handle_info({{read, ReadRef}, Data}, State) ->
    read_reply(ReadRef, Data, State);
handle_info(#swatch_trigger{ref = {Caller, Watch}}, State = #state{watchers = Watcher}) ->
    Caller ! #swatch_trigger{ref = Watch},
    ets:delete(Watcher, {Caller, Watch}),
    {noreply, State};

handle_info(?DISCONNECT_MSG, State) ->
    {stop, ?DISCONNECT_MSG, State};

handle_info({leader, NewLeader}, State) ->
    lager:warning("Leader changed to ~p", [NewLeader]),
    State1 = change_leader(NewLeader, State),
    %%Actualy no requests will be restart if leader has not changed,because epoch doesn't change
    State2 = restart_requets(State1),
    {noreply, State2};
handle_info({'DOWN', Ref, process, _, _}, State = #state{leader_mref = Ref}) ->
    lager:warning("Current leader has failed"),
    State1 = change_leader(failed, State),
    State2 = restart_requets(State1),
    {noreply, State2};
handle_info({'DOWN', Ref, process, Caller, _}, State) ->
    State1 = caller_down(Caller, Ref, State),
    {noreply, State1};

handle_info({timeout, TimerRef, session_timeout}, State = #state{timer = TimerRef}) ->
    lager:warning("Session timeout"),
    {stop, pong_timeout, State};
handle_info({timeout, TimerRef, connect_timeout}, State = #state{timer = TimerRef}) ->
    lager:warning("Connection timeout"),
    {stop, connection_timeout, State};
handle_info({timeout, TimerRef, {request, ReqRef}}, State) ->
    State1 = request_timeout(TimerRef, ReqRef, State),
    {noreply, State1};

handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


ping(State = #state{acc_upto = To, timer = Timer, timeout = Timeout}) ->
    cancel_timer(Timer),
    NewTimer = erlang:start_timer(Timeout div 2, self(), session_timeout),
    Req = #swrite{message_id = ?CLIENT_PING, acc_upto = To, from = self(), data = NewTimer},
    write_to_raft(Req, State),
    State#state{timer = NewTimer}.

connect(State = #state{timeout = Timeout, session = Session, timer = Timer}) ->
    cancel_timer(Timer),
    Leader = zraft_session_obj:leader(Session),
    Mref = erlang:monitor(process, Leader),
    NewTimer = erlang:start_timer(Timeout, self(), connect_timeout),
    Req = #swrite{message_id = ?CLIENT_CONNECT, acc_upto = 0, from = self(), data = Timeout},
    write_to_raft(Req, State),
    State#state{leader_mref = Mref, timer = NewTimer}.


%%send timeout error to client and clean temporary data
request_timeout(TRef, ReqRef, State = #state{requests = Requests}) ->
    case ets:lookup(Requests, ReqRef) of
        [{ReqRef, _, From, TRef, _}] ->
            gen_server:reply(From, {error, timeout}),
            erlang:demonitor(ReqRef),
            ets:delete(Requests, ReqRef),
            State;
        [{ReqRef, _Req, From, TRef, MRef, _E}] ->
            gen_server:reply(From, {error, timeout}),
            erlang:demonitor(MRef),
            ets:delete(Requests, ReqRef),
            ets:delete(Requests, MRef),
            update_upto(State);
        _ ->
            State
    end.

%%clean all temporary data.
caller_down(Caller, MRef, State = #state{requests = Requests,watchers = Watchers}) ->
    ets:delete(Watchers,Caller),
    case ets:lookup(Requests, MRef) of
        [{MRef, _, _From, TRef, _}] ->
            cancel_timer(TRef),
            ets:delete(Requests, MRef),
            State;
        [{MRef, ID}] ->
            case ets:lookup(Requests, ID) of
                [{ID, _Req, _From, TRef, MRef, _E}] ->
                    cancel_timer(TRef),
                    ets:delete(Requests,ID),
                    ets:delete(Requests, MRef),
                    update_upto(State);
                _ ->
                    State
            end;
        _ ->
            State
    end.

handle_query({Query, Watch, Timeout}, From, State = #state{requests = Requests, epoch = E}) ->
    {Caller, _} = From,
    Req = case Watch of
              false ->
                  {Query, Watch, Timeout};
              _ ->
                  {Query, {Caller, Watch}, Timeout}
          end,
    MRef = erlang:monitor(process, Caller),
    TRef = erlang:start_timer(Timeout, self(), {request, MRef}),
    read_from_raft(MRef, Req, State),
    ets:insert(Requests, {MRef, Req, From, TRef, E}),
    {noreply, State}.

repeat_read(Ref, State = #state{requests = Requests, epoch = E}) ->
    case ets:lookup(Requests, Ref) of
        [{Ref, Req, _From, _TRef, _E0}] ->
            ets:update_element(Requests, Ref, {5, E}),
            read_from_raft(Ref, Req, State),
            {noreply, State};
        _ ->
            {noreply, State}
    end.

handle_write({Request, Temporary, Timeout}, From, State = #state{message_id = ID, acc_upto = To, requests = Requests, epoch = E}) ->
    Req = #swrite{message_id = ID, acc_upto = To, from = self(), temporary = Temporary, data = Request},
    write_to_raft(Req, State),
    {Caller, _} = From,
    TRef = erlang:start_timer(Timeout, self(), {request, ID}),
    MRef = erlang:monitor(process, Caller),
    ets:insert(Requests, {ID, Req, From, TRef, MRef, E}),
    ets:insert(Requests, {MRef, ID}),
    {noreply, State#state{message_id = ID + 1}}.
repeat_write(ID, State = #state{requests = Requests, acc_upto = To, epoch = E}) ->
    case ets:lookup(Requests, ID) of
        [{ID, Req, _From, _TRef, _MRef, _E0}] ->
            ets:update_element(Requests, ID, {6, E}),
            Req1 = Req#swrite{acc_upto = To},
            write_to_raft(Req1, State),
            {noreply, State};
        _ ->
            {noreply, State}
    end.

write_to_raft(Req, #state{session = Session}) ->
    Leader = zraft_session_obj:leader(Session),
    zraft_consensus:send_swrite(Leader, Req).
read_from_raft(Ref, {Query, Watch, Timeout}, #state{session = Session}) ->
    Leader = zraft_session_obj:leader(Session),
    zraft_consensus:async_query(Leader, {read, Ref}, Watch, Query, Timeout).

read_reply(Ref, Result, State = #state{requests = Requests}) ->
    Reply = case Result of
                {ok, R1} ->
                    R1;
                _ ->
                    Result
            end,
    case ets:lookup(Requests, Ref) of
        [{Ref, Req, From, TRef, _Epoch}] ->
            register_watcher(Req, State),
            gen_server:reply(From, Reply),
            erlang:demonitor(Ref),
            cancel_timer(TRef),
            ets:delete(Requests, Ref),
            {noreply, State};
        _ ->
            {noreply, State}
    end.

register_watcher({_, false, _}, _State) ->
    ok;
register_watcher({_, Watch, _}, #state{watchers = Watchers}) ->
    ets:insert(Watchers, Watch).

write_reply(ID, Result, State = #state{requests = Requests}) ->
    case ets:lookup(Requests, ID) of
        [{ID, _Req, From, TRef, MRef, _Epoch}] ->
            gen_server:reply(From, Result),
            erlang:demonitor(MRef),
            cancel_timer(TRef),
            ets:delete(Requests, ID),
            ets:delete(Requests, MRef),
            State1 = update_upto(State),
            {noreply, State1};
        _ ->
            {noreply, State}
    end.

update_upto(State = #state{requests = Requests, message_id = ID}) ->
    case ets:next(Requests, 0) of
        K when is_integer(K) ->
            State#state{acc_upto = K};
        _ ->
            State1 = State#state{acc_upto = ID - 1},
            ping(State1)
    end.

change_leader(failed, State = #state{session = Session}) ->
    case zraft_session_obj:fail(Session) of
        {error, all_failed} ->
            exit({error, no_peer_available});
        NewSession ->
            State1 = State#state{session = NewSession},
            install_leader(State1)
    end;
change_leader(undefined, State = #state{session = Session}) ->
    NewSession = zraft_session_obj:next(Session),
    State1 = State#state{session = NewSession},
    install_leader(State1);

change_leader(NewLeader, State = #state{session = Session}) ->
    case zraft_session_obj:leader(Session) of
        NewLeader ->
            State;
        _ ->
            NewSession = zraft_session_obj:set_leader(NewLeader, Session),
            State1 = State#state{session = NewSession},
            install_leader(State1)
    end.

install_leader(State = #state{session = Session, leader_mref = PrevRef, epoch = E}) ->
    trigger_all_watcher(State),
    case PrevRef of
        undefined ->
            ok;
        _ ->
            erlang:demonitor(PrevRef)
    end,
    Leader = zraft_session_obj:leader(Session),
    Mref = erlang:monitor(process, Leader),
    State1 = State#state{leader_mref = Mref, epoch = E + 1},
    ping(State1).

trigger_all_watcher(#state{watchers = Watchers}) ->
    All = ets:tab2list(Watchers),
    lists:foreach(fun({Caller, Watch}) ->
        Caller ! #swatch_trigger{ref = Watch} end, All),
    ets:delete_all_objects(Watchers).

restart_requets(State = #state{connected = false}) ->
    connect(State);
restart_requets(State = #state{requests = Requests, epoch = E}) ->
    MatchWrite = [{{'$1', '_', '_', '_', '_', '$2'}, [{'<', '$2', {const, E}}], ['$1']}],
    W = ets:select(Requests, MatchWrite),
    lists:foreach(fun(ID) ->
        repeat_write(ID, State) end, W),
    MatchRead = [{{'$1', '_', '_', '_', '$2'}, [{'<', '$2', {const, E}}], ['$1']}],
    R = ets:select(Requests, MatchRead),
    lists:foreach(fun(Ref) ->
        repeat_read(Ref, State) end, R),
    case W of
        [] ->
            ping(State);
        _ ->
            State
    end.

cancel_timer(#state{timer = Timer}) ->
    if
        Timer /= undefined ->
            erlang:cancel_timer(Timer);
        true ->
            ok
    end.