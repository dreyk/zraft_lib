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
-export([
    start_link/2,
    start_link/3,
    query/3,
    query/4,
    write/3,
    write/4,
    stop/1
]).

-export_type([
    session/0
]).

-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-include_lib("zraft_lib/include/zraft.hrl").


-define(CONNECTING, connecting).
-define(CONNECTED, connected).
-define(PENDFING, pending).

-type session() :: pid().

-record(state, {
    session,
    requests,
    watchers,
    message_id,
    acc_upto,
    leader_mref,
    epoch,
    timer,
    timeout,
    pending,
    connected,
    last_send}).


-spec query(session(), term(), timeout()) -> {ok, term()}.
query(Session, Query, Timeout) ->
    query(Session, Query, false, Timeout).
-spec query(session(), term(), reference()|false, timeout()) -> {ok, term()}.
query(Session, Query, Watch, Timeout) ->
    gen_server:call(Session, {query, {Query, Watch, Timeout}}, inc_timeout(Timeout)).

-spec write(session(), term(), timeout()) -> {ok, term()}.
write(Session, Data, Timeout) ->
    write(Session, Data, false, Timeout).
-spec write(session(), term(), true|false, timeout()) -> {ok, term()}.
write(Session, Data, Temporary, Timeout) ->
    gen_server:call(Session, {write, {Data, Temporary, Timeout}}, inc_timeout(Timeout)).

-spec inc_timeout(timeout())->timeout().
inc_timeout(T) when is_integer(T)->
    round(T*1.5);
inc_timeout(T)->
    T.

-spec stop(session()) -> ok.
stop(Session) ->
    gen_server:cast(Session, stop).

-spec start_link(atom(),zraft_consensus:peer_id()|list(zraft_consensus:peer_id()), timeout()) ->
    {ok, pid()} | {error, Reason :: term()}.
start_link(Name,Peer, Timeout) ->
    gen_server:start_link({local,Name},?MODULE, [Peer, Timeout], []).

-spec start_link(zraft_consensus:peer_id()|list(zraft_consensus:peer_id()), timeout()) ->
    {ok, pid()} | {error, Reason :: term()}.
start_link(Peer, Timeout) ->
    gen_server:start_link(?MODULE, [Peer, Timeout], []).

init([Peer, Timeout]) ->
    ETime = zraft_consensus:get_election_timeout(),
    Session = zraft_client:light_session(Peer, ETime * 4, ETime*2),
    Requests = ets:new(client_session, [ordered_set, {write_concurrency, false}, {read_concurrency, false}]),
    Watchers = ets:new(client_watchers, [bag, {write_concurrency, false}, {read_concurrency, false}]),
    ConnectTimer = erlang:start_timer(Timeout, self(), connect_timeout),
    State = #state{
        session = Session,
        requests = Requests,
        watchers = Watchers,
        message_id = 1,
        epoch = 1,
        timeout = Timeout,
        connected = false,
        timer = ConnectTimer,
        pending = false
    },
    State1 = connect(State),
    {ok, State1}.


handle_call({write, Req}, From, State) ->
    handle_write(Req, From, State);
handle_call({query, Req}, From, State) ->
    handle_query(Req, From, State).

handle_cast(stop, State) ->
    stop_session(State),
    {stop, normal, State};
handle_cast(_Request, State) ->
    {noreply, State}.



handle_info(#swrite_error{sequence = Sequence, error = not_leader, leader = NewLeader}, State) ->
    %%attempt resend only one failed request. Other maybe ok.
    case change_leader(NewLeader, State) of
        {false, State1} ->
            {noreply, State1};
        {true, State1} ->
            if
                Sequence == ?CLIENT_PING ->
                    NewState = repeat_ping(State1);
                Sequence == ?CLIENT_CONNECT ->
                    NewState = connect(State1);
                is_integer(Sequence) ->
                    NewState = repeat_write(Sequence, State1)
            end,
            {noreply, NewState}
    end;


handle_info(#swrite_reply{sequence = ?CLIENT_PING}, State) ->
    {noreply, State};

handle_info(#swrite_reply{sequence = ?CLIENT_CONNECT}, State = #state{connected = true}) ->
    %%we can send connect request twice
    {noreply,State};
handle_info(#swrite_reply{sequence = ?CLIENT_CONNECT}, State = #state{epoch = E,timer = TRef}) ->
    cancel_timer(TRef),
    State1 = restart_requets(State#state{connected = true,epoch = E+1,timer=undefined}),
    State2 = ping(State1),
    {noreply, State2};

handle_info(#swrite_reply{sequence = ID, data = Result}, State) when is_integer(ID) ->
    NewState = write_reply(ID, Result, State),
    {noreply, NewState};


handle_info({{read, ReadRef}, #sread_reply{data = Data}}, State) ->
    NewState = read_reply(ReadRef, Data, State),
    {noreply, NewState};
handle_info({{read, ReadRef}, {leader, NewLeader}}, State) ->
    %%repeat only this request. other requests will be restart on session change leader event
    case change_leader(NewLeader, State) of
        {false,State1}->
            {noreply,State1};
        {true,State1}->
            NewState = repeat_read(ReadRef, State1)    ,
            {noreply, NewState}
    end;
handle_info({{read, ReadRef}, Data}, State) ->
    Reply = case Data of
                {ok, R1} ->
                    R1;
                _ ->
                    Data
            end,
    NewState = read_reply(ReadRef, Reply, State),
    {noreply, NewState};
handle_info(#swatch_trigger{ref = {Caller, Watch}, reason = Reson}, State = #state{watchers = Watcher}) ->
    Caller ! #swatch_trigger{ref = Watch, reason = Reson},
    ets:delete_object(Watcher, {Caller, Watch}),
    {noreply, State};

handle_info(?DISCONNECT_MSG, State) ->
    {stop, ?DISCONNECT_MSG, State};

handle_info({leader, NewLeader}, State) ->
    lager:warning("Leader changed to ~p", [NewLeader]),
    case set_leader(NewLeader,State) of
        {false,State1}->
            {noreply, State1};
        {true,State1}->
            State2 = restart_requets(State1),
            {noreply, State2}
    end;
handle_info({'DOWN', Ref, process, _, _}, State = #state{leader_mref = Ref}) ->
    lager:warning("Current leader has failed"),
    State1 = State#state{leader_mref = undefined},
    case change_leader(failed, State1) of
        {false,State2}->
            {noreply,State2};
        {true,State2}->
            State3 = restart_requets(State2),
            {noreply, State3}
    end;
handle_info({'DOWN', Ref, process, Caller, _}, State) ->
    State1 = caller_down(Caller, Ref, State),
    {noreply, State1};

handle_info({timeout, TimerRef, pending}, State = #state{pending = TimerRef}) ->
    FreashSession = zraft_session_obj:reset(State#state.session),
    case install_leader(State#state{session = FreashSession,pending = false}) of
        {false,State1}->
            {noreply,State1};
        {true,State1}->
            State2 = restart_requets(State1),
            {noreply,State2}
    end;
handle_info({timeout, TimerRef, ping_timeout}, State = #state{timer = TimerRef}) ->
    State1 = ping(State),
    {noreply, State1};
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


pending(State = #state{pending = P}) when P /= false ->
    State;
pending(State) ->
    NewTimer = erlang:start_timer(zraft_consensus:get_election_timeout()*2, self(), pending),
    State#state{pending = NewTimer}.


ping(State = #state{timer = Timer, timeout = Timeout,last_send = Last}) ->
    cancel_timer(Timer),
    PingTimeout = Timeout div 2,
    case zraft_util:is_expired(Last,PingTimeout) of
        true->
            NewTimer = erlang:start_timer(PingTimeout, self(),ping_timeout),
            repeat_ping(State#state{timer = NewTimer});
        {false,T1}->
            NewTimer = erlang:start_timer(T1, self(),ping_timeout),
            State#state{timer = NewTimer}

    end.
repeat_ping(State = #state{acc_upto = To})->
    Req = #swrite{message_id = ?CLIENT_PING, acc_upto = To, from = self(), data = <<>>},
    write_to_raft(Req, State),
    State#state{last_send = os:timestamp()}.

connect(State = #state{connected = true}) ->
    State;
connect(State = #state{timeout = Timeout, timer = Timer}) ->
    cancel_timer(Timer),
    case install_leader(State) of
        {false,State1}->
            State1;
        {true,State1}->
            Req = #swrite{message_id = ?CLIENT_CONNECT, acc_upto = 0, from = self(), data = Timeout},
            write_to_raft(true,Req, State),
            State1#state{last_send = os:timestamp()}
    end.

stop_session(State = #state{acc_upto = To, timer = Timer}) ->
    cancel_timer(Timer),
    Req = #swrite{message_id = ?CLIENT_CLOSE, from = self(), data = <<>>, acc_upto = To},
    write_to_raft(true,Req, State).


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
caller_down(Caller, MRef, State = #state{requests = Requests, watchers = Watchers}) ->
    ets:delete(Watchers, Caller),
    case ets:lookup(Requests, MRef) of
        [{MRef, _, _From, TRef, _}] ->
            cancel_timer(TRef),
            ets:delete(Requests, MRef),
            State;
        [{MRef, ID}] ->
            case ets:lookup(Requests, ID) of
                [{ID, _Req, _From, TRef, MRef, _E}] ->
                    cancel_timer(TRef),
                    ets:delete(Requests, ID),
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
        [{Ref, Req, _From, _TRef, E0}] when E0 < E ->
            ets:update_element(Requests, Ref, {5, E}),
            read_from_raft(Ref, Req, State),
            State;
        _ ->
            State
    end.

read_reply(Ref, Result, State = #state{requests = Requests}) ->
    case ets:lookup(Requests, Ref) of
        [{Ref, Req, From, TRef, _Epoch}] ->
            register_watcher(Req, State),
            gen_server:reply(From, Result),
            erlang:demonitor(Ref),
            cancel_timer(TRef),
            ets:delete(Requests, Ref),
            State;
        _ ->
            State
    end.

handle_write({Request, Temporary, Timeout}, From, State = #state{message_id = ID, acc_upto = To, requests = Requests, epoch = E}) ->
    Req = #swrite{message_id = ID, acc_upto = To, from = self(), temporary = Temporary, data = Request},
    write_to_raft(Req, State),
    {Caller, _} = From,
    TRef = erlang:start_timer(Timeout, self(), {request, ID}),
    MRef = erlang:monitor(process, Caller),
    ets:insert(Requests, {ID, Req, From, TRef, MRef, E}),
    ets:insert(Requests, {MRef, ID}),
    {noreply, State#state{message_id = ID + 1,last_send = os:timestamp()}}.
repeat_write(ID, State = #state{requests = Requests, acc_upto = To, epoch = E}) ->
    case ets:lookup(Requests, ID) of
        [{ID, Req, _From, _TRef, _MRef, E0}] when E0 < E ->
            ets:update_element(Requests, ID, {6, E}),
            Req1 = Req#swrite{acc_upto = To},
            write_to_raft(Req1, State),
            State#state{last_send = os:timestamp()};
        _ ->
            State
    end.

write_reply(ID, Result, State = #state{requests = Requests}) ->
    case ets:lookup(Requests, ID) of
        [{ID, _Req, From, TRef, MRef, _Epoch}] ->
            gen_server:reply(From, Result),
            erlang:demonitor(MRef),
            cancel_timer(TRef),
            ets:delete(Requests, ID),
            ets:delete(Requests, MRef),
            State1 = update_upto(State),
            State1;
        _ ->
            State
    end.

write_to_raft(Req, State = #state{connected = Connected}) ->
    write_to_raft(Connected,Req,State).

write_to_raft(false,_Req,_State) ->
    ok;
write_to_raft(_,_Req, #state{pending = P}) when P /= false->
    ok;
write_to_raft(_,Req, #state{session = Session}) ->
    Leader = zraft_session_obj:leader(Session),
    zraft_consensus:send_swrite(Leader, Req).
read_from_raft(_,_, #state{pending = P}) when P /= false->
    ok;
read_from_raft(_,_, #state{connected = false}) ->
    ok;
read_from_raft(Ref, {Query, Watch, Timeout}, #state{session = Session}) ->
    Leader = zraft_session_obj:leader(Session),
    zraft_consensus:async_query(Leader, {read, Ref}, Watch, Query, Timeout).

register_watcher({_, false, _}, _State) ->
    ok;
register_watcher({_, Watch, _}, #state{watchers = Watchers}) ->
    ets:insert(Watchers, Watch).

update_upto(State = #state{requests = Requests, message_id = ID}) ->
    case ets:next(Requests, 0) of
        K when is_integer(K) ->
            State#state{acc_upto = K-1};
        _ ->
            State#state{acc_upto = ID - 1}
    end.


set_leader(NewLeader, State = #state{session = Session,pending = Pending}) ->
    if
        Pending /= false->
            %%if  pending state just resend all reset pending
            NewSession = zraft_session_obj:set_leader(NewLeader,Session),
            install_leader(State#state{session = NewSession});
        true->
            %%it's not pending state. change leader then nessary
            case zraft_session_obj:leader(Session) of
                NewLeader ->
                    {true,State};
                _ ->
                    NewSession = zraft_session_obj:set_leader(NewLeader,Session),
                    install_leader(State#state{session = NewSession})
            end
    end.

change_leader(failed, State) ->
    Fun = fun zraft_session_obj:fail/1,
    change_leader_fn(Fun, State);
change_leader(undefined, State) ->
    Fun = fun zraft_session_obj:next/1,
    change_leader_fn(Fun, State);
change_leader(NewLeader, State = #state{session = Session,pending = Pending}) ->
    case zraft_session_obj:leader(Session) of
        NewLeader when Pending==false->
            {true,State};
        NewLeader->
            {false,State};
        _ ->
            Fun = fun(S) ->
                zraft_session_obj:change_leader(NewLeader, S) end,
            change_leader_fn(Fun, State)
    end.

change_leader_fn(Fun, State = #state{session = Session}) ->
    case Fun(Session) of
        {error, etimeout} ->
            State1 = pending(State),
            {false, State1};
        {error, all_failed} ->
            exit({error, no_peer_available});
        NewSession ->
            State1 = State#state{session = NewSession},
            install_leader(State1)
    end.

install_leader(State = #state{session = Session, leader_mref = PrevRef, epoch = E}) ->
    case PrevRef of
        undefined ->
            ok;
        _ ->
            erlang:demonitor(PrevRef)
    end,
    Leader = zraft_session_obj:leader(Session),
    MRef = erlang:monitor(process, Leader),
    receive
        {'DOWN', MRef, process, _, _} ->
            change_leader(failed, State)
    after 0 ->
        if
            State#state.pending /= false->
                cancel_timer(State#state.pending);
            true->
                ok
        end,
        trigger_all_watcher(State),
        {true, State#state{leader_mref = MRef, epoch = E + 1,pending = false}}
    end.

trigger_all_watcher(#state{watchers = Watchers}) ->
    ets:foldl(fun({Caller, Watch}, Acc) ->
        Caller ! #swatch_trigger{ref = Watch, reason = change_leader}, Acc end, 0, Watchers),
    ets:delete_all_objects(Watchers).

restart_requets(State = #state{connected = false})->
    connect(State);
restart_requets(State = #state{requests = Requests, epoch = E}) ->
    MatchWrite = [{{'$1', '_', '_', '_', '_', '$2'}, [{'<', '$2', {const, E}}], ['$1']}],
    W = ets:select(Requests, MatchWrite),
    State1 = lists:foldl(fun(ID,StateAcc1) ->
        repeat_write(ID, StateAcc1) end,State, W),
    MatchRead = [{{'$1', '_', '_', '_', '$2'}, [{'<', '$2', {const, E}}], ['$1']}],
    R = ets:select(Requests, MatchRead),
    State2 = lists:foldl(fun(Ref,StateAcc2) ->
        repeat_read(Ref, StateAcc2) end,State1,R),
    State2.

cancel_timer(Timer) ->
    if
        Timer /= undefined ->
            erlang:cancel_timer(Timer);
        true ->
            ok
    end.