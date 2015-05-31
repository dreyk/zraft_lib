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
-module(zraft_snapshot_receiver).
-author("dreyk").

-behaviour(gen_fsm).

%% API
-export([start/2]).

%% gen_fsm callbacks
-export([init/1,
    handle_event/3,
    handle_sync_event/4,
    handle_info/3,
    terminate/3,
    code_change/4,
    listen/2,
    listen/3,
    prepare/2,
    prepare/3,
    fileinfo/2,
    fileinfo/3,
    filedata/2,
    filedata/3]).

-export([stop/1]).
-export([copy_to/4, copy_info/1, copy_files/4, discard_files_info/1]).

-define(LISTEN_TIMEOUT, 30000).
-define(DATA_TIMEOUT, 10000).
-define(NEXT_HEARBEAT, <<1:8>>).

-include_lib("zraft_lib/include/zraft.hrl").

-ifdef(TEST).
-define(SNAPHOT_LISTENER_ADDR, "127.0.0.1").
-else.
-define(SNAPHOT_LISTENER_ADDR, zraft_util:get_env(snapshot_listener_addr, "0.0.0.0")).
-endif.

-define(INFO(State,S, As),?MINFO("~p: "++S,[print_id(State)|As])).
-define(INFO(State,S), ?MINFO("~p: "++S,[print_id(State)])).
-define(ERROR(State,S, As),?MERROR("~p: "++S,[print_id(State)|As])).
-define(ERROR(State,S), ?MERROR("~p: "++S,[print_id(State)])).
-define(DEBUG(State,S, As),?MDEBUG("~p: "++S,[print_id(State)|As])).
-define(DEBUG(State,S), ?MDEBUG("~p: "++S,[print_id(State)])).
-define(WARNING(State,S, As),?MWARNING("~p: "++S,[print_id(State)|As])).
-define(WARNING(State,S), ?MWARNING("~p: "++S,[print_id(State)])).

-record(state, {peer_id,dir, lsock, rsock, fd, size,expected_size, curfile}).

start(PeerID,Dir) ->
    {ok, Pid} = gen_fsm:start(?MODULE, [PeerID], []),
    {ok, {Addr, Port}} = gen_fsm:sync_send_event(Pid, {start, Dir}),
    {ok, {{Addr, Port}, Pid}}.

stop(P) ->
    gen_fsm:sync_send_all_state_event(P, stop).

init([PeerID]) ->
    {ok, prepare, #state{peer_id = PeerID}, ?LISTEN_TIMEOUT}.


prepare(timeout, State) ->
    ?ERROR(State,"No init info ~p msec.", [?LISTEN_TIMEOUT]),
    {stop, {error, timeout}, State};
prepare(_, State) ->
    {stop, {error, not_supported}, State}.

prepare({start, Directory}, _From, State) ->
    Addr = ?SNAPHOT_LISTENER_ADDR,
    RAddr = case Addr of
                "0.0.0.0" ->
                    Default = zraft_util:node_addr(node()),
                    zraft_util:get_env(snapshot_receiver_addr, Default);
                Else ->
                    zraft_util:get_env(snapshot_receiver_addr, Else)
            end,
    case inet_parse:address(Addr) of
        {ok, IpAdd} ->
            listen_on(RAddr, IpAdd, State#state{dir = Directory});
        Err ->
            ?ERROR(State,"Can't start snapshot receiver listener on ~p: ~p.", [Addr, Err]),
            {stop, Err, Err, State}
    end;
prepare(_, _, State) ->
    {stop, {error, not_supported}, {error, not_supported}, State}.

listen(timeout, State) ->
    ?ERROR(State,"No connection to snapshot receiver during ~p msec.", [?LISTEN_TIMEOUT]),
    {stop, {error, timeout}, State};
listen(_, State) ->
    {stop, {error, not_supported}, State}.
listen(_, _, State) ->
    {stop, {error, not_supported}, {error, not_supported}, State}.

fileinfo(timeout, State) ->
    ?ERROR(State,"No data during ~p msec.", [?DATA_TIMEOUT]),
    {stop, {error, timeout}, State};
fileinfo(_, State) ->
    {stop, {error, not_supported}, State}.
fileinfo(_, _, State) ->
    {stop, {error, not_supported}, {error, not_supported}, State}.

filedata(timeout, State) ->
    ?ERROR(State,"No data during ~p msec.", [?DATA_TIMEOUT]),
    {stop, {error, timeout}, State};
filedata(_, State) ->
    {stop, {error, not_supported}, State}.
filedata(_, _, State) ->
    {stop, {error, not_supported}, {error, not_supported}, State}.

handle_event(_Event, _StateName, State) ->
    {stop, {error, not_supported}, State}.

handle_sync_event(stop, _From, _StateName, State) ->
    {stop, normal, ok, State};
handle_sync_event(_Event, _From, _StateName, State) ->
    {stop, {error, not_supported}, {error, not_supported}, State}.

handle_info({inet_async, _ListSock, _Ref, {ok, CliSocket}}, listen, State) ->
    inet_db:register_socket(CliSocket, inet_tcp),
    inet:setopts(CliSocket, [{active, once}, {packet, 4}, {linger, {true, 30}}]),
    {next_state, fileinfo, State#state{rsock = CliSocket}, ?DATA_TIMEOUT};

handle_info({tcp_closed, _Socket}, _StateName, State) ->
    ?WARNING(State,"Snapshot receiver socket closed."),
    {stop, tcp_closed, State};
handle_info({tcp_error, _Socket,Reason}, _StateName, State) ->
    ?WARNING(State,"Snaphot receiving socket error: ~p.",[Reason]),
    {stop, tcp_error, State};
handle_info({tcp, _, <<0:64, "done">>}, fileinfo, State) ->
    ok = gen_tcp:send(State#state.rsock, ?NEXT_HEARBEAT),
    {stop, normal, State};
handle_info({tcp, _, MsgData}, fileinfo, State = #state{dir = Dir, rsock = Sock}) ->
    <<Size:64, Name/binary>> = MsgData,
    FName = filename:join(Dir, binary_to_list(Name)),
    ?INFO(State,"Prepare receive file ~s of size ~p.", [FName, Size]),
    ok = gen_tcp:send(Sock, ?NEXT_HEARBEAT),
    if
        Size == 0 ->
            ?INFO(State,"Receive ~s direcory has created.", [FName]),
            zraft_util:make_dir(FName),
            ok = inet:setopts(Sock, [{active, once}]),
            {next_state, fileinfo, State#state{fd = undefined, size = 0}, ?DATA_TIMEOUT};
        true ->
            case filename:dirname(FName) of
                "." ->
                    ok;
                ParentDir ->
                    ok = zraft_util:make_dir(ParentDir)
            end,
            {ok, FD} = file:open(FName, [binary, write, exclusive, raw, delayed_write]),
            ok = inet:setopts(Sock, [{active, once}, {packet, 0}]),
            {next_state, filedata, State#state{fd = FD, size = 0,expected_size = Size, curfile = FName}, ?DATA_TIMEOUT}
    end;
handle_info({tcp, _, MsgData}, filedata,
    State = #state{fd = FD, size = Size,expected_size = ESize,rsock = Sock, curfile = File}) ->
    ok = file:write(FD, MsgData),
    NewSize = Size + size(MsgData),
    ?INFO(State,"Receive ~p of ~p bytes ~s", [NewSize,ESize,File]),
    if
        NewSize == ESize ->
            ok = file:datasync(FD),
            ok = file:close(FD),
            inet:setopts(Sock, [{active, once}, {packet, 4}]),
            ok = gen_tcp:send(Sock, ?NEXT_HEARBEAT),
            {next_state, fileinfo, State#state{fd = undefined, size = 0, curfile = undefined}, ?DATA_TIMEOUT};
        NewSize > ESize ->
            file:datasync(FD),
            file:close(FD),
            {stop, {error, invalid_size}, State#state{fd = undefined, size = 0}};
        true ->
            ok = inet:setopts(Sock, [{active, once}]),
            {next_state, filedata, State#state{size = NewSize}, ?DATA_TIMEOUT}
    end;

handle_info(_Info, _StateName, State) ->
    {stop, {error, not_supported}, State}.

terminate(Reason, _StateName, State) ->
    ?WARNING(State,"Receiver is being stoped. Reason is ~p",[Reason]),
    close_sock(State#state.rsock),
    close_sock(State#state.lsock),
    if
        State#state.fd /= undefined ->
            file:close(State#state.fd);
        true ->
            ok
    end,
    ok.

close_sock(undefined) ->
    ok;
close_sock(Sock) ->
    gen_tcp:close(Sock).

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State, ?DATA_TIMEOUT}.

listen_on(RAddr, IpAddr, State) ->
    SockOpts = [{ip, IpAddr}, binary, {packet, 4}, {reuseaddr, true}, {keepalive, true}, {backlog, 1024}, {active, false}],
    ListenerPort = zraft_util:get_env(snapshot_listener_port, 0),
    case gen_tcp:listen(ListenerPort, SockOpts) of
        {ok, LSock} ->
            {ok, _Ref} = prim_inet:async_accept(LSock, -1),
            {ok, Port} = inet:port(LSock),
            {reply, {ok, {RAddr, Port}}, listen, State#state{lsock = LSock}, ?LISTEN_TIMEOUT};
        Err ->
            ?ERROR(State,"Can't start snapshot receiver listener on ~p:~p. Reason is ~p", [IpAddr,ListenerPort,Err]),
            {stop, Err, Err, State}
    end.

copy_to(PeerID,Dir, Addr, Port) ->
    Files = copy_info(Dir),
    copy_files(PeerID,Files, Addr, Port).

copy_files(PeerID,CopyInfo, Addr, Port) ->
    case gen_tcp:connect(Addr, Port, [binary, {active, false}, {packet, 0}, {linger, {true, 30}}]) of
        {ok, Sock} ->
            Res = do_copy(PeerID,CopyInfo, Sock),
            Res1 = case Res of
                       ok ->
                           case send_fileinfo(0, "done", Sock) of
                               ok ->
                                   ok;
                               Else ->
                                   Else
                           end;
                       _ ->
                           Res
                   end,
            gen_tcp:close(Sock),
            Res1;
        Else ->
            Else
    end.


do_copy(_PeerID,[], _Sock) ->
    ok;
do_copy(PeerID,[{File, Size, FD} | T], Sock) ->
    case copy_file(PeerID,Size, File, FD, Sock) of
        ok ->
            do_copy(PeerID,T, Sock);
        Else ->
            Else
    end.

copy_file(PeerID,Size, File, FD, Sock) ->
    Res = case send_fileinfo(Size, File, Sock) of
              ok ->
                  ?INFO(PeerID,"Transfer snapshot file ~s of ~p bytes.", [File, Size]),
                  case catch file:sendfile(FD, Sock, 0, Size, []) of
                      {ok, Size} ->
                          ?INFO(PeerID,"Snapshot file ~s has been transfered.", [File]),
                          check_next_hearbeat(Sock);
                      {ok, BadSize} ->
                          ?INFO(PeerID,"Snapshot file ~s has been transfered wrong number of ~p bytes", [File, BadSize]),
                          {error, less_byte_sended};
                      Else ->
                          ?ERROR("Snapshot file ~s has failed to transfer. Reason is ~p.", [File, Else]),
                          Else
                  end;
              Else ->
                  Else
          end,
    Res.

send_fileinfo(Size, File, Sock) ->
    BName = list_to_binary(File),
    Packet = <<(size(BName) + 8):32, Size:64, BName/binary>>,
    case gen_tcp:send(Sock, Packet) of
        ok ->
            check_next_hearbeat(Sock);
        Else ->
            Else
    end.

check_next_hearbeat(Sock) ->
    case gen_tcp:recv(Sock, 5, ?DATA_TIMEOUT) of
        {ok, <<1:32, 1:8>>} ->
            ok;
        {ok, _} ->
            {error, invalid_next_hearbeat};
        Else ->
            Else
    end.

dest_name([], Suffix) ->
    Suffix;
dest_name(Preffix, Suffix) ->
    filename:join(Preffix, Suffix).

copy_info(Dir) ->
    copy_info(Dir, [], []).
copy_info(SrcDir, DestDir, FilesAcc) ->
    {ok, Files} = file:list_dir(SrcDir),
    copy_info(Files, SrcDir, DestDir, FilesAcc).

copy_info([], _SrcDir, _DestDir, FilesAcc) ->
    lists:ukeysort(1, FilesAcc);
copy_info([F | T], SrcDir, DestDir, FilesAcc) ->
    SrcName = filename:join(SrcDir, F),
    DestName = dest_name(DestDir, F),
    case filelib:is_dir(SrcName) of
        true ->
            FilesAcc1 = copy_info(SrcName, DestName, FilesAcc),
            copy_info(T, SrcDir, DestDir, FilesAcc1);
        _ ->
            {ok, File} = file:open(SrcName, [read, raw, binary]),
            Size = filelib:file_size(SrcName),
            copy_info(T, SrcDir, DestDir, [{DestName, Size, File} | FilesAcc])
    end.

discard_files_info(Files) ->
    lists:foreach(fun({_, _, FD}) ->
        file:close(FD) end, Files).

print_id(#state{peer_id = ID})->
    ID;
print_id(ID)->
    ID.

-ifdef(TEST).
setup() ->
    zraft_util:del_dir("test_fs_copy"),
    zraft_util:make_dir("test_fs_copy"),
    ok.
clear_setup(_) ->
    zraft_util:del_dir("test_fs_copy"),
    ok.

copy_snapshot_test_() ->
    {
        setup,
        fun setup/0,
        fun clear_setup/1,
        fun(_X) ->
            [
                do_copy()
            ]
        end
    }.

do_copy() ->
    {"copy_snapshot", fun() ->
        File1 = "test_fs_copy/test1",
        file:write_file(File1, <<"test1">>),
        File2 = "test_fs_copy/test2",
        file:write_file(File2, <<"test2">>),
        CopyTo = "test_fs_copy/copy",
        zraft_util:make_dir(CopyTo),
        {ok, {{Addr, Port}, _Pid}} = start(test,CopyTo),
        {ok, Sock} = gen_tcp:connect(Addr, Port, [binary, {active, false}, {packet, 0}, {linger, {true, 30}}]),
        make_dir_("d1", Sock),
        copy_file_(File1, "d1/test1", Sock),
        make_dir_("d2", Sock),
        copy_file_(File2, "d2/test2", Sock),
        Packet = <<12:32, 0:64, <<"done">>/binary>>,
        ok = gen_tcp:send(Sock, Packet),
        Res1 = gen_tcp:recv(Sock, 5, ?DATA_TIMEOUT),
        ?assertMatch({ok, <<1:32, 1:8>>}, Res1),
        C1 = file:read_file("test_fs_copy/copy/d1/test1"),
        ?assertMatch({ok, <<"test1">>}, C1),
        C2 = file:read_file("test_fs_copy/copy/d2/test2"),
        ?assertMatch({ok, <<"test2">>}, C2)
    end}.

make_dir_(Name, Sock) ->
    Packet = <<(length(Name) + 8):32, 0:64, (list_to_binary(Name))/binary>>,
    ok = gen_tcp:send(Sock, Packet),
    Res = gen_tcp:recv(Sock, 5, ?DATA_TIMEOUT),
    ?assertMatch({ok, <<1:32, 1:8>>}, Res).
copy_file_(SrcName, DestName, Sock) ->
    SndSize = filelib:file_size(SrcName),
    Packet = <<(length(DestName) + 8):32, SndSize:64, (list_to_binary(DestName))/binary>>,
    ok = gen_tcp:send(Sock, Packet),
    Res1 = gen_tcp:recv(Sock, 5, ?DATA_TIMEOUT),
    ?assertMatch({ok, <<1:32, 1:8>>}, Res1),
    Res2 = file:sendfile(SrcName, Sock),
    ?assertMatch({ok, _}, Res2),
    Res3 = gen_tcp:recv(Sock, 5, ?DATA_TIMEOUT),
    ?assertMatch({ok, <<1:32, 1:8>>}, Res3).

copy_fs_test_() ->
    {
        setup,
        fun setup/0,
        fun clear_setup/1,
        fun(_X) ->
            [
                do_copy_fs()
            ]
        end
    }.

big_fs_test_() ->
    {
        setup,
        fun setup/0,
        fun clear_setup/1,
        fun(_X) ->
            [
                big_copy_fs()
            ]
        end
    }.

do_copy_fs() ->
    {"copy_fs", fun() ->
        zraft_util:make_dir("test_fs_copy/src/d1"),
        File1 = "test_fs_copy/src/d1/test1",
        file:write_file(File1, <<"test1">>),
        zraft_util:make_dir("test_fs_copy/src/d2"),
        File2 = "test_fs_copy/src/d2/test2",
        file:write_file(File2, <<"test2">>),
        File0 = "test_fs_copy/src/test0",
        file:write_file(File0, <<"test0">>),
        CopyTo = "test_fs_copy/copy",
        zraft_util:make_dir(CopyTo),
        {ok, {{Addr, Port}, _Pid}} = start(test,CopyTo),
        Files = copy_info("test_fs_copy/src"),
        ok = file:rename("test_fs_copy/src", "test_fs_copy/src1"),
        Res = copy_files(test,Files, Addr, Port),
        ?assertEqual(ok, Res),
        C1 = file:read_file("test_fs_copy/copy/d1/test1"),
        ?assertMatch({ok, <<"test1">>}, C1),
        C2 = file:read_file("test_fs_copy/copy/d2/test2"),
        ?assertMatch({ok, <<"test2">>}, C2),
        C0 = file:read_file("test_fs_copy/copy/test0"),
        ?assertMatch({ok, <<"test0">>}, C0)
    end}.


big_copy_fs() ->
    {"copy big file", fun() ->
        zraft_util:make_dir("test_fs_copy/src/d1"),
        File1 = "test_fs_copy/src/d1/test1",
        {ok, FD} = file:open(File1, [binary, raw, write]),
        lists:foreach(fun(I) ->
            ok = file:write(FD, <<I:64>>) end, lists:seq(1, 1024)),
        CopyTo = "test_fs_copy/copy",
        zraft_util:make_dir(CopyTo),
        {ok, {{Addr, Port}, _Pid}} = start(test,CopyTo),
        Files = copy_info("test_fs_copy/src"),
        Res = copy_files(test,Files, Addr, Port),
        discard_files_info(Files),
        ?assertEqual(ok,Res)
    end}.

-endif.