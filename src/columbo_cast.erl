%%
%% Copyright 2014 Joaquim Rocha <jrocha@gmailbox.org>
%% 
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%

-module(columbo_cast).

-define(NO_UDP_PORT, none).
-define(ALL_INTERFACES, "*").
-define(BROADCAST_IP, {255, 255, 255, 255}).
-define(DEFAULT_UDP, ?NO_UDP_PORT).
-define(DEFAULT_INTERFACE, ?ALL_INTERFACES).
-define(UDP_MSG, <<"columbo:">>).

%% ====================================================================
%% API functions
%% ====================================================================
-export([open_socket/0, close_socket/1]).
-export([process_msg/1]).

open_socket() ->
	UDPPort = application:get_env(columbo, udp_port, ?DEFAULT_UDP),
	open_socket(UDPPort).

close_socket(?NO_UDP_PORT) -> ok;
close_socket(UDPSocket) -> 
	gen_udp:close(UDPSocket).

process_msg(Msg) ->
	case binary:split(Msg, ?UDP_MSG) of
		[<<>>, NodeBin] ->
			Node = binary_to_atom(NodeBin, utf8),
			case net_adm:ping(Node) of
				pong -> columbo:add_node(Node);
				_ -> ok
			end;
		_ -> ok
	end.

%% ====================================================================
%% Internal functions
%% ====================================================================

open_socket(?NO_UDP_PORT) -> {ok, ?NO_UDP_PORT};
open_socket(UDPPort) ->
	SocketOptions = get_socket_options(),
	{ok, Socket} = gen_udp:open(UDPPort, SocketOptions),
	ok = send_introduction(Socket, UDPPort),
	{ok, Socket}.

get_socket_options() ->
	Global = [binary, inet, {active, true}, {broadcast, true}, {reuseaddr, true}],
	Specific = case os:type() of
		{unix, OsName} ->
			case lists:member(OsName, [darwin, freebsd, openbsd, netbsd]) of
				true -> [{raw, 16#ffff, 16#0200, <<1:32/native>>}];
				false -> []
			end;
		_ -> []
	end,
	Global ++ Specific.

send_introduction(Socket, UDPPort) ->
	Node = atom_to_binary(node(), utf8),
	Msg = <<?UDP_MSG/binary, Node/binary>>,
	BroadcastIP = broadcast_addr(),
	gen_udp:send(Socket, BroadcastIP, UDPPort, Msg).

broadcast_addr() ->
	case application:get_env(columbo, interface, ?DEFAULT_INTERFACE) of
		?ALL_INTERFACES -> ?BROADCAST_IP;
		Interface ->
			case inet:getifaddrs() of
				{ok, NetConfig} ->
					case lists:keyfind(Interface, 1, NetConfig) of
						false -> ?BROADCAST_IP;
						{_, Props} ->
							Flags = proplists:get_value(flags, Props, []),
							Up = lists:member(up, Flags),
							Broadcast = lists:member(broadcast, Flags),
							LoopBack =  lists:member(loopback, Flags),
							P2P = lists:member(pointtopoint, Flags),
							if 
								Up =:= true, Broadcast =:= true, LoopBack /= true andalso P2P /= true ->
									case lists:keyfind(broadaddr, 1, Props) of
										false -> ?BROADCAST_IP;
										{_, IP} -> IP
									end;
								true -> ?BROADCAST_IP
							end
					end;
				_ -> ?BROADCAST_IP
			end
	end.
