%%
%% Copyright 2014-2015 Joaquim Rocha <jrocha@gmailbox.org>
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

-module(columbo).

-behaviour(gen_server).

-define(SERVER, {local, ?MODULE}).

-define(ALL_NOTIFICATION_NODES, []).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0, refresh/0]).
-export([add_node/1, add_nodes/1, delete_node/1, known_nodes/0, online_nodes/0]).
-export([whereis_service/1, whereis_service/2]).
-export([send_to_all/2, send_to_nodes/3]).
-export([subscribe/1, unsubscribe/1]).

start_link() ->
	gen_server:start_link(?SERVER, ?MODULE, [], []).

-spec refresh() -> ok.
refresh() ->
	gen_server:call(?MODULE, {refresh}).

-spec add_node(Node :: atom()) -> ok.
add_node(Node) ->
	add_nodes([Node]).

-spec add_nodes(Nodes :: [atom(), ...]) -> ok.
add_nodes(Nodes) ->
	gen_server:cast(?MODULE, {add_nodes, Nodes}).

-spec delete_node(Node :: atom()) -> ok.
delete_node(Node) when is_atom(Node) ->
	gen_server:cast(?MODULE, {delete_node, Node}).

-spec known_nodes() -> [atom(), ...].
known_nodes() ->
	gen_server:call(?MODULE, {get_known_nodes}).

-spec online_nodes() -> [atom(), ...].
online_nodes() ->
	gen_server:call(?MODULE, {get_online_nodes}).

-spec whereis_service(Service :: atom()) -> [atom(), ...].
whereis_service(Service) ->
	whereis_service(Service, true).

-spec whereis_service(Service :: atom(), IncludeLocal :: boolean()) -> [atom(), ...].
whereis_service(Service, false) ->
	columbo_service:find(Service);
whereis_service(Service, true) ->
	LocalServices = erlang:registered(),
	Local = case lists:member(Service, LocalServices) of
		true -> [node()];
		false -> []
	end,
	Remote = whereis_service(Service, false),
	Local ++ Remote.

-spec send_to_all(Service :: atom(), Msg :: any()) -> integer().
send_to_all(Service, Msg) ->
	Nodes = whereis_service(Service, false),
	send_msg(Service, Nodes, Msg).

-spec send_to_nodes(Service :: atom(), Nodes :: [atom(), ...], Msg :: any()) -> integer().
send_to_nodes(_Service, [], _Msg) -> 0;
send_to_nodes(Service, Nodes, Msg) ->
	OnlineNodes = whereis_service(Service, false),
	SelectedNodes = columbo_util:common(Nodes, OnlineNodes),
	send_msg(Service, SelectedNodes, Msg).

-spec subscribe(Service :: atom()) -> true.
subscribe(Service) ->
	columbo_notify:subscribe(Service, self()).

-spec unsubscribe(Service :: atom()) -> true.
unsubscribe(Service) ->
	columbo_notify:unsubscribe(Service, self()).

%% ====================================================================
%% Behavioural functions 
%% ====================================================================
-record(state, {known_nodes, online_nodes, refresh_timer, gossip_timer, udp}).

%% init
init([]) ->
	process_flag(trap_exit, true),	
	create_tables(),
	{ok, RefreshInterval} = application:get_env(columbo, refresh_interval),
	{ok, GossipInterval} = application:get_env(columbo, gossip_interval),
	{ok, RefreshTimer} = timer:send_interval(RefreshInterval, {run_update}),
	{ok, GossipTimer} = timer:send_interval(GossipInterval, {run_gossip}),
	{ok, UDPSocket} = columbo_cast:open_socket(),
	error_logger:info_msg("Just one more thing, ~p [~p] is starting...\n", [?MODULE, self()]),
	MasterNodes = get_master_nodes(),
	{KnownNodes, OnlineNodes} = columbo_update:run(MasterNodes, []),
	State = #state{known_nodes=KnownNodes, 
			online_nodes=OnlineNodes, 
			refresh_timer=RefreshTimer,
			gossip_timer=GossipTimer,
			udp=UDPSocket},
	{ok, State}.

%% handle_call
handle_call({get_known_nodes}, _From, State=#state{known_nodes=KnownNodes}) ->
	{reply, KnownNodes, State};

handle_call({get_online_nodes}, _From, State=#state{online_nodes=OnlineNodes}) ->
	{reply, OnlineNodes, State};

handle_call({refresh}, _From, State) ->
	{KnownNodes, OnlineNodes} = columbo_update:run(State#state.known_nodes, State#state.online_nodes),
	{reply, ok, State#state{known_nodes=KnownNodes, online_nodes=OnlineNodes}}.

%% handle_cast
handle_cast({add_nodes, Nodes}, State) ->
	NState = update_nodes(State, Nodes),
	{noreply, NState};

handle_cast({delete_node, Node}, State=#state{known_nodes=KnownNodes}) ->
	NKnownNodes = lists:delete(Node, KnownNodes),
	{noreply, State#state{known_nodes=NKnownNodes}}.

%% handle_info
handle_info({gossip, Nodes}, State) ->
	NState = update_nodes(State, Nodes),
	{noreply, NState};

handle_info({run_update}, State) ->
	{KnownNodes, OnlineNodes} = columbo_update:run(State#state.known_nodes, State#state.online_nodes),
	{noreply, State#state{known_nodes=KnownNodes, online_nodes=OnlineNodes}};

handle_info({run_gossip}, State=#state{known_nodes=KnownNodes}) ->
	send_to_all(?MODULE, {gossip, KnownNodes}),
	{noreply, State};

handle_info({udp, _Socket, _Host, _Port, Bin}, State) ->
	columbo_cast:process_msg(Bin),
	{noreply, State};

handle_info({nodedown, _Node}, State) ->
	{KnownNodes, OnlineNodes} = columbo_update:run(State#state.known_nodes, State#state.online_nodes),
	{noreply, State#state{known_nodes=KnownNodes, online_nodes=OnlineNodes}}.

%% terminate
terminate(_Reason, #state{refresh_timer=RefreshTimer, gossip_timer=GossipTimer, udp=UDPSocket}) ->
	timer:cancel(RefreshTimer),
	timer:cancel(GossipTimer),
	columbo_cast:close_socket(UDPSocket),
	drop_tables(),
	ok.

%% code_change
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

create_tables() ->
	columbo_service:create(),
	columbo_notify:create().

drop_tables() ->
	columbo_service:drop(),
	columbo_notify:drop().

get_master_nodes() ->
	case application:get_env(columbo, master_nodes) of
		undefined -> [];
		{ok, Configured} -> lists:delete(node(), Configured)
	end.

update_nodes(State=#state{known_nodes=KnownNodes}, Nodes) ->
	NewNodes = lists:delete(node(), Nodes),
	NKnownNodes = columbo_util:add_if_not_member(NewNodes, KnownNodes),
	if erlang:length(KnownNodes) /= erlang:length(NKnownNodes) ->
			{NKnownNodes1, NOnlineNodes1} = columbo_update:run(NKnownNodes, State#state.online_nodes),
			State#state{known_nodes=NKnownNodes1, online_nodes=NOnlineNodes1};
		true -> State
	end.

send_msg(_Service, [], _Msg) -> 0;
send_msg(Service, Nodes, Msg) ->
	lists:foldl(fun(Node, Acc) -> 
				{Service, Node} ! Msg, 
				Acc + 1 
		end, 0, Nodes).