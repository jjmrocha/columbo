%%
%% Copyright 2013 Joaquim Rocha
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

-define(DEFAULT_REFRESH, 30000).
-define(DEFAULT_GOSSIP, 60000).
-define(ALL_NOTIFICATION_NODES, []).
-define(COLUMBO_TABLE, ?MODULE).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0, refresh/0]).
-export([add_node/1, add_nodes/1, delete_node/1, known_nodes/0, online_nodes/0]).
-export([whereis_service/1, whereis_service/2]).
-export([send_to_all/2, send_to_nodes/3]).

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
	case ets:lookup(?COLUMBO_TABLE, Service) of
		[] -> [];
		[{_, Nodes}] -> Nodes
	end;
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
	SelectedNodes = common(Nodes, OnlineNodes),
	send_msg(Service, SelectedNodes, Msg).

%% ====================================================================
%% Behavioural functions 
%% ====================================================================
-record(state, {known_nodes, online_nodes, refresh_timer, gossip_timer}).

%% init
init([]) ->
	process_flag(trap_exit, true),	
	create_table(),
	KnownNodes = get_master_nodes(),
	RefreshInterval = application:get_env(columbo, refresh_interval, ?DEFAULT_REFRESH),
	GossipInterval = application:get_env(columbo, gossip_interval, ?DEFAULT_GOSSIP),
	{ok, RefreshTimer} = timer:send_interval(RefreshInterval, {run_update}),
	{ok, GossipTimer} = timer:send_interval(GossipInterval, {run_gossip}),
	error_logger:info_msg("Just one more thing, ~p [~p] is starting...\n", [?MODULE, self()]),
	State = run_update(#state{known_nodes=KnownNodes, 
				online_nodes=[], 
				refresh_timer=RefreshTimer,
				gossip_timer=GossipTimer}),
	{ok, State}.

%% handle_call
handle_call({get_known_nodes}, _From, State=#state{known_nodes=KnownNodes}) ->
	{reply, KnownNodes, State};

handle_call({get_online_nodes}, _From, State=#state{online_nodes=OnlineNodes}) ->
	{reply, OnlineNodes, State};

handle_call({refresh}, _From, State) ->
	NState = run_update(State),
	{reply, ok, NState}.

%% handle_cast
handle_cast({add_nodes, Nodes}, State=#state{known_nodes=KnownNodes}) ->
	NKnownNodes = add_if_not_member(Nodes, KnownNodes),
	{noreply, State#state{known_nodes=NKnownNodes}};

handle_cast({delete_node, Node}, State=#state{known_nodes=KnownNodes}) ->
	NKnownNodes = lists:delete(Node, KnownNodes),
	{noreply, State#state{known_nodes=NKnownNodes}}.

%% handle_info
handle_info({gossip, Nodes}, State=#state{known_nodes=KnownNodes}) ->
	NKnownNodes = add_if_not_member(Nodes, KnownNodes),
	{noreply, State#state{known_nodes=NKnownNodes}};

handle_info({run_update}, State) ->
	NState = run_update(State),
	{noreply, NState};

handle_info({run_gossip}, State=#state{known_nodes=KnownNodes}) ->
	send_to_all(?MODULE, {gossip, KnownNodes}),
	{noreply, State};

handle_info({nodedown, _Node}, State) ->
	NState = run_update(State),
	{noreply, NState}.

%% terminate
terminate(_Reason, #state{refresh_timer=RefreshTimer, gossip_timer=GossipTimer}) ->
	timer:cancel(RefreshTimer),
	timer:cancel(GossipTimer),
	drop_table(),
	ok.

%% code_change
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

create_table() ->
	Options = [set, public, named_table, {read_concurrency, true}],
	ets:new(?COLUMBO_TABLE, Options).

get_master_nodes() ->
	case application:get_env(columbo, master_nodes) of
		undefined -> [];
		{ok, Configured} -> lists:delete(node(), Configured)
	end.

add_if_not_member([Value|T], List) ->
	case lists:member(Value, List) of
		true -> add_if_not_member(T, List);
		false -> add_if_not_member(T, [Value|List])
	end;
add_if_not_member([], List) -> List.

run_update(State) ->
	KnownNodes = add_if_not_member(nodes(), State#state.known_nodes),
	OnlineNodes = get_active_nodes(KnownNodes),
	monitor_nodes(OnlineNodes, State#state.online_nodes),
	NodeData = get_node_data(OnlineNodes),
	ServiceData = get_service_data(OnlineNodes, NodeData),
	AllServices = get_all_services(ServiceData),
	ets:insert(?COLUMBO_TABLE, dict:to_list(AllServices)),
	State#state{known_nodes=KnownNodes, online_nodes=OnlineNodes}.

get_active_nodes(Nodes) ->
	Fun = fun(Node) ->
			net_adm:ping(Node) =:= pong
	end,
	lists:filter(Fun, Nodes).

monitor_nodes([Node|T], Previous) ->
	case lists:member(Node, Previous) of
		false -> monitor_node(Node, true);
		true -> ok
	end,
	monitor_nodes(T, Previous);
monitor_nodes([], _Previous) -> ok.

get_node_data(Nodes) -> 
	Fun = fun(Node, Dict) ->
			case rpc:call(Node, erlang, registered, []) of
				{badrpc, _Reason} -> Dict;
				Registered -> dict:store(Node, Registered, Dict)
			end
	end,
	lists:foldl(Fun, dict:new(), Nodes).

get_service_data(Nodes, NodeData) ->
	Fun = fun(Node, Acc) ->
			{ok, Registered} = dict:find(Node, NodeData),
			add_node_to_services(Registered, Node, Acc)
	end,
	lists:foldl(Fun, dict:new(), Nodes).

add_node_to_services([Service|T], Node, Dict) ->
	NDict = case dict:find(Service, Dict) of
		error -> dict:store(Service, [Node], Dict);
		{ok, NodeList} -> dict:store(Service, [Node|NodeList], Dict)
	end,
	add_node_to_services(T, Node, NDict);
add_node_to_services([], _Node, Dict) -> Dict.

get_all_services(ServiceData) ->
	Fun = fun({Service, _}, Dict) ->
			case dict:find(Service, Dict) of
				error -> dict:store(Service, [], Dict);
				_ -> Dict
			end
	end,
	ets:foldl(Fun, ServiceData, ?COLUMBO_TABLE).

drop_table() ->
	ets:delete(?COLUMBO_TABLE).

common(_List1, []) -> [];
common([], _List2) -> [];
common(List1, List2) -> 
	lists:filter(fun(Elem) -> 
		lists:member(Elem, List2) 
	end, List1).

send_msg(_Service, [], _Msg) -> 0;
send_msg(Service, Nodes, Msg) ->
	lists:foldl(fun(Node, Acc) -> 
		{Service, Node} ! Msg, 
		Acc + 1 
	end, 0, Nodes).