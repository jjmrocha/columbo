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

-define(APP_NAME, columbo).
-define(CONFIG_REFRESH, refresh_interval).
-define(CONFIG_MASTER_NODES, master_nodes).
-define(DEFAULT_REFRESH, 30000).

-define(ALL_NOTIFICATION_NODES, []).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0, refresh/0]).
-export([add_node/1, add_node/3, delete_node/1, delete_node/3, known_nodes/0]).
-export([whereis_service/1, whereis_service/2]).
-export([request_notification/2, request_notification/3, delete_notification/2]).

start_link() ->
	gen_server:start_link(?SERVER, ?MODULE, [], []).

refresh() ->
	gen_server:call(?MODULE, {refresh}).

add_node(Node) when is_atom(Node) ->
	gen_server:cast(?MODULE, {add_node, Node}).

add_node(Service, Ref, Node) when is_atom(Service) andalso is_reference(Ref) andalso is_atom(Node) ->
	gen_server:cast(?MODULE, {add_node, Service, Ref, Node}).

delete_node(Node) when is_atom(Node) ->
	gen_server:cast(?MODULE, {delete_node, Node}).

delete_node(Service, Ref, Node) when is_atom(Service) andalso is_reference(Ref) andalso is_atom(Node) ->
	gen_server:cast(?MODULE, {delete_node, Service, Ref, Node}).

known_nodes() ->
	gen_server:call(?MODULE, {get_known_nodes}).

whereis_service(Service) when is_atom(Service) ->
	whereis_service(Service, true).

whereis_service(Service, IncludeLocal) when is_atom(Service) andalso is_boolean(IncludeLocal) ->
	gen_server:call(?MODULE, {whereis_service, Service, IncludeLocal}).

request_notification(Service, Fun) when is_atom(Service) andalso is_function(Fun, 1) ->
	request_notification(Service, Fun, ?ALL_NOTIFICATION_NODES).

request_notification(Service, Fun, Nodes) when is_atom(Service) andalso is_function(Fun, 1) andalso is_list(Nodes)->
	gen_server:call(?MODULE, {request_notification, Service, Fun, Nodes}).

delete_notification(Service, Ref) when is_atom(Service) andalso is_reference(Ref) ->
	gen_server:cast(?MODULE, {delete_notification, Service, Ref}).

%% ====================================================================
%% Behavioural functions 
%% ====================================================================
-record(notification, {function, requested_nodes}).
-record(request, {service, notifications}).
-record(state, {known_nodes, nodes, services, requests, timer}).

%% init
init([]) ->
	process_flag(trap_exit, true),	
	KnownNodes = get_master_nodes(),
	TimerInterval = application:get_env(?APP_NAME, ?CONFIG_REFRESH, ?DEFAULT_REFRESH),
	{ok, Timer} = timer:send_interval(TimerInterval, {run_update}),
	error_logger:info_msg("Just one more thing, ~p [~p] is starting...\n", [?MODULE, self()]),
	{ok, run_update(#state{known_nodes=KnownNodes, nodes=dict:new(), services=dict:new(), requests=dict:new(), timer=Timer})}.

%% handle_call
handle_call({whereis_service, Service, IncludeLocal}, From, State=#state{services=Services}) ->
	run_whereis_service(Service, IncludeLocal, Services, From),
	{noreply, State};

handle_call({get_known_nodes}, _From, State=#state{known_nodes=KnownNodes}) ->
	{reply, KnownNodes, State};

handle_call({refresh}, _From, State) ->
	NState = run_update(State),
	{reply, ok, NState};

handle_call({request_notification, Service, Fun, Nodes}, _From, State=#state{known_nodes=KnownNodes, services=Services, requests=Requests}) ->
	Ref = erlang:make_ref(),
	Notification = #notification{function=Fun, requested_nodes=Nodes},
	NKnownNodes = join(Nodes, KnownNodes),
	NRequests = case dict:find(Service, Requests) of
		error -> 
			Notifications = dict:store(Ref, Notification, dict:new()),
			Request = #request{service=Service, notifications=Notifications},
			dict:store(Service, Request, Requests);
		{ok, Request} ->
			Notifications = dict:store(Ref, Notification, Request#request.notifications),
			dict:store(Service, Request#request{notifications=Notifications}, Requests)
	end,	
	RemoteNodes = whereis_service(Service, false, Services),
	RequestedNodes = case Nodes of
		?ALL_NOTIFICATION_NODES -> RemoteNodes;
		_ -> common(RemoteNodes, [], Nodes)
	end,
	{reply, {Ref, RequestedNodes}, State#state{known_nodes=NKnownNodes, requests=NRequests}}.

%% handle_cast
handle_cast({add_node, Node}, State=#state{known_nodes=KnownNodes}) ->
	{noreply, State#state{known_nodes=join([Node], KnownNodes)}};

handle_cast({add_node, Service, Ref, Node}, State=#state{known_nodes=KnownNodes, requests=Requests}) ->
	NKnownNodes = join([Node], KnownNodes),
	NRequests = case dict:find(Service, Requests) of
		error -> Requests;
		{ok, Request} ->
			Notifications = case dict:find(Ref, Request#request.notifications) of
				error -> Request#request.notifications;
				{ok, Notification} ->
					Nodes = join([Node], Notification#notification.requested_nodes),
					dict:store(Ref, Notification#notification{requested_nodes=Nodes}, Request#request.notifications)
			end,
			dict:store(Service, Request#request{notifications=Notifications}, Requests)
	end,	
	{noreply, State#state{known_nodes=NKnownNodes, requests=NRequests}};

handle_cast({delete_node, Service, Ref, Node}, State=#state{requests=Requests}) ->
	NRequests = case dict:find(Service, Requests) of
		error -> Requests;
		{ok, Request} ->
			Notifications = case dict:find(Ref, Request#request.notifications) of
				error -> Request#request.notifications;
				{ok, Notification} ->
					Nodes = lists:delete(Node, Notification#notification.requested_nodes),
					dict:store(Ref, Notification#notification{requested_nodes=Nodes}, Request#request.notifications)
			end,
			dict:store(Service, Request#request{notifications=Notifications}, Requests)
	end,	
	{noreply, State#state{requests=NRequests}};

handle_cast({delete_node, Node}, State=#state{known_nodes=KnownNodes}) ->
	NKnownNodes = lists:delete(Node, KnownNodes),
	{noreply, State#state{known_nodes=NKnownNodes}};

handle_cast({delete_notification, Service, Ref}, State=#state{requests=Requests}) ->
	NRequests = case dict:find(Service, Requests) of
		error -> Requests;
		{ok, Request} ->
			Notifications = dict:erase(Ref, Request#request.notifications),
			case dict:size(Notifications) of
				0 -> dict:erase(Service, Requests);
				_ -> dict:store(Service, Request#request{notifications=Notifications}, Requests)
			end
	end,
	{noreply, State#state{requests=NRequests}}.

%% handle_info
handle_info({run_update}, State) ->
	NState = run_update(State),
	{noreply, NState}.

%% terminate
terminate(_Reason, #state{timer=Timer}) ->
	timer:cancel(Timer),
	ok.

%% code_change
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

run_whereis_service(Service, IncludeLocal, Services, From) ->
	Fun = fun() ->
			Reply = whereis_service(Service, IncludeLocal, Services),
			gen_server:reply(From, Reply)
	end,
	spawn(Fun).

whereis_service(Service, false, Services) ->
	case dict:find(Service, Services) of
		error -> [];
		{ok, Nodes} -> Nodes
	end;
whereis_service(Service, true, Services) ->
	LocalServices = erlang:registered(),
	Local = case lists:member(Service, LocalServices) of
		true -> [node()];
		false -> []
	end,
	Remote = whereis_service(Service, false, Services),
	Local ++ Remote.

get_master_nodes() ->
	case application:get_env(?APP_NAME, ?CONFIG_MASTER_NODES) of
		undefined -> [];
		{ok, Configured} -> lists:delete(node(), Configured)
	end.

run_update(State) ->
	KnownNodes = join(nodes(), State#state.known_nodes),
	OnlineNodes = get_active_nodes(KnownNodes, []),
	Nodes = get_registered_services(OnlineNodes, dict:new()),
	Services = get_services_nodes(OnlineNodes, Nodes, dict:new()),
	OldServices = State#state.services,
	spawn(fun() -> process_notifications(OldServices, Services, State#state.requests) end),
	State#state{known_nodes=KnownNodes, nodes=Nodes, services=Services}.

join([], List) -> List;
join([Value|T], List) ->
	case lists:member(Value, List) of
		true -> join(T, List);
		false -> join(T, [Value|List])
	end.

get_active_nodes([], Nodes) -> Nodes;
get_active_nodes([Node|T], List) ->
	case net_adm:ping(Node) of
		pong -> get_active_nodes(T, [Node|List]);
		pang -> get_active_nodes(T, List)
	end.

get_registered_services([], Nodes) -> Nodes;
get_registered_services([Node|T], Nodes) -> 
	Registered = rpc:call(Node, erlang, registered, []),
	NNodes = dict:store(Node, Registered, Nodes),
	get_registered_services(T, NNodes).

get_services_nodes([], _Nodes, Services) -> Services;
get_services_nodes([Node|T], Nodes, Services) ->
	{ok, Registered} = dict:find(Node, Nodes),
	NServices = add_services(Registered, Node, Services),
	get_services_nodes(T, Nodes, NServices).

add_services([], _Node, Services) -> Services;
add_services([Service|T], Node, Services) ->
	NServices = case dict:find(Service, Services) of
		error -> dict:store(Service, [Node], Services);
		{ok, NodeList} -> dict:store(Service, [Node|NodeList], Services)
	end,
	add_services(T, Node, NServices).

process_notifications(OldServices, NewServices, Requests) ->
	Services = dict:fetch_keys(Requests),
	notifications_for_service(Services, OldServices, NewServices, Requests).

notifications_for_service([], _OldServices, _NewServices, _Requests) -> ok;
notifications_for_service([Service|T], OldServices, NewServices, Requests) -> 
	{ok, Request} = dict:find(Service, Requests),
	NotificationList = dict:to_list(Request#request.notifications),
	case dict:find(Service, OldServices) of
		error -> 			
			case dict:find(Service, NewServices) of
				error -> ok;
				{ok, NewNodes} ->
					send_notification(Service, NotificationList, NewNodes, [], new)
			end;
		{ok, OldNodes} ->
			case dict:find(Service, NewServices) of
				error -> 
					send_notification(Service, NotificationList, OldNodes, [], remove);
				{ok, NewNodes} ->
					NewList = not_in(NewNodes, [], OldNodes),
					RemoveList = not_in(OldNodes, [], NewNodes),
					send_notification(Service, NotificationList, RemoveList, [], remove),
					send_notification(Service, NotificationList, NewList, [], new)
			end
	end,
	notifications_for_service(T, OldServices, NewServices, Requests).

send_notification(_Service, [], _Nodes, _NodesDone, _Type) -> ok;
send_notification(Service, [_Notification|TN], [], NodesDone, Type) -> 
	send_notification(Service, TN, NodesDone, [], Type);
send_notification(Service, [{Ref, Notification}|TN], [Node|TD], NodesDone, Type) ->
	case Notification#notification.requested_nodes of
		?ALL_NOTIFICATION_NODES -> 
			notify(Service, Ref, Notification#notification.function, Node, Type);
		_ ->
			case lists:member(Node, Notification#notification.requested_nodes) of
				true -> notify(Service, Ref, Notification#notification.function, Node, Type);
				false -> ok
			end
	end,
	send_notification(Service, [Notification|TN], TD, [Node|NodesDone], Type).

notify(Service, Ref, Fun, Node, new) ->
	run(Fun, {new, Service, Ref, Node});
notify(Service, Ref, Fun, Node, remove) ->
	run(Fun, {remove, Service, Ref, Node}).

run(Fun, Arg) ->
	F = fun() ->
			try Fun(Arg)
			catch
				_:_ -> ok
			end
	end,
	spawn(F).

not_in([], NotInList, _RefList) -> NotInList;
not_in([Value|T], NotInList, RefList) ->
	case lists:member(Value, RefList) of
		true -> not_in(T, NotInList, RefList);
		false -> not_in(T, [Value|NotInList], RefList)
	end.

common(_List, _CommonList, []) -> [];
common([], CommonList, _RefList) -> CommonList;
common([Value|T], CommonList, RefList) ->
	case lists:member(Value, RefList) of
		true -> common(T, [Value|CommonList], RefList);
		false -> common(T, CommonList, RefList)
	end.