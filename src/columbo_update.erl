%%
%% Copyright 2015-16 Joaquim Rocha <jrocha@gmailbox.org>
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

-module(columbo_update).

%% ====================================================================
%% API functions
%% ====================================================================
-export([run/2]).

run(CurrentKnownNodes, CurrentOnlineNodes) ->
	KnownNodes = columbo_util:add_if_not_member(nodes(), CurrentKnownNodes),
	OnlineNodes = get_active_nodes(KnownNodes),
	monitor_nodes(OnlineNodes, CurrentOnlineNodes),
	NodeData = get_node_data(OnlineNodes),
	ServiceData = get_service_data(OnlineNodes, NodeData),
	AllServices = get_all_services(ServiceData),
	columbo_service:store(dict:to_list(AllServices)),
	{KnownNodes, OnlineNodes}.

%% ====================================================================
%% Internal functions
%% ====================================================================

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
	columbo_service:fold(Fun, ServiceData).

