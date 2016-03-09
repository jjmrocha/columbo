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

-module(columbo_notify).

-include("columbo_events.hrl").

-define(NOTIFY_TABLE, columbo_notify_ets).

-define(RECORD(Service, Nodes), {Service, Nodes}).

%% ====================================================================
%% API functions
%% ====================================================================
-export([create/0, drop/0]).
-export([notify/1]).

create() ->
	Options = [set, public, named_table],
	ets:new(?NOTIFY_TABLE, Options).

drop() ->
	ets:delete(?NOTIFY_TABLE).

notify(Services) ->
	async:run(fun run_notify/1, [Services]).

%% ====================================================================
%% Internal functions
%% ====================================================================

run_notify(Services) ->
	ets:foldl(fun(?RECORD(Service, Nodes), Acc) -> 
				case dict:find(Service, Services) of
					error ->
						if length(Nodes) > 0 ->
								ets:insert(?NOTIFY_TABLE, ?RECORD(Service, [])),
								Count = notify(Service, fun down_node/2, Nodes),
								Acc + Count;
							true -> Acc
						end;
					{ok, ActiveNodes} ->
						Down = not_in(Nodes, ActiveNodes),
						New = not_in(ActiveNodes, Nodes),
						if length(Down) > 0 orelse length(New) > 0 ->
								ets:insert(?NOTIFY_TABLE, ?RECORD(Service, ActiveNodes)),
								CountNew = notify(Service, fun new_node/2, New),
								CountDown = notify(Service, fun down_node/2, Down),
								Acc + CountNew + CountDown;
							true -> Acc
						end
				end
		end, 0, ?NOTIFY_TABLE).

not_in([], _List2) -> [];
not_in(List1, []) -> List1;
not_in(List1, List2) ->
	lists:filter(fun(E) -> 
				not lists:member(E, List2) 
		end, List1).

notify(_Service, _Fun, []) -> 0;
notify(Service, Fun, Nodes) ->
	lists:foldl(fun(Node, C) ->
				Fun(Service, Node),
				C + 1
		end, 0, Nodes).

new_node(Service, Node) -> send_event(?COLUMBO_NEW_NODE, Service, Node).

down_node(Service, Node) -> send_event(?COLUMBO_DOWN_NODE, Service, Node).

send_event(EventName, Service, Node) ->
	Info = #{?COLUMBO_EVENT_PROP_NODE => Node},
	event_broker:publish(EventName, Service, Info).
