%%
%% Copyright 2015 Joaquim Rocha <jrocha@gmailbox.org>
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

-module(columbo_service).

-define(SERVICE_TABLE, columbo_service_ets).

%% ====================================================================
%% API functions
%% ====================================================================
-export([create/0, drop/0]).
-export([find/1, fold/2]).
-export([store/1]).

create() ->
	Options = [set, public, named_table, {read_concurrency, true}],
	ets:new(?SERVICE_TABLE, Options).

drop() ->
	ets:delete(?SERVICE_TABLE).

find(Service) ->
	case ets:lookup(?SERVICE_TABLE, Service) of
		[] -> [];
		[{_, Nodes}] -> Nodes
	end.

fold(Fun, Data) ->
	ets:foldl(Fun, Data, ?SERVICE_TABLE).

store(List) ->
	ets:insert(?SERVICE_TABLE, List).

%% ====================================================================
%% Internal functions
%% ====================================================================


