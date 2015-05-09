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

-module(columbo_util).

%% ====================================================================
%% API functions
%% ====================================================================
-export([add_if_not_member/2,
	common/2]).

add_if_not_member([Value|T], List) ->
	case lists:member(Value, List) of
		true -> add_if_not_member(T, List);
		false -> add_if_not_member(T, [Value|List])
	end;
add_if_not_member([], List) -> List.

common(_List1, []) -> [];
common([], _List2) -> [];
common(List1, List2) -> 
	lists:filter(fun(Elem) -> 
				lists:member(Elem, List2) 
		end, List1).

%% ====================================================================
%% Internal functions
%% ====================================================================


