%%%-------------------------------------------------------------------
%% @doc barabaka public API
%% @end
%%%-------------------------------------------------------------------

-module(barabaka_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    barabaka_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
