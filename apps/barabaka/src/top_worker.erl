-module(top_worker).
-behaviour(gen_server).

%% API)
-export([start_link/0, get_status/0]).

%% gen_server callbacks
-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).

-export([get_assembled_products2/2]).

-define(SERVER, ?MODULE).
-define(TIMER, 15000).
-define(PORT, 8886).

-include_lib("stdlib/include/ms_transform.hrl").

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

get_status() ->
    gen_server:call(?SERVER, get_status).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% start timer and listening server, create ets tables
init([]) ->
    
  ets:new(products, [ordered_set, public, named_table, {keypos, 1}]),
  ets:new(timestamps, [set, public, named_table, {keypos, 1}]),

  TRef = erlang:start_timer(?TIMER, self(), timer_tick_msg),

  self() ! start_server,

  ProxyFlag = case os:getenv("WITH_PROXY") of

    false ->

      false;

    _V ->

      true

  end,

  {ok, #{proxy_flag => ProxyFlag, tref => TRef, timer_int => ?TIMER, total => 0}}.


handle_call(get_status, _From, State) ->
  {reply, {ok, State}, State};

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.


handle_info( start_server, State) ->

  Rez = server:start22(?PORT),
  io:format("server start22 rezult ~p~n", [Rez]),
  io:format("here i in process ~p  should start server to listen ~n", [self()]),

  {noreply, State};


handle_info({socket_connected, Socket, _Pid}, State) ->
  {noreply, State#{ socket => Socket}};


%% return all data, no filters
handle_info({ok, _Pid, return_assembled_products}, #{socket := Socket} = State) ->
  
  OutData = get_assembled_products(),
  gen_tcp:send(Socket, server:prepare_response_json(OutData)),

  {noreply, State};

%% return data by filter - start, end and product_id
%% Start, End, Id -- integers

handle_info({ok, _Pid, return_assembled_products1, #{start_date := Start , end_date := End, id := Id}},  #{socket := Socket} = State) ->

StartTS = calendar:rfc3339_to_system_time(Start, [{unit, second}]),
EndTS = calendar:rfc3339_to_system_time(End, [{unit, second}]),

OutData = get_assembled_products1(StartTS, EndTS, binary_to_integer(Id)),
  
gen_tcp:send(Socket, server:prepare_response_json(OutData)),

{noreply, State};

handle_info({error, _Pid, _Error}, #{socket := Socket} = State ) ->

  gen_tcp:send(Socket, server:response404()),
  {noreply, State};

handle_info({error, _Pid, illegal_data_in_qs}, #{socket := Socket} = State) ->

  gen_tcp:send(Socket, server:prepare_response("<h1>Illegal Data in QS</h1>")),
  {noreply, State};

%% return products from start_date to end_date
%% data examples
%%
%% Start <<"2026-04-16T05:41:18Z">> End <<"2026-04-16T05:45:18Z">>

handle_info({ok, _Pid, return_assembled_products2, #{start_date := Start , end_date := End}}, #{socket := Socket} = State) ->

  StartTS = calendar:rfc3339_to_system_time(Start, [{unit, second}]),
  EndTS = calendar:rfc3339_to_system_time(End, [{unit, second}]),
   
  OutData = get_assembled_products2(StartTS, EndTS),
  
  gen_tcp:send(Socket, server:prepare_response_json(OutData)),

  {noreply, State};  

%% return products with specified id
handle_info({ok, _Pid, return_assembled_products3, #{id := Id}}, #{socket := Socket} = State) ->

  OutData = get_assembled_products3(binary_to_integer(Id)),
  
  gen_tcp:send(Socket, server:prepare_response_json(OutData)),

  %%gen_tcp:send(Socket, server:prepare_response1(json:encode(Result))),
  
  {noreply, State};

handle_info({set_timer, _Pid, #{<<"cmd">> := <<"set_timer">>,<<"value">> := T} = Data }, #{tref := Tref, socket := Socket} = State) ->

  io:format("set timer command received, new Timer value is: ~p~n", [T]),
  erlang:cancel_timer(Tref),
  NewTRef = erlang:start_timer(T, self(), timer_tick_msg),
  Result = #{result => ok, value => T},
  gen_tcp:send(Socket, server:prepare_response1(json:encode(Result))),

  {noreply, State#{tref => NewTRef, timer_int => T}};

%% timer ticks handling
handle_info({timeout, Tref, timer_tick_msg}, #{proxy_flag := Pflag, timer_int := Tint, total := Total}= State) ->

  %% NewTRef = erlang:start_timer(Tint, self(), timer_tick_msg),
  
    FetchTS = erlang:system_time(second),
    io:format("TIMESTAMP when insert to TS table: ~p~n", [FetchTS]),
    ets:insert(timestamps, {FetchTS}),

  case api_client:fetch_data(FetchTS, Pflag) of
    {error,econnrefused} ->  %% something wrong with connection to Endpoint
      io:format("failed to fetch data from endpoint, timer removed! ~n"),
      %% remove timer, and stop fetching, and set status = stopped
      erlang:cancel_timer(Tref),
      {noreply, State#{status => stopped, tref => void}};
    ProdsNum ->
       NewTRef = erlang:start_timer(Tint, self(), timer_tick_msg),
      {noreply, State#{tref => NewTRef, status => running, total => Total + ProdsNum}}
  end;

 
handle_info(_Info, State) ->
  io:format("Info received in handle_info in top_worker ~p~n", [_Info]),
  {noreply, State}.

terminate(_Reason, State) ->
    %%gen_tcp:close(Socket),  maybe need close socket?
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Utils
%%%===================================================================


%% get all products from products table and create final json to send to client
%% TEST: curl -i 'http://localhost:8886/assembled-products

get_assembled_products() ->

   MS1 = [{{  '$1'  }, [], ['$1']}], % list of all timestamps
   Rows1 = ets:select(timestamps, MS1),
   lists:map(fun(TS) -> 

               MS = [{{ {'$1', '$2'}, '$3' }, [{'==', '$1', TS}],  [#{id => '$2', price => '$3'}]}], % Возвращает список вида [[TS, ID, Price], ...]

               Rows = ets:select(products, MS),

               Date = list_to_binary(calendar:system_time_to_rfc3339(TS, [{offset, "Z"}])),

                json:encode (#{ Date => Rows})

              end, Rows1).    

%% get all products from products from StartTS to EndTS table and create final json to send to client
get_assembled_products2(Start, End) ->

  MS1 = ets:fun2ms(fun({TS}) when TS >= Start, TS =< End -> 
                                TS
                           end),
  Rows1 = ets:select(timestamps, MS1),

  lists:map(fun(TS) -> 

               MS2 = [{{ {'$1', '$2'}, '$3' }, [{'==', '$1', TS}],  [#{id => '$2', price => '$3'}]}],
               Rows = ets:select(products, MS2),

               Date = list_to_binary(calendar:system_time_to_rfc3339(TS, [{offset, "Z"}])),
               json:encode (#{ Date => Rows})

              end, Rows1).

%% get products with specified id
get_assembled_products3(QId) ->

  MS1 = ets:fun2ms(fun({ {TS, Id}, Price  }) when Id == QId -> 
                                { TS, #{id =>Id, price =>Price} }
                           end),


  Rows = ets:select(products, MS1),

  lists:map(fun({TS, Map}) -> 

                     Date = list_to_binary(calendar:system_time_to_rfc3339(TS, [{offset, "Z"}])),
                     json:encode (#{ Date => Map})
             end, Rows).

%% get products by filter - start, end and ID
get_assembled_products1(StartTS, EndTS, QId) ->

  MS1 = ets:fun2ms(fun({ {TS, Id}, Price  }) when TS >= StartTS, TS =< EndTS, Id == QId  -> 
                                { TS, #{id =>Id, price =>Price} }
                           end),
  Rows = ets:select(products, MS1),

  lists:map(fun({TS, Map}) -> 

                Date = list_to_binary(calendar:system_time_to_rfc3339(TS, [{offset, "Z"}])),
                json:encode (#{ Date => Map})
              end, Rows).

%% curl -i 'http://localhost:8886/assembled-products'
%% curl -i 'http://localhost:8886/assembled-products?id=123'    curl to get ids 
%% curl -i 'http://localhost:8886/assembled-products?start_date=2026-04-16T05:41:18Z&end_date=2026-04-16T05:45:18Z'
%% curl -i 'http://localhost:8886/assembled-products?start_date=2026-04-16T07:00:18Z&end_date=2026-04-16T07:02:18Z&id=123'
%% curl -X POST http://localhost:8886/assembled-products -H "Content-Type: application/json" -d '{"cmd": "set_timer", "value": 100}'

%% curl -X POST http://localhost:8886/assembled-products      -H "Content-Type: application/json"      -d '{"cmd": "set_timer", "value": 30000}'
