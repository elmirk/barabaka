-module(api_client).
-export([get_ets/0, test2/0, test/0, fetch_data/2]).

fetch_data(TS, Pflag) -> get_data(Pflag, TS, 0, 0, -1).

get_data(_Pflag, _TS,_, Total, Total) -> Total;
get_data(Pflag, TS, Skip, Processed, Total) ->
    
    %%application:ensure_all_started(hackney), uncomment when no release
    SkipBin = integer_to_binary(Skip),
    Url = <<"https://dummyjson.com/products?skip=",SkipBin/binary,"&select=id,price">>,
    Method = get,
    Headers = [{<<"Content-Type">>, <<"application/json">>}],
    Payload = <<>>,

     Options = 
        if Pflag ->
          [{proxy, {socks5, "127.0.0.1", 1080}},
            {with_body, true}];

          true ->
             [{with_body, true}]

          end,

    case hackney:request(Method, Url, Headers, Payload, Options) of
        {ok, StatusCode, _RespHeaders, Body} ->
            %%io:format("RESP headers ~p~n", [_RespHeaders]),
            {ok, Limit, Num, Total1} = handle_response(TS, StatusCode, Body),
            get_data(Pflag, TS, Skip + Limit, Processed + Num, Total1);

        {error, Reason} ->
            {error, Reason}
    end.

handle_response(TS, 200, Body) ->
 
    Data = json:decode(Body),
    Total = maps:get(<<"total">>, Data),
    Skip = maps:get(<<"skip">>, Data),
    Limit = maps:get(<<"limit">>, Data),
    Products = maps:get(<<"products">>, Data),

    %%io:format("type of data ~p~n", [is_map(Data)]),
    %%io:format("Total data ~p~n", [Total]),
    %%io:format("Skip data ~p~n", [Skip]),
    %%io:format("Limit data ~p~n", [Limit]),
    %%io:format("Products length ~p~n", [length(Products)]),
    %%io:format("Proudcts ~p~n", [Products]),

    lists:foreach(fun(#{<<"id">> := Id,<<"price">> := Price}) -> 
                                          ets:insert(products, {{TS, Id}, Price})
                      end, Products),

    {ok, Limit, length(Products), Total};

handle_response(_, Status, _Body) ->
    {error, {bad_status, Status}}.


%% ниже идет тестовый код, который просто испольовался при написании решения, как песочника, можно  не смотреть


 test() ->

 URL = <<"https://dummyjson.com/products">>,

 {ok, Ref} = hackney:get(URL, [], <<>>, [async]),

stream_loop(Ref).

stream_loop(Ref) ->
    receive
        {hackney_response, Ref, {status, Status, _}} ->
            io:format("Status: ~p~n", [Status]),
            stream_loop(Ref);
        {hackney_response, Ref, {headers, Headers}} ->
            io:format("Headers: ~p~n", [Headers]),
            stream_loop(Ref);
        {hackney_response, Ref, done} ->
            ok;
        {hackney_response, Ref, Chunk} when is_binary(Chunk) ->
            process_chunk(Chunk),
            stream_loop(Ref)
    end.   


 process_chunk(Data) ->
 io:format("Chunk byte size is ~p~n", [byte_size(Data)]).   


 test2() ->


  Url = <<"https://dummyjson.com/products">>,
    Method = get,
    Headers = [{<<"Content-Type">>, <<"application/json">>}],
    Payload = <<>>,
    Options = [async, {async, once}], %% hackney сам вычитает тело и вернет его
%%Options = []

    {ok, Ref} =  hackney:request(Method, Url, Headers, Payload, Options),

    %%io:format("R after hackney req: ~p~n", [R]),

 Rez = hackney:stream_next(Ref),
 io:format("Rez = ~p~n", [Rez]),

 receive
        DataR ->
            io:format("Message from mailbox: ~p~n", [DataR])
      %%      receive_loop(Ref);
      %%  {hackney_response, Ref, {headers, Headers}} ->
      %%      io:format("Headers: ~p~n", [Headers]),
      %%      receive_loop(Ref);
      %%  {hackney_response, Ref, Chunk} when is_binary(Chunk) ->
      %%      io:format("Got chunk: ~p bytes~n", [byte_size(Chunk)]),
      %%      receive_loop(Ref);
      %%  {hackney_response, Ref, done} ->
      %%      ok
    end,

receive
        DataR1 ->
            io:format("Message from mailbox: ~p~n", [DataR1])
      %%      receive_loop(Ref);
      %%  {hackney_response, Ref, {headers, Headers}} ->
      %%      io:format("Headers: ~p~n", [Headers]),
      %%      receive_loop(Ref);
      %%  {hackney_response, Ref, Chunk} when is_binary(Chunk) ->
      %%      io:format("Got chunk: ~p bytes~n", [byte_size(Chunk)]),
      %%      receive_loop(Ref);
      %%  {hackney_response, Ref, done} ->
      %%      ok
    end,

receive
        DataR2 ->
            io:format("Message from mailbox: ~p~n", [DataR2])
      %%      receive_loop(Ref);
      %%  {hackney_response, Ref, {headers, Headers}} ->
      %%      io:format("Headers: ~p~n", [Headers]),
      %%      receive_loop(Ref);
      %%  {hackney_response, Ref, Chunk} when is_binary(Chunk) ->
      %%      io:format("Got chunk: ~p bytes~n", [byte_size(Chunk)]),
      %%      receive_loop(Ref);
      %%  {hackney_response, Ref, done} ->
      %%      ok
    end.

get_ets() ->

   MS1 = [{{  '$1'  },
           [],
           ['$1']}], % Возвращает список вида [[TS, ID, Price], ...]

Rows1 = ets:select(timestamps, MS1),

io:format("timestamps rows ~p~n", [Rows1]),  

%%MS = [{{ {'$1', '$2'}, '$3' },
%%           [],
%%           ['$$']}], % Возвращает список вида [[TS, ID, Price], ...]

lists:map(fun(TS) -> 

                   MS = [{{ {'$1', '$2'}, '$3' },
           [{'==', '$1', TS}],
           [#{id => '$2', price => '$3'}]}], % Возвращает список вида [[TS, ID, Price], ...]

            Rows = ets:select(products, MS),

            Date = list_to_binary(calendar:system_time_to_rfc3339(TS, [{offset, "Z"}])),

            json:encode (#{ Date => Rows})

                  end, Rows1).

    % Match Specification:
    % {{ {Timestamp, ProductId}, Price }}
    %MS = [{{ {'$1', '$2'}, '$3' },
     %      [],
     %      ['$$']}], % Возвращает список вида [[TS, ID, Price], ...]
    
    %Rows = ets:select(products, MS),

    %%#{
        %%time => TS, % время самого запроса
    %    Out = [ #{ts => TS, id => ID, price => Price} || [TS, ID, Price] <- Rows ].

        %%lists:foldl(fun(#{ts := TS, id := Id, price := Price}, {Keys, Acc}) -> 
        
       %% Elem = #{"id" => Id, "price" => Price}
        
       %% X + Sum end, {[], []}, Out).
%%15
    %%}.
