-module(server).

-export([start22/1, server/2, prepare_response_json/1, prepare_response1/1, response404/0, prepare_response/1]).


start22(LPort) ->
    case gen_tcp:listen(LPort, [binary, {packet, 0}, {active, false}, {reuseaddr, true}]) of
        {ok, ListenSock} ->
            start_servers(100,ListenSock),
            {ok, started};
        {error,Reason} ->
            {error,Reason}
    end.


start_servers(0,_) -> ok;
start_servers(N, Socket) ->
  
  spawn_link(?MODULE,server,[Socket, self()]), %%self is Pid of top_worker, who call the spawn_link function
  start_servers(N-1, Socket).

%% this function run in spawned new process
server(Socket, Pid) ->  

  case gen_tcp:accept(Socket) of
        {ok,S} ->
            io:format("socket connected ~p~n", [S]),
            Pid ! {socket_connected, S, self() },      
            loop(S, [], Pid),
            server(Socket, Pid);
        Other ->
            io:format("accept returned ~w - goodbye!~n",[Other]),
            ok
  end.

%% loop for data receive and state collect
loop(S, State, Pid) ->
  inet:setopts(S, [{packet, http_bin}]),
    case gen_tcp:recv(S, 0) of
        {ok, {http_request, Method, {abs_path, Path}, _} } ->
            S2 = [ {method, Method} | State],
            loop(S, [ {path, Path} | S2 ], Pid);
        {ok, {http_header,_,_, Header, Value}} ->
            io:format("Header receieved Header: ~p and Value :~p ~n", [Header, Value]),
            loop(S, [ {Header, Value} | State], Pid);    
        {ok, http_eoh} ->  %%receive end of headers
                      
            case proplists:get_value(method, State) of

            'POST' -> 
                inet:setopts(S, [{packet, raw}]), %% shift socket option to read raw body
                 L = binary_to_integer(proplists:get_value(<<"Content-Length">>, State)),
                 {ok, Payload} = gen_tcp:recv(S, L),
                 Pid ! {set_timer, self(), json:decode(Payload)};
                 %%io:format("R1 received in POST processing ~p~n", [R1]);

            'GET' ->

                 Pid ! prepare_resp(State), 
                 io:format("state after eoh: ~p~n", [State])
            end;
        {error, closed} ->
            io:format("error closed received in gen tcp recv ~n"),
            ok;

        Info ->
          io:format("some other info received: ~p~n", []),
          loop(S, State, Pid)    

            end.    


prepare_response(Body) ->
    %% Преобразуем тело в бинарный вид для точного подсчета байтов
    BinBody = iolist_to_binary(Body),
    ContentLength = byte_size(BinBody),
    
    %% Формируем HTTP-пакет
    [
        "HTTP/1.1 200 OK\r\n",
        "Content-Type: text/html\r\n",
        "Content-Length: ", integer_to_list(ContentLength), "\r\n",
        "Connection: close\r\n",
        "\r\n",
        BinBody
    ].

prepare_response1(Body) ->
    %% Преобразуем тело в бинарный вид для точного подсчета байтов
    BinBody = iolist_to_binary(Body),
    ContentLength = byte_size(BinBody),
    
    %% Формируем HTTP-пакет
    [
        "HTTP/1.1 200 OK\r\n",
        "Content-Type: application/json\r\n",
        "Content-Length: ", integer_to_list(ContentLength), "\r\n",
        "Connection: close\r\n",
        "\r\n",
        BinBody
    ].

%% Body is iolist iodata here
prepare_response_json(Body) ->
    %% Преобразуем тело в бинарный вид для точного подсчета байтов
    %%BinBody = iolist_to_binary(Body),
    %% ContentLength = byte_size(BinBody),
    ContentLength =  erlang:iolist_size(Body), 
    
    %% Формируем HTTP-пакет
    [
        "HTTP/1.1 200 OK\r\n",
        "Content-Type: application/json\r\n",
        "Content-Length: ", integer_to_list(ContentLength), "\r\n",
        "Connection: close\r\n",
        "\r\n",
        Body
    ].



response404() ->
    %% Преобразуем тело в бинарный вид для точного подсчета байтов
    %%BinBody = iolist_to_binary(Body),
    %%ContentLength = byte_size(BinBody),
    
    %% Формируем HTTP-пакет
    [
        "HTTP/1.1 404 OK\r\n",
        "Content-Type: text/html\r\n",
        %%"Content-Length: ", integer_to_list(ContentLength), "\r\n",
        "Connection: close\r\n",
        "\r\n"
        %%BinBody
    ].    

prepare_resp(Data) -> 
   case lists:keyfind(method, 1, Data) of
    {_, 'GET'} ->
         process_get(Data);

        %%io:format("Need to process GET method: ~p~n", [Data]);
    {_, 'POST'} -> ok;
          
    false -> 
        io:format("Параметр не найден~n")
end.

process_get(Data) ->

case lists:keyfind(path, 1, Data) of
    {_, Path} ->
         Components = uri_string:parse(Path),

        %% #{path => <<"/assembled-products">>,
        %%    query => <<"start_date=2025-01-01&end_date=2025-12-31">>}

         case maps:find(path, Components) of
           {ok, <<"/assembled-products">>}  -> 
              
              case maps:find(query, Components) of
                {ok, Qs} -> 
                   QueryList = uri_string:dissect_query(Qs),
                   Query = maps:from_list(QueryList),
                   handle2(Query);
                error ->
                    {ok, self(), return_assembled_products}
               end;
    

           {ok, _V} -> {error, self(), illegal_path};    
           error -> 
                    
                    {error, self(), illegal_path}
        end;

                    
    false -> 
        io:format("Параметр не найден~n"),
        {error, path_not_found}
end.

%% все собранные товары http://localhost/assembled-products
%% http://localhost/assembled-products?start_date=2025-01-01&end_date=2025-12-31
%% http://localhost/assembled-products?start_date=2025-01-01&end_date=2025-12-31&id=194
%% http://localhost/assembled-products?id=194

%% function to prepare response that will be send to top_worker process

handle2( #{<<"start_date">> := Start, <<"end_date">> := End, <<"id">> := Id}) ->
io:format("received params Start: ~p End ~p Id ~p ~n", [Start, End, Id]),
{ok, self(), return_assembled_products1, #{start_date => Start , end_date => End, id => Id}};

handle2( #{<<"start_date">> := Start, <<"end_date">> := End} ) ->
io:format("received params no Id, Start ~p End ~p ~n", [Start, End]),
{ok, self(), return_assembled_products2, #{start_date => Start , end_date => End}};

handle2( #{<<"id">> := Id} ) ->
io:format("received params Id only ~p ~n", [Id]),
{ok, self(), return_assembled_products3, #{id => Id}};

handle2(_) ->
  io:format("some illegal qs data ~n"),
  {error, self(), illegal_data_in_qs}.