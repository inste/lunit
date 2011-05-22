-module(lunit_hibari_dump).

-export([dump/0]).

-include("lunit.hrl").
-include("lunit_hibari.hrl").

dump(Index, Pid) when (Index == 1) and is_integer(Index) ->
	ok;

dump(Index, Pid) when (Index > 1) and is_integer(Index) ->
	{reply, {ok, _TS, Res}, none} = ubf_client:rpc(Pid, {get, ?HIBARI_ARC_TABLE,
		lunit_hibari:gen_key_a("idx" ++ integer_to_list(Index)),
		[], ?HIBARI_OP_TIMEOUT}),
	io:format("~p : ~p~n", [Index, binary_to_term(Res)]),
	dump(Index - 1, Pid);

dump([], Pid) ->
	ok;

dump([Head | Tail], Pid) when is_integer(Head) ->
	{reply, {ok, _TS, Res}, none} = ubf_client:rpc(Pid, {get, ?HIBARI_TABLE,
		lunit_hibari:gen_key(integer_to_list(Head)), [], ?HIBARI_OP_TIMEOUT}),
	io:format("~p : ~p~n", [Head, binary_to_term(Res)]),
	dump(Tail, Pid).

dump() ->
        {ok, Pid, _} = ubf_client:connect(?HIBARI_HOSTNAME,
				?HIBARI_PORT,
				[{proto, ?HIBARI_PROTO}],
				?HIBARI_TIMEOUT),
	ubf_client:rpc(Pid, {startSession, {'#S', ?HIBARI_GDSS}, []}),
	{reply, {ok, _TS, Answer}, none} =
		ubf_client:rpc(Pid, {get, ?HIBARI_ARC_TABLE, lunit_hibari:gen_key_a("index"), [], ?HIBARI_OP_TIMEOUT}),
	Index = binary_to_term(Answer),
	io:format("Archive data fiber (~p) contains:~n", [?HIBARI_ARC_TABLE]),
	dump(Index, Pid),
	
	io:format("Working data fiber (~p) contains:~n", [?HIBARI_TABLE]),
	{reply, {ok, _TTS, Ans}, none} = 
		ubf_client:rpc(Pid, {get, ?HIBARI_TABLE, lunit_hibari:gen_key("index"), [], ?HIBARI_OP_TIMEOUT}),
	io:format("~p~n", [binary_to_term(Ans)]),
	dump(binary_to_term(Ans), Pid),
	

	ubf_client:stop(Pid).


