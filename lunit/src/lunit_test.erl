-module(lunit_test).

-export([do/0]).


make_ts() ->
	{M, S, U} = now(),
	M * 1000000 * 1000000 + S * 1000000 + U.

do_insert(_HP, Val) when Val < 5 ->
	ok;

do_insert(HP, Val) when Val >= 5 ->
	gen_server:call(HP, {insert, Val, make_ts()}),
	do_insert(HP, Val - 1).


do_remove(_HP, Val) when Val < 5 ->
	ok;

do_remove(HP, Val) when Val >= 5 ->
	gen_server:call(HP, {remove, Val}),
	do_remove(HP, Val - 1).


do() ->
	{ok, SVpid} = lunit_sv:start_link(),
	{ok, HP} = gen_server:call(SVpid, get_handler_pid),
	
	do_insert(HP, 100),
	
	%io:format("~p~n", [gen_server:call(HP, get_all)]).

	do_remove(HP, 80),

	io:format("~p~n", [gen_server:call(HP, get_all)]).


