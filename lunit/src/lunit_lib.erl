%%% ---------------------------------------------------------------------------
%%% LUnit library module
%%% Version 0.1
%%% INSTE, 2011
%%% ---------------------------------------------------------------------------

-module(lunit_lib).
-vsn("0.1").
-export([insert_at/3, list_pos/2]).
-export([fifo_init/0, fifo_push/2, fifo_last/1, fifo_del_last/1]).
-export([log/5]).

%% ----------------------------------------------------------------------------
%% Logging events
%% (list, list, list, list, list) -> ok
%% ----------------------------------------------------------------------------
log(Module, Function, Type, Reason, Dump) ->
	io:format(" --> [~s] by ~s@~s: ~s (Dump: ~p)~n",
		[Type, Function, Module, Reason, Dump]).


insert_at([], _, CurrentPos, Acc) when CurrentPos < 0 ->
    lists:reverse(Acc);

insert_at([], Record, CurrentPos, Acc) when CurrentPos >= 0 ->
    lists:reverse([Record | Acc]);

insert_at([H | T], Record, CurrentPos, Acc) when CurrentPos > 0 ->
    insert_at(T, Record, CurrentPos - 1, [H | Acc]);

insert_at(List, Record, CurrentPos, Acc) when CurrentPos == 0 ->
    insert_at([], Record, CurrentPos - 1, lists:reverse(List) ++ [Record] ++ Acc).

insert_at(Cache, Record, Pos) ->
    insert_at(Cache, Record, Pos, []). 



list_pos([], _, _) ->
	-1;

list_pos([Item | _Tail], Item, Pos) ->
	Pos;

list_pos([_ | Tail], Item, Pos) ->
	list_pos(Tail, Item, Pos + 1).

list_pos(List, Item) ->
	list_pos(List, Item, 1).



fifo_init() ->
	[].

fifo_push(List, Item) ->
	[Item | List].

fifo_last([]) ->
	null;

fifo_last([H | T]) ->
	lists:last([H | T]).

fifo_del_last([], Acc) ->
	lists:reverse(Acc);

fifo_del_last([_], Acc) ->
	lists:reverse(Acc);

fifo_del_last([Head | Tail], Acc) ->
	fifo_del_last(Tail, [Head | Acc]).

fifo_del_last(List) ->
	fifo_del_last(List, []).

