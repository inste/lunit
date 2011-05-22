-module(lunit_lib_tests).

-include_lib("eunit/include/eunit.hrl").

list_pos_test() ->
	?assertEqual(lunit_lib:list_pos([], 1), -1),
	?assertEqual(lunit_lib:list_pos([1,2,3], 4), -1),
	?assertEqual(lunit_lib:list_pos([1], 1), 1),
	?assertEqual(lunit_lib:list_pos([1,2,3], 3), 3). 

insert_at_test() ->
    % Test on abstract datatype
    ?assertEqual(lunit_lib:insert_at([], 10, 0), [10]),
    ?assertEqual(lunit_lib:insert_at([10], 11, 0), [11, 10]),
    ?assertEqual(lunit_lib:insert_at([10], 11, 1), [10, 11]),
    ?assertEqual(lunit_lib:insert_at([10, 11, 12], 13, 3), [10, 11, 12, 13]),
    ?assertEqual(lunit_lib:insert_at([10, 11, 12], 13, 0), [13, 10, 11, 12]),
    ?assertEqual(lunit_lib:insert_at([10, 11, 12], 13, 1), [10, 13, 11, 12]).

fifo_push_test() ->
	?assertEqual(lunit_lib:fifo_push([], 1), [1]),
	?assertEqual(lunit_lib:fifo_push([1, 2, 3], 1), [1, 1, 2, 3]).

fifo_last_test() ->
	?assertEqual(lunit_lib:fifo_last([]), null),
	?assertEqual(lunit_lib:fifo_last([1, 2, 3]), 3).

fifo_del_last_test() ->
	?assertEqual(lunit_lib:fifo_del_last([]), []),
	?assertEqual(lunit_lib:fifo_del_last([1]), []),
	?assertEqual(lunit_lib:fifo_del_last([1, 2]), [1]),
	?assertEqual(lunit_lib:fifo_del_last([1, 2, 3, 4]), [1, 2, 3]),
	?assertEqual(lunit_lib:fifo_del_last([{ins, 1,2}, {ins, 2,3}]), [{ins, 1, 2}]),
	?assertEqual(lunit_lib:fifo_del_last([{ins, 2, 3}]), []).

