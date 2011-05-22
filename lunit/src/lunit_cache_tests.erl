-module(lunit_cache_tests).

-include_lib("eunit/include/eunit.hrl").
-include("lunit.hrl").

cache_insert_at_test() ->
    %Test on drvid
    ?assertEqual(lunit_cache:cache_insert_at([], #pqr{drvid = 337, ts = 2}, 0),
					     [#pqr{drvid = 337, ts = 2}]),
    ?assertEqual(lunit_cache:cache_insert_at([#pqr{drvid = 34, ts = 2},
					      #pqr{drvid = 10, ts = 6},
					      #pqr{drvid = 16, ts = 10}],
					      #pqr{drvid = 14, ts = 3}, 1),
					      [ #pqr{drvid = 34, ts = 2},
					        #pqr{drvid = 14, ts = 3},
					        #pqr{drvid = 10, ts = 6},
					        #pqr{drvid = 16, ts = 10} ]).

cache_insert_at_the_end_test() ->
    % Abstract DT
    ?assertEqual(lunit_cache:cache_insert_at_the_end([], 10), [10]),
    ?assertEqual(lunit_cache:cache_insert_at_the_end([10], 11), [10, 11]),
    ?assertEqual(lunit_cache:cache_insert_at_the_end([10, 11, 12], 13), [10, 11, 12, 13]),

    % drvid
    ?assertEqual(lunit_cache:cache_insert_at_the_end([#pqr{drvid = 5, ts = 6}],
						      #pqr{drvid = 6, ts = 7}),
						     [#pqr{drvid = 5, ts = 6},
						      #pqr{drvid = 6, ts = 7}]).


cache_get_pos_test() ->
    CacheThree = [#pqr{drvid = 15}, #pqr{drvid = 34}, #pqr{drvid = 5}],
    ?assertEqual(lunit_cache:cache_get_pos([], 15), -1),
    ?assertEqual(lunit_cache:cache_get_pos([#pqr{drvid = 15}], 16), -1),
    ?assertEqual(lunit_cache:cache_get_pos([#pqr{drvid = 15}], 15), 1),
    ?assertEqual(lunit_cache:cache_get_pos(CacheThree, 35), -1),
    ?assertEqual(lunit_cache:cache_get_pos(CacheThree, 15), 1),
    ?assertEqual(lunit_cache:cache_get_pos(CacheThree, 5), 3).

cache_remove_test() ->
	CacheThree = [#pqr{drvid = 15}, #pqr{drvid = 34}, #pqr{drvid = 5}],
	?assertEqual(lunit_cache:cache_remove([], 15), []),
	?assertEqual(lunit_cache:cache_remove([#pqr{drvid = 16}], 15),
						[#pqr{drvid = 16}]),
	?assertEqual(lunit_cache:cache_remove([#pqr{drvid = 16}], 16),
						[]),
	?assertEqual(lunit_cache:cache_remove(CacheThree, 34),
			[#pqr{drvid = 15}, #pqr{drvid = 5}]),
	?assertEqual(lunit_cache:cache_remove(CacheThree, 15),
			[#pqr{drvid = 34}, #pqr{drvid = 5}]).


cache_get_all_test() ->
	CacheThree = [#pqr{drvid = 15, ts = 5}, #pqr{drvid = 34, ts = 7},
		      #pqr{drvid = 5, ts = 10}],
	?assertEqual(lunit_cache:cache_get_all([]), []),
	?assertEqual(lunit_cache:cache_get_all([#pqr{drvid = 15, ts = 2}]), [{15, 2}]),
	?assertEqual(lunit_cache:cache_get_all(CacheThree), [{15, 5}, {34, 7}, {5, 10}]).

cache_get_test() ->
	CacheThree = [#pqr{drvid = 1, ts = 2}, #pqr{drvid = 3, ts = 4}, 
		      #pqr{drvid = 5, ts = 6}],
	?assertEqual(lunit_cache:cache_get([], 15), null),
	?assertEqual(lunit_cache:cache_get([#pqr{drvid = 2, ts = 3}], 4), null),
	?assertEqual(lunit_cache:cache_get([#pqr{drvid = 2, ts = 3}], 2), #pqr{drvid = 2, ts = 3}),
	?assertEqual(lunit_cache:cache_get(CacheThree, 5), #pqr{drvid = 5, ts = 6}).

handle_call_test() ->
	?assertEqual(lunit_cache:handle_call({insert, 5, 15}, self(), []), {reply, ok, [#pqr{drvid = 5, ts = 15}]}),
	?assertEqual(lunit_cache:handle_call({insert, 6, 15}, self(), [#pqr{drvid = 5, ts = 6}]),
				{reply, ok, [#pqr{drvid = 5, ts = 6}, #pqr{drvid = 6, ts = 15}]}),
	?assertEqual(lunit_cache:handle_call({insert_at, 1, 10, 12}, self(), [#pqr{drvid = 2, ts = 4}]),
				{reply, ok, [#pqr{drvid = 10, ts = 12}, #pqr{drvid = 2, ts = 4}]}),
	?assertEqual(lunit_cache:handle_call({insert_at, 2, 5, 6}, self(), [#pqr{drvid = 2, ts = 4},
				#pqr{drvid = 4, ts = 6}, #pqr{drvid = 6, ts = 8}]),
				{reply, ok, [#pqr{drvid = 2, ts = 4}, #pqr{drvid = 5, ts = 6},
					     #pqr{drvid = 4, ts = 6}, #pqr{drvid = 6, ts = 8}]}),
	?assertEqual(lunit_cache:handle_call({insert_at, 0, 1, 2}, self(), []),
				{reply, {error, wrong_id}, []}),
	
	?assertEqual(lunit_cache:handle_call({get_pos, -2}, self(), []),
				{reply, {error, wrong_id}, []}),
	?assertEqual(lunit_cache:handle_call({get_pos, 44}, self(), []),
				{reply, {error, wrong_id}, []}),
	?assertEqual(lunit_cache:handle_call({get_pos, 35}, self(), [#pqr{drvid = 5, ts = 1},
						#pqr{drvid = 35, ts = 1}]),
				{reply, {ok, {pos, 2}}, [#pqr{drvid = 5, ts = 1},
						#pqr{drvid = 35, ts = 1}]}),

	?assertEqual(lunit_cache:handle_call({remove, 33}, self(), []), {reply, ok, []}),
	?assertEqual(lunit_cache:handle_call({remove, 33}, self(), [#pqr{drvid = 15}, #pqr{drvid = 33},
						#pqr{drvid = 16}]),
				{reply, ok, [#pqr{drvid = 15}, #pqr{drvid = 16}]}),
	?assertEqual(lunit_cache:handle_call({remove, -2}, self(), [#pqr{drvid = 2}, #pqr{drvid = 3}]),
				{reply, ok, [#pqr{drvid = 2}, #pqr{drvid = 3}]}),
	
	?assertEqual(lunit_cache:handle_call(get_all, self(), []), {reply, {ok, []}, []}),
	?assertEqual(lunit_cache:handle_call(get_all, self(), [#pqr{ts = 34, drvid = 5}, #pqr{ts = 2, drvid = 3}]),
				{reply, {ok, [{5, 34}, {3, 2}]}, [#pqr{ts = 34, drvid = 5}, 
								#pqr{ts = 2, drvid = 3}]}),
	?assertEqual(lunit_cache:handle_call({get, 1}, self(), [#pqr{drvid = 1, ts = 2}]),
				{reply, #pqr{drvid = 1, ts = 2}, [#pqr{drvid = 1, ts = 2}]}),
	?assertEqual(lunit_cache:handle_call({get, 1}, self(), [#pqr{drvid = 2, ts = 3}]),
				{reply, null, [#pqr{drvid = 2, ts = 3}]}).




