-module(lunit_hibari_tests).

-include_lib("eunit/include/eunit.hrl").

hibari_make_request_test() ->
	?assertEqual(lunit_hibari:hibari_make_request([], []), []),
	?assertEqual(lunit_hibari:hibari_make_request([12, 13, 14], []), 
		[{get,<<"/prk04/12">>,[]},
		{get,<<"/prk04/13">>,[]},
		{get,<<"/prk04/14">>,[]}]).


