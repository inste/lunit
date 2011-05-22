%%% ---------------------------------------------------------------------------
%%% The LUnit infrastructure unit
%%% Hibari backend
%%% INSTE, 2011
%%% ---------------------------------------------------------------------------

-module(lunit_hibari).

-behaviour(gen_server).

-vsn("0.1").

%% Functions for external use

-export([start_link/0]).
-export([init/1, handle_cast/2, handle_call/3, terminate/2]).

%% Functions of internal library
-export([make_ts/0, gen_key/1, gen_key_a/1, try_to_connect/0, ping_hibari/1]).

-export([hibari_insert_req/6, handle_request/2, hibari_remove/4,
		hibari_update_archive/5, hibari_bootstrap_worker/4,
		hibari_rpc/2]).

-export([hibari_make_request/2, hibari_fetch_workingfiber/2]).

-include("lunit_hibari.hrl").
-include("lunit.hrl").

%% ----------------------------------------------------------------------------
%% Starting module
%% ----------------------------------------------------------------------------
start_link() ->
    gen_server:start_link(?MODULE, 1, []).

%% ----------------------------------------------------------------------------
%% Trying to connect to Hibari, if succ. then starts, else die
%% Also bootstrapping new DB if needed
%% _ -> {ok, pid} | {stop, db_unavail}
%% ----------------------------------------------------------------------------
init(_) ->
	case try_to_connect() of 
		{ok, Pid} ->
			case hibari_bootstrap(Pid) of
				{ok, inited} ->
					{ok, #hs{hpid = Pid}};
				error ->
					{stop, db_unavail}
			end;
		{error, db_unavail} ->
			{stop, db_unavail}
	end.

%% ----------------------------------------------------------------------------
%% Terminating stub (it doesn't need to do something on dyers eve in this unit)
%% ----------------------------------------------------------------------------
terminate(_, _) ->
    ok.

%% ----------------------------------------------------------------------------
%% Handling insert request (with making clojure :))
%% ({pid, {insert, drvid, ts}}, #hs{pid}) -> {noreply, #hs{pid}} |
%%					     {stop, db_fault}
%% ----------------------------------------------------------------------------
handle_cast({Caller, {insert, DrvID, TS}}, #hs{hpid = HPid}) ->
	handle_request(
		fun (DrvList, Pid) ->
			hibari_insert_req(Caller, DrvID,
				TS, DrvList ++ [DrvID],
				length(DrvList) + 1, Pid) end,
		HPid);

%% ----------------------------------------------------------------------------
%% Handling insert_at request
%% ({pid, {insert_at, pos, drvid, ts}}, #hs{pid}) -> {noreply, #hs{pid}} |
%%						     {stop, db_fault}
%% ----------------------------------------------------------------------------
handle_cast({Caller, {insert_at, Pos, DrvID, TS}}, #hs{hpid = HPid}) ->
	handle_request(
		fun (DrvList, Pid) ->
			hibari_insert_req(Caller, DrvID, TS,
				lunit_lib:insert_at(DrvList, DrvID, Pos),
				Pos, Pid) end,
		HPid);

%% ----------------------------------------------------------------------------
%% Handling remove request
%% ({pid, {remove, drvid, ts}}, #hs{pid}) -> {noreply, #hs{pid}} |
%%					     {stop, db_fault}
%% ----------------------------------------------------------------------------
handle_cast({Caller, {remove, DrvID, TS}}, #hs{hpid = HPid}) ->
	Res = handle_request(
		fun (DrvList, Pid) ->
			hibari_update_archive(Caller,
				lunit_lib:list_pos(DrvList, DrvID),
				DrvID, TS, Pid) end,
		HPid),
	case Res of
		{ok, added, NewPid} ->
			handle_request(
				fun (DrvList, Pid) ->
					hibari_remove(Caller,
						[X || X <- DrvList, X /= DrvID],
						DrvID,
						Pid) end,
				NewPid);
		_Other ->
			gen_server:cast(Caller, {error, db_failure}),
			{noreply, #hs{hpid = HPid}}
	end;

%% ----------------------------------------------------------------------------
%% Dump current state from DB
%% ({pid, dump_current}, #hs{pid}) -> {noreply, #hs{pid}}
%% ----------------------------------------------------------------------------
handle_cast({Caller, dump_current}, #hs{hpid = HPid}) ->
	handle_request(
		fun (DrvList, Pid) ->
			gen_server:cast(Caller, {ok, dump_current, 
				hibari_fetch_workingfiber(Pid, DrvList)}),
			{noreply, #hs{hpid = Pid}}
			end,
		HPid);

%% ----------------------------------------------------------------------------
%% Stopping stub
%% ----------------------------------------------------------------------------
handle_cast(stop, LoopData) ->
    {stop, normal, LoopData}.

%% ----------------------------------------------------------------------------
%% Dump current state from working fiber
%% (get_dump, _From, #hs) -> {reply, {ok, [#pqr | Tail]}, #hs}
%% ----------------------------------------------------------------------------
handle_call(get_dump, _From, #hs{hpid = HPid}) ->
	handle_request(
		fun (DrvList, Pid) ->
			{reply, hibari_fetch_workingfiber(Pid, DrvList), 
				#hs{ hpid = Pid }}
			end,
		HPid).

% -----------------------------------------------------------------------------
%           INTERNAL FUNCTIONS
% -----------------------------------------------------------------------------


%% ----------------------------------------------------------------------------
%% Building a transaction to Hibari to get last state
%% ([DrvID | Tail]) -> [{get, ......} | Tail]
%% ----------------------------------------------------------------------------
hibari_make_request([], Acc) ->
	lists:reverse(Acc);

hibari_make_request([Head | Tail], Acc) ->
	hibari_make_request(Tail,
		[{get, gen_key(integer_to_list(Head)), []} | Acc]).
	
%% ----------------------------------------------------------------------------
%% Transforming answer from Hibari into cache state
%% ({ok, [{.....} | Tail], [DrvID | Tail]}) -> {ok, [#pqr | Tail]}
%%					     | {error, db_fault}
%% ----------------------------------------------------------------------------
hibari_parse_answer([], [], Acc) ->
	{ok, lists:reverse(Acc)};

hibari_parse_answer([{ok, _TS, Data} | Tail], [DrvID | DTail],  Acc) ->
	case binary_to_term(Data) of
		[insert, TS] ->
			Pos = -1;
		[insert, TS, Pos] ->
			ok
	end,
	hibari_parse_answer(Tail, DTail, 
		[#pqr{drvid = DrvID, ts = TS, pos = Pos} | Acc]);

hibari_parse_answer([{error, _, _} | _], _, _Acc) ->
	{error, db_fault}.

%% ----------------------------------------------------------------------------
%% Fetching last state from Hibari
%% (pid, [DrvID | Tail]) -> {ok, [#pqr | Tail]} | {error, db_fault}
%% ----------------------------------------------------------------------------
hibari_fetch_workingfiber(HPid, DrvList) ->
	Request = {do, ?HIBARI_TABLE, [txn | hibari_make_request(DrvList, [])],
			[], ?HIBARI_OP_TIMEOUT},
	Res = hibari_rpc(HPid, Request),
	case Res of
		{reply, Answer, none} ->
			hibari_parse_answer(Answer, DrvList, []);
		_Other ->
			{error, db_fault}
	end.

%% ----------------------------------------------------------------------------
%% Bootstrapping handler
%% (pid) -> {ok, inited} | error
%% ----------------------------------------------------------------------------
hibari_bootstrap(Pid) ->
	WRes = hibari_bootstrap_worker(Pid, ?HIBARI_TABLE,
		gen_key(?INDEX_WORKING), term_to_binary([])),
	ARes = hibari_bootstrap_worker(Pid, ?HIBARI_ARC_TABLE,
		gen_key_a(?INDEX_ARCHIVE), term_to_binary(
			?ARCHIVE_START_INDEX)),

	case WRes of
		{ok, _} ->
			case ARes of
				{ok, _} ->
					{ok, inited};
				{error, _} ->
					error
			end;
		_Other ->
			error
	end.

%% ----------------------------------------------------------------------------
%% Data fiber bootstrapping worker
%% (pid, table, key, value) -> {ok, were_good} | {ok, now_good} | 
%%				{error, failed_to_get}
%% ----------------------------------------------------------------------------
hibari_bootstrap_worker(Pid, HTable, Key, Value) ->
	Res = hibari_rpc(Pid, {get, HTable,
		Key, [], ?HIBARI_OP_TIMEOUT}),
	case Res of
		{reply, {ok, _TS, _A}, _O} ->
			% Allright
			{ok, were_good};
		{reply, key_not_exist, none} ->
			% Try to bootstrap
			NewRes = hibari_rpc(Pid, {set,
				HTable, Key, Value,
				0, [], ?HIBARI_OP_TIMEOUT}),
			case NewRes of
				{reply, ok, none} ->
					{ok, now_good};
				_Other ->
					{error, failed_to_set}
			end;
		_Other ->
			{error, failed_to_get}
	end.

%% ----------------------------------------------------------------------------
%% Request processor
%% Trying to do request, if connection is lost then trying to reconnect once
%% (Operator#(X, Y), HPid) -> {noreply, #hs{pid}} | {stop, db_fault}
%% ----------------------------------------------------------------------------
handle_request(Operator, HPid) ->
	case ping_hibari(HPid) of
		{ok, HPid, DrvList} ->
			Operator(DrvList, HPid);
		{error, HPid} ->
			case try_to_connect() of
				{ok, NewPid} ->
					case ping_hibari(NewPid) of
						{ok, NewPid, NDrvList} ->
							Operator(NDrvList,
								NewPid);
						{error, NewPid} ->
							{stop, db_fault}
					end;
				{error, db_unavail} ->
					{stop, db_fault}
			end
	end.
								
%% ----------------------------------------------------------------------------
%% Insert transaction to hibari
%% (Caller, DrvID, TS, DrvList, HPid) -> {noreply, #hs{hpid}}
%% ----------------------------------------------------------------------------
hibari_insert_req(Caller, DrvID, TS, DrvList, Pos, HPid) ->
	Res = hibari_rpc(HPid,
		{do, ?HIBARI_TABLE, [txn,
			{set,
				gen_key(?INDEX_WORKING),
				make_ts(),
				term_to_binary(DrvList),
				0, [] },
			{set,
				gen_key(integer_to_list(DrvID)),
				make_ts(),
				term_to_binary([insert, TS, Pos]),
				0, [] } ],
			[], ?HIBARI_OP_TIMEOUT}),
	case Res of
		{reply, [ok, ok], none} ->
			gen_server:cast(Caller, {ok, {insert, DrvID, TS}}),
			{noreply, #hs{hpid = HPid}};
		_Other ->
			gen_server:cast(Caller, {error, {insert, DrvID, TS}}),
			{noreply, #hs{hpid = HPid}}
	end.

%% ----------------------------------------------------------------------------
%% Remove DrvID from active working data fiber
%% (Caller, DrvList, DrvID, HPid) -> {noreply, #hs{hpid}} | {error, failed}
%% ----------------------------------------------------------------------------
hibari_remove(Caller, DrvList, DrvID, HPid) ->
	Res = hibari_rpc(HPid,
		{set, ?HIBARI_TABLE,
			gen_key(?INDEX_WORKING),
			term_to_binary(DrvList),
			0, [], ?HIBARI_OP_TIMEOUT}),
	case Res of
		{reply, ok, none} ->
			gen_server:cast(Caller, {ok, {remove, DrvID}}),
			{noreply, #hs{hpid = HPid}};
		% Try to set even if TS error happened
		% Ugly hack as I see, but works
		{reply, {ts_error, TS}, none} ->
			RRes = hibari_rpc(HPid,
				{set, ?HIBARI_TABLE,
					gen_key(?INDEX_WORKING),
					term_to_binary(DrvList),
					0, [{'testset', TS}],
					?HIBARI_OP_TIMEOUT}),
			case RRes of
				{reply, ok, none} ->
					gen_server:cast(Caller, {ok,
						{remove, DrvID}}),
					{noreply, #hs{hpid = HPid}};
				_Other ->
					gen_server:cast(Caller, {error,
						{remove, DrvID}}),
					{error, failed}
			end;
		_Other ->
			gen_server:cast(Caller, {error, {remove, DrvID}}),
			{error, failed}
	end.

%% ----------------------------------------------------------------------------
%% Update archive data fiber with new data
%% (pid, integer, integer, integer, pid) -> {ok, added, pid} | {error, failed}
%% ----------------------------------------------------------------------------
hibari_update_archive(Caller, Pos, DrvID, TS, HPid) ->
	Res = hibari_rpc(HPid,
		{get, ?HIBARI_ARC_TABLE, gen_key_a(?INDEX_ARCHIVE),
			[], ?HIBARI_OP_TIMEOUT}),
	case Res of
		{reply, {ok, _Ts, PackedIndex}, _Another} ->
			Index = binary_to_term(PackedIndex) + 1,
			TransRes = hibari_rpc(HPid,
				{do, ?HIBARI_ARC_TABLE, [txn,
					{set,
						gen_key_a(?INDEX_ARCHIVE),
						make_ts(),
						term_to_binary(Index),
						0, [] },
					{set,
						gen_key_a(?ARCHIVE_KEY_PREFIX ++
							integer_to_list(Index)),
						make_ts(),
						term_to_binary([DrvID, TS, Pos]),
						0, [] } ],
					[], ?HIBARI_OP_TIMEOUT}),
			case TransRes of
				{reply, [ok, ok], none} ->
					{ok, added, HPid};
				_Other ->
					gen_server:cast(Caller,
						{error, {remove, DrvID}}),
					{error, failed}
			end;
		_Other ->
			gen_server:cast(Caller, {error, {remove, DrvID}}),
			{error, failed}
	end.

%% ----------------------------------------------------------------------------
%% Making timestamp
%% () -> integer
%% ----------------------------------------------------------------------------
make_ts() ->
	{M, S, U} = now(),
	(M * 1000000 * 1000000) + (S * 1000000) + U.

%% ----------------------------------------------------------------------------
%% Building key from prefix and suffix
%% (List, List) -> binary
%% ----------------------------------------------------------------------------
gen_key(Key, Type) when is_list(Key) ->
	list_to_binary("/" ++ ?TABLES_PREFIX ++
		Type ++ ?HIBARI_PARK_INDEX ++ "/" ++ Key).

gen_key(Key) when is_list(Key) ->
	gen_key(Key, []).

gen_key_a(Key) when is_list(Key) ->
	gen_key(Key, [?ARCHIVE_PREFIX]).

%% ----------------------------------------------------------------------------
%% Trying to connect to Hibari
%% () -> {ok, Pid} | {error, db_unavail}
%% ----------------------------------------------------------------------------
try_to_connect() ->
	Res = ubf_client:connect(?HIBARI_HOSTNAME,
				?HIBARI_PORT,
				[{proto, ?HIBARI_PROTO}],
				?HIBARI_TIMEOUT),
	case Res of
		{ok, Pid, _} ->
			NRes = hibari_rpc(Pid,
				{startSession, {'#S', ?HIBARI_GDSS}, []}),
			case NRes of
				{reply, {ok, ok}, none} ->
					{ok, Pid};
				_Other ->
					{error, db_unavail}
			end;
		_Other -> {error, db_unavail}
	end.

%% ----------------------------------------------------------------------------
%% Getting IDList from Hibari
%% (Pid) -> {ok, Pid, Drvlist} | {error, Pid}
%% ----------------------------------------------------------------------------
ping_hibari(Pid) ->
	Res = hibari_rpc(Pid, {get, ?HIBARI_TABLE,
		gen_key(?INDEX_WORKING), [], ?HIBARI_OP_TIMEOUT}),
	case Res of
		{reply, {ok, _TS, DrvList}, none} ->
			{ok, Pid, binary_to_term(DrvList)};
		_Other ->
			{error, Pid}
	end.

%% ----------------------------------------------------------------------------
%% UBF RPC wrapper
%% ----------------------------------------------------------------------------
hibari_rpc(Pid, Req) ->
	R = 	ubf_client:rpc(Pid, Req),
	%io:format("~p -> ~p~n", [Req, R]),
	R.


