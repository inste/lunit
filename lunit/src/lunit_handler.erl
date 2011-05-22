%%% ---------------------------------------------------------------------------
%%% LUnit messages handler module
%%% INSTE, 2011
%%% ---------------------------------------------------------------------------
-module(lunit_handler).

-behaviour(gen_server).

-vsn("0.1").

-export([start_link/0]).
-export([init/1, handle_cast/2, handle_call/3, terminate/2]).

-export([process_insert/4, process_queue/4]).

-include("lunit.hrl").

start_link() ->
	gen_server:start_link(?MODULE, 1, []).

%% ----------------------------------------------------------------------------
%% Initiliazing new queue
%% ----------------------------------------------------------------------------
init(_) ->
	{ok, #state{
    		pids = #pids{hp = 1},
		qit = lunit_lib:fifo_init()
		}}.

terminate(_, _) ->
	ok.


handle_cast(stop, LoopData) ->
	{stop, normal, LoopData};

%% ----------------------------------------------------------------------------
%% Got positive answer from Hibari, so process next message in queue
%% ({ok, Msg}, #state) -> {noreply, #state}
%% ----------------------------------------------------------------------------
handle_cast({ok, Msg}, #state{pids = PIDS, qit = QIT}) ->
	case lunit_lib:fifo_last(QIT) of
		{insert, _DrvID, _TS} = Lasted ->
			Last = Lasted;
		{insert_at, _Pos, DrvID, TS} ->
			Last = {insert, DrvID, TS};
		{remove, DrvID, _TS} ->
			Last = {remove, DrvID};
		Other ->
			Last = Other
	end,
	process_queue(PIDS, QIT, Msg, Last);

%% ----------------------------------------------------------------------------
%% Got error from Hibari, so try to resend message
%% !FIXME! Need to implement DB failure handler
%% ({error, Msg}, #state) -> {noreply, #state}
%% ----------------------------------------------------------------------------
handle_cast({error, _}, #state{pids = PIDS, qit = QIT} = LoopData) ->
	%% Obviuosly the error happened, then try to retry
	gen_server:cast(PIDS#pids.hp,
		{self(), lunit_lib:fifo_last(QIT)}),
	{noreply, LoopData}.

%% ----------------------------------------------------------------------------
%% Recieving pids of hibari and cache processes we will to talk with
%% ----------------------------------------------------------------------------
handle_call({pids, {hibari, HP}, {cache, CP}, {sv, SVP}}, _From,
		#state{qit = QIT}) ->
	{reply, ok, #state{
    		pids = #pids{cp = CP, hp = HP, sp = SVP},
		qit = QIT}
		};

%% ----------------------------------------------------------------------------
%% Handling 'insert' request from outside
%% ({insert, DrvID, TS}, From, #state) -> {reply, ok, #state}
%%					| {reply, {error, already_exists},
%%								#state}
%% ----------------------------------------------------------------------------
handle_call({insert, DrvID, _TS} = Msg, _From,
		#state{pids = PIDS, qit = QIT}) ->
	process_insert(PIDS, QIT, DrvID, Msg);

%% ----------------------------------------------------------------------------
%% Handling 'insert_at' request from outside
%% ({insert_at, DrvID, TS}, From, #state) -> {reply, ok, #state}
%%					   | {reply, {error, already_exists},
%%								#state}
%% ----------------------------------------------------------------------------
handle_call({insert_at, _Pos, DrvID, _TS} = Msg, _From,
		#state{pids = PIDS, qit = QIT}) ->
	process_insert(PIDS, QIT, DrvID, Msg);

%% ----------------------------------------------------------------------------
%% Handling 'get_pos' request from outside
%% ({get_pos, DrvID}, From, #state) -> {reply, {ok, {pos, integer}}, #state}
%%				     | {reply, {error, wrong_id}, #state}
%% ----------------------------------------------------------------------------
handle_call({get_pos, _} = Msg, _From,
		#state{pids = PIDS} = LoopData) ->
	{reply, gen_server:call(PIDS#pids.cp, Msg), LoopData};

%% ----------------------------------------------------------------------------
%% Handling 'remove' request from outside
%% ({remove, DrvID}, From, #state) -> {reply, ok, #state}
%%				    | {reply, not_exists, #state}
%% ----------------------------------------------------------------------------
handle_call({remove, DrvID} = Msg, _From, 
		#state{pids = PIDS, qit = QIT} = LoopData) ->
	case gen_server:call(PIDS#pids.cp, {get, DrvID}) of
		null -> 
			{reply, not_exists, LoopData};
		#pqr{drvid = DrvID, ts = TS} ->
			gen_server:call(PIDS#pids.cp, Msg),
			HM = {self(), {remove, DrvID, TS}},
			gen_server:cast(PIDS#pids.hp,
				HM),
			{reply, ok, #state{pids = PIDS,
				qit = lunit_lib:fifo_push(QIT, 
					{remove, DrvID, TS})}}
	end;

%% ----------------------------------------------------------------------------
%% Handling 'get_all' request from outside
%% (get_all, From, #state) -> {reply, {ok, [#pqr | Tail]}, #state}
%% ----------------------------------------------------------------------------
handle_call(get_all, _From, #state{pids = PIDS} = LoopData) ->
	{reply, gen_server:call(PIDS#pids.cp, get_all), LoopData}.

% -----------------------------------------------------------------------------
%                 INTERNAL FUNCTIONS
% -----------------------------------------------------------------------------


%% ----------------------------------------------------------------------------
%% Processing insert request
%% (#pids, #qit, DrvID, Msg) -> {reply, ok, #state}
%%			      | {reply, {error, already_exists}, #state}
%% ----------------------------------------------------------------------------
process_insert(PIDS, QIT, DrvID, Msg) ->
	case gen_server:call(PIDS#pids.cp, {get_pos, DrvID}) of
		{error, wrong_id} ->
			gen_server:cast(PIDS#pids.hp, {self(), Msg}),
			gen_server:call(PIDS#pids.cp, Msg),
			{reply, ok, #state{
				pids = PIDS,
				qit = lunit_lib:fifo_push(QIT, Msg)
				}};
		{ok, {pos, _}} ->
			{reply, {error, already_exists}, #state{
				pids = PIDS, qit = QIT}}
	end.

%% ----------------------------------------------------------------------------
%% Processing Hibari queue (if Result from Hibari equals to last in queue)
%% (#pids, #qit, Msg, Msg) -> {noreply, #state}
%% ----------------------------------------------------------------------------
process_queue(PIDS, QIT, Msg, Msg) ->
	NewQIT = lunit_lib:fifo_del_last(QIT),
	case lunit_lib:fifo_last(NewQIT) of
		null ->
			% Queue is empty
			{noreply, #state{pids = PIDS, qit = NewQIT}};
		Other ->
			% Still processing queue
			%gen_server:cast(PIDS#pids.hp, {self(), Other}),
			{noreply, #state{pids = PIDS, qit = NewQIT}}
	end;

process_queue(PIDS, QIT, Msg, null) ->
	io:format("error~n", []),
	{noreply, #state{pids = PIDS, qit = QIT}};

%% ----------------------------------------------------------------------------
%% Processing Hibari queue (if Result from Hibari differs from last in queue,
%% trying to resend)
%% (#pids, #qit, Msg1, Msg2) -> {noreply, #state}
%% ----------------------------------------------------------------------------
process_queue(PIDS, QIT, Msg, LastInQueue) when (LastInQueue /= null) ->
	gen_server:cast(PIDS#pids.hp, {self(), Msg}),
	{noreply, #state{pids = PIDS, qit = QIT}}.


