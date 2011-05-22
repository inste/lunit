%%% ---------------------------------------------------------------------------
%%% LUnit dispatcher module
%%% INSTE, 2011
%%% ---------------------------------------------------------------------------
-module(lunit_sv).

-behaviour(gen_server).
-vsn("0.1").

-export([start_link/0]).
-export([init/1, handle_cast/3, handle_call/3, handle_cast/2,
	handle_info/2, terminate/2]).

-export([inform_all/1, warm_cache/2]).

-include("lunit.hrl").

start_link() ->
	gen_server:start_link(?MODULE, 1, []).

%% ----------------------------------------------------------------------------
%% Initiliazing dispatcher, starting childs
%% ----------------------------------------------------------------------------
init(_) ->
	process_flag(trap_exit, true),
	{ok, HandlerPid} = lunit_handler:start_link(),
	{ok, CachePid} = lunit_cache:start_link(),
	{ok, HibariPid} = lunit_hibari:start_link(),
	Pids = #svstate{handlerpid = HandlerPid,
		cachepid = CachePid, hibaripid = HibariPid},
	% Sending current hibari and cache pids to handler
	inform_all(Pids),
	% Try to load last state from DB
	case warm_cache(HibariPid, CachePid) of
		{error, db_fault} ->
			lunit_lib:log("lunit_sv", "init", "error",
				"Hibari is unreachable", []),
			{stop, reason};
		{ok, done} ->
			process_flag(trap_exit, true),
			{ok, Pids}
	end.

terminate(_, _) ->
    ok.

handle_call(get_handler_pid, _From, #svstate{handlerpid = Pid} = LoopData) ->
	{reply, {ok, Pid}, LoopData}.

handle_cast({'EXIT', _Pid, _Reason}, _From, LoopData) ->
	{reply, ok, LoopData}.

%% ----------------------------------------------------------------------------
%% Handler died, restart it
%% ----------------------------------------------------------------------------
handle_info({'EXIT', Pid, _Reason}, #svstate{handlerpid = Pid,
	     hibaripid = HP, cachepid = CP}) ->
	{ok, HandlerPid} = lunit_handler:start_link(),

	lunit_lib:log("lunit_sv", "handle_info", "info",
		"Handler worker is down, restarting", []),
	NewState = #svstate{handlerpid = HandlerPid, hibaripid = HP,
		cachepid = CP},
 	inform_all(NewState),
	{noreply, NewState};

%% ----------------------------------------------------------------------------
%% Hibari died, restart it, and send new pid to handler
%% ----------------------------------------------------------------------------
handle_info({'EXIT', Pid, _Reason}, #svstate{handlerpid = HP,
		hibaripid = Pid, cachepid = CP}) ->
	{ok, HibariPid} = lunit_hibari:start_link(),

	lunit_lib:log("lunit_sv", "handle_info", "info",
		"Hibari worker is down, restarting", []),
	NewState = #svstate{handlerpid = HP, hibaripid = HibariPid,
		cachepid = CP},
	inform_all(NewState),
	{noreply, NewState};

%% ----------------------------------------------------------------------------
%% Cache died, restart it, load last state, and send new pid to handler
%% ---------------------------------------------------------------------------
handle_info({'EXIT', Pid, _Reason}, #svstate{handlerpid = HP,
		hibaripid = HibP, cachepid = Pid}) ->
	{ok, CachePid} = lunit_cache:start_link(),

	lunit_lib:log("lunit_sv", "handle_info", "info",
		"Cache worker is down, restarting", []),
	NewState = #svstate{handlerpid = HP, hibaripid = HibP,
		cachepid = CachePid},
	inform_all(NewState),
	warm_cache(HibP, CachePid),
	{noreply, NewState};

handle_info(_, State) ->
	{noreply, State}.

handle_cast(stop, LoopData) ->
	{stop, normal, LoopData}.

% -----------------------------------------------------------------------------
%                  INTERNAL FUNCTIONS
% -----------------------------------------------------------------------------


%% ----------------------------------------------------------------------------
%% Informing handler about hibari and cache pids
%% ----------------------------------------------------------------------------
inform_all(#svstate{handlerpid = HP, cachepid = CP, hibaripid = HibP}) ->
	lunit_lib:log("lunit_sv", "inform_all", "info",
		"New pids: Handler, Hibari, Cache", 
		[HP, HibP, CP]),
	gen_server:call(HP, {pids, {hibari, HibP}, {cache, CP}, {sv, self()}}).

%% ----------------------------------------------------------------------------
%% Loading cache from Hibari
%% ----------------------------------------------------------------------------
warm_cache(HPid, CPid) ->
	Res = gen_server:call(HPid, get_dump), 
	case Res of
		{error, db_fault} ->
			{error, db_fault};
		{ok, Answer} ->
			gen_server:call(CPid, {pushall, Answer}),
			{ok, done}
	end.

