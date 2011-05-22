%%% ---------------------------------------------------------------------------
%%% LUnit cache module
%%% Version 0.1
%%% INSTE, 2011
%%% ---------------------------------------------------------------------------
-module(lunit_cache).

-behaviour(gen_server).

-vsn("0.1").

-export([start_link/0]).
-export([init/1, handle_cast/2, handle_call/3, terminate/2]).

-export([cache_insert_at/3, cache_count/1, cache_insert_at_the_end/2,
	 cache_get_pos/2, cache_remove/2, cache_get_all/1, cache_get/2,
	 cache_pushall/1]).

-include("lunit.hrl").

start_link() ->
    gen_server:start_link(?MODULE, 1, []).

init(_) ->
    {ok, cache_init()}.

terminate(_, _) ->
    ok.

handle_cast(stop, LoopData) ->
    {stop, normal, LoopData}.

%% ----------------------------------------------------------------------------
%% Handling calls from handler
%% ----------------------------------------------------------------------------
handle_call({insert, DrvID, TS}, _From, LoopData) ->		
	{reply, ok, 
		cache_insert_at_the_end(LoopData,
			#pqr{drvid = DrvID, ts = TS})};

handle_call({insert_at, Pos, DrvID, TS}, _From, LoopData) when Pos > 0 ->
	{reply, ok,
		cache_insert_at(LoopData,
			#pqr{drvid = DrvID, ts = TS}, Pos - 1)};

handle_call({insert_at, Pos, _DrvID, _TS}, _, LoopData) when Pos =< 0 ->
	{reply, {error, wrong_id}, LoopData};

handle_call({get_pos, DrvID}, _From, LoopData) ->
	Res = cache_get_pos(LoopData, DrvID),
	case Res of
		-1 -> {reply, {error, wrong_id}, LoopData};
		_Other -> {reply, {ok, {pos, Res}}, LoopData}
	end;

handle_call({remove, DrvID}, _From, LoopData) ->
	{reply, ok, cache_remove(LoopData, DrvID)};

handle_call({get, DrvID}, _From, LoopData) ->
	{reply, cache_get(LoopData, DrvID), LoopData};

handle_call(get_all, _From, LoopData) ->
	{reply, {ok, cache_get_all(LoopData)}, LoopData};

handle_call({pushall, Data}, _From, _LoopData) ->
	{reply, ok, Data}.

% -----------------------------------------------------------------------------
% 			INTERNAL FUNCTIONS
% -----------------------------------------------------------------------------

%% ----------------------------------------------------------------------------
%% Cache initialization
%% () -> Data
%% ----------------------------------------------------------------------------
cache_init() ->
    [].

%% ----------------------------------------------------------------------------
%% Fetching all data from the cache
%% (Data) -> [#pqr | Tail]
%% ----------------------------------------------------------------------------
cache_pushall(Data) ->
	Data.

%% ----------------------------------------------------------------------------
%% Inserting record at the end of cache
%% (Data, #pqr) -> Data
%% ----------------------------------------------------------------------------
cache_insert_at_the_end(Cache, Record) ->
    Cache ++ [Record].

%% ----------------------------------------------------------------------------
%% Inserting at any position of cache
%% (Data, #pqr, Pos) -> Data
%% ----------------------------------------------------------------------------
cache_insert_at(Cache, Record, Pos) ->
    lunit_lib:insert_at(Cache, Record, Pos).

%% ----------------------------------------------------------------------------
%% Number of units in cache
%% (Data) -> Count
%% ----------------------------------------------------------------------------
cache_count(Cache) ->
    length(Cache).

%% ----------------------------------------------------------------------------
%% Getting position of element in cache
%% (Data, ID) -> Position | -1
%% ----------------------------------------------------------------------------
cache_get_pos([], _, _) ->
    -1;

cache_get_pos([#pqr{drvid = DrvID} | _Tail], DrvID, Position) ->
    Position;

cache_get_pos([_ | Tail], DrvID, Position) ->
    cache_get_pos(Tail, DrvID, Position + 1).


cache_get_pos(Cache, DrvID) ->
    cache_get_pos(Cache, DrvID, 1).


%% ----------------------------------------------------------------------------
%% Accessing to element of the cache by ID
%% (Data, ID) -> #pqr
%% ----------------------------------------------------------------------------
cache_get([], _) ->
	null;

cache_get([#pqr{drvid = DrvID} = Value | _Tail], DrvID) ->
	Value;

cache_get([_ | Tail], DrvID) ->
	cache_get(Tail, DrvID).


%% ----------------------------------------------------------------------------
%% Removing record from the cache
%% (Data, ID) -> Data
%% ----------------------------------------------------------------------------

cache_remove(Cache, DrvID) ->
	[X || X <- Cache, X#pqr.drvid /= DrvID].

%% ----------------------------------------------------------------------------
%% Serializing cache
%% (Data) -> [{DrvID, TS} | Tail]
%% ----------------------------------------------------------------------------
cache_get_all([], Acc) ->
	lists:reverse(Acc);

cache_get_all([#pqr{drvid = DrvID, ts = TS} | Tail], Acc) ->
	cache_get_all(Tail, [{DrvID, TS} | Acc]).

cache_get_all(Cache) ->
	cache_get_all(Cache, []).

