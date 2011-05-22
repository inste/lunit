%%% ---------------------------------------------------------------------------
%%% LUnit main header file
%%% INSTE, 2011
%%% ---------------------------------------------------------------------------

% -----------------------------------------------------------------------------
% #pqr record definition (cache item)
% -----------------------------------------------------------------------------
-record(pqr, {drvid, ts, pos}).

% -----------------------------------------------------------------------------
% #hs record (hibari module state)
% -----------------------------------------------------------------------------
-record(hs, {hpid, connected, drvlist}).

% -----------------------------------------------------------------------------
% #pids record (handler pids)
% -----------------------------------------------------------------------------
-record(pids, {hp, cp, sp}).

% -----------------------------------------------------------------------------
% #state record (handler state)
% -----------------------------------------------------------------------------
-record(state, {pids, qit}).

% -----------------------------------------------------------------------------
% #svstate record (dispatcher state)
% -----------------------------------------------------------------------------
-record(svstate, {handlerpid, cachepid, hibaripid}).

