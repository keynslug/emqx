%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_utils_demux).

% -include_lib("emqx/include/logger.hrl").

-export([start_link/0]).
-export([
    fetch/4,
    invalidate/3
]).

-behaviour(gen_server).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-export([start_link_demuxer/1]).

-define(name(REC), {n, l, REC}).
-define(via(REC), {via, gproc, ?name(REC)}).

%%

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, sup).

fetch(M, F, Args, Lifetime) ->
    try
        call_fetch(M, F, Args, Lifetime)
    catch
        exit:{noproc, _} ->
            case supervisor:start_child(?MODULE, [{M, F, Args}]) of
                {ok, Pid} ->
                    call_fetch(Pid, Lifetime);
                {error, {already_started, Pid}} ->
                    call_fetch(Pid, Lifetime)
            end
    end.

invalidate(M, F, Args) ->
    gen_server:cast(?via({?MODULE, {M, F, Args}}), invalidate).

call_fetch(M, F, Args, Lifetime) ->
    gen_server:call(?via({?MODULE, {M, F, Args}}), {fetch, Lifetime}).

call_fetch(Pid, Lifetime) ->
    gen_server:call(Pid, {fetch, Lifetime}).

start_link_demuxer(MFArgs) ->
    gen_server:start_link(?via({?MODULE, MFArgs}), ?MODULE, MFArgs, []).

%%

init(sup) ->
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 1000,
        period => 1
    },
    ChildSpec = #{
        id => demuxer,
        start => {?MODULE, start_link_demuxer, []},
        type => worker,
        restart => temporary,
        shutdown => 1000
    },
    {ok, {SupFlags, [ChildSpec]}};
init(MFArgs = {_M, _F, _}) ->
    % _ = erlang:process_flag(trap_exit, true),
    {ok, #{mfargs => MFArgs}}.

handle_call({fetch, Lifetime}, _From, St) ->
    T = erlang:monotonic_time(millisecond),
    case St of
        #{cached := VCached, since := TSince} when T < TSince + Lifetime ->
            % ?HERE("CACHED:~0p", [VCached]),
            {reply, VCached, St, Lifetime};
        #{mfargs := {M, F, Args}} ->
            V = erlang:apply(M, F, Args),
            TSince = erlang:monotonic_time(millisecond),
            NSt = St#{cached => V, since => TSince},
            % ok = drain_invalidate(),
            {reply, V, NSt, Lifetime}
    end;
handle_call(_Call, _From, St) ->
    % ?SLOG(error, #{msg => "unexpected_call", cast => _Call, from => _From}),
    {noreply, St}.

drain_invalidate() ->
    receive
        {'$gen_cast', invalidate} ->
            drain_invalidate()
    after 0 ->
        ok
    end.

handle_cast(invalidate, St) ->
    % ?HERE("INVALIDATED", []),
    {noreply, maps:without([cached, since], St)};
handle_cast(_Cast, St) ->
    % ?SLOG(error, #{msg => "demux_unexpected_cast", cast => _Cast}),
    {noreply, St}.

handle_info(timeout, St) ->
    {noreply, maps:without([cached, since], St)};
handle_info(_Info, St) ->
    % ?SLOG(error, #{msg => "demux_unexpected_info", info => _Info}),
    {noreply, St}.

terminate(_Reason, _St) ->
    ok.
