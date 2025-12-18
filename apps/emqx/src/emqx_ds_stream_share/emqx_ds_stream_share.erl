%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_stream_share).

-include_lib("snabbkaffe/include/trace.hrl").

-include("emqx_ds_stream_share_proto.hrl").

%% API
-export([
    get_leader_sync/2,
    leader_wanted/2,
    start_local/2
]).

%% Internal exports:
-export([
    start_link/0
]).

-type share() :: _TopicFilter :: emqx_types:topic().
-type strategy() :: _TODO.
-type options() :: #{
    start_time => emqx_ds:time(),
    strategy => strategy()
}.

-behaviour(supervisor).
-export([init/1]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec get_leader_sync(share(), options()) ->
    {ok, pid()} | emqx_ds:error(_).
get_leader_sync(ShareTF, Options) ->
    emqx_ds_stream_share_leader:wait_leader(ensure_local(ShareTF, Options)).

-spec get_leader_nowait(share()) -> {ok, pid()} | undefined.
get_leader_nowait(ShareTF) ->
    emqx_ds_stream_share_leader:whereis_leader(ShareTF).

-spec leader_wanted(emqx_ds_stream_share_proto:borrower(), share()) ->
    ok | emqx_ds:error(_).
leader_wanted(BorrowerId, ShareTopic) ->
    maybe
        %% FIXME: do it async. Tests should expect that though
        {ok, Pid} ?= get_leader_nowait(ShareTopic),
        emqx_ds_shared_sub_proto:send_to_leader(Pid, ?borrower_connect(BorrowerId, ShareTopic))
    end,
    ok.

-spec ensure_local(share(), options()) ->
    pid().
ensure_local(ShareTopic, Options) ->
    case start_local(ShareTopic, Options) of
        {ok, Pid} ->
            Pid;
        {error, {already_started, Pid}} when is_pid(Pid) ->
            Pid
    end.

-spec start_local(share(), options()) ->
    supervisor:startchild_ret().
start_local(ShareTopic, Options) ->
    supervisor:start_child(?MODULE, [ShareTopic, Options]).

%%------------------------------------------------------------------------------
%% supervisor behaviour callbacks
%%------------------------------------------------------------------------------

init([]) ->
    Children = [
        #{
            id => worker,
            start => {emqx_ds_stream_share_leader, start_link, []},
            shutdown => 5_000,
            type => worker,
            restart => transient
        }
    ],
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 100,
        period => 1
    },
    {ok, {SupFlags, Children}}.
