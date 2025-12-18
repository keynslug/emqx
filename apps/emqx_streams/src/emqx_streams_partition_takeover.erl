%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_partition_takeover).

-include("emqx_streams_internal.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").

-export([launch/4]).

-export([start_takeover/4]).

%%

launch(Consumer, Group, Stream, Takeover) ->
    emqx_pool:async_submit_to_pool(
        ?POOL_GENERIC,
        fun ?MODULE:start_takeover/4,
        [Consumer, Group, Stream, Takeover]
    ).

start_takeover(Consumer, Group, Stream, {takeover, Partition, DeadConsumer, HB}) ->
    SGroup = ?streamgroup(Group, Stream),
    TraceCtx = #{
        initiator => Consumer,
        streamgroup => SGroup,
        partition => Partition,
        from => DeadConsumer
    },
    %% TODO undefined?
    Offset = emqx_streams_state_db:partition_progress_dirty(SGroup, Partition),
    case
        emqx_streams_state_db:release_partition_async(SGroup, Partition, DeadConsumer, Offset, HB)
    of
        {async, Ref, Ret} ->
            ?tp(debug, "streams_partition_takeover_tx_started", TraceCtx#{tx => Ref}),
            receive
                ?ds_tx_commit_reply(Ref, Reply) ->
                    Outcome = emqx_streams_state_db:progress_partition_tx_result(Ret, Ref, Reply)
            end;
        Outcome ->
            ok
    end,
    case Outcome of
        ok ->
            ?tp(debug, "streams_partition_takeover_released", TraceCtx);
        {invalid, Reason} ->
            ?tp(notice, "streams_partition_takeover_invalid", TraceCtx#{reason => Reason});
        Error ->
            ?tp(warning, "streams_partition_takeover_error", TraceCtx#{reason => Error})
    end.
