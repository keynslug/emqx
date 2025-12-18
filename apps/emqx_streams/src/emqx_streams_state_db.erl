%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_state_db).

-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("emqx_utils/include/emqx_ds_dbs.hrl").

-include("emqx_streams_internal.hrl").

-export([
    open/0,
    close/0,
    wait_readiness/1
]).

-export([
    progress_partitions/3,
    lease_partition_async/5,
    release_partition_async/5,
    partition_tx_result/3,

    announce_consumer/4,
    reannounce_consumer/4,
    deannounce_consumer/3,

    group_progress_dirty/1,
    partition_progress_dirty/2,
    group_revision_dirty/1,
    group_state_dirty/1
]).

-type streamgroup() :: binary().

-define(DB, ?STREAMS_STATE_DB).

-define(topic_pdisp_group(SGROUP, TAIL), [<<"$pdisp">>, SGROUP | TAIL]).
-define(topic_part_lease(SGROUP, PART), ?topic_pdisp_group(SGROUP, [<<"ls">>, PART])).
-define(topic_part_hbeat(SGROUP, PART), ?topic_pdisp_group(SGROUP, [<<"hb">>, PART])).
-define(topic_consumer_announce(SGROUP), ?topic_pdisp_group(SGROUP, [<<"ann">>])).
-define(topic_group_revision(SGROUP), ?topic_pdisp_group(SGROUP, [<<"rev">>])).

-define(topic_part_progress(SGROUP, PART), [<<"$pdisp:pr">>, SGROUP, PART]).

%%

open() ->
    Config = emqx_ds_schema:db_config_streams_states(),
    ok = emqx_ds:open_db(?DB, Config#{
        storage => {emqx_ds_storage_skipstream_lts_v2, #{}}
    }).

close() ->
    emqx_ds:close_db(?DB).

-spec wait_readiness(timeout()) -> ok | timeout.
wait_readiness(Timeout) ->
    emqx_ds:wait_db(?DB, all, Timeout).

%%

-define(offset_ahead(O1, O2), (is_integer(O1) andalso O1 > O2)).

lease_partition_async(SGroup, Partition, Consumer, OffsetNext, Heartbeat) ->
    async_tx(SGroup, fun() ->
        TopicLease = ?topic_part_lease(SGroup, Partition),
        TopicProgress = ?topic_part_progress(SGroup, Partition),
        TopicRevision = ?topic_group_revision(SGroup),
        emqx_ds:tx_ttv_assert_absent(TopicLease, 0),
        case ttv_extract_offset(emqx_ds:tx_read(#{end_time => 1}, TopicProgress)) of
            Offset when not ?offset_ahead(Offset, OffsetNext) ->
                emqx_ds:tx_write({TopicLease, 0, Consumer}),
                emqx_ds:tx_write(mk_ttv_heartbeat(SGroup, Partition, Heartbeat)),
                emqx_ds:tx_write({TopicProgress, 0, enc_offset(OffsetNext)}),
                emqx_ds:tx_write({TopicRevision, 0, ?ds_tx_serial});
            Offset ->
                {invalid, {offset_ahead, Partition, Offset}}
        end
    end).

release_partition_async(SGroup, Partition, Consumer, OffsetNext, LastHeartbeat) ->
    async_tx(SGroup, fun() ->
        TopicLease = ?topic_part_lease(SGroup, Partition),
        TopicHeartbeat = ?topic_part_hbeat(SGroup, Partition),
        TopicProgress = ?topic_part_progress(SGroup, Partition),
        TopicRevision = ?topic_group_revision(SGroup),
        emqx_ds:tx_ttv_assert_present(TopicLease, 0, Consumer),
        emqx_ds:tx_ttv_assert_present(TopicHeartbeat, 0, enc_timestamp(LastHeartbeat)),
        case ttv_extract_offset(emqx_ds:tx_read(#{end_time => 1}, TopicProgress)) of
            Offset when not ?offset_ahead(Offset, OffsetNext) ->
                emqx_ds:tx_del_topic(TopicLease, 0, 1),
                emqx_ds:tx_write({TopicProgress, 0, enc_offset(OffsetNext)}),
                emqx_ds:tx_write({TopicRevision, 0, ?ds_tx_serial});
            Offset ->
                {invalid, {offset_ahead, Partition, Offset}}
        end
    end).

progress_partitions(SGroup, Partitions, Heartbeat) ->
    Ret = sync_tx(SGroup, fun() ->
        lists:foreach(
            fun({Partition, Consumer, OffsetNext}) ->
                TopicLease = ?topic_part_lease(SGroup, Partition),
                TopicProgress = ?topic_part_progress(SGroup, Partition),
                emqx_ds:tx_ttv_assert_present(TopicLease, 0, Consumer),
                case ttv_extract_offset(emqx_ds:tx_read(#{end_time => 1}, TopicProgress)) of
                    Offset when not ?offset_ahead(Offset, OffsetNext) ->
                        has_offset_changed(Offset, OffsetNext) andalso
                            emqx_ds:tx_write({TopicProgress, 0, enc_offset(OffsetNext)}),
                        emqx_ds:tx_write(mk_ttv_heartbeat(SGroup, Partition, Heartbeat));
                    Offset ->
                        emqx_ds:reset_trans({invalid, {offset_ahead, Partition, Offset}})
                end
            end,
            Partitions
        )
    end),
    case Ret of
        ok ->
            ok;
        ?err_rec({_Reset, Invalid = {invalid, _Reason}}) ->
            Invalid;
        Error ->
            map_partition_tx_error(Error)
    end.

has_offset_changed(undefined, _) ->
    true;
has_offset_changed(Offset1, Offset2) ->
    Offset2 > Offset1.

ttv_extract_offset([{_Topic, 0, V}]) ->
    dec_offset(V);
ttv_extract_offset([]) ->
    undefined.

mk_ttv_heartbeat(SGroup, Partition, Ts) ->
    {?topic_part_hbeat(SGroup, Partition), 0, enc_timestamp(Ts)}.

partition_tx_result(Ret, Ref, Reply) ->
    partition_tx_result(Ret, emqx_ds:tx_commit_outcome(?DB, Ref, Reply)).

partition_tx_result(ok, Outcome) ->
    case Outcome of
        {ok, _} ->
            ok;
        Error ->
            map_partition_tx_error(Error)
    end;
partition_tx_result(Invalid, _Outcome) ->
    Invalid.

map_partition_tx_error(?err_unrec({precondition_failed, [How | _]})) ->
    case How of
        #{topic := ?topic_part_lease(_, Partition), unexpected := DifferentConsumer} ->
            {invalid, {leased, Partition, DifferentConsumer}};
        #{topic := ?topic_part_lease(_, Partition), got := DifferentConsumer} ->
            {invalid, {leased, Partition, DifferentConsumer}};
        #{topic := ?topic_part_hbeat(_, Partition), got := Different} ->
            DifferentHeartbeat = emqx_maybe:apply(fun dec_timestamp/1, Different),
            {invalid, {heartbeat_mismatch, Partition, DifferentHeartbeat}}
    end;
map_partition_tx_error(?err_rec({read_conflict, _Conflict})) ->
    {invalid, conflict};
map_partition_tx_error(Error) ->
    Error.

announce_consumer(SGroup, Consumer, Heartbeat, Lifetime) ->
    sync_tx(SGroup, fun() ->
        TopicAnnounce = ?topic_consumer_announce(SGroup),
        emqx_ds:tx_del_topic(TopicAnnounce, 0, Heartbeat - Lifetime),
        emqx_ds:tx_write({TopicAnnounce, Heartbeat, Consumer})
    end).

reannounce_consumer(SGroup, Consumer, Heartbeat, HeartbeatPrev) ->
    sync_tx(SGroup, fun() ->
        TopicAnnounce = ?topic_consumer_announce(SGroup),
        emqx_ds:tx_del_topic(TopicAnnounce, HeartbeatPrev, HeartbeatPrev + 1),
        emqx_ds:tx_write({TopicAnnounce, Heartbeat, Consumer})
    end).

deannounce_consumer(SGroup, Consumer, Heartbeat) ->
    Ret = sync_tx(SGroup, fun() ->
        TopicAnnounce = ?topic_consumer_announce(SGroup),
        emqx_ds:tx_ttv_assert_present(TopicAnnounce, Heartbeat, Consumer),
        emqx_ds:tx_del_topic(TopicAnnounce, Heartbeat, Heartbeat + 1)
    end),
    case Ret of
        ok ->
            ok;
        ?err_unrec({precondition_failed, [#{topic := ?topic_consumer_announce(_)} | _]}) ->
            {invalid, undefined};
        Error ->
            Error
    end.

group_progress_dirty(SGroup) ->
    TTVs = emqx_ds:dirty_read(
        #{
            db => ?DB,
            shard => emqx_ds:shard_of(?DB, SGroup),
            generation => 1,
            end_time => 1
        },
        ?topic_part_progress(SGroup, '+')
    ),
    lists:foldl(
        fun({?topic_part_progress(_, Partition), _, V}, Acc) ->
            Acc#{Partition => dec_offset(V)}
        end,
        #{},
        TTVs
    ).

partition_progress_dirty(SGroup, Partition) ->
    ttv_extract_offset(
        emqx_ds:dirty_read(
            #{
                db => ?DB,
                shard => emqx_ds:shard_of(?DB, SGroup),
                generation => 1,
                end_time => 1
            },
            ?topic_part_progress(SGroup, Partition)
        )
    ).

group_revision_dirty(SGroup) ->
    TTVs = emqx_ds:dirty_read(
        #{
            db => ?DB,
            shard => emqx_ds:shard_of(?DB, SGroup),
            generation => 1,
            end_time => 1
        },
        ?topic_group_revision(SGroup)
    ),
    case TTVs of
        [{_TopicAllocVsn, _, Vsn} | _] ->
            Vsn;
        [] ->
            undefined
    end.

group_state_dirty(SGroup) ->
    TTVs = emqx_ds:dirty_read(
        #{
            db => ?DB,
            shard => emqx_ds:shard_of(?DB, SGroup),
            generation => 1
            % end_time => 1
        },
        ?topic_pdisp_group(SGroup, ['#'])
    ),
    Leases = lists:foldl(
        fun
            ({?topic_part_lease(_, Partition), _, Consumer}, Acc) ->
                Acc#{Partition => Consumer};
            (_TTV, Acc) ->
                Acc
        end,
        #{},
        TTVs
    ),
    PartitionHBs = lists:foldl(
        fun
            ({?topic_part_hbeat(_, Partition), _, V}, Acc) ->
                Acc#{Partition => dec_timestamp(V)};
            (_TTV, Acc) ->
                Acc
        end,
        #{},
        TTVs
    ),
    Consumers = lists:foldl(
        fun
            ({?topic_consumer_announce(_), Heartbeat, Consumer}, Acc) ->
                Acc#{Consumer => max(Heartbeat, maps:get(Consumer, Acc, 0))};
            (_TTV, Acc) ->
                Acc
        end,
        #{},
        TTVs
    ),
    Revision = lists:foldl(
        fun
            ({?topic_group_revision(_), _, Vsn}, _Acc) ->
                Vsn;
            (_TTV, Acc) ->
                Acc
        end,
        undefined,
        TTVs
    ),
    #pdisp_group_st{
        revision = Revision,
        leases = Leases,
        consumers = Consumers,
        partitions = PartitionHBs
    }.

async_tx(SGroup, Fun) ->
    TxRet = emqx_ds:trans(
        #{
            db => ?DB,
            shard => {auto, SGroup},
            generation => 1,
            sync => false,
            retries => 0
        },
        Fun
    ),
    case TxRet of
        {async, _Ref, _Ret} = TxAsync ->
            TxAsync;
        {nop, Ret} ->
            Ret;
        Error ->
            Error
    end.

sync_tx(SGroup, Fun) ->
    TxRet = emqx_ds:trans(
        #{
            db => ?DB,
            shard => {auto, SGroup},
            generation => 1,
            sync => true,
            retries => 0
        },
        Fun
    ),
    case TxRet of
        {atomic, _Serial, Ret} ->
            Ret;
        {nop, Ret} ->
            Ret;
        Error ->
            Error
    end.

enc_offset(Offset) ->
    integer_to_binary(Offset).

enc_timestamp(Offset) ->
    integer_to_binary(Offset).

dec_offset(Value) ->
    binary_to_integer(Value).

dec_timestamp(Value) ->
    binary_to_integer(Value).
