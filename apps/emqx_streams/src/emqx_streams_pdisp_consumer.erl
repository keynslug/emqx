%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_pdisp_consumer).

-include("emqx_streams_internal.hrl").

-export([
    new/0,
    n_leases/1,
    lookup_lease/2,
    progress/6,
    release/5,
    handle_tx_reply/6
]).

-export_type([st/0, streamgroup/0]).

-type consumer() :: emqx_types:clientid().
-type streamgroup() :: binary().
-type partition() :: emqx_streams_types:partition().
-type offset() :: non_neg_integer().
-type heartbeat_ms() :: non_neg_integer().

-type st() :: #{
    {part, partition()} := partition_st()
}.

-type partition_st() :: #{
    lease := boolean() | releasing,
    offset_last := offset(),
    offset_committed := offset() | undefined,
    heartbeat_last := heartbeat_ms(),
    %% Introspection:
    offset_committed_last => offset(),
    offset_committed_max => offset()
}.

%%

-spec new() -> st().
new() ->
    #{}.

-spec n_leases(st()) -> non_neg_integer().
n_leases(St) ->
    maps:fold(
        fun
            ({part, _}, #{lease := true}, N) -> N + 1;
            ({part, _}, #{lease := releasing}, N) -> N + 1;
            (_, _, N) -> N
        end,
        0,
        St
    ).

-spec lookup_lease(partition(), st()) -> partition_st() | undefined.
lookup_lease(Partition, St) ->
    case St of
        #{{part, Partition} := PartSt} ->
            PartSt;
        #{} ->
            undefined
    end.

-spec progress(consumer(), streamgroup(), partition(), offset(), heartbeat_ms(), st()) ->
    st()
    | {tx, reference(), _Ctx, st()}
    | {invalid, _Reason, st()}
    | emqx_ds:error(_).
progress(Consumer, SGroup, Partition, Offset, Heartbeat, St) ->
    case St of
        #{{part, Partition} := PartSt0 = #{lease := true}} ->
            Result = progress_part(Consumer, SGroup, Partition, Offset, Heartbeat, PartSt0);
        #{{part, Partition} := #{lease := releasing}} ->
            Result = {invalid, releasing};
        #{} ->
            Result = lease_part(Consumer, SGroup, Partition, Offset, Heartbeat)
    end,
    case Result of
        {tx, Ref, Ret, PartSt} ->
            Ctx = {Ret, Partition, Offset, Heartbeat},
            {tx, Ref, {progress, Ctx}, St#{{part, Partition} => PartSt}};
        PartRet ->
            return_update_part(Partition, PartRet, St)
    end.

lease_part(Consumer, SGroup, Partition, Offset, HB) ->
    case emqx_streams_state_db:lease_partition_async(SGroup, Partition, Consumer, Offset, HB) of
        ok ->
            #{
                lease => true,
                offset_last => Offset,
                offset_committed => Offset,
                heartbeat_last => HB
            };
        {async, Ref, Ret} ->
            {tx, Ref, Ret, #{
                lease => false,
                offset_last => Offset,
                offset_committed => undefined,
                heartbeat_last => HB
            }};
        Other ->
            Other
    end.

progress_part(Consumer, SGroup, Partition, Offset, HB, St = #{offset_last := OffsetLast}) when
    OffsetLast =< Offset
->
    case emqx_streams_state_db:progress_partition_async(SGroup, Partition, Consumer, Offset, HB) of
        ok ->
            ?tp_debug("pdisp_group_progress", #{
                leased => maps:get(lease, St),
                consumer => Consumer,
                partition => Partition,
                offset => Offset
            }),
            {ok, St#{
                lease := true,
                heartbeat_last := HB,
                offset_last := Offset,
                offset_committed := Offset
            }};
        {async, Ref, Ret} ->
            {tx, Ref, Ret, St#{
                offset_last := Offset,
                heartbeat_last := HB
            }};
        Other ->
            Other
    end;
progress_part(_Consumer, _SGroup, _Partition, _Offset, _HB, #{offset_last := OffsetLast}) ->
    {invalid, {offset_going_backwards, OffsetLast}}.

-spec handle_tx_reply(consumer(), streamgroup(), reference(), _Reply, _Ctx, st()) ->
    st()
    | {tx, reference(), _Ctx, st()}
    | {invalid, _Reason, st()}
    | emqx_ds:error(_).
handle_tx_reply(Consumer, SGroup, Ref, Reply, {progress, Ctx}, St) ->
    handle_progress_tx(Consumer, SGroup, Ref, Reply, Ctx, St);
handle_tx_reply(_Consumer, _SGroup, Ref, Reply, {release, Ctx}, St) ->
    handle_release_tx(Ref, Reply, Ctx, St).

-spec handle_progress_tx(consumer(), streamgroup(), reference(), _Reply, _Ctx, st()) ->
    st()
    | {tx, reference(), _Ctx, st()}
    | {invalid, _Reason, st()}
    | emqx_ds:error(_).
handle_progress_tx(Consumer, SGroup, Ref, Reply, Ctx, St0) ->
    {Ret, Partition, Offset, Heartbeat} = Ctx,
    #{{part, Partition} := PartSt0} = St0,
    Result = emqx_streams_state_db:progress_partition_tx_result(Ret, Ref, Reply),
    case handle_part_progress(Result, Consumer, Partition, Offset, Heartbeat, PartSt0) of
        {restart, PartSt} ->
            St = update_part(Partition, PartSt, St0),
            progress(Consumer, SGroup, Partition, Offset, Heartbeat, St);
        PartRet ->
            return_update_part(Partition, PartRet, St0)
    end.

handle_part_progress(Result, Consumer, _Partition, Offset, Heartbeat, St) ->
    #{
        offset_committed := OffsetCommitted,
        heartbeat_last := HBLast
    } = St,
    case Result of
        ok ->
            ?tp_debug("pdisp_group_progress", #{
                leased => maps:get(lease, St),
                consumer => Consumer,
                partition => _Partition,
                offset => Offset
            }),
            St#{
                lease := true,
                heartbeat_last := max(Heartbeat, emqx_maybe:define(HBLast, Heartbeat)),
                offset_committed := max(Offset, emqx_maybe:define(OffsetCommitted, Offset))
            };
        {invalid, {leased, Consumer}} ->
            undefined = OffsetCommitted,
            {restart, St#{lease := true}};
        {invalid, Reason = {leased, _}} ->
            {invalid, Reason, St#{lease := false}};
        {invalid, Reason} ->
            {invalid, Reason, handle_invalid(Reason, St)};
        Other ->
            Other
    end.

release(Consumer, SGroup, Partition, Offset, St) ->
    case St of
        #{{part, Partition} := PartSt0 = #{lease := true, heartbeat_last := HBLast}} ->
            case release_part(Consumer, SGroup, Partition, Offset, HBLast, PartSt0) of
                {tx, Ref, Ret, PartSt} ->
                    Ctx = {Ret, Partition, Offset},
                    {tx, Ref, {release, Ctx}, update_part(Partition, PartSt, St)};
                PartRet ->
                    return_update_part(Partition, PartRet, St)
            end;
        #{{part, Partition} := #{lease := releasing}} ->
            {invalid, releasing};
        #{{part, Partition} := #{lease := false}} ->
            %% TODO if offset updated?
            St;
        #{} ->
            St
    end.

release_part(Consumer, SGroup, Partition, Offset, HBLast, St) ->
    #{offset_last := OffsetLast} = St,
    case OffsetLast =< Offset of
        true ->
            case
                emqx_streams_state_db:release_partition_async(
                    SGroup, Partition, Consumer, Offset, HBLast
                )
            of
                ok ->
                    St#{
                        lease := false,
                        offset_last := Offset,
                        offset_committed := Offset
                    };
                {async, Ref, Ret} ->
                    {tx, Ref, Ret, St#{
                        lease := releasing,
                        offset_last := Offset
                    }};
                Other ->
                    Other
            end;
        false ->
            {invalid, {offset_going_backwards, OffsetLast}}
    end.

-spec handle_release_tx(reference(), _Reply, _Ctx, st()) ->
    st()
    | {invalid, _Reason, st()}
    | emqx_ds:error(_).
handle_release_tx(Ref, Reply, Ctx, St) ->
    {Ret, Partition, Offset} = Ctx,
    #{{part, Partition} := PartSt} = St,
    Result = emqx_streams_state_db:progress_partition_tx_result(Ret, Ref, Reply),
    return_update_part(Partition, handle_part_release(Result, Offset, PartSt), St).

handle_part_release(Result, Offset, St = #{offset_committed := OffsetCommitted}) ->
    case Result of
        ok ->
            St#{
                lease := false,
                offset_committed := max(Offset, emqx_maybe:define(OffsetCommitted, Offset))
            };
        {invalid, {leased, _DifferentConsumer}} ->
            St#{
                lease := false,
                offset_committed := undefined,
                offset_committed_last => OffsetCommitted
            };
        {invalid, Reason} ->
            {invalid, Reason, handle_invalid(Reason, St#{lease := true})};
        Other ->
            Other
    end.

return_update_part(Partition, PartSt = #{}, St) ->
    update_part(Partition, PartSt, St);
return_update_part(Partition, {invalid, Reason, PartSt}, St) ->
    {invalid, Reason, update_part(Partition, PartSt, St)};
return_update_part(_Partition, {invalid, Reason}, St) ->
    {invalid, Reason, St};
return_update_part(_Partition, Error, _St) ->
    Error.

update_part(Partition, PartSt = #{}, St) ->
    St#{{part, Partition} := PartSt}.

handle_invalid({offset_ahead, Offset}, St) ->
    St#{offset_committed_max => Offset};
handle_invalid(_, St) ->
    St.
