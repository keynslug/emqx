%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_pdisp_provision).

-include("emqx_streams_internal.hrl").

-export([
    new/4,
    revision/1,
    propose_changes/1,
    propose_takeovers/1,
    propose_announcements/1,
    current_allocation/2
]).

-type consumer() :: emqx_types:clientid().
-type partition() :: emqx_streams_types:partition().
-type heartbeat_ms() :: emqx_streams_pdisp_group:heartbeat_ms().

-record(provision, {
    db :: dbstate(),
    consumers :: [consumer()],
    allocation :: emqx_streams_allocation:alloc(partition(), consumer()),
    hb_watermark :: heartbeat_ms()
}).

-type dbstate() :: #pdisp_group_st{}.
-type provision() :: #provision{}.
-type revision() :: term() | undefined.

%%

-spec new([partition()], [consumer()], heartbeat_ms(), dbstate()) -> provision().
new(Partitions, LocalConsumers, HBWatermark, DBGroupSt = #pdisp_group_st{consumers = Consumers}) ->
    LiveConsumers = maps:keys(maps:filter(fun(_C, HB) -> HB >= HBWatermark end, Consumers)),
    AllocGlobal = current_allocation(Partitions, DBGroupSt),
    Alloc = lists:foldl(
        fun emqx_streams_allocation:add_member/2,
        AllocGlobal,
        LocalConsumers ++ LiveConsumers
    ),
    #provision{
        db = DBGroupSt,
        consumers = LocalConsumers,
        hb_watermark = HBWatermark,
        allocation = Alloc
    }.

-spec revision(provision()) -> revision().
revision(#provision{db = #pdisp_group_st{revision = Revision}}) ->
    Revision.

-spec propose_changes(provision()) ->
    [{lease | release, partition(), consumer()}].
propose_changes(
    #provision{
        db = #pdisp_group_st{consumers = Consumers},
        consumers = LocalConsumers,
        hb_watermark = HBWatermark,
        allocation = Alloc0
    }
) ->
    LiveConsumers = maps:keys(maps:filter(fun(_C, HB) -> HB >= HBWatermark end, Consumers)),
    Alloc = lists:foldl(
        fun emqx_streams_allocation:add_member/2,
        Alloc0,
        LiveConsumers
    ),
    AllocationsAll = emqx_streams_allocation:allocate([], Alloc),
    Allocations0 = [{lease, P, C} || {P, C} <- AllocationsAll, lists:member(C, LocalConsumers)],
    Allocations = phash_order(node(), Allocations0),
    case Allocations of
        [_ | _] ->
            Allocations;
        [] when AllocationsAll =:= [] ->
            RebalancesAll = emqx_streams_allocation:rebalance([], Alloc),
            Rebalances = [
                {release, P, C}
             || {P, C, _} <- RebalancesAll, lists:member(C, LocalConsumers)
            ],
            case Rebalances of
                [_ | _] ->
                    Rebalances;
                [] ->
                    []
            end;
        [] ->
            []
    end.

propose_announcements(
    #provision{
        consumers = LocalConsumers,
        allocation = Alloc
    }
) ->
    RebalancesAll = emqx_streams_allocation:rebalance([], Alloc),
    case RebalancesAll of
        [_ | _] ->
            [{announce, C} || {_, _, C} <- RebalancesAll, lists:member(C, LocalConsumers)];
        [] ->
            []
    end.

propose_takeovers(
    #provision{
        db = #pdisp_group_st{consumers = Consumers, partitions = PartitionHBs},
        consumers = LocalConsumers,
        hb_watermark = HBWatermark,
        allocation = Alloc
    }
) ->
    DeadConsumers = maps:filter(fun(_C, HB) -> HB < HBWatermark end, Consumers),
    case maps:size(DeadConsumers) of
        0 ->
            [];
        _ ->
            DeadCost = fun(_Partition, C) ->
                HeartbeatLast = maps:get(C, DeadConsumers, HBWatermark),
                max(0, HBWatermark - HeartbeatLast)
            end,
            Rebalances = emqx_streams_allocation:rebalance([DeadCost], Alloc),
            [
                {takeover, Partition, DeadC, maps:get(Partition, PartitionHBs)}
             || {Partition, DeadC, C} <- Rebalances,
                maps:is_key(DeadC, DeadConsumers),
                lists:member(C, LocalConsumers)
            ]
    end.

-spec current_allocation([partition()], dbstate()) ->
    emqx_streams_allocation:t(consumer(), partition()).
current_allocation(Partitions, #pdisp_group_st{leases = Leases}) ->
    maps:fold(
        fun(Partition, Consumer, Alloc0) ->
            Alloc = emqx_streams_allocation:add_member(Consumer, Alloc0),
            emqx_streams_allocation:occupy_resource(Partition, Consumer, Alloc)
        end,
        emqx_streams_allocation:new(Partitions, []),
        Leases
    ).

phash_order(Seed, Es) ->
    %% TODO describe rand purpose
    [E || {_, E} <- lists:sort([{erlang:phash2([Seed | E]), E} || E <- Es])].
