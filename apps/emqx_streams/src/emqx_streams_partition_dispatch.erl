%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_partition_dispatch).

-include("emqx_streams_internal.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_channel.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

-export([
    on_subscription/3,
    on_unsubscription/3,
    on_puback/3,
    on_tx_commit/4,
    on_command/3
    %% TODO
    %% on_terminate/2
]).

%% FIXME
% -export([list_stream_partitions/2]).

-type consumer() :: emqx_types:clientid().
-type group() :: binary().
-type stream() :: binary().
% -type shard() :: binary().
% -type offset() :: non_neg_integer().

-type st() :: #{
    consumer := emqx_types:clientid(),
    {group, group()} => emqx_streams_shard_disp_group:st(),
    {tx, reference()} => _Context,
    {timer, group(), _Command} => reference()
}.

-define(N_CONCURRENT_PROPOSALS, 1).
-define(N_CONCURRENT_TAKEOVERS, 2).
-define(REPROVISION_CONFLICT_TIMEOUT, 1_000).
-define(REPROVISION_RETRY_TIMEOUT, 2_500).

-define(HEARTBEAT_LIFETIME, 30_000).
-define(ANNOUNCEMENT_LIFETIME, 15_000).

-ifdef(TEST).
-undef(HEARTBEAT_LIFETIME).
-undef(ANNOUNCEMENT_LIFETIME).
-define(HEARTBEAT_LIFETIME, 15_000).
-define(ANNOUNCEMENT_LIFETIME, 10_000).
-endif.

%% Protocol interaction

on_subscription(ClientInfo, Topic, St) ->
    case parse_subtopic(Topic) of
        {consume, Group, Stream} ->
            Consumer = maps:get(clientid, ClientInfo),
            on_subscription_consume(Consumer, Group, Stream, St);
        false ->
            protocol_error({subscribe, Topic})
    end.

on_subscription_consume(Consumer, Group, Stream, St0) ->
    SGroup = ?streamgroup(Group, Stream),
    % put('$consumer', Consumer),
    maybe
        undefined ?= sgroup_state(Group, St0),
        {ok, _Partitions} ?= list_stream_partitions(Stream),
        %% Instantiate new group:
        GSt = emqx_streams_pdisp_group:new(),
        St1 = set_consumer(Consumer, St0),
        St = set_sgroup_state(Group, GSt#{stream => Stream}, St1),
        _ = postpone_command(Group, reprovision, "new consumer"),
        {ok, St}
    else
        #{stream := Stream} ->
            %% Already started.
            ok;
        #{stream := StreamAnother} ->
            protocol_error({stream_mismatch, SGroup, StreamAnother});
        {error, Reason} ->
            ?tp(info, "streams_partition_dispatch_start_error", #{
                consumer => Consumer,
                streamgroup => SGroup,
                reason => Reason
            })
    end.

on_unsubscription(_ClientInfo, Topic, St) ->
    case parse_subtopic(Topic) of
        {consume, Stream, Group} ->
            ok;
        false ->
            protocol_error({subscribe, Topic})
    end.

on_puback(PacketId, #message{}, St0) ->
    error(todo).

launch_proposals(Consumer, Group, #{proposals := Proposals}, St) ->
    lists:foldl(
        fun(P, StAcc) -> launch_proposal(Consumer, Group, P, StAcc) end,
        St,
        Proposals
    ).

launch_proposal(Consumer, Group, {Verb, Partition}, St0) ->
    GSt0 = sgroup_state(Group, St0),
    SGroup = streamgroup(Group, GSt0),
    Offset = emqx_streams_state_db:partition_progress_dirty(SGroup, Partition),
    TraceCtx = #{
        consumer => Consumer,
        streamgroup => SGroup,
        proposal => Verb,
        partition => Partition,
        offset => Offset
    },
    %% Deannounce consumer once has at least 1 leased shard:
    % GSt1 = deannounce_slogger(Consumer, SGroup, GSt0),
    case Verb of
        lease ->
            Ret = progress(Consumer, SGroup, Partition, Offset, GSt0);
        release ->
            Ret = emqx_streams_pdisp_group:release(Consumer, SGroup, Partition, Offset, GSt0)
    end,
    case Ret of
        {tx, Ref, Ctx, GSt} ->
            ?tp(debug, "streams_partition_dispatch_tx_started", TraceCtx#{tx => Ref}),
            St = update_sgroup_state(Group, GSt, St0),
            stash_tx(Ref, {Verb, Group, Ctx}, St);
        Outcome ->
            handle_request_outcome(Consumer, Group, Verb, Outcome, TraceCtx, St0)
    end.

progress(Consumer, SGroup, Partition, Offset, GSt) ->
    HB = heartbeat(),
    emqx_streams_pdisp_group:progress(Consumer, SGroup, Partition, Offset, HB, GSt).

on_tx_commit(Ref, Reply, RetAcc, St0) ->
    Consumer = consumer(St0),
    case pop_tx(Ref, St0) of
        {{takeover, Group, Ctx}, St1} ->
            St = handle_takeover_tx_commit(Consumer, Group, Ref, Reply, Ctx, St1),
            {stop, RetAcc, St};
        {{Verb, Group, Ctx}, St1} ->
            St = handle_tx_commit(Consumer, Group, Verb, Ref, Reply, Ctx, St1),
            {stop, RetAcc, St};
        error ->
            %% Not our transaction apparently:
            ok
    end.

handle_tx_commit(Consumer, Group, Verb, Ref, Reply, Ctx, St0) ->
    GSt0 = sgroup_state(Group, St0),
    SGroup = streamgroup(Group, GSt0),
    TraceCtx = #{
        consumer => Consumer,
        streamgroup => SGroup,
        proposal => Verb,
        tx => Ref
    },
    case emqx_streams_pdisp_group:handle_tx_reply(Consumer, SGroup, Ref, Reply, Ctx, GSt0) of
        {tx, NRef, NCtx, GSt} ->
            ?tp(debug, "streams_partition_dispatch_progress_tx_restarted", TraceCtx#{tx => NRef}),
            St = update_sgroup_state(Group, GSt, St0),
            stash_tx(NRef, {Verb, Group, NCtx}, St);
        Outcome ->
            handle_request_outcome(Consumer, Group, Verb, Outcome, TraceCtx, St0)
    end.

handle_request_outcome(Consumer, Group, progress, Ret, TraceCtx, St) ->
    handle_progress_outcome(Consumer, Group, Ret, TraceCtx, St);
handle_request_outcome(Consumer, Group, release, Ret, TraceCtx, St) ->
    handle_release_outcome(Consumer, Group, Ret, TraceCtx, St).

handle_progress_outcome(_Consumer, Group, GSt = #{}, TraceCtx, St0) ->
    ?tp(debug, "streams_partition_dispatch_progress_success", TraceCtx),
    case GSt of
        #{proposed := []} ->
            %% FIXME
            _ = postpone_command(Group, reprovision, "no proposals");
        #{proposed := [_ | _]} ->
            ok
    end,
    update_sgroup_state(Group, GSt, St0);
handle_progress_outcome(Consumer, Group, {invalid, Reason, GSt0}, TraceCtx, St0) ->
    %% FIXME loglevel
    ?tp(notice, "streams_partition_dispatch_progress_invalid", TraceCtx#{reason => Reason}),
    GSt = invalidate_proposals(GSt0),
    St1 = update_sgroup_state(Group, GSt, St0),
    case Reason of
        {leased, _} ->
            reannounce_retry(Consumer, Group, ?REPROVISION_CONFLICT_TIMEOUT, St1);
        conflict ->
            reannounce_retry(Consumer, Group, ?REPROVISION_CONFLICT_TIMEOUT, St1);
        _ ->
            St1
    end;
handle_progress_outcome(_Consumer, _Group, Error, TraceCtx, St) ->
    %% FIXME error handling
    ?tp(warning, "streams_partition_dispatch_progress_error", TraceCtx#{reason => Error}),
    St.

handle_release_outcome(_Consumer, Group, GSt = #{}, TraceCtx, St0) ->
    ?tp(debug, "streams_partition_dispatch_release_success", TraceCtx),
    update_sgroup_state(Group, GSt, St0);
handle_release_outcome(_Consumer, Group, {invalid, Reason, GSt0}, TraceCtx, St0) ->
    ?tp(notice, "streams_partition_dispatch_release_invalid", TraceCtx#{reason => Reason}),
    GSt = invalidate_proposals(GSt0),
    update_sgroup_state(Group, GSt, St0);
handle_release_outcome(_Consumer, _Group, Error, TraceCtx, St) ->
    %% FIXME error handling
    ?tp(warning, "streams_partition_dispatch_release_error", TraceCtx#{reason => Error}),
    St.

on_command(#pdisp_command{group = Group, c = reprovision}, RetAcc, St0) ->
    Consumer = consumer(St0),
    GSt0 = #{stream := Stream} = sgroup_state(Group, St0),
    case reprovision(Consumer, Group, Stream, GSt0) of
        GSt = #{} ->
            St1 = update_sgroup_state(Group, GSt, St0),
            St2 = launch_proposals(Consumer, Group, GSt, St1),
            St = launch_takeovers(Consumer, Group, GSt, St2),
            {stop, RetAcc, St};
        skipped ->
            {stop, RetAcc};
        empty ->
            St = reannounce_retry(Consumer, Group, St0),
            {stop, RetAcc, St}
    end.

reprovision(_Consumer, _Group, _Stream, #{proposed := [_ | _]}) ->
    skipped;
reprovision(Consumer, Group, Stream, GSt) ->
    %% FIXME error handling
    {ok, Partitions} = list_stream_partitions(Stream),
    DBGroupSt = emqx_streams_state_db:group_state_dirty(?streamgroup(Group, Stream)),
    HBWatermark = timestamp_ms(),
    Provision = emqx_streams_pdisp_group:new_provision(Partitions, HBWatermark, DBGroupSt),
    provision(Consumer, Provision, GSt).

provision(Consumer, Provision, GSt) ->
    %% TODO too many consumers?
    Provisional = emqx_streams_pdisp_group:provision_changes(Consumer, Provision),
    case propose_leases(Provisional) of
        [_ | _] = Proposals ->
            %% NOTE piggyback
            GSt#{proposed => Proposals};
        [] ->
            case provision_takeovers(Consumer, Provision, GSt) of
                Ret when Ret =/= empty ->
                    Ret;
                empty ->
                    case propose_releases(Provisional) of
                        [_ | _] = Proposals ->
                            %% NOTE piggyback
                            GSt#{proposed => Proposals};
                        [] ->
                            empty
                    end
            end
    end.

propose_leases(Provisions) ->
    ProvisionalLeases = [L || L = {lease, _} <- Provisions],
    lists:sublist(ProvisionalLeases, ?N_CONCURRENT_PROPOSALS).

provision_takeovers(_Consumer, _Provision, #{takeovers := [_ | _]}) ->
    skipped;
provision_takeovers(Consumer, Provision, GSt) ->
    Provisional = emqx_streams_pdisp_group:provision_takeovers(Consumer, Provision),
    case propose_takeovers(Provisional) of
        [_ | _] = Takeovers ->
            %% NOTE piggyback
            GSt#{takeovers => Takeovers};
        [] ->
            empty
    end.

propose_takeovers(Provisions) ->
    lists:sublist(Provisions, ?N_CONCURRENT_TAKEOVERS).

propose_releases(Provisions) ->
    ProvisionalReleases = [L || L = {release, _} <- Provisions],
    lists:sublist(ProvisionalReleases, ?N_CONCURRENT_PROPOSALS).

remove_proposal(Proposal, GSt) ->
    Proposals = maps:get(proposed, GSt, []),
    GSt#{proposed => Proposals -- [Proposal]}.

invalidate_proposals(GSt) ->
    GSt#{proposed => []}.

remove_takeover(Partition, GSt) ->
    Takeovers = maps:get(takeovers, GSt, []),
    GSt#{takeovers => lists:keydelete(Partition, 2, Takeovers)}.

%% Takeovers

launch_takeovers(Consumer, Group, #{takeovers := Takeovers}, St0) ->
    lists:foldl(
        fun(T, St) -> launch_takeover(Consumer, Group, T, St) end,
        St0,
        Takeovers
    ).

launch_takeover(Consumer, Group, {takeover, Partition, DeadConsumer, HB}, St) ->
    SGroup = streamgroup(Group, sgroup_state(Group, St)),
    TraceCtx = #{
        consumer => Consumer,
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
            ?tp(debug, "streams_partition_dispatch_takeover_tx_started", TraceCtx#{tx => Ref}),
            stash_tx(Ref, {takeover, Group, {Ret, Partition, DeadConsumer}}, St);
        Outcome ->
            handle_takeover_outcome(Group, Partition, Outcome, TraceCtx, St)
    end.

handle_takeover_tx_commit(Consumer, Group, Ref, Reply, {Ret, Partition, DeadConsumer}, St) ->
    TraceCtx = #{
        consumer => Consumer,
        % streamgroup => sgroup(Group, St),
        partition => Partition,
        from => DeadConsumer
    },
    Outcome = emqx_streams_state_db:progress_partition_tx_result(Ret, Ref, Reply),
    handle_takeover_outcome(Group, Partition, Outcome, TraceCtx, St).

handle_takeover_outcome(Group, Partition, Ret, TraceCtx, St) ->
    case Ret of
        ok ->
            ?tp(debug, "streams_partition_dispatch_takeover_success", TraceCtx);
        {invalid, Reason} ->
            ?tp(notice, "streams_partition_dispatch_takeover_invalid", TraceCtx#{reason => Reason});
        Error ->
            ?tp(warning, "streams_partition_dispatch_takeover_error", TraceCtx#{reason => Error})
    end,
    GSt = remove_takeover(Partition, sgroup_state(Group, St)),
    update_sgroup_state(Group, GSt, St).

%% Announcements

reannounce_retry(Consumer, Group, St) ->
    reannounce_retry(Consumer, Group, ?REPROVISION_RETRY_TIMEOUT, St).

reannounce_retry(Consumer, Group, RetryTimeout, St0) ->
    GSt0 = sgroup_state(Group, St0),
    case emqx_streams_pdisp_group:n_leases(GSt0) of
        0 ->
            %% Make others aware of us:
            GSt = announce_myself(Consumer, Group, GSt0),
            St = update_sgroup_state(Group, GSt, St0),
            schedule_command(RetryTimeout, Group, reprovision, "reannounce", St);
        _ ->
            %% Wait for rebalance:
            St0
    end.

announce_myself(Consumer, Group, GSt0) ->
    SGroup = streamgroup(Group, GSt0),
    Lifetime = ?ANNOUNCEMENT_LIFETIME,
    HB = timestamp_ms() + Lifetime,
    case emqx_streams_pdisp_group:announce(Consumer, SGroup, HB, Lifetime, GSt0) of
        GSt = #{} ->
            GSt;
        Error ->
            ?tp(info, "streams_partition_dispatch_announce_error", #{
                consumer => Consumer,
                streamgroup => SGroup,
                reason => Error
            }),
            GSt0
    end.

deannounce_slogger(Consumer, SGroup, GSt) ->
    case emqx_streams_pdisp_group:n_leases(GSt) of
        0 ->
            GSt;
        _ ->
            deannounce_myself(Consumer, SGroup, GSt)
    end.

deannounce_myself(Consumer, SGroup, GSt0) ->
    case emqx_streams_pdisp_group:deannounce(Consumer, SGroup, GSt0) of
        GSt = #{} ->
            GSt;
        Error ->
            ?tp(info, "streams_partition_dispatch_deannounce_error", #{
                consumer => Consumer,
                streamgroup => SGroup,
                reason => Error
            }),
            GSt0
    end.

%%

protocol_error(Details) ->
    error({?MODULE, protocol_error, Details}).

heartbeat() ->
    timestamp_ms() + ?HEARTBEAT_LIFETIME.

timestamp_ms() ->
    erlang:system_time(millisecond).

%% State

-spec set_consumer(consumer(), emqx_maybe:t(st())) -> st().
set_consumer(Consumer, St = #{}) ->
    St#{consumer => Consumer};
set_consumer(Consumer, undefined) ->
    #{consumer => Consumer}.

-spec consumer(emqx_maybe:t(st())) -> consumer() | undefined.
consumer(#{consumer := Consumer}) ->
    Consumer;
consumer(_St) ->
    undefined.

-spec streamgroup(group(), emqx_streams_shard_disp_group:st()) ->
    emqx_streams_shard_disp_group:streamgroup().
streamgroup(Group, #{stream := Stream}) ->
    ?streamgroup(Group, Stream).

-spec sgroup_state(group(), emqx_maybe:t(st())) ->
    emqx_streams_shard_disp_group:st() | undefined.
sgroup_state(Group, St) ->
    case St of
        #{{group, Group} := GroupSt} ->
            GroupSt;
        _ ->
            undefined
    end.

-spec set_sgroup_state(group(), emqx_streams_shard_disp_group:st(), st()) ->
    st().
set_sgroup_state(Group, GroupSt, St = #{}) ->
    St#{{group, Group} => GroupSt}.

-spec update_sgroup_state(group(), emqx_streams_shard_disp_group:st(), st()) ->
    st().
update_sgroup_state(Group, GroupSt, St = #{}) ->
    St#{{group, Group} := GroupSt}.

stash_tx(Ref, Context, St = #{}) ->
    K = {tx, Ref},
    false = maps:is_key(K, St),
    St#{K => Context}.

pop_tx(Ref, St = #{}) ->
    maps:take({tx, Ref}, St).

postpone_command(Group, Command) ->
    postpone_command(Group, Command, undefined).

postpone_command(Group, Command, Context) ->
    self() ! #pdisp_command{group = Group, c = Command, context = Context}.

schedule_command(Timeout, Group, Command, Context, St) ->
    Timer = {timer, Group, Command},
    ok = emqx_utils:cancel_timer(maps:get(Timer, St, undefined)),
    TRef = erlang:send_after(
        Timeout,
        self(),
        #pdisp_command{group = Group, c = Command, context = Context}
    ),
    St#{Timer => TRef}.

%% Protocol structures

parse_subtopic(Topic) when is_binary(Topic) ->
    parse_subtopic(emqx_topic:tokens(Topic));
parse_subtopic([_SDisp, <<"consume">>, Group | StreamTokens]) ->
    Stream = emqx_topic:join(StreamTokens),
    {consume, Group, Stream};
parse_subtopic(_) ->
    false.

%%

list_stream_partitions(Stream) ->
    %% FIXME
    {ok, lists:map(fun integer_to_binary/1, lists:seq(1, 16))}.
