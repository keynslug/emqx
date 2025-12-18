%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_pdisp_local_group).

-include("emqx_streams_internal.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("emqx/include/logger.hrl").

-export([
    join/3,
    join/2,
    leave/2,
    progressed/4,
    released/3
]).

-export([
    start_link/2
]).

-behaviour(gen_server).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-type partition() :: emqx_streams_types:partition().
-type offset() :: non_neg_integer().
-type heartbeat_ms() :: non_neg_integer().

-define(N_CONCURRENT_PROPOSALS, 2).
-define(HEARTBEAT_LIFETIME, 30_000).
-define(ANNOUNCEMENT_LIFETIME, 15_000).

-ifdef(TEST).
-undef(HEARTBEAT_LIFETIME).
-undef(ANNOUNCEMENT_LIFETIME).
-define(HEARTBEAT_LIFETIME, 15_000).
-define(ANNOUNCEMENT_LIFETIME, 10_000).
-endif.

-define(TIMEOUT_PROVISION, 5_000).
-define(TIMEOUT_PROVISION_FORCE, 100).
-define(TIMEOUT_FLUSH_PROGRESS, 5_000).
% -define(TIMEOUT_FLUSH_PROGRESS_FORCE, 100).

-define(name(SGROUP), {n, l, {?MODULE, SGROUP}}).

%%

-record(call_group_join, {consumer, pid}).
-record(call_group_leave, {consumer}).
-record(cast_progressed, {consumer, partition, offset}).
-record(cast_released, {consumer, partition}).

join(Group, Stream, Consumer) ->
    ensure_call(Group, Stream, #call_group_join{consumer = Consumer, pid = self()}).

join(Pid, Consumer) ->
    try_call(Pid, #call_group_join{consumer = Consumer, pid = self()}).

leave(Pid, Consumer) ->
    try_call(Pid, #call_group_leave{consumer = Consumer}).

progressed(Pid, Consumer, Partition, Offset) ->
    gen_server:cast(
        Pid,
        #cast_progressed{consumer = Consumer, partition = Partition, offset = Offset}
    ).

released(Pid, Consumer, Partition) ->
    gen_server:cast(
        Pid,
        #cast_released{consumer = Consumer, partition = Partition}
    ).

ensure_call(Group, Stream, Call) ->
    case try_call(Group, Stream, Call) of
        {error, noproc} ->
            Pid = ensure_started(Group, Stream),
            try_call(Pid, Call);
        Reply ->
            Reply
    end.

try_call(Group, Stream, Call) ->
    case gproc:whereis_name(?name(?streamgroup(Group, Stream))) of
        Pid when is_pid(Pid) ->
            try_call(Pid, Call);
        undefined ->
            {error, noproc}
    end.

try_call(Pid, Call) when is_pid(Pid) ->
    try
        gen_server:call(Pid, Call, infinity)
    catch
        exit:{noproc, _} -> {error, noproc};
        exit:{shutdown, _} -> {error, noproc};
        exit:{{shutdown, _}, _} -> {error, noproc}
    end.

% try_cast(Group, Stream, Cast) ->
%     case gproc:whereis_name(?name(?streamgroup(Group, Stream))) of
%         Pid when is_pid(Pid) ->
%             gen_server:cast(Pid, Cast);
%         undefined ->
%             {error, noproc}
%     end.

ensure_started(Group, Stream) ->
    error(todo).

%%

start_link(Group, Stream) ->
    ServerName = ?name(?streamgroup(Group, Stream)),
    gen_server:start_link({via, gproc, ServerName}, ?MODULE, {Group, Stream}, []).

%%

-record(st, {
    stream,
    group,
    consumers,
    leases,
    group_revision,
    proposal_queue,
    % txs,
    pending,
    timer_provision,
    timer_progress
}).

-record(consumer, {
    pid,
    monitor,
    announcement
    % leases
}).

-record(lease, {
    status,
    consumer,
    offset_last,
    offset_committed,
    heartbeat_last
}).

-record(pending_takeover, {partition, from}).

-record(timeout_provision, {}).
-record(timeout_progress, {}).

init({Group, Stream}) ->
    logger:set_process_metadata(#{
        streamgroup => ?streamgroup(Group, Stream)
    }),
    {ok, #st{
        group = Group,
        stream = Stream,
        consumers = #{},
        leases = #{},
        proposal_queue = [],
        pending = #{}
    }}.

handle_call(#call_group_join{consumer = Consumer, pid = Pid}, _From, St0) ->
    case group_join(Consumer, Pid, St0) of
        {ok, St} ->
            {reply, {ok, self()}, St};
        {error, _} = Error ->
            {reply, Error, St0}
    end;
handle_call(#call_group_leave{consumer = Consumer}, _From, St0) ->
    case group_leave(Consumer, St0) of
        {ok, St} ->
            {reply, ok, St};
        {error, _} = Error ->
            {reply, Error, St0}
    end.

handle_cast(#cast_progressed{consumer = Consumer, partition = Partition, offset = Offset}, St0) ->
    St1 = update_progress(Consumer, Partition, Offset, St0),
    St = schedule_flush_progress(St1),
    {noreply, St};
handle_cast(#cast_released{consumer = Consumer, partition = Partition}, St0) ->
    St = consumer_release(Partition, Consumer, St0),
    {noreply, St};
handle_cast(Cast, St) ->
    ?SLOG(warning, "streams_pdisp_group_unexpected_cast", #{cast => Cast}),
    {noreply, St}.

handle_info(#timeout_progress{}, St0) ->
    St1 = flush_progress(St0),
    St = schedule_flush_progress(St1),
    {noreply, St};
handle_info(#timeout_provision{}, St0) ->
    St1 = try_provision(St0),
    St2 = kickoff_proposals(St1),
    St = try_schedule_next_provision(St2),
    {noreply, St};
handle_info(?ds_tx_commit_reply(Ref, _) = Reply, St0) ->
    case on_tx_commit(Ref, Reply, St0) of
        St1 = #st{} ->
            St = try_schedule_next_provision(St1),
            {noreply, St};
        unknown ->
            handle_info_cont(Reply, St0)
    end;
handle_info(Msg, St) ->
    handle_info_cont(Msg, St).

handle_info_cont({'DOWN', MRef, process, Pid, _}, St) ->
    case lookup_consumer_by_mref(MRef, St) of
        undefined ->
            ?SLOG(warning, "streams_pdisp_group_unexpected_down", #{
                streamgroup => streamgroup(St),
                pid => Pid,
                monitor => MRef
            }),
            {noreply, St};
        Consumer ->
            {noreply, on_consumer_down(Consumer, St)}
    end;
handle_info_cont(Msg, St) ->
    ?SLOG(warning, "streams_pdisp_group_unexpected_info", #{msg => Msg}),
    {noreply, St}.

terminate(_Reason, St) ->
    %% FIXME
    % on_group_down(St).
    error(todo).

%%

group_join(Consumer, Pid, St0) ->
    maybe
        undefined ?= lookup_consumer(Consumer, St0),
        ConsumerSt = new_consumer(Pid),
        St1 = set_consumer(Consumer, ConsumerSt, St0),
        St2 = force_provision(St1),
        {ok, St2}
    else
        #consumer{} ->
            {error, exists}
    end.

group_leave(Consumer, St0) ->
    %% FIXME
    error(todo).

%%

try_provision(St = #st{proposal_queue = PQ}) when PQ =/= [] ->
    St;
try_provision(St = #st{group_revision = RevisionLast}) ->
    %% FIXME error handling
    case emqx_streams_state_db:group_revision_dirty(streamgroup(St)) of
        RevisionLast ->
            St;
        _RevisionCurrent ->
            provision(St)
    end.

provision(St0 = #st{stream = Stream}) ->
    %% FIXME error handling
    {ok, Partitions} = list_stream_partitions(Stream),
    DBSt = emqx_streams_state_db:group_state_dirty(streamgroup(St0)),
    HBWatermark = timestamp_ms(),
    Consumers = list_consumers(St0),
    Provision = emqx_streams_pdisp_provision:new(Partitions, Consumers, HBWatermark, DBSt),
    Revision = emqx_streams_pdisp_provision:revision(Provision),
    St = St0#st{group_revision = Revision},
    propose(Provision, St#st{group_revision = Revision}).

propose(Provision, St) ->
    Changes = emqx_streams_pdisp_provision:propose_changes(Provision),
    case Changes of
        [_ | _] ->
            enqueue_proposals(Changes, St);
        [] ->
            propose_takeovers(Provision, St)
    end.

propose_takeovers(Provision, St) ->
    Takeovers = emqx_streams_pdisp_provision:propose_takeovers(Provision),
    case Takeovers of
        [_ | _] ->
            enqueue_proposals(Takeovers, St);
        [] ->
            propose_announcements(Provision, St)
    end.

propose_announcements(Provision, St) ->
    enqueue_proposals(emqx_streams_pdisp_provision:propose_announcements(Provision), St).

enqueue_proposals(Ps, St) ->
    St#st{proposal_queue = Ps}.

invalidate_proposals(St) ->
    St#st{proposal_queue = []}.

kickoff_proposals(St) ->
    kickoff_proposals(?N_CONCURRENT_PROPOSALS, St).

kickoff_proposals(N, St = #st{pending = Pending}) when map_size(Pending) >= N ->
    St;
kickoff_proposals(N, St = #st{proposal_queue = [Proposal | PQRest]}) when N > 0 ->
    kickoff_proposals(N - 1, start_proposal(Proposal, St#st{proposal_queue = PQRest}));
kickoff_proposals(_, St) ->
    St.

start_proposal(Proposal, St) ->
    case Proposal of
        {lease, Partition, Consumer} ->
            try_lease(Partition, Consumer, St);
        {release, Partition, Consumer} ->
            ask_consumer_release(Partition, Consumer, St);
        {takeover, Partition, DeadConsumer, HB} ->
            try_takeover(Partition, DeadConsumer, HB, St);
        {announce, Consumer} ->
            announce_consumer(Consumer, St)
    end.

schedule_provision(St = #st{timer_provision = undefined}) ->
    TRef = erlang:start_timer(?TIMEOUT_PROVISION, self(), #timeout_provision{}),
    St#st{timer_provision = TRef};
schedule_provision(St = #st{timer_provision = _TRef}) ->
    St.

force_provision(St = #st{timer_provision = TRef0}) ->
    ok = emqx_utils:cancel_timer(TRef0),
    TRef = erlang:start_timer(?TIMEOUT_PROVISION_FORCE, self(), #timeout_provision{}),
    St#st{timer_provision = TRef}.

try_schedule_next_provision(St) ->
    case St of
        #st{timer_provision = TRef} when is_reference(TRef) ->
            St;
        #st{pending = Pending} when map_size(Pending) > 0 ->
            St;
        #st{consumers = Consumers} when map_size(Consumers) =:= 0 ->
            St;
        #st{} ->
            schedule_provision(St)
    end.

%% Progress

update_progress(Consumer, Partition, Offset, St) ->
    case lookup_lease(Partition, St) of
        #lease{consumer = Consumer, status = S, offset_last = Last} = Lease when
            Last =< Offset andalso (S =:= live orelse S =:= leasing)
        ->
            set_lease(Partition, Lease#lease{status = live, offset_last = Offset}, St);
        Lease ->
            kill_consumer(Consumer, {pdisp_invalid_progress, Offset, Lease}, St),
            St
    end.

flush_progress(St = #st{leases = Leases0}) ->
    SGroup = streamgroup(St),
    HB = timestamp_ms() + ?HEARTBEAT_LIFETIME,
    Updates = compute_progress_update(Leases0),
    case emqx_streams_state_db:progress_partitions(SGroup, Updates, HB) of
        ok ->
            Leases = apply_progress_update(Updates, HB, Leases0),
            St#st{leases = Leases};
        {invalid, Conflict} ->
            error({pdisp_progress_conflict, SGroup, Conflict});
        Error ->
            ?tp(warning, "streams_partition_dispatch_progress_error", #{
                streamgroup => SGroup,
                reason => Error
            }),
            %% NOTE: Retrying later.
            St
    end.

compute_progress_update(Leases) ->
    Updates = maps:fold(fun lease_dirty_update/3, [], Leases),
    UpdatesIdle = maps:fold(
        fun(P, L, Acc) -> lease_idle_update(P, L, Updates, Acc) end,
        [],
        Leases
    ),
    UpdatesIdle ++ Updates.

lease_dirty_update(Partition, Lease = #lease{consumer = Consumer}, Acc) ->
    case lease_is_dirty(Lease) of
        {true, Offset} ->
            [{Partition, Consumer, Offset} | Acc];
        false ->
            Acc
    end.

lease_idle_update(Partition, Lease = #lease{consumer = Consumer}, Updates, Acc) ->
    case Lease of
        %% Offset unchanged:
        #lease{status = live, offset_last = Offset, offset_committed = Offset} ->
            case lists:keymember(Consumer, 2, Updates) of
                %% Consumer has not progressed any other partition:
                false ->
                    %% Inject idle progress update:
                    lists:keystore(Consumer, 2, Acc, {Partition, Consumer, Offset});
                true ->
                    Acc
            end;
        _ ->
            Acc
    end.

lease_is_dirty(#lease{
    status = Status,
    offset_last = Offset,
    offset_committed = OffsetCommitted
}) when Offset < OffsetCommitted ->
    %% FIXME assertion
    live = Status,
    {true, Offset};
lease_is_dirty(_) ->
    false.

apply_progress_update([{P, _, Offset} | Rest], HB, Leases) ->
    Lease0 = maps:get(P, Leases),
    Lease = Lease0#lease{offset_committed = Offset, heartbeat_last = HB},
    apply_progress_update(Rest, HB, Leases#{P => Lease});
apply_progress_update([], _, Leases) ->
    Leases.

schedule_flush_progress(St = #st{timer_progress = undefined}) ->
    TRef = erlang:send_after(?TIMEOUT_FLUSH_PROGRESS, self(), #timeout_progress{}),
    St#st{timer_progress = TRef};
schedule_flush_progress(St = #st{timer_progress = _TRef}) ->
    St.

%% Leases

try_lease(Partition, Consumer, St) ->
    case lookup_lease(Partition, St) of
        undefined ->
            lease(Partition, Consumer, St);
        #lease{status = released} ->
            %% Previously released:
            lease(Partition, Consumer, St);
        #lease{status = live, consumer = Consumer} ->
            %% Idempotence:
            St;
        Lease ->
            ?tp(warning, "streams_partition_dispatch_proposed_lease_inconsistent", #{
                consumer => Consumer,
                partition => Partition,
                %% TODO friendlier log
                existing => Lease
            }),
            %% NOTE: Retrying on next provision.
            St
    end.

lease(Partition, Consumer, St0) ->
    SGroup = streamgroup(St0),
    Offset0 = emqx_streams_state_db:partition_progress_dirty(SGroup, Partition),
    Offset = emqx_maybe:define(Offset0, 0),
    HB = timestamp_ms() + ?HEARTBEAT_LIFETIME,
    Lease = #lease{
        status = leasing,
        consumer = Consumer,
        offset_last = Offset,
        heartbeat_last = HB
    },
    case emqx_streams_state_db:lease_partition_async(SGroup, Partition, Consumer, Offset, HB) of
        {async, Ref, Ret} ->
            St = set_lease(Partition, Lease, St0),
            Cont = {fun handle_lease_tx_commit/5, [Partition, Ret]},
            stash_tx(Ref, Cont, St);
        Outcome ->
            handle_lease_outcome(Outcome, Partition, Lease, St0)
    end.

handle_lease_tx_commit(Partition, Ret, Ref, Reply, St) ->
    Lease = lookup_lease(Partition, St),
    Outcome = emqx_streams_state_db:partition_tx_result(Ret, Ref, Reply),
    handle_lease_outcome(Outcome, Partition, Lease, St).

handle_lease_outcome(Outcome, Partition, Lease0, St) ->
    #lease{
        consumer = Consumer,
        offset_last = Offset
    } = Lease0,
    TraceCtx = #{
        streamgroup => streamgroup(St),
        partition => Partition,
        consumer => Consumer
    },
    case Outcome of
        ok ->
            ?tp(debug, "streams_partition_dispatch_lease_success", TraceCtx),
            Lease1 = Lease0#lease{status = leased},
            Lease = lease_update_committed(Lease1),
            send_consumer(Consumer, {leased, Partition, Offset}, St),
            deannounce_consumer(Consumer, set_lease(Partition, Lease, St));
        %% FIXME
        %% {invalid, {leased, _, Consumer}} ->
        %%     ?tp(debug, "streams_partition_dispatch_lease_self_conflict", TraceCtx),
        {invalid, Reason} ->
            ?tp(notice, "streams_partition_dispatch_lease_invalid", TraceCtx#{reason => Reason}),
            invalidate_proposals(forget_lease(Partition, St));
        Error ->
            ?tp(warning, "streams_partition_dispatch_lease_error", TraceCtx#{reason => Error}),
            %% NOTE: Retrying on next provision.
            St
    end.

lease_update_committed(Lease = #lease{offset_last = Last, offset_committed = Committed}) ->
    true = Last >= Committed,
    Lease#lease{offset_committed = Last}.

%% Releases

ask_consumer_release(Partition, Consumer, St) ->
    case lookup_lease(Partition, St) of
        #lease{consumer = Consumer, status = live} ->
            send_consumer(Consumer, {release, Partition}, St);
        Lease ->
            ?tp(warning, "streams_partition_dispatch_proposal_release_inconsistent", #{
                consumer => Consumer,
                partition => Partition,
                %% TODO friendlier log
                existing => Lease
            })
    end,
    St.

consumer_release(Partition, Consumer, St) ->
    case lookup_lease(Partition, St) of
        #lease{consumer = Consumer} = Lease ->
            ensure_release(Partition, Lease, St);
        Lease ->
            kill_consumer(Consumer, {pdisp_invalid_release, Partition, Lease}, St)
    end.

ensure_release(Partition, Lease, St) ->
    case Lease of
        #lease{status = live} = Lease ->
            release(Partition, Lease, St);
        #lease{status = leasing} = Lease ->
            release(Partition, Lease, St);
        #lease{status = released} ->
            %% Previously released:
            St;
        #lease{status = releasing} ->
            %% Currently releasing:
            St
    end.

release(Partition, Lease, St0) ->
    #lease{
        consumer = Consumer,
        offset_last = Offset,
        heartbeat_last = HBLast
    } = Lease,
    SGroup = streamgroup(St0),
    case
        emqx_streams_state_db:release_partition_async(SGroup, Partition, Consumer, Offset, HBLast)
    of
        {async, Ref, Ret} ->
            St = set_lease(Partition, Lease#lease{status = releasing}, St0),
            Cont = {fun handle_release_tx_commit/5, [Partition, Ret]},
            stash_tx(Ref, Cont, St);
        Outcome ->
            handle_release_outcome(Outcome, Partition, Lease, St0)
    end.

handle_release_tx_commit(Partition, Ret, Ref, Reply, St) ->
    Lease = lookup_lease(Partition, St),
    Outcome = emqx_streams_state_db:partition_tx_result(Ret, Ref, Reply),
    handle_lease_outcome(Outcome, Partition, Lease, St).

handle_release_outcome(Outcome, Partition, Lease0 = #lease{consumer = Consumer}, St) ->
    TraceCtx = #{
        streamgroup => streamgroup(St),
        partition => Partition,
        consumer => Consumer
    },
    case Outcome of
        ok ->
            ?tp(debug, "streams_partition_dispatch_release_success", TraceCtx),
            Lease1 = Lease0#lease{status = released, consumer = undefined},
            Lease = lease_update_committed(Lease1),
            force_provision(set_lease(Partition, Lease, St));
        {invalid, Reason} ->
            ?tp(notice, "streams_partition_dispatch_release_invalid", TraceCtx#{reason => Reason}),
            invalidate_proposals(forget_lease(Partition, St));
        Error ->
            ?tp(warning, "streams_partition_dispatch_release_error", TraceCtx#{reason => Error}),
            %% TODO retries
            St
    end.

on_consumer_down(Consumer, St0 = #st{leases = Leases}) ->
    ConsumerLeases = maps:filter(fun(_, #lease{consumer = C}) -> C =:= Consumer end, Leases),
    maps:fold(
        fun(Partition, Lease, St) -> release(Partition, Lease, St) end,
        St0,
        ConsumerLeases
    ).

%% Takeovers

try_takeover(Partition, DeadConsumer, HB, St) ->
    SGroup = streamgroup(St),
    TraceCtx = #{
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
            Pending = #pending_takeover{partition = Partition, from = DeadConsumer},
            Cont = {fun handle_takeover_tx_commit/5, [Pending, Ret]},
            stash_tx(Ref, Cont, St);
        Outcome ->
            handle_takeover_outcome(Outcome, TraceCtx, St)
    end.

handle_takeover_tx_commit(Takeover, Ret, Ref, Reply, St) ->
    TraceCtx = #{
        streamgroup => streamgroup(St),
        partition => Takeover#pending_takeover.partition,
        from => Takeover#pending_takeover.from
    },
    Outcome = emqx_streams_state_db:partition_tx_result(Ret, Ref, Reply),
    handle_takeover_outcome(Outcome, TraceCtx, St).

handle_takeover_outcome(Outcome, TraceCtx, St) ->
    case Outcome of
        ok ->
            ?tp(debug, "streams_partition_dispatch_takeover_success", TraceCtx);
        {invalid, Reason} ->
            ?tp(notice, "streams_partition_dispatch_takeover_invalid", TraceCtx#{reason => Reason});
        Error ->
            ?tp(warning, "streams_partition_dispatch_takeover_error", TraceCtx#{reason => Error})
    end,
    St.

%% Announcements

announce_consumer(Consumer, St) ->
    SGroup = streamgroup(St),
    Lifetime = ?HEARTBEAT_LIFETIME,
    Heartbeat = timestamp_ms() + Lifetime,
    CSt0 = #consumer{} = lookup_consumer(Consumer, St),
    case announce_consumer(SGroup, Consumer, Heartbeat, Lifetime, CSt0) of
        CSt = #consumer{} ->
            set_consumer(Consumer, CSt, St);
        Error ->
            ?tp(warning, "streams_partition_dispatch_announce_error", #{
                streamgroup => SGroup,
                consumer => Consumer,
                reason => Error
            }),
            St
    end.

announce_consumer(SGroup, Consumer, Heartbeat, Lifetime, CSt) ->
    case CSt of
        #consumer{announcement = undefined} ->
            case emqx_streams_state_db:announce_consumer(SGroup, Consumer, Heartbeat, Lifetime) of
                ok ->
                    CSt#consumer{announcement = Heartbeat};
                Error ->
                    Error
            end;
        #consumer{announcement = HBPrev} when Heartbeat - HBPrev < Lifetime div 2 ->
            CSt;
        #consumer{announcement = HBPrev} ->
            case emqx_streams_state_db:reannounce_consumer(SGroup, Consumer, Heartbeat, HBPrev) of
                ok ->
                    CSt#consumer{announcement = Heartbeat};
                Error ->
                    Error
            end
    end.

deannounce_consumer(Consumer, St) ->
    SGroup = streamgroup(St),
    case lookup_consumer(Consumer, St) of
        #consumer{announcement = undefined} ->
            St;
        #consumer{announcement = Heartbeat} = CSt0 ->
            case emqx_streams_state_db:deannounce_consumer(SGroup, Consumer, Heartbeat) of
                ok ->
                    ConsumerSt = CSt0#consumer{announcement = undefined},
                    set_consumer(Consumer, ConsumerSt, St);
                {invalid, undefined} ->
                    ConsumerSt = CSt0#consumer{announcement = undefined},
                    set_consumer(Consumer, ConsumerSt, St);
                Error ->
                    ?tp(warning, "streams_partition_dispatch_deannounce_error", #{
                        streamgroup => SGroup,
                        consumer => Consumer,
                        reason => Error
                    }),
                    St
            end
    end.

%%

new_consumer(Pid) ->
    MRef = erlang:monitor(process, Pid),
    #consumer{
        pid = Pid,
        monitor = MRef,
        announcement = undefined
    }.

set_consumer(Consumer, ConsumerSt, St = #st{consumers = Cs}) ->
    St#st{consumers = Cs#{Consumer => ConsumerSt}}.

lookup_consumer(Consumer, #st{consumers = Cs}) ->
    case Cs of
        #{Consumer := ConsumerSt} ->
            ConsumerSt;
        #{} ->
            undefined
    end.

lookup_consumer_by_mref(MRef, #st{consumers = Consumers}) ->
    Found = maps:filter(fun(_, #consumer{monitor = M}) -> M =:= MRef end, Consumers),
    case maps:keys(Found) of
        [Consumer] ->
            Consumer;
        [] ->
            undefined
    end.

list_consumers(#st{consumers = Consumers}) ->
    maps:keys(Consumers).

send_consumer(Consumer, Msg, St = #st{group = Group}) ->
    #consumer{pid = Pid} = lookup_consumer(Consumer, St),
    _ = Pid ! #pdisp_message{group = Group, msg = Msg},
    ok.

kill_consumer(Consumer, Reason, St) ->
    #consumer{pid = Pid} = lookup_consumer(Consumer, St),
    erlang:exit(Pid, Reason).

set_lease(Partition, Lease, St = #st{leases = Leases}) ->
    St#st{leases = Leases#{Partition => Lease}}.

forget_lease(Partition, St = #st{leases = Leases}) ->
    St#st{leases = maps:remove(Partition, Leases)}.

lookup_lease(Partition, #st{leases = Leases}) ->
    case Leases of
        #{Partition := Lease} ->
            Lease;
        #{} ->
            undefined
    end.

%%

% push_pending(Job, St = #st{pending = Pending}) ->
%     St#st{pending = [Job | Pending]}.

% drop_pending(Job, St = #st{pending = Pending0}) ->
%     Pending = lists:delete(Job, Pending0),
%     true = Pending =/= Pending0,
%     St#st{pending = Pending}.

%%

stash_tx(Ref, Continuation, St = #st{pending = Pending}) ->
    St#st{
        pending = Pending#{
            {tx, Ref} => Continuation
        }
    }.

on_tx_commit(Ref, Reply, St = #st{pending = Pending0}) ->
    case maps:take({tx, Ref}, Pending0) of
        {{Fun, Args}, Pending} ->
            Fun(Args ++ [Ref, Reply, St#st{pending = Pending}]);
        error ->
            unknown
    end.

%%

streamgroup(#st{group = Group, stream = Stream}) ->
    ?streamgroup(Group, Stream).

timestamp_ms() ->
    erlang:system_time(millisecond).

%%

list_stream_partitions(Stream) ->
    %% FIXME
    {ok, lists:map(fun integer_to_binary/1, lists:seq(1, 16))}.
