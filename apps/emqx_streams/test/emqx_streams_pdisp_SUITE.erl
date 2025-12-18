%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_pdisp_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-include("../src/emqx_streams_internal.hrl").

-define(N_SHARDS, 8).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_testcase(TestCase, Config) ->
    Apps =
        emqx_cth_suite:start(
            [
                {emqx,
                    emqx_streams_test_utils:cth_config(emqx, #{
                        <<"durable_storage">> => #{
                            <<"streams_messages">> => #{<<"n_shards">> => ?N_SHARDS}
                        }
                    })},
                {emqx_mq, #{config => #{<<"enable">> => false}}},
                {emqx_streams, #{config => #{<<"enable">> => true}}}
            ],
            #{work_dir => emqx_cth_suite:work_dir(TestCase, Config)}
        ),
    ok = snabbkaffe:start_trace(),
    ok = emqx_streams_app:wait_readiness(15_000),
    [{suite_apps, Apps} | Config].

end_per_testcase(_TestCase, Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

%%

-define(sgroup, atom_to_binary(?FUNCTION_NAME)).

t_smoke(_Config) ->
    ?assertEqual(
        #pdisp_group_st{
            revision = undefined,
            leases = #{},
            partitions = #{},
            consumers = #{}
        },
        emqx_streams_state_db:group_state_dirty(?sgroup)
    ).

t_provision(_Config) ->
    Partitions = [<<"P1">>, <<"P2">>, <<"P3">>, <<"P4">>],
    Consumers = [<<"c1">>, <<"c2">>],
    P0 = emqx_streams_pdisp_provision:new(Partitions, Consumers, 0, sgroup_st(?sgroup)),
    Changes0 = emqx_streams_pdisp_provision:propose_changes(P0),
    ?assertMatch([_, _], [P || {lease, P, <<"c1">>} <- Changes0]),
    ?assertMatch([_, _], [P || {lease, P, <<"c2">>} <- Changes0]),
    ok = lease_partition(?sgroup, <<"P2">>, <<"c1">>, 1, 0),
    ok = lease_partition(?sgroup, <<"P3">>, <<"c1">>, 1, 0),
    P1 = emqx_streams_pdisp_provision:new(Partitions, Consumers, 0, sgroup_st(?sgroup)),
    Changes1 = emqx_streams_pdisp_provision:propose_changes(P1),
    ?assertSameSet([{lease, <<"P1">>, <<"c2">>}, {lease, <<"P4">>, <<"c2">>}], Changes1).

t_provision_rebalance(_Config) ->
    Partitions = [<<"P1">>, <<"P2">>, <<"P3">>, <<"P4">>],
    Consumers = [<<"c1">>, <<"c2">>],
    lists:foreach(
        fun(P) -> ok = lease_partition(?sgroup, P, <<"c1">>, 1, 0) end,
        Partitions
    ),
    P1 = emqx_streams_pdisp_provision:new(Partitions, Consumers, 0, sgroup_st(?sgroup)),
    Changes1 = emqx_streams_pdisp_provision:propose_changes(P1),
    ?assertMatch(
        [{release, _, <<"c1">>}, {release, _, <<"c1">>}],
        Changes1
    ),
    lists:foreach(
        fun({release, P, C}) -> ok = release_partition(?sgroup, P, C, 1, 0) end,
        Changes1
    ),
    P2 = emqx_streams_pdisp_provision:new(Partitions, Consumers, 0, sgroup_st(?sgroup)),
    Changes2 = emqx_streams_pdisp_provision:propose_changes(P2),
    ?assertSameSet(
        [P || {release, P, <<"c1">>} <- Changes1],
        [P || {lease, P, <<"c2">>} <- Changes2],
        Changes2
    ).

t_provision_announce(_Config) ->
    Partitions = [<<"P1">>, <<"P2">>, <<"P3">>, <<"P4">>],
    lists:foreach(
        fun(P) -> ok = lease_partition(?sgroup, P, <<"c1">>, 1, 0) end,
        Partitions
    ),
    P1 = emqx_streams_pdisp_provision:new(Partitions, [<<"c1">>], 0, sgroup_st(?sgroup)),
    ?assertEqual([], emqx_streams_pdisp_provision:propose_changes(P1)),
    P2 = emqx_streams_pdisp_provision:new(Partitions, [<<"c2">>, <<"c3">>], 0, sgroup_st(?sgroup)),
    ?assertEqual([], emqx_streams_pdisp_provision:propose_changes(P2)),
    ?assertSameSet(
        [{announce, <<"c2">>}, {announce, <<"c3">>}],
        emqx_streams_pdisp_provision:propose_announcements(P2)
    ).

t_provision_takeover(_Config) ->
    Partitions = [<<"P1">>, <<"P2">>, <<"P3">>, <<"P4">>],
    CDead = <<"dead">>,
    lists:foreach(
        fun({I, P}) ->
            Offset = I,
            Heartbeat = I * 10,
            ok = lease_partition(?sgroup, P, CDead, Offset, Heartbeat)
        end,
        lists:enumerate(Partitions)
    ),
    Consumers = [<<"c1">>, <<"c2">>],
    P1 = emqx_streams_pdisp_provision:new(Partitions, Consumers, 0, sgroup_st(?sgroup)),
    ?assertEqual([], emqx_streams_pdisp_provision:propose_changes(P1)),
    ?assertEqual([], emqx_streams_pdisp_provision:propose_takeovers(P1)),
    P1 = emqx_streams_pdisp_provision:new(Partitions, Consumers, 25, sgroup_st(?sgroup)),
    Takeovers1 = emqx_streams_pdisp_provision:propose_takeovers(P1),
    ?assertSameSet(
        [{takeover, <<"P1">>, CDead, 10}, {takeover, <<"P2">>, CDead, 10}],
        Takeovers1
    ),
    P2 = emqx_streams_pdisp_provision:new(Partitions, Consumers, 50, sgroup_st(?sgroup)),
    Takeovers2 = emqx_streams_pdisp_provision:propose_takeovers(P2),
    ?assertSameSet(
        Partitions,
        [P || {takeover, P, C, _} <- Takeovers2, C =:= CDead],
        Takeovers2
    ).

sgroup_st(SGroup) ->
    emqx_streams_state_db:group_state_dirty(SGroup).

%%

lease_partition(SGroup, Partition, Consumer, Offset, HB) ->
    run_tx(
        emqx_streams_state_db:lease_partition_async(SGroup, Partition, Consumer, Offset, HB)
    ).

release_partition(SGroup, Partition, Consumer, Offset, HBLast) ->
    run_tx(
        emqx_streams_state_db:release_partition_async(SGroup, Partition, Consumer, Offset, HBLast)
    ).

run_tx({async, Ref, Ret}) ->
    ?ds_tx_commit_reply(Ref, Reply) = ?assertReceive(?ds_tx_commit_reply(Ref, _)),
    emqx_streams_state_db:partition_tx_result(Ret, Ref, Reply);
run_tx(Ret) ->
    Ret.
