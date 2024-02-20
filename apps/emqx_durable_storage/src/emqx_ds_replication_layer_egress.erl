%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% @doc Egress servers are responsible for proxing the outcoming
%% `store_batch' requests towards EMQX DS shards.
%%
%% They re-assemble messages from different local processes into
%% fixed-sized batches, and introduce centralized channels between the
%% nodes. They are also responsible for maintaining backpressure
%% towards the local publishers.
%%
%% There is (currently) one egress process for each shard running on
%% each node, but it should be possible to have a pool of egress
%% servers, if needed.
-module(emqx_ds_replication_layer_egress).

-include_lib("emqx_utils/include/emqx_message.hrl").

-behaviour(gen_server).

%% API:
-export([start_link/2, store_batch/3]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([]).

-export_type([]).

-include("emqx_ds_replication_layer.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(ACCUM_TIMEOUT, 3).
-define(DRAIN_TIMEOUT, 1).

-define(COOLDOWN_TIMEOUT_MIN, 1000).
-define(COOLDOWN_TIMEOUT_MAX, 5000).

-define(name(DB, Shard), {n, l, {?MODULE, DB, Shard}}).
-define(via(DB, Shard), {via, gproc, ?name(DB, Shard)}).

%%================================================================================
%% API functions
%%================================================================================

-spec start_link(emqx_ds:db(), emqx_ds_replication_layer:shard_id()) -> {ok, pid()}.
start_link(DB, Shard) ->
    gen_server:start_link(?via(DB, Shard), ?MODULE, [DB, Shard], []).

-spec store_batch(emqx_ds:db(), [emqx_types:message()], emqx_ds:message_store_opts()) ->
    [ok | error].
store_batch(DB, Messages = [First | Rest], #{atomic := true}) ->
    Shard = shard_of_message(DB, First),
    case lists:all(fun(M) -> shard_of_message(DB, M) == Shard end, Rest) of
        true ->
            Pid = self(),
            Ref = erlang:make_ref(),
            _ = gproc:send(?name(DB, Shard), {Pid, Ref, {atomic, length(Messages), Messages}}),
            %% FIXME
            receive
                {Ref, Result} -> Result
            end;
        false ->
            {error, unrecoverable, multi_shard_atomic_batch}
    end;
store_batch(_DB, [], #{atomic := true}) ->
    ok;
store_batch(DB, Messages, #{}) ->
    Pid = self(),
    Refs = lists:map(
        fun(Message) ->
            Ref = erlang:make_ref(),
            Shard = shard_of_message(DB, Message),
            _ = gproc:send(?name(DB, Shard), {Pid, Ref, Message}),
            Ref
        end,
        Messages
    ),
    %% FIXME
    lists:map(
        fun(Ref) ->
            receive
                {Ref, Result} -> Result
            end
        end,
        Refs
    ).

shard_of_message(DB, Message) ->
    emqx_ds_replication_layer:shard_of_message(DB, Message, clientid).

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s, {
    db :: emqx_ds:db(),
    shard :: emqx_ds_replication_layer:shard_id()
}).

init([DB, Shard]) ->
    process_flag(trap_exit, true),
    process_flag(message_queue_data, off_heap),
    S = #s{
        db = DB,
        shard = Shard
    },
    {ok, S}.

handle_call(_Call, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info(Req = {_Pid, _Ref, _}, S) ->
    ok = timer:sleep(?ACCUM_TIMEOUT),
    Requests = drain_requests_start(Req, max_batch_size()),
    _ = flush(Requests, S),
    true = erlang:garbage_collect(),
    {noreply, S};
handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, _S) ->
    ok.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

drain_requests_start(Req = {_Pid, _Ref, #message{}}, MaxBatchSize) ->
    [Req | drain_requests(1, MaxBatchSize)];
drain_requests_start(Req = {_Pid, _Ref, {atomic, N, _}}, MaxBatchSize) ->
    [Req | drain_requests(N, MaxBatchSize)].

drain_requests(M, M) ->
    %% NOTE: Sticking the batch size to the end of the batch itself.
    [M];
drain_requests(N, M) ->
    receive
        Req = {_Pid, _Ref, #message{}} ->
            [Req | drain_requests(N + 1, M)];
        Req = {_Pid, _Ref, {atomic, N, _}} ->
            [Req | drain_requests(N, M)]
    after ?DRAIN_TIMEOUT ->
        %% TODO: Performs ugly if rate of incoming messages is close to 1 msg/ms,
        [N]
    end.

flush(Requests, #s{db = DB, shard = Shard}) ->
    Messages = make_batch(Requests),
    case emqx_ds_replication_layer:ra_store_batch(DB, Shard, Messages) of
        ok ->
            Size = reply(ok, Requests),
            ?tp(
                emqx_ds_replication_layer_egress_flush,
                #{db => DB, shard => Shard, size => Size}
            ),
            ok;
        {error, Reason} ->
            Size = reply(error, Requests),
            ?tp(
                warning,
                emqx_ds_replication_layer_egress_flush_failed,
                #{db => DB, shard => Shard, size => Size, reason => Reason}
            ),
            ok = cooldown(),
            {error, Reason}
    end.

make_batch([{_Pid, _Ref, {atomic, _, Messages}} | Rest]) ->
    Messages ++ make_batch(Rest);
make_batch([{_Pid, _Ref, Message} | Rest]) ->
    [Message | make_batch(Rest)].

reply(Result, [{Pid, Ref, _} | Rest]) ->
    erlang:send(Pid, {Ref, Result}),
    reply(Result, Rest);
reply(_Result, [Size]) when is_integer(Size) ->
    Size.

cooldown() ->
    Timeout = ?COOLDOWN_TIMEOUT_MIN + rand:uniform(?COOLDOWN_TIMEOUT_MAX - ?COOLDOWN_TIMEOUT_MIN),
    timer:sleep(Timeout).

max_batch_size() ->
    max(1, application:get_env(emqx_durable_storage, egress_batch_size, 1000)).
