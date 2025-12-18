%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams).

-moduledoc """
The module is responsible for integrating the Streams application into the EMQX core.

The write part is integrated via registering `message.publish` hook.
The read part is integrated via registering an ExtSub handler.
""".

-include("emqx_streams_internal.hrl").

-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").

-export([register_hooks/0, unregister_hooks/0]).

-export([
    on_message_publish_stream/1

]).

-export([
    on_message_puback/4,
    on_session_subscribed/3,
    on_session_unsubscribed/3,
    on_client_handle_info/3
]).

%%

-spec register_hooks() -> ok.
register_hooks() ->
    ok = register_stream_hooks(),
    ok = register_sdisp_hooks(),
    ok.

-spec unregister_hooks() -> ok.
unregister_hooks() ->
    ok = unregister_sdisp_hooks(),
    ok = unregister_stream_hooks(),
    ok.

-spec register_sdisp_hooks() -> ok.
register_sdisp_hooks() ->
    %% FIXME: prios
    ok = emqx_hooks:add('message.puback', {?MODULE, on_message_puback, []}, ?HP_HIGHEST),
    ok = emqx_hooks:add('session.subscribed', {?MODULE, on_session_subscribed, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('session.unsubscribed', {?MODULE, on_session_unsubscribed, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('client.handle_info', {?MODULE, on_client_handle_info, []}, ?HP_LOWEST).
    % ok = emqx_extsub_handler_registry:register(emqx_streams_partition_dispatch, #{
    %     handle_generic_messages => true,
    %     multi_topic => true
    % }).

-spec register_stream_hooks() -> ok.
register_stream_hooks() ->
    ok = emqx_hooks:add('message.publish', {?MODULE, on_message_publish_stream, []}, ?HP_HIGHEST),
    ok = emqx_extsub_handler_registry:register(emqx_streams_extsub_handler, #{
        handle_generic_messages => true,
        multi_topic => true
    }).

-spec unregister_sdisp_hooks() -> ok.
unregister_sdisp_hooks() ->
    emqx_hooks:del('message.puback', {?MODULE, on_message_puback}),
    emqx_hooks:del('session.subscribed', {?MODULE, on_session_subscribed}),
    emqx_hooks:del('session.unsubscribed', {?MODULE, on_session_unsubscribed}),
    emqx_hooks:del('client.handle_info', {?MODULE, on_client_handle_info}).

-spec unregister_stream_hooks() -> ok.
unregister_stream_hooks() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish_stream}),
    emqx_extsub_handler_registry:unregister(emqx_streams_extsub_handler).

%%

on_message_publish_stream(#message{topic = Topic} = Message) ->
    ?tp_debug(streams_on_message_publish_stream, #{topic => Topic}),
    Streams = emqx_streams_registry:match(Topic),
    lists:foreach(
        fun(Stream) ->
            {Time, Result} = timer:tc(fun() -> publish_to_stream(Stream, Message) end),
            case Result of
                ok ->
                    emqx_streams_metrics:inc(ds, inserted_messages),
                    ?tp_debug(streams_on_message_publish_to_queue, #{
                        topic_filter => emqx_streams_prop:topic_filter(Stream),
                        message_topic => emqx_message:topic(Message),
                        time_us => Time,
                        result => ok
                    });
                {error, Reason} ->
                    ?tp(error, streams_on_message_publish_queue_error, #{
                        topic_filter => emqx_streams_prop:topic_filter(Stream),
                        message_topic => emqx_message:topic(Message),
                        time_us => Time,
                        reason => Reason
                    })
            end
        end,
        Streams
    ).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

on_message_puback(PacketId, #message{topic = <<"$sdisp/", _/binary>>} = Message, _Res, _RC) ->
    ?tp_debug("streams_on_message_puback", #{topic => Message#message.topic}),
    St = partition_dispatch_state(),
    Ret = emqx_streams_partition_dispatch:on_puback(PacketId, Message, St),
    partition_dispatch_handle_ret(?FUNCTION_NAME, Ret);
on_message_puback(_PacketId, _Message, _Res, _RC) ->
    ok.

on_client_handle_info(_ClientInfo, ?ds_tx_commit_reply(Ref, _) = Reply, Acc) ->
    ?tp_debug("on_client_handle_info", #{message => Reply}),
    St = partition_dispatch_state(),
    Ret = emqx_streams_partition_dispatch:on_tx_commit(Ref, Reply, Acc, St),
    partition_dispatch_handle_ret(?FUNCTION_NAME, Ret);
% on_client_handle_info(_ClientInfo, #shard_dispatch_command{} = Command, Acc) ->
%     ?tp_debug("on_client_handle_info", #{command => Command}),
%     St = partition_dispatch_state(),
%     Ret = emqx_streams_partition_dispatch:on_command(Command, Acc, St),
%     partition_dispatch_handle_ret(?FUNCTION_NAME, Ret);
% on_client_handle_info(_ClientInfo, #pdisp_info{} = Info, Acc) ->
%     ?tp_debug("streams_on_client_handle_info", #{info => Info}),
%     St = partition_dispatch_state(),
%     Ret = emqx_streams_partition_dispatch:on_info(Info, Acc, St),
%     partition_dispatch_handle_ret(?FUNCTION_NAME, Ret);
% on_client_handle_info(_ClientInfo, #pdisp_extsub{group = Group, msg = Msg} = Info, _Acc) ->
%     ?tp_debug("streams_on_client_handle_info", #{info => Info}),
on_client_handle_info(_ClientInfo, _Info, _Acc) ->
    ok.

on_session_subscribed(ClientInfo, Topic = <<"$sg/", _/binary>>, _SubOpts) ->
    ?tp_debug("streams_on_session_subscribed", #{topic => Topic, subopts => _SubOpts}),
    St = partition_dispatch_state(),
    Ret = emqx_streams_partition_dispatch:on_subscription(ClientInfo, Topic, St),
    partition_dispatch_handle_ret(?FUNCTION_NAME, Ret);
on_session_subscribed(_ClientInfo, _Topic, _SubOpts) ->
    ok.

on_session_unsubscribed(ClientInfo, Topic = <<"$sg/", _/binary>>, _SubOpts) ->
    ?tp_debug("streams_on_session_unsubscribed", #{topic => Topic, subopts => _SubOpts}),
    St = partition_dispatch_state(),
    Ret = emqx_streams_partition_dispatch:on_unsubscription(ClientInfo, Topic, St),
    partition_dispatch_handle_ret(?FUNCTION_NAME, Ret);
on_session_unsubscribed(_ClientInfo, _Topic, _SubOpts) ->
    ok.

%%------------------------------------------------------------------------------

publish_to_stream(Stream, #message{} = Message) ->
    emqx_streams_message_db:insert(Stream, Message).

%%------------------------------------------------------------------------------
%% Partition Dispatch
%%------------------------------------------------------------------------------

-define(pd_pdisp_state, emqx_streams_partition_dispatch_state).

partition_dispatch_handle_ret(_Hook, Ret) ->
    case Ret of
        {stop, Acc, StNext} ->
            partition_dispatch_update_state(StNext),
            {stop, Acc};
        {stop, Acc} ->
            {stop, Acc};
        {ok, StNext} ->
            partition_dispatch_update_state(StNext),
            ok;
        ok ->
            ok
    end.

partition_dispatch_state() ->
    erlang:get(?pd_pdisp_state).

partition_dispatch_update_state(St) ->
    erlang:put(?pd_pdisp_state, St).
