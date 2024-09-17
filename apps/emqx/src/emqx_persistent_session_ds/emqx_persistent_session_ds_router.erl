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

-module(emqx_persistent_session_ds_router).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include("emqx_ps_ds_int.hrl").

-export([init_tables/0]).

%% Route APIs
-export([
    add_route/2,
    add_route/3,
    delete_route/2,
    delete_route/3,
    has_any_route/1,
    match_routes/1,
    match_routes/2,
    lookup_routes/1,
    lookup_routes/2,
    foldr_routes/2,
    foldl_routes/2
]).

%% Topics API
-export([
    stream/1,
    stream/2,
    stats/1
]).

-export([topics/0]).

%% Test-only APIs
-export([has_route/2]).

-type route() :: #ps_route{}.
-type dest() :: emqx_persistent_session_ds:id() | #share_dest{}.

%% Subscription scope.
%% Scopes enable limited form of _selective routing_.
%%  * `root` scope routes "work" for every message.
%%  * `noqos0` scope routes work only for QoS 1/2 messages.
%% Mirrors `emqx_broker:subscope()`.
-type subscope() :: root | noqos0.

%% 32#NQ0 = 24384
-define(SCOPE_NOQOS0, 32#NQ0).

-export_type([dest/0, route/0]).

%%--------------------------------------------------------------------
%% Table Initialization
%%--------------------------------------------------------------------

init_tables() ->
    mria_config:set_dirty_shard(?PS_ROUTER_SHARD, true),
    ok = mria:create_table(?PS_ROUTER_TAB, [
        {type, bag},
        {rlog_shard, ?PS_ROUTER_SHARD},
        {storage, disc_copies},
        {record_name, ps_route},
        {attributes, record_info(fields, ps_route)},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, true}
            ]}
        ]}
    ]),
    ok = mria:create_table(?PS_FILTERS_TAB, [
        {type, ordered_set},
        {rlog_shard, ?PS_ROUTER_SHARD},
        {storage, disc_copies},
        {record_name, ps_routeidx},
        {attributes, record_info(fields, ps_routeidx)},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, auto}
            ]}
        ]}
    ]),
    %% NOTE
    %% Holds non-wildcard routes with any scopes other than the `root` scope.
    ok = mria:create_table(?PS_ROUTER_EXT_TAB, [
        {type, ordered_set},
        {rlog_shard, ?PS_ROUTER_SHARD},
        {storage, disc_copies},
        {record_name, ps_route_ext},
        {attributes, record_info(fields, ps_route_ext)},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, auto}
            ]}
        ]}
    ]),
    %% NOTE
    %% Holds wildcard route index with routes of any scopes other than the `root` scope.
    ok = mria:create_table(?PS_FILTERS_EXT_TAB, [
        {type, ordered_set},
        {rlog_shard, ?PS_ROUTER_SHARD},
        {storage, disc_copies},
        {record_name, ps_routeidx},
        {attributes, record_info(fields, ps_routeidx)},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, auto}
            ]}
        ]}
    ]),
    ok = mria:wait_for_tables([?PS_ROUTER_TAB, ?PS_FILTERS_TAB]),
    ok.

%%--------------------------------------------------------------------
%% Route APIs
%%--------------------------------------------------------------------

-spec add_route(emqx_types:topic(), dest()) -> ok | {error, term()}.
add_route(Topic, Dest) ->
    mria_insert_route(Topic, Dest, root).

-spec add_route(emqx_types:topic(), dest(), subscope()) -> ok | {error, term()}.
add_route(Topic, Dest, Scope) ->
    mria_insert_route(Topic, Dest, Scope).

-spec delete_route(emqx_types:topic(), dest()) -> ok | {error, term()}.
delete_route(Topic, Dest) ->
    mria_delete_route(Topic, Dest, root).

-spec delete_route(emqx_types:topic(), dest(), subscope()) -> ok | {error, term()}.
delete_route(Topic, Dest, Scope) ->
    mria_delete_route(Topic, Dest, Scope).

%% @doc Takes a real topic (not filter) as input, and returns whether there is any
%% matching filters.
-spec has_any_route(emqx_types:message()) -> boolean().
has_any_route(#message{topic = Topic, qos = ?QOS_0}) ->
    has_route_match(Topic);
has_any_route(#message{topic = Topic}) ->
    has_route_match(Topic) orelse has_scope_route_match(Topic, noqos0).

%% @doc Take a real topic (not filter) as input, return the matching topics and topic
%% filters associated with route destination.
-spec match_routes(emqx_types:topic()) -> [route()].
match_routes(Topic) when is_binary(Topic) ->
    match_routes(Topic, root).

-spec match_routes(emqx_types:topic(), subscope()) -> [route()].
match_routes(Topic, root) when is_binary(Topic) ->
    lookup_route_tab(Topic) ++
        [match_to_route(M) || M <- match_filters(Topic)];
match_routes(Topic, Scope) when is_binary(Topic) ->
    lookup_scope_route_tab(Topic, Scope) ++
        [match_to_route(M) || M <- match_scope_filters(Topic, Scope)].

%% @doc Take a topic or filter as input, and return the existing routes with exactly
%% this topic or filter.
-spec lookup_routes(emqx_types:topic()) -> [route()].
lookup_routes(Topic) ->
    case emqx_topic:wildcard(Topic) of
        true ->
            lookup_filter_tab(Topic) ++ lookup_scope_filter_tab(Topic, '_');
        false ->
            lookup_route_tab(Topic) ++ lookup_scope_route_tab(Topic, '_')
    end.

-spec lookup_routes(emqx_types:topic(), subscope()) -> [route()].
lookup_routes(Topic, Scope) ->
    case emqx_topic:wildcard(Topic) of
        true ->
            lookup_scope_filter_tab(Topic, Scope);
        false ->
            lookup_scope_route_tab(Topic, Scope)
    end.

-spec has_route(emqx_types:topic(), dest()) -> boolean().
has_route(Topic, Dest) ->
    case emqx_topic:wildcard(Topic) of
        true ->
            [] =/= lookup_filter_tab(Topic, Dest);
        false ->
            [] =/= lookup_route_tab(Topic, Dest)
    end.

-spec topics() -> list(emqx_types:topic()).
topics() ->
    list_topics() ++ list_scope_topics().

-spec foldl_routes(fun((emqx_types:route(), Acc) -> Acc), Acc) -> Acc.
foldl_routes(FoldFun, AccIn) ->
    fold_routes(foldl, FoldFun, AccIn).

-spec foldr_routes(fun((emqx_types:route(), Acc) -> Acc), Acc) -> Acc.
foldr_routes(FoldFun, AccIn) ->
    fold_routes(foldr, FoldFun, AccIn).

%%--------------------------------------------------------------------
%% Topic API
%%--------------------------------------------------------------------

%% @doc Create a `emqx_utils_stream:stream(#route{})` out of the router state,
%% potentially filtered by a topic or topic filter. The stream emits `#route{}`
%% records since this is what `emqx_mgmt_api_topics` knows how to deal with.
-spec stream(_MTopic :: '_' | emqx_types:topic()) ->
    emqx_utils_stream:stream(emqx_types:route()).
stream(MTopic) ->
    emqx_utils_stream:chain([
        stream_tab(?PS_ROUTER_TAB, MTopic),
        stream_tab(?PS_ROUTER_EXT_TAB, MTopic, '_'),
        stream_tab(?PS_FILTERS_TAB, MTopic),
        stream_tab(?PS_FILTERS_EXT_TAB, MTopic, '_')
    ]).

-spec stream(_MTopic :: '_' | emqx_types:topic(), subscope()) ->
    emqx_utils_stream:stream(emqx_types:route()).
stream(MTopic, root) ->
    emqx_utils_stream:chain(
        stream_tab(?PS_ROUTER_TAB, MTopic),
        stream_tab(?PS_FILTERS_TAB, MTopic)
    );
stream(MTopic, Scope) ->
    emqx_utils_stream:chain(
        stream_tab(?PS_ROUTER_EXT_TAB, MTopic, Scope),
        stream_tab(?PS_FILTERS_EXT_TAB, MTopic, Scope)
    ).

%% @doc Retrieve router stats.
%% n_routes: total number of routes, should be equal to the length of `stream('_')`.
-spec stats(n_routes) -> non_neg_integer().
stats(n_routes) ->
    NTopics = ets:info(?PS_ROUTER_TAB, size),
    NFilters = ets:info(?PS_FILTERS_TAB, size),
    NTopicsExt = ets:info(?PS_ROUTER_EXT_TAB, size),
    NFiltersExt = ets:info(?PS_FILTERS_EXT_TAB, size),
    emqx_maybe:define(NTopics, 0) + emqx_maybe:define(NFilters, 0) +
        emqx_maybe:define(NTopicsExt, 0) + emqx_maybe:define(NFiltersExt, 0).

%%--------------------------------------------------------------------
%% Internal fns
%%--------------------------------------------------------------------

%% Make Dialyzer happy

%% Equivalent of `#ps_route{topic = TOPIC, _ = '_'}`:
-define(PS_ROUTE_PAT(TOPIC),
    erlang:make_tuple(record_info(size, ps_route), '_', [{#ps_route.topic, TOPIC}])
).

%% Equivalent of `#ps_route{topic = TOPIC, dest = DEST, _ = '_'}`:
-define(PS_ROUTE_PAT(TOPIC, DEST),
    erlang:make_tuple(record_info(size, ps_route), '_', [
        {#ps_route.topic, TOPIC},
        {#ps_route.dest, DEST}
    ])
).

%% Equivalent of `#ps_route_ext{entry = ENTRY, _ = '_'}`:
-define(PS_ROUTE_EXT_PAT(ENTRY),
    erlang:make_tuple(record_info(size, ps_route_ext), '_', [{#ps_route_ext.entry, ENTRY}])
).

%% Equivalent of `#ps_routeidx{entry = ENTRY, _ = '_'}`:
-define(PS_ROUTEIDX_PAT(ENTRY),
    erlang:make_tuple(record_info(size, ps_routeidx), '_', [{#ps_routeidx.entry, ENTRY}])
).

mria_insert_route(Topic, Dest, Scope) ->
    case emqx_trie_search:filter(Topic) of
        Words when is_list(Words) ->
            mria_filter_tab_insert(Words, Dest, Scope);
        false ->
            mria_route_tab_insert(Topic, Dest, Scope)
    end.

mria_delete_route(Topic, Dest, Scope) ->
    case emqx_trie_search:filter(Topic) of
        Words when is_list(Words) ->
            mria_filter_tab_delete(Words, Dest, Scope);
        false ->
            mria_route_tab_delete(Topic, Dest, Scope)
    end.

mria_route_tab_insert(Topic, Dest, root) ->
    Record = #ps_route{topic = Topic, dest = Dest},
    mria:dirty_write(?PS_ROUTER_TAB, Record);
mria_route_tab_insert(Topic, Dest, Scope) ->
    Record = #ps_route_ext{entry = {Topic, scope_tag(Scope), Dest}},
    mria:dirty_write(?PS_ROUTER_EXT_TAB, Record).

mria_route_tab_delete(Topic, Dest, root) ->
    Record = #ps_route{topic = Topic, dest = Dest},
    mria:dirty_delete_object(?PS_ROUTER_TAB, Record);
mria_route_tab_delete(Topic, Dest, Scope) ->
    K = {Topic, scope_tag(Scope), Dest},
    mria:dirty_delete(?PS_ROUTER_EXT_TAB, K).

mria_filter_tab_insert(Words, Dest, root) ->
    K = emqx_topic_index:make_key(Words, Dest),
    mria:dirty_write(?PS_FILTERS_TAB, #ps_routeidx{entry = K});
mria_filter_tab_insert(Words, Dest, Scope) ->
    K = emqx_topic_index:make_key(Words, Dest),
    ScopeTag = scope_tag(Scope),
    mria:dirty_write(?PS_FILTERS_EXT_TAB, #ps_routeidx{entry = {ScopeTag, K}}).

mria_filter_tab_delete(Words, Dest, root) ->
    K = emqx_topic_index:make_key(Words, Dest),
    mria:dirty_delete(?PS_FILTERS_TAB, K);
mria_filter_tab_delete(Words, Dest, Scope) ->
    K = emqx_topic_index:make_key(Words, Dest),
    ScopeTag = scope_tag(Scope),
    mria:dirty_delete(?PS_FILTERS_EXT_TAB, {ScopeTag, K}).

has_route_match(Topic) ->
    ets:member(?PS_ROUTER_TAB, Topic) orelse
        false =/= emqx_topic_index:match(Topic, ?PS_FILTERS_TAB).

has_scope_route_match(Topic, Scope) ->
    ScopeTag = scope_tag(Scope),
    MatchPat = ?PS_ROUTE_EXT_PAT({Topic, ScopeTag, '_'}),
    ets_match_member(?PS_ROUTER_EXT_TAB, MatchPat) orelse
        begin
            NextF = mk_scope_nextf(?PS_FILTERS_EXT_TAB, ScopeTag),
            false =/= emqx_trie_search:match(Topic, NextF)
        end.

mk_scope_nextf(Tab, ScopeTag) ->
    fun(K) ->
        case ets:next(Tab, {ScopeTag, K}) of
            {ScopeTag, NK} -> NK;
            {_Another, _} -> '$end_of_table';
            '$end_of_table' -> '$end_of_table'
        end
    end.

lookup_route_tab(Topic) ->
    ets:lookup(?PS_ROUTER_TAB, Topic).

lookup_route_tab(Topic, Dest) ->
    ets:match_object(?PS_ROUTER_TAB, ?PS_ROUTE_PAT(Topic, Dest)).

lookup_filter_tab(Topic) ->
    lookup_filter_tab(Topic, '$1').

lookup_filter_tab(Topic, Dest) ->
    MatchPat = ?PS_ROUTEIDX_PAT(emqx_topic_index:make_key(Topic, Dest)),
    Contruct = ?PS_ROUTE_PAT(Topic, Dest),
    ets:select(?PS_FILTERS_TAB, [{MatchPat, [], [{Contruct}]}]).

lookup_scope_route_tab(Topic, Scope) ->
    MatchPat = ?PS_ROUTE_EXT_PAT({Topic, scope_pat(Scope), '$1'}),
    Contruct = ?PS_ROUTE_PAT(Topic, '$1'),
    ets:select(?PS_ROUTER_TAB, [{MatchPat, [], [{Contruct}]}]).

lookup_scope_filter_tab(Topic, Scope) ->
    %% TODO: Fullscan?
    MatchPat = ?PS_ROUTEIDX_PAT({scope_pat(Scope), emqx_topic_index:make_key(Topic, '$1')}),
    Contruct = ?PS_ROUTE_PAT(Topic, '$1'),
    ets:select(?PS_ROUTER_TAB, [{MatchPat, [], [{Contruct}]}]).

match_filters(Topic) ->
    emqx_topic_index:matches(Topic, ?PS_FILTERS_TAB, []).

match_scope_filters(Topic, Scope) ->
    ScopeTag = scope_tag(Scope),
    emqx_trie_search:matches(Topic, mk_scope_nextf(?PS_FILTERS_EXT_TAB, ScopeTag), []).

scope_pat('_') ->
    '_';
scope_pat(Scope) ->
    scope_tag(Scope).

scope_tag(noqos0) ->
    ?SCOPE_NOQOS0.

ets_match_member(Tab, MatchPat) ->
    case ets:match(Tab, MatchPat, 1) of
        {[_], _Cont} ->
            true;
        _ ->
            false
    end.

fold_routes(FunName, FoldFun, AccIn) ->
    Acc1 = ets:FunName(mk_fold_fun(fun export_route/1, FoldFun), AccIn, ?PS_ROUTER_TAB),
    Acc2 = ets:FunName(mk_fold_fun(fun export_route_ext/1, FoldFun), Acc1, ?PS_ROUTER_EXT_TAB),
    Acc3 = ets:FunName(mk_fold_fun(fun export_routeidx/1, FoldFun), Acc2, ?PS_FILTERS_TAB),
    ets:FunName(mk_fold_fun(fun export_routeidx_ext/1, FoldFun), Acc3, ?PS_FILTERS_EXT_TAB).

mk_fold_fun(ExportFun, FoldFun) ->
    fun(Record, Acc) -> FoldFun(ExportFun(Record), Acc) end.

match_to_route(M) ->
    #ps_route{topic = emqx_topic_index:get_topic(M), dest = emqx_topic_index:get_id(M)}.

list_topics() ->
    %% NOTE: This code is far from efficient, should be fine as long as it's test-only.
    RPat = ?PS_ROUTE_PAT('$1'),
    RTopics = ets:select(?PS_ROUTER_TAB, [{RPat, [], ['$1']}]),
    FPat = ?PS_ROUTEIDX_PAT('$1'),
    FTopics = [emqx_topic_index:get_topic(K) || [K] <- ets:match(?PS_FILTERS_TAB, FPat)],
    lists:usort(RTopics) ++ FTopics.

list_scope_topics() ->
    %% NOTE: This code is far from efficient, should be fine as long as it's test-only.
    RPat = ?PS_ROUTEIDX_PAT({'$1', _RScopeTag = '_', _Dest = '_'}),
    RTopics = ets:select(?PS_ROUTER_EXT_TAB, [{RPat, [], ['$1']}]),
    FPat = ?PS_ROUTEIDX_PAT({_FScopeTag = '_', _K = '$1'}),
    FTopics = [emqx_topic_index:get_topic(K) || [K] <- ets:match(?PS_FILTERS_EXT_TAB, FPat)],
    lists:usort(RTopics ++ FTopics).

%% @doc Create a `emqx_utils_stream:stream(#route{})` out of contents of either of
%% 4 route tables, optionally filtered by a topic or topic filter. If the latter is
%% specified, then it doesn't make sense to scan through `?PS_ROUTER_TAB` if it's
%% a wildcard topic, and vice versa for `?PS_FILTERS_TAB` if it's not, so we optimize
%% it away by returning an empty stream in those cases.
stream_tab(Tab = ?PS_ROUTER_TAB, MTopic = '_') ->
    stream_route_tab(Tab, MTopic);
stream_tab(Tab = ?PS_FILTERS_TAB, '_') ->
    mk_tab_stream(Tab, ?PS_ROUTEIDX_PAT('_'), fun export_routeidx/1);
stream_tab(Tab, MTopic) ->
    case emqx_topic:wildcard(MTopic) of
        false when Tab == ?PS_ROUTER_TAB ->
            stream_route_tab(Tab, MTopic);
        true when Tab == ?PS_FILTERS_TAB ->
            stream_filter_tab(Tab, MTopic);
        _ ->
            emqx_utils_stream:empty()
    end.

stream_route_tab(Tab = ?PS_ROUTER_TAB, MTopic) ->
    mk_tab_stream(Tab, ?PS_ROUTE_PAT(MTopic), fun export_route/1).

stream_filter_tab(Tab = ?PS_FILTERS_TAB, MTopic) ->
    MatchSpec = ?PS_ROUTEIDX_PAT(emqx_trie_search:make_pat(MTopic, '_')),
    mk_tab_stream(Tab, MatchSpec, fun export_routeidx/1).

stream_tab(Tab = ?PS_ROUTER_EXT_TAB, MTopic = '_', MScope) ->
    stream_route_tab(Tab, MTopic, MScope);
stream_tab(Tab = ?PS_FILTERS_EXT_TAB, MTopic = '_', MScope) ->
    stream_filter_tab(Tab, MTopic, MScope);
stream_tab(Tab, MTopic, MScope) ->
    case emqx_topic:wildcard(MTopic) of
        false when Tab == ?PS_ROUTER_EXT_TAB ->
            stream_route_tab(Tab, MTopic, MScope);
        true when Tab == ?PS_FILTERS_EXT_TAB ->
            stream_filter_tab(Tab, MTopic, MScope);
        _ ->
            emqx_utils_stream:empty()
    end.

stream_route_tab(Tab = ?PS_ROUTER_EXT_TAB, MTopic, _MScope) ->
    %% TODO: Only one scope so far, but could be accomodated for multiple scopes.
    MScopeTag = scope_pat(noqos0),
    MatchSpec = ?PS_ROUTE_EXT_PAT({MTopic, MScopeTag, '_'}),
    mk_tab_stream(Tab, MatchSpec, fun export_route_ext/1).

stream_filter_tab(Tab = ?PS_FILTERS_EXT_TAB, MTopic, _MScope) ->
    %% TODO: Only one scope so far, but could be accomodated for multiple scopes.
    MScopeTag = scope_pat(noqos0),
    MatchSpec = ?PS_ROUTEIDX_PAT({MScopeTag, emqx_trie_search:make_pat(MTopic, '_')}),
    mk_tab_stream(Tab, MatchSpec, fun export_route_ext/1).

mk_tab_stream(Tab, MatchSpec, Mapper) ->
    %% NOTE: Currently relying on the fact that tables are backed by ETSes.
    emqx_utils_stream:map(
        Mapper,
        emqx_utils_stream:ets(fun
            (undefined) -> ets:match_object(Tab, MatchSpec, 1);
            (Cont) -> ets:match_object(Cont)
        end)
    ).

export_route(#ps_route{topic = Topic, dest = Dest}) ->
    #route{topic = Topic, dest = Dest}.

export_route_ext(#ps_route_ext{entry = {Topic, _Scope, Dest}}) ->
    #route{topic = Topic, dest = Dest}.

export_routeidx(#ps_routeidx{entry = M}) ->
    export_match(M).

export_routeidx_ext(#ps_routeidx{entry = {_Scope, M}}) ->
    export_match(M).

export_match(M) ->
    #route{topic = emqx_topic_index:get_topic(M), dest = emqx_topic_index:get_id(M)}.
