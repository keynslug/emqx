%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_auth).
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard_rbac.hrl").

-behaviour(emqx_db_backup).

%% API
-export([mnesia/1]).
-boot_mnesia({mnesia, [boot]}).
-behaviour(emqx_config_handler).

-export([
    create/5,
    read/1,
    update/5,
    delete/1,
    list/0,
    init_bootstrap_file/0,
    format/1
]).

-export([authorize/4]).
-export([post_config_update/5]).

-export([backup_tables/0]).

%% Internal exports (RPC)
-export([
    do_update/5,
    do_delete/1,
    do_create_app/3,
    do_force_create_app/3
]).

-ifdef(TEST).
-export([create/6]).
-endif.

-define(APP, emqx_app).

-record(?APP, {
    name = <<>> :: binary() | '_',
    api_key = <<>> :: binary() | '_',
    api_secret_hash = <<>> :: binary() | '_',
    enable = true :: boolean() | '_',
    %% Since v5.4.0 the `desc` has changed to `extra`
    %% desc = <<>> :: binary() | '_',
    extra = #{} :: binary() | map() | '_',
    expired_at = 0 :: integer() | undefined | infinity | '_',
    created_at = 0 :: integer() | '_'
}).

mnesia(boot) ->
    Fields = record_info(fields, ?APP),
    ok = mria:create_table(?APP, [
        {type, set},
        {rlog_shard, ?COMMON_SHARD},
        {storage, disc_copies},
        {record_name, ?APP},
        {attributes, Fields}
    ]).

%%--------------------------------------------------------------------
%% Data backup
%%--------------------------------------------------------------------

backup_tables() -> [?APP].

post_config_update([api_key], _Req, NewConf, _OldConf, _AppEnvs) ->
    #{bootstrap_file := File} = NewConf,
    case init_bootstrap_file(File) of
        ok ->
            ?SLOG(debug, #{msg => "init_bootstrap_api_keys_from_file_ok", file => File});
        {error, Reason} ->
            Msg = "init_bootstrap_api_keys_from_file_failed",
            ?SLOG(error, #{msg => Msg, reason => Reason, file => File})
    end,
    ok.

-spec init_bootstrap_file() -> ok | {error, _}.
init_bootstrap_file() ->
    File = bootstrap_file(),
    ?SLOG(debug, #{msg => "init_bootstrap_api_keys_from_file", file => File}),
    init_bootstrap_file(File).

create(Name, Enable, ExpiredAt, Desc, Role) ->
    ApiSecret = generate_api_secret(),
    create(Name, ApiSecret, Enable, ExpiredAt, Desc, Role).

create(Name, ApiSecret, Enable, ExpiredAt, Desc, Role) ->
    case mnesia:table_info(?APP, size) < 100 of
        true -> create_app(Name, ApiSecret, Enable, ExpiredAt, Desc, Role);
        false -> {error, "Maximum ApiKey"}
    end.

read(Name) ->
    case mnesia:dirty_read(?APP, Name) of
        [App] -> {ok, to_map(App)};
        [] -> {error, not_found}
    end.

update(Name, Enable, ExpiredAt, Desc, Role) ->
    case valid_role(Role) of
        ok ->
            trans(fun ?MODULE:do_update/5, [Name, Enable, ExpiredAt, Desc, Role]);
        Error ->
            Error
    end.

do_update(Name, Enable, ExpiredAt, Desc, Role) ->
    case mnesia:read(?APP, Name, write) of
        [] ->
            mnesia:abort(not_found);
        [App0 = #?APP{enable = Enable0, extra = Extra0}] ->
            #{desc := Desc0} = Extra = normalize_extra(Extra0),
            App =
                App0#?APP{
                    expired_at = ExpiredAt,
                    enable = ensure_not_undefined(Enable, Enable0),
                    extra = Extra#{desc := ensure_not_undefined(Desc, Desc0), role := Role}
                },
            ok = mnesia:write(App),
            to_map(App)
    end.

delete(Name) ->
    trans(fun ?MODULE:do_delete/1, [Name]).

do_delete(Name) ->
    case mnesia:read(?APP, Name) of
        [] -> mnesia:abort(not_found);
        [_App] -> mnesia:delete({?APP, Name})
    end.

format(App = #{expired_at := ExpiredAt, created_at := CreateAt}) ->
    format_app_extend(App#{
        expired_at => format_epoch(ExpiredAt),
        created_at => format_epoch(CreateAt)
    }).

format_epoch(infinity) ->
    <<"infinity">>;
format_epoch(Epoch) ->
    emqx_utils_calendar:epoch_to_rfc3339(Epoch, second).

list() ->
    to_map(ets:match_object(?APP, #?APP{_ = '_'})).

authorize(<<"/api/v5/users", _/binary>>, _Req, _ApiKey, _ApiSecret) ->
    {error, <<"not_allowed">>};
authorize(<<"/api/v5/api_key", _/binary>>, _Req, _ApiKey, _ApiSecret) ->
    {error, <<"not_allowed">>};
authorize(<<"/api/v5/logout", _/binary>>, _Req, _ApiKey, _ApiSecret) ->
    {error, <<"not_allowed">>};
authorize(_Path, Req, ApiKey, ApiSecret) ->
    Now = erlang:system_time(second),
    case find_by_api_key(ApiKey) of
        {ok, true, ExpiredAt, SecretHash, Role} when ExpiredAt >= Now ->
            case emqx_dashboard_admin:verify_hash(ApiSecret, SecretHash) of
                ok -> check_rbac(Req, ApiKey, Role);
                error -> {error, "secret_error"}
            end;
        {ok, true, _ExpiredAt, _SecretHash, _Role} ->
            {error, "secret_expired"};
        {ok, false, _ExpiredAt, _SecretHash, _Role} ->
            {error, "secret_disable"};
        {error, Reason} ->
            {error, Reason}
    end.

find_by_api_key(ApiKey) ->
    Fun = fun() -> mnesia:match_object(#?APP{api_key = ApiKey, _ = '_'}) end,
    case mria:ro_transaction(?COMMON_SHARD, Fun) of
        {atomic, [
            #?APP{
                api_secret_hash = SecretHash, enable = Enable, expired_at = ExpiredAt, extra = Extra
            }
        ]} ->
            {ok, Enable, ExpiredAt, SecretHash, get_role(Extra)};
        _ ->
            {error, "not_found"}
    end.

ensure_not_undefined(undefined, Old) -> Old;
ensure_not_undefined(New, _Old) -> New.

to_map(Apps) when is_list(Apps) ->
    [to_map(App) || App <- Apps];
to_map(#?APP{
    name = N, api_key = K, enable = E, expired_at = ET, created_at = CT, extra = Extra0
}) ->
    #{role := Role, desc := Desc} = normalize_extra(Extra0),
    #{
        name => N,
        api_key => K,
        enable => E,
        expired_at => ET,
        created_at => CT,
        desc => Desc,
        expired => is_expired(ET),
        role => Role
    }.

is_expired(undefined) -> false;
is_expired(ExpiredTime) -> ExpiredTime < erlang:system_time(second).

create_app(Name, ApiSecret, Enable, ExpiredAt, Desc, Role) ->
    App =
        #?APP{
            name = Name,
            enable = Enable,
            expired_at = ExpiredAt,
            extra = #{desc => Desc, role => Role},
            created_at = erlang:system_time(second),
            api_secret_hash = emqx_dashboard_admin:hash(ApiSecret),
            api_key = list_to_binary(emqx_utils:gen_id(16))
        },
    case create_app(App) of
        {ok, Res} ->
            {ok, Res#{api_secret => ApiSecret}};
        Error ->
            Error
    end.

create_app(App = #?APP{api_key = ApiKey, name = Name, extra = #{role := Role}}) ->
    case valid_role(Role) of
        ok ->
            trans(fun ?MODULE:do_create_app/3, [App, ApiKey, Name]);
        Error ->
            Error
    end.

force_create_app(NamePrefix, App = #?APP{api_key = ApiKey}) ->
    trans(fun ?MODULE:do_force_create_app/3, [App, ApiKey, NamePrefix]).

do_create_app(App, ApiKey, Name) ->
    case mnesia:read(?APP, Name) of
        [_] ->
            mnesia:abort(name_already_existed);
        [] ->
            case mnesia:match_object(?APP, #?APP{api_key = ApiKey, _ = '_'}, read) of
                [] ->
                    ok = mnesia:write(App),
                    to_map(App);
                _ ->
                    mnesia:abort(api_key_already_existed)
            end
    end.

do_force_create_app(App, ApiKey, NamePrefix) ->
    case mnesia:match_object(?APP, #?APP{api_key = ApiKey, _ = '_'}, read) of
        [] ->
            NewName = generate_unique_name(NamePrefix),
            ok = mnesia:write(App#?APP{name = NewName});
        [#?APP{name = Name}] ->
            ok = mnesia:write(App#?APP{name = Name})
    end.

generate_unique_name(NamePrefix) ->
    New = list_to_binary(NamePrefix ++ emqx_utils:gen_id(16)),
    case mnesia:read(?APP, New) of
        [] -> New;
        _ -> generate_unique_name(NamePrefix)
    end.

trans(Fun, Args) ->
    case mria:transaction(?COMMON_SHARD, Fun, Args) of
        {atomic, Res} -> {ok, Res};
        {aborted, Error} -> {error, Error}
    end.

generate_api_secret() ->
    Random = crypto:strong_rand_bytes(32),
    emqx_base62:encode(Random).

bootstrap_file() ->
    emqx:get_config([api_key, bootstrap_file], <<>>).

init_bootstrap_file(<<>>) ->
    ok;
init_bootstrap_file(File) ->
    case file:open(File, [read, binary]) of
        {ok, Dev} ->
            {ok, MP} = re:compile(<<"(\.+):(\.+$)">>, [ungreedy]),
            init_bootstrap_file(File, Dev, MP);
        {error, Reason0} ->
            Reason = emqx_utils:explain_posix(Reason0),
            ?SLOG(
                error,
                #{
                    msg => "failed_to_open_the_bootstrap_file",
                    file => File,
                    reason => Reason
                }
            ),
            {error, Reason}
    end.

init_bootstrap_file(File, Dev, MP) ->
    try
        add_bootstrap_file(File, Dev, MP, 1)
    catch
        throw:Error -> {error, Error};
        Type:Reason:Stacktrace -> {error, {Type, Reason, Stacktrace}}
    after
        file:close(Dev)
    end.

-define(BOOTSTRAP_TAG, <<"Bootstrapped From File">>).

add_bootstrap_file(File, Dev, MP, Line) ->
    case file:read_line(Dev) of
        {ok, Bin} ->
            case re:run(Bin, MP, [global, {capture, all_but_first, binary}]) of
                {match, [[AppKey, ApiSecret]]} ->
                    App =
                        #?APP{
                            enable = true,
                            expired_at = infinity,
                            extra = #{desc => ?BOOTSTRAP_TAG, role => ?ROLE_API_DEFAULT},
                            created_at = erlang:system_time(second),
                            api_secret_hash = emqx_dashboard_admin:hash(ApiSecret),
                            api_key = AppKey
                        },
                    case force_create_app("from_bootstrap_file_", App) of
                        {ok, ok} ->
                            add_bootstrap_file(File, Dev, MP, Line + 1);
                        {error, Reason} ->
                            throw(#{file => File, line => Line, content => Bin, reason => Reason})
                    end;
                _ ->
                    Reason = "invalid_format",
                    ?SLOG(
                        error,
                        #{
                            msg => "failed_to_load_bootstrap_file",
                            file => File,
                            line => Line,
                            content => Bin,
                            reason => Reason
                        }
                    ),
                    throw(#{file => File, line => Line, content => Bin, reason => Reason})
            end;
        eof ->
            ok;
        {error, Reason} ->
            throw(#{file => File, line => Line, reason => Reason})
    end.

get_role(#{role := Role}) ->
    Role;
%% Before v5.4.0,
%% the field in the position of the `extra` is `desc` which is a binary for description
get_role(_Desc) ->
    ?ROLE_API_DEFAULT.

normalize_extra(Map) when is_map(Map) ->
    Map;
normalize_extra(Desc) ->
    #{desc => Desc, role => ?ROLE_API_DEFAULT}.

-if(?EMQX_RELEASE_EDITION == ee).
check_rbac(Req, ApiKey, Role) ->
    case emqx_dashboard_rbac:check_rbac(Req, ApiKey, Role) of
        true ->
            ok;
        _ ->
            {error, unauthorized_role}
    end.

format_app_extend(App) ->
    App.

valid_role(Role) ->
    emqx_dashboard_rbac:valid_api_role(Role).

-else.

check_rbac(_Req, _ApiKey, _Role) ->
    ok.

format_app_extend(App) ->
    maps:remove(role, App).

valid_role(?ROLE_API_DEFAULT) ->
    ok;
valid_role(_) ->
    {error, <<"Role does not exist">>}.

-endif.
