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

-module(emqx_ft_exporter_fs).

-include_lib("kernel/include/file.hrl").

%% Writer API
-export([start/3]).
-export([write/3]).
-export([complete/2]).
-export([discard/2]).

-export([list/1]).

-type storage() :: _TODO.
-type transfer() :: emqx_ft:transfer().
-type filemeta() :: emqx_ft:filemeta().
-type transferinfo() :: #{
    path := file:name(),
    timestamp := emqx_datetime:epoch_second(),
    size := _Bytes :: non_neg_integer(),
    meta => filemeta()
}.

-type file_error() :: emqx_ft_storage_fs:file_error().

-record(writer, {
    path :: file:name(),
    handle :: io:device(),
    result :: file:name(),
    meta :: filemeta(),
    hash :: crypto:hash_state()
}).

-type writer() :: #writer{}.

-define(TEMPDIR, "tmp").
-define(MANIFEST, ".MANIFEST.json").

%% NOTE
%% Bucketing of resulting files is to accomodate the storage backend for considerably
%% large (e.g. > 10s of millions) amount of files.
-define(BUCKET_HASH, sha1).

%% 2 symbols = at most 256 directories on the upper level
-define(BUCKET1_LEN, 2).
%% 2 symbols = at most 256 directories on the second level
-define(BUCKET2_LEN, 2).

%%

-spec start(storage(), transfer(), filemeta()) ->
    {ok, writer()} | {error, file_error()}.
start(Storage, Transfer, Filemeta = #{name := Filename}) ->
    TempFilepath = mk_temp_filepath(Storage, Transfer, Filename),
    ResultFilepath = mk_filepath(Storage, Transfer, result, Filename),
    _ = filelib:ensure_dir(TempFilepath),
    case file:open(TempFilepath, [write, raw, binary]) of
        {ok, Handle} ->
            {ok, #writer{
                path = TempFilepath,
                handle = Handle,
                result = ResultFilepath,
                meta = Filemeta,
                hash = init_checksum(Filemeta)
            }};
        {error, _} = Error ->
            Error
    end.

-spec write(storage(), writer(), iodata()) ->
    {ok, writer()} | {error, file_error()}.
write(_Storage, {Filepath, IoDevice, Ctx}, IoData) ->
    case file:write(IoDevice, IoData) of
        ok ->
            {ok, {Filepath, IoDevice, update_checksum(Ctx, IoData)}};
        {error, _} = Error ->
            Error
    end.

-spec complete(storage(), writer()) ->
    ok | {error, {checksum, _Algo, _Computed}} | {error, file_error()}.
complete(
    Storage,
    Writer = #writer{
        path = Filepath,
        handle = Handle,
        result = ResultFilepath,
        meta = Filemeta,
        hash = Ctx
    }
) ->
    case verify_checksum(Ctx, Filemeta) of
        ok ->
            ok = file:close(Handle),
            _ = filelib:ensure_dir(ResultFilepath),
            _ = file:write_file(mk_manifest_filepath(ResultFilepath), encode_filemeta(Filemeta)),
            file:rename(Filepath, ResultFilepath);
        {error, _} = Error ->
            _ = discard(Storage, Writer),
            Error
    end.

-spec discard(storage(), writer()) ->
    ok.
discard(_Storage, #writer{path = Filepath, handle = Handle}) ->
    ok = file:close(Handle),
    file:delete(Filepath).

%%

-spec list(storage()) ->
    {ok, #{transfer() => transferinfo()}}.
list(Storage) ->
    % Buckets = try_list_dir(get_storage_root(Storage)),
    Pattern = [
        _Bucket1 = '*',
        _Bucket2 = '*',
        _Rest = '*',
        _ClientId = '*',
        _FileId = '*',
        fun filter_manifest/1
    ],
    emqx_ft_fs_util:fold(fun read_transferinfo/4, #{}, get_storage_root(Storage), Pattern).

filter_manifest(?MANIFEST) ->
    % Filename equals `?MANIFEST`, there should also be a manifest for it.
    false;
filter_manifest(Filename) ->
    ?MANIFEST =/= string:find(Filename, ?MANIFEST, trailing).

read_transferinfo(Filepath, Fileinfo = #file_info{type = regular}, Stack, Acc) ->
    [_, _, _, ClientId, FileId, _] = Stack,
    Transfer = {ClientId, FileId},
    Info = mk_transferinfo(Filepath, Fileinfo),
    Acc#{Transfer => [Info | maps:get(Transfer, Acc, [])]};
read_transferinfo(Filepath, Fileinfo = #file_info{}, _Stack, Acc) ->
    % TODO worth logging?
    Acc;
read_transferinfo(Filepath, {error, Reason}, _Stack, Acc) ->
    % TODO worth logging?
    Acc.

mk_transferinfo(Filepath, Fileinfo) ->
    try_read_filemeta(
        mk_manifest_filepath(Filepath),
        #{
            path => Filepath,
            timestamp => Fileinfo#file_info.mtime,
            size => Fileinfo#file_info.size
        }
    ).

try_read_filemeta(Filepath, Info) ->
    case emqx_ft_fs_util:read_decode_file(Filepath, fun decode_filemeta/1) of
        {ok, Filemeta} ->
            Info#{meta => Filemeta};
        {error, Reason} ->
            % TODO worth logging?
            Info
    end.

%%

init_checksum(#{checksum := {Algo, _}}) ->
    crypto:hash_init(Algo);
init_checksum(#{}) ->
    undefined.

update_checksum(Ctx, IoData) when Ctx /= undefined ->
    crypto:hash_update(Ctx, IoData);
update_checksum(undefined, _IoData) ->
    undefined.

verify_checksum(Ctx, #{checksum := {Algo, Digest}}) when Ctx /= undefined ->
    case crypto:hash_final(Ctx) of
        Digest ->
            ok;
        Mismatch ->
            {error, {checksum, Algo, binary:encode_hex(Mismatch)}}
    end;
verify_checksum(undefined, _) ->
    ok.

%%

-define(PRELUDE(Vsn, Meta), [<<"filemeta">>, Vsn, Meta]).

encode_filemeta(Meta) ->
    Schema = emqx_ft_schema:schema(filemeta),
    Term = hocon_tconf:make_serializable(Schema, emqx_map_lib:binary_key_map(Meta), #{}),
    emqx_json:encode(?PRELUDE(_Vsn = 1, Term)).

decode_filemeta(Binary) when is_binary(Binary) ->
    ?PRELUDE(_Vsn = 1, Map) = emqx_json:decode(Binary, [return_maps]),
    case emqx_ft:decode_filemeta(Map) of
        {ok, Meta} ->
            Meta;
        {error, Reason} ->
            error(Reason)
    end.

mk_manifest_filepath(Filepath) when is_list(Filepath) ->
    Filepath ++ ?MANIFEST;
mk_manifest_filepath(Filepath) when is_binary(Filepath) ->
    <<Filepath/binary, ?MANIFEST>>.

mk_temp_filepath(Storage, Transfer, Filename) ->
    Unique = erlang:unique_integer([positive]),
    TempFilename = integer_to_list(Unique) ++ "." ++ Filename,
    filename:join(mk_filedir(Storage, Transfer, temporary), TempFilename).

mk_filedir(Storage, _Transfer, temporary) ->
    filename:join([get_storage_root(Storage), ?TEMPDIR]);
mk_filedir(Storage, Transfer, target) ->
    filename:join([get_storage_root(Storage) | mk_target_dirname(Transfer)]).

mk_filepath(Storage, Transfer, What, Filename) ->
    filename:join(mk_filedir(Storage, Transfer, What), Filename).

mk_target_dirname(Transfer = {ClientId, FileId}) ->
    Hash = mk_transfer_hash(Transfer),
    <<
        Bucket1:?BUCKET1_LEN/binary,
        Bucket2:?BUCKET2_LEN/binary,
        BucketRest/binary
    >> = binary:encode_hex(Hash),
    [Bucket1, Bucket2, BucketRest, ClientId, FileId].

mk_transfer_hash({ClientId, FileId}) ->
    crypto:hash(?BUCKET_HASH, {bin(ClientId), bin(FileId)}).

get_storage_root(Storage) ->
    maps:get(root, Storage, filename:join([emqx:data_dir(), "ft", "files"])).

bin(A) when is_atom(A) ->
    atom_to_binary(A);
bin(B) when is_binary(B) ->
    B.
