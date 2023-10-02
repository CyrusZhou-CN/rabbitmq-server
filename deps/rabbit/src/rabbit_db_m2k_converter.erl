%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_m2k_converter).

-behaviour(mnesia_to_khepri_converter).

-include_lib("kernel/include/logger.hrl").
-include_lib("khepri/include/khepri.hrl").
-include_lib("khepri_mnesia_migration/src/kmm_logging.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%% Functions for `rabbit_db_*_m2k_converter' modules to call.
-export([with_correlation_id/2,
         get_store_id/1]).

%% `mnesia_to_khepri_converter' callbacks.
-export([init_copy_to_khepri/4,
         copy_to_khepri/3,
         delete_from_khepri/3,
         finish_copy_to_khepri/1]).

-define(MAX_ASYNC_REQUESTS, 64).

-type migration() :: {mnesia_to_khepri:mnesia_table(),
                      mnesia_to_khepri:converter_mod()}.

-type migrations() :: [migration()].

-type correlation_id() :: non_neg_integer().

-type async_request_fun() :: fun((correlation_id()) -> ok | {error, any()}).

-record(?MODULE, {store_id :: khepri:store_id(),
                  migrations :: migrations(),
                  sub_states :: #{module() => any()},
                  seq_no = 0 :: correlation_id(),
                  last_acked_seq_no = 0 :: correlation_id(),
                  async_requests = #{} :: #{correlation_id() =>
                                            async_request_fun()}}).

-opaque state() :: #?MODULE{}.

-export_type([state/0]).

-spec with_correlation_id(Fun, State) -> Ret when
      Fun :: async_request_fun(),
      State :: state(),
      Ret :: {ok, NewState} | {error, Reason},
      NewState :: state(),
      Reason :: any().

with_correlation_id(
  Fun,
  #?MODULE{seq_no = SeqNo0,
           last_acked_seq_no = LastAckedSeqNo,
           async_requests = AsyncRequests0} = State0) ->
    case SeqNo0 - LastAckedSeqNo >= ?MAX_ASYNC_REQUESTS of
        true ->
            case wait_for_async_requests(State0) of
                {ok, State} ->
                    with_correlation_id(Fun, State);
                {error, _} = Error ->
                    Error
            end;
        false ->
            SeqNo = SeqNo0 + 1,
            AsyncRequests = AsyncRequests0#{SeqNo => Fun},
            State = State0#?MODULE{seq_no = SeqNo,
                                   async_requests = AsyncRequests},
            case Fun(SeqNo) of
                ok ->
                    {ok, State};
                {error, _} = Error ->
                    Error
            end
    end.

-spec get_store_id(state()) -> khepri:store_id().

get_store_id(#?MODULE{store_id = StoreId}) ->
    StoreId.

%% `mnesia_to_khepri_converter' callbacks

-spec init_copy_to_khepri(StoreId, MigrationId, Tables, Migrations) ->
    Ret when
      StoreId :: khepri:store_id(),
      MigrationId :: mnesia_to_khepri:migration_id(),
      Tables :: [mnesia_to_khepri:mnesia_table()],
      Migrations :: migrations(),
      Ret :: {ok, Priv},
      Priv :: #?MODULE{}.
%% @private

init_copy_to_khepri(StoreId, MigrationId, _Tables, Migrations) ->
    TablesPerMod = lists:foldl(
                     fun
                         ({Table, Mod}, Acc) ->
                             Tables0 = maps:get(Mod, Acc, []),
                             Tables1 = Tables0 ++ [Table],
                             Acc#{Mod => Tables1};
                         (_Table, Acc) ->
                             Acc
                     end, #{}, Migrations),

    SubStates = maps:fold(
                  fun(Mod, Tables, Acc) ->
                          {ok, SubState} =
                          case Mod of
                              {ActualMod, Args} ->
                                  ActualMod:init_copy_to_khepri(
                                    StoreId, MigrationId,
                                    Tables, Args);
                              _ ->
                                  Mod:init_copy_to_khepri(
                                    StoreId, MigrationId,
                                    Tables)
                          end,
                          Acc#{Mod => SubState}
                  end, #{}, TablesPerMod),

    State = #?MODULE{store_id = StoreId,
                     migrations = Migrations,
                     sub_states = SubStates},
    {ok, State}.

-spec copy_to_khepri(Table, Record, State) -> Ret when
      Table :: mnesia_to_khepri:mnesia_table(),
      Record :: tuple(),
      State :: state(),
      Ret :: {ok, NewState} | {error, Reason},
      NewState :: state(),
      Reason :: any().
%% @private

copy_to_khepri(
  Table, Record, #?MODULE{migrations = Migrations} = State) ->
    case proplists:get_value(Table, Migrations) of
        true ->
            {ok, State};
        Mod when Mod =/= undefined ->
            ActualMod = actual_mod(Mod),
            case ActualMod:copy_to_khepri(Table, Record, State) of
                {ok, State1} ->
                    {ok, State1};
                {error, _} = Error ->
                    Error
            end
    end.

-spec delete_from_khepri(Table, Key, State) -> Ret when
      Table :: mnesia_to_khepri:mnesia_table(),
      Key :: any(),
      State :: state(),
      Ret :: {ok, NewState} | {error, Reason},
      NewState :: state(),
      Reason :: any().
%% @private

delete_from_khepri(
  Table, Key, #?MODULE{migrations = Migrations} = State) ->
    case proplists:get_value(Table, Migrations) of
        true ->
            {ok, State};
        Mod when Mod =/= undefined ->
            ActualMod = actual_mod(Mod),
            case ActualMod:delete_from_khepri(Table, Key, State) of
                {ok, State1} ->
                    {ok, State1};
                {error, _} = Error ->
                    Error
            end
    end.

-spec finish_copy_to_khepri(State) -> Ret when
      State :: state(),
      Ret :: ok.
%% @private

finish_copy_to_khepri(State) ->
    {ok, _} = wait_for_all_async_requests(State),
    ok.

wait_for_all_async_requests(
  #?MODULE{seq_no = SeqNo,
           last_acked_seq_no = LastAckedSeqNo} = State) ->
    case SeqNo - LastAckedSeqNo > 0 of
        true ->
            case wait_for_async_requests(State) of
                {ok, State1} ->
                    wait_for_all_async_requests(State1);
                {error, _} = Error ->
                    Error
            end;
        false ->
            {ok, State}
    end.

wait_for_async_requests(
  #?MODULE{store_id = StoreId,
           seq_no = SeqNo0,
           async_requests = AsyncRequests0} = State0) ->
    receive
        {ra_event, _CurrentLeader, {applied, Correlations}} ->
            lists:foldl(
              fun (_, {error, _} = Error) ->
                      Error;
                  ({SeqNo, Resp}, {ok, State}) ->
                      #?MODULE{async_requests = AsyncRequests,
                               last_acked_seq_no = LastAcked} = State,
                      LastAcked1 = erlang:max(SeqNo, LastAcked),
                      AsyncRequests1 = maps:remove(SeqNo, AsyncRequests),
                      State1 = State#?MODULE{last_acked_seq_no = LastAcked1,
                                             async_requests = AsyncRequests1},
                      case Resp of
                          ok ->
                              {ok, State1};
                          {ok, _} ->
                              {ok, State1};
                          {error, _} = Error ->
                              Error;
                          {exception, _, _, _} = Exception ->
                              khepri_machine:handle_tx_exception(
                                Exception)
                      end
              end, {ok, State0}, Correlations);
        {ra_event,
         FromId,
         {rejected, {not_leader, MaybeLeader, FailedSeqNo}}} ->
            %% If the leader changes during an async request, retry the failed
            %% request against the new leader.
            ok = khepri_cluster:cache_leader_if_changed(
                   StoreId, FromId, MaybeLeader),
            {Fun, AsyncRequests} = maps:take(FailedSeqNo, AsyncRequests0),
            SeqNo = SeqNo0 + 1,
            AsyncRequests1 = AsyncRequests#{SeqNo => Fun},
            State = State0#?MODULE{seq_no = SeqNo,
                                   async_requests = AsyncRequests1},
            case Fun(SeqNo) of
                ok ->
                    {ok, State};
                {error, _} = Error ->
                    Error
            end
    after 5_000 ->
            {error, timeout}
    end.

actual_mod({Mod, _}) -> Mod;
actual_mod(Mod)      -> Mod.
