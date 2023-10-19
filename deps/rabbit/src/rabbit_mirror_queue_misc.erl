%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2010-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mirror_queue_misc).
-behaviour(rabbit_policy_validator).
-behaviour(rabbit_policy_merge_strategy).

-include_lib("stdlib/include/assert.hrl").

-include("amqqueue.hrl").

-export([validate_policy/1,
         merge_policy_value/3]).

%% Deprecated feature callback.
-export([are_cmqs_permitted/0,
         are_cmqs_used/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

-rabbit_deprecated_feature(
   {classic_queue_mirroring,
    #{deprecation_phase => removed,
      messages =>
      #{when_permitted =>
        "Classic mirrored queues are deprecated.\n"
        "By default, they can still be used for now.\n"
        "Their use will not be permitted by default in the next minor"
        "RabbitMQ version (if any) and they will be removed from "
        "RabbitMQ 4.0.0.\n"
        "To continue using classic mirrored queues when they are not "
        "permitted by default, set the following parameter in your "
        "configuration:\n"
        "    \"deprecated_features.permit.classic_queue_mirroring = true\"\n"
        "To test RabbitMQ as if they were removed, set this in your "
        "configuration:\n"
        "    \"deprecated_features.permit.classic_queue_mirroring = false\"",

        when_denied =>
        "Classic mirrored queues are deprecated.\n"
        "Their use is not permitted per the configuration (overriding the "
        "default, which is permitted):\n"
        "    \"deprecated_features.permit.classic_queue_mirroring = false\"\n"
        "Their use will not be permitted by default in the next minor "
        "RabbitMQ version (if any) and they will be removed from "
        "RabbitMQ 4.0.0.\n"
        "To continue using classic mirrored queues when they are not "
        "permitted by default, set the following parameter in your "
        "configuration:\n"
        "    \"deprecated_features.permit.classic_queue_mirroring = true\"",

        when_removed =>
        "Classic mirrored queues have been removed.\n"
       },
      doc_url => "https://blog.rabbitmq.com/posts/2021/08/4.0-deprecation-announcements/#removal-of-classic-queue-mirroring",
      callbacks => #{is_feature_used => {?MODULE, are_cmqs_used}}
     }}).

-rabbit_boot_step(
   {?MODULE,
    [{description, "HA policy validation"},
     {mfa, {rabbit_registry, register,
            [policy_validator, <<"ha-mode">>, ?MODULE]}},
     {mfa, {rabbit_registry, register,
            [policy_validator, <<"ha-params">>, ?MODULE]}},
     {mfa, {rabbit_registry, register,
            [policy_validator, <<"ha-sync-mode">>, ?MODULE]}},
     {mfa, {rabbit_registry, register,
            [policy_validator, <<"ha-sync-batch-size">>, ?MODULE]}},
     {mfa, {rabbit_registry, register,
            [policy_validator, <<"ha-promote-on-shutdown">>, ?MODULE]}},
     {mfa, {rabbit_registry, register,
            [policy_validator, <<"ha-promote-on-failure">>, ?MODULE]}},
     {mfa, {rabbit_registry, register,
            [operator_policy_validator, <<"ha-mode">>, ?MODULE]}},
     {mfa, {rabbit_registry, register,
            [operator_policy_validator, <<"ha-params">>, ?MODULE]}},
     {mfa, {rabbit_registry, register,
            [operator_policy_validator, <<"ha-sync-mode">>, ?MODULE]}},
     {mfa, {rabbit_registry, register,
            [policy_merge_strategy, <<"ha-mode">>, ?MODULE]}},
     {mfa, {rabbit_registry, register,
            [policy_merge_strategy, <<"ha-params">>, ?MODULE]}},
     {mfa, {rabbit_registry, register,
            [policy_merge_strategy, <<"ha-sync-mode">>, ?MODULE]}},
     {requires, rabbit_registry},
     {enables, recovery}]}).


%%----------------------------------------------------------------------------

are_cmqs_permitted() ->
    FeatureName = classic_queue_mirroring,
    rabbit_deprecated_features:is_permitted(FeatureName).

are_cmqs_used(_) ->
    case rabbit_khepri:get_feature_state() of
        enabled ->
            false;
        _ ->
            %% If we are using Mnesia, we want to check manually if the table
            %% exists first. Otherwise it can conflict with the way
            %% `rabbit_khepri:handle_fallback/1` works. Indeed, this function
            %% and `rabbit_khepri:handle_fallback/1` rely on the `no_exists`
            %% exception.
            AllTables = mnesia:system_info(tables),
            RuntimeParamsReady = lists:member(
                                   rabbit_runtime_parameters, AllTables),
            case RuntimeParamsReady of
                true ->
                    %% We also wait for the table because it could exist but
                    %% may be unavailable. For instance, Mnesia needs another
                    %% replica on another node before it considers it to be
                    %% available.
                    rabbit_table:wait(
                      [rabbit_runtime_parameters], _Retry = true),
                    are_cmqs_used1();
                false ->
                    false
            end
    end.

are_cmqs_used1() ->
    try
        LocalPolicies = rabbit_policy:list(),
        LocalOpPolicies = rabbit_policy:list_op(),
        has_ha_policies(LocalPolicies ++ LocalOpPolicies)
    catch
        exit:{aborted, {no_exists, _}} ->
            %% This node is being initialized for the first time. Therefore it
            %% must have no policies.
            ?assert(rabbit_mnesia:is_running()),
            false
    end.

has_ha_policies(Policies) ->
    lists:any(
      fun(Policy) ->
              KeyList = proplists:get_value(definition, Policy),
              does_policy_configure_cmq(KeyList)
      end, Policies).

does_policy_configure_cmq(KeyList) ->
    lists:keymember(<<"ha-mode">>, 1, KeyList).

validate_policy(KeyList) ->
    Mode = proplists:get_value(<<"ha-mode">>, KeyList, none),
    Params = proplists:get_value(<<"ha-params">>, KeyList, none),
    SyncMode = proplists:get_value(<<"ha-sync-mode">>, KeyList, none),
    SyncBatchSize = proplists:get_value(
                      <<"ha-sync-batch-size">>, KeyList, none),
    PromoteOnShutdown = proplists:get_value(
                          <<"ha-promote-on-shutdown">>, KeyList, none),
    PromoteOnFailure = proplists:get_value(
                          <<"ha-promote-on-failure">>, KeyList, none),
    case {are_cmqs_permitted(), Mode, Params, SyncMode, SyncBatchSize, PromoteOnShutdown, PromoteOnFailure} of
        {_, none, none, none, none, none, none} ->
            ok;
        {false, _, _, _, _, _, _} ->
            %% If the policy configures classic mirrored queues and this
            %% feature is disabled, we consider this policy not valid and deny
            %% it.
            FeatureName = classic_queue_mirroring,
            Warning = rabbit_deprecated_features:get_warning(FeatureName),
            {error, "~ts", [Warning]}
    end.

merge_policy_value(_, Val, _) ->
    Val.
