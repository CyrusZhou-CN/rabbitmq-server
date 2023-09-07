%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 VMware, Inc. or its affiliates.  All rights reserved.

-module(system_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-include("oauth2_client.hrl").

-compile(export_all).

-define(MOCK_TOKEN_ENDPOINT, <<"/token">>).
-define(AUTH_PORT, 8000).
-define(GRANT_ACCESS_TOKEN,  #{request => #{
																method => <<"POST">>,
																path => ?MOCK_TOKEN_ENDPOINT,
																parameters => [
																	{?REQUEST_CLIENT_ID, <<"guest">>},
	                        		  	{?REQUEST_CLIENT_SECRET, <<"password">>}
																]},
															response => [
																{code, 200},
																{content_type, ?CONTENT_JSON_WITH_CHARSET},
																{payload, [
																	{access_token, <<"some access token">>},
																	{token_type, <<"Bearer">>}
																]}
															]
														}).
-define(DENIES_ACCESS_TOKEN, #{request => #{
																method => <<"POST">>,
																path => ?MOCK_TOKEN_ENDPOINT,
																parameters => [
																	{?REQUEST_CLIENT_ID, <<"invalid_client">>},
	                        		  	{?REQUEST_CLIENT_SECRET, <<"password">>}
																]},
															response => [
																{code, 400},
																{content_type, ?CONTENT_JSON},
																{payload, [
																	{error, <<"invalid_client">>},
																	{error_description, <<"invalid client found">>}
																]}
															]
														}).

-define(AUTH_SERVER_ERROR,   #{request => #{
																method => <<"POST">>,
																path => ?MOCK_TOKEN_ENDPOINT,
																parameters => [
																	{?REQUEST_CLIENT_ID, <<"guest">>},
	                        		  	{?REQUEST_CLIENT_SECRET, <<"password">>}
																]},
															response => [
																{code, 500}
															]
														}).

-define(NON_JSON_PAYLOAD,   #{request => #{
																method => <<"POST">>,
																path => ?MOCK_TOKEN_ENDPOINT,
																parameters => [
																	{?REQUEST_CLIENT_ID, <<"guest">>},
	                        		  	{?REQUEST_CLIENT_SECRET, <<"password">>}
																]},
															response => [
																{code, 400},
																{content_type, ?CONTENT_JSON},
																{payload, <<"{ some illegal json}">>}
															]
														}).

-define(GET_OPENID_CONFIGURATION,
														#{request => #{
																method => <<"GET">>,
																path => ?DEFAULT_OPENID_CONFIGURATION_PATH
												 			},
															response => [
																{code, 200},
																{content_type, ?CONTENT_JSON},
																{payload, [
																	{issuer, <<"http://localhost:8000">>},
																	{authorization_endpoint, <<"http://localhost:8000/authorize">>},
																	{token_endpoint, <<"http://localhost:8000/token">>},
																	{jwks_uri, <<"http://localhost:8000/jwks_uri">>}
																]}
															]
														}).
-define(GRANTS_REFRESH_TOKEN,
														#{request => #{
																method => <<"POST">>,
																path => ?MOCK_TOKEN_ENDPOINT,
																parameters => [
																	{?REQUEST_CLIENT_ID, <<"guest">>},
	                        		  	{?REQUEST_CLIENT_SECRET, <<"password">>},
																	{?REQUEST_REFRESH_TOKEN, <<"some refresh token">>}
																]
															},
												 			response => [
																{code, 200},
																{content_type, ?CONTENT_JSON},
																{payload, [
																	{access_token, <<"some refreshed access token">>},
																	{token_type, <<"Bearer">>}
																]}
															]
														}).

all() ->
    [
      {group, http_up},
			{group, http_down},
      {group, https}
    ].

groups() ->
    [
     {http_up, [], [
                  grants_access_token,
                  denies_access_token,
                  auth_server_error,
                  non_json_payload,
									get_openid_configuration,
									grants_refresh_token,
									grants_access_token_using_oauth2_provider_id
                 ]},
		 {http_down, [], [
									connection_error
								 ]},
     {https, [], [
		 							grants_access_token_with_ssl,
                  ssl_connection_error
                 ]}
    ].

init_per_suite(Config) ->
    [	{grants_access_token, ?GRANT_ACCESS_TOKEN},
			{denies_access_token, ?DENIES_ACCESS_TOKEN},
		 	{auth_server_error, ?AUTH_SERVER_ERROR},
			{non_json_payload, ?NON_JSON_PAYLOAD},
			{grants_access_token_with_ssl, ?GRANT_ACCESS_TOKEN},
			{ssl_connection_error, ?GRANT_ACCESS_TOKEN},
			{get_openid_configuration, ?GET_OPENID_CONFIGURATION},
			{grants_refresh_token, ?GRANTS_REFRESH_TOKEN},
			{grants_access_token_using_oauth2_provider_id, ?GRANT_ACCESS_TOKEN}
			| Config].

init_per_group(https, Config) ->
	{ok, _} = application:ensure_all_started(ssl),
  application:ensure_all_started(cowboy),
	Config0 = rabbit_ct_helpers:run_setup_steps(Config),
	CertsDir = ?config(rmq_certsdir, Config0),
	CaCertFile = filename:join([CertsDir, "testca", "cacert.pem"]),
	WrongCaCertFile = filename:join([CertsDir, "server", "server.pem"]),
	[{group, https},
		{oauth_provider_id, <<"uaa">>},
		{oauth_provider, build_https_oauth_provider(CaCertFile)},
		{issuer, build_issuer("https")},
		{oauth_provider_with_wrong_ca, build_https_oauth_provider(WrongCaCertFile)} |
	 	Config0];

init_per_group(http_up, Config) ->
	{ok, _} = application:ensure_all_started(inets),
  application:ensure_all_started(cowboy),
	[{group, http_up},
		{oauth_provider_id, <<"uaa">>},
		{issuer, build_issuer("http")},
		{oauth_provider, build_http_oauth_provider()} | Config];

init_per_group(GroupName, Config) ->
	[{group, GroupName},
		{openiissuerd_configuration_uri, build_issuer("http")},
		{oauth_provider_id, <<"uaa">>},
		{oauth_provider, build_http_oauth_provider()} | Config].

init_per_testcase(TestCase, Config) ->
	OAuthProvider = ?config(oauth_provider, Config),
	OAuthProviders = #{ ?config(oauth_provider_id, Config) => oauth_provider_to_map(OAuthProvider) },
	application:set_env(rabbitmq_auth_backend_oauth2, oauth2_providers, OAuthProviders),

	case ?config(group, Config) of
		http_up ->
			start_http_oauth_server(?AUTH_PORT, ?config(TestCase, Config));
		https ->
			start_https_oauth_server(?AUTH_PORT, ?config(rmq_certsdir, Config),
				?config(TestCase, Config));
		_ -> ok
	end,
	Config.


end_per_testcase(_, Config) ->
	application:unset_env(rabbitmq_auth_backend_oauth2, oauth2_providers),
	case ?config(group, Config) of
		http_up ->
  		stop_http_auth_server();
		https ->
			stop_http_auth_server();
		_ -> ok
	end,
	Config.

end_per_group(_, Config) ->
	Config.

end_per_suite(Config) ->
  Config.


grants_access_token_using_oauth2_provider_id(Config) ->
	#{request := #{parameters := Parameters},
	  response := [ {code, 200}, {content_type, _CT}, {payload, JsonPayload}] }
		= ?config(grants_access_token, Config),

	{ok, #successful_access_token_response{access_token = AccessToken, token_type = TokenType} } =
		oauth2_client:get_access_token(?config(oauth_provider_id, Config), build_access_token_request(Parameters)),
	?assertEqual(proplists:get_value(token_type, JsonPayload), TokenType),
	?assertEqual(proplists:get_value(access_token, JsonPayload), AccessToken).

grants_access_token(Config) ->
  #{request := #{parameters := Parameters},
	  response := [ {code, 200}, {content_type, _CT}, {payload, JsonPayload}] }
		= ?config(grants_access_token, Config),

	{ok, #successful_access_token_response{access_token = AccessToken, token_type = TokenType} } =
		oauth2_client:get_access_token(?config(oauth_provider, Config), build_access_token_request(Parameters)),
	?assertEqual(proplists:get_value(token_type, JsonPayload), TokenType),
	?assertEqual(proplists:get_value(access_token, JsonPayload), AccessToken).

grants_refresh_token(Config) ->
  #{request := #{parameters := Parameters},
	  response := [ {code, 200}, {content_type, _CT}, {payload, JsonPayload}] }
		= ?config(grants_refresh_token, Config),

	{ok, #successful_access_token_response{access_token = AccessToken, token_type = TokenType} } =
		oauth2_client:refresh_access_token(?config(oauth_provider, Config), build_refresh_token_request(Parameters)),
	?assertEqual(proplists:get_value(token_type, JsonPayload), TokenType),
	?assertEqual(proplists:get_value(access_token, JsonPayload), AccessToken).

denies_access_token(Config) ->
  #{request := #{parameters := Parameters},
		response := [ {code, 400}, {content_type, _CT}, {payload, JsonPayload}] }
		= ?config(denies_access_token, Config),
	{error, #unsuccessful_access_token_response{error = Error, error_description = ErrorDescription} } =
		oauth2_client:get_access_token(?config(oauth_provider, Config),build_access_token_request(Parameters)),
	?assertEqual(proplists:get_value(error, JsonPayload), Error),
	?assertEqual(proplists:get_value(error_description, JsonPayload), ErrorDescription).

auth_server_error(Config) ->
  #{request := #{parameters := Parameters},
		response := [ {code, 500} ] } = ?config(auth_server_error, Config),
	{error, "Internal Server Error"} =
		oauth2_client:get_access_token(?config(oauth_provider, Config), build_access_token_request(Parameters)).

non_json_payload(Config) ->
  #{request := #{parameters := Parameters}} = ?config(non_json_payload, Config),
	{error, {failed_to_decode_json, _ErrorArgs}} =
		oauth2_client:get_access_token(?config(oauth_provider, Config), build_access_token_request(Parameters)).

connection_error(Config) ->
  #{request := #{parameters := Parameters}} = ?config(grants_access_token, Config),
	{error, {failed_connect, _ErrorArgs} } = oauth2_client:get_access_token(
		?config(oauth_provider, Config), build_access_token_request(Parameters)).

grants_access_token_with_ssl(Config) ->
  #{request := #{parameters := Parameters}} = ?config(grants_access_token_with_ssl, Config),

	{ok, #successful_access_token_response{access_token = _AccessToken, token_type = _TokenType} } =
		oauth2_client:get_access_token(?config(oauth_provider, Config), build_access_token_request(Parameters)).

ssl_connection_error(Config) ->
	#{request := #{parameters := Parameters}} = ?config(ssl_connection_error, Config),

	{error, {failed_connect, _} } = oauth2_client:get_access_token(
		?config(oauth_provider_with_wrong_ca, Config), build_access_token_request(Parameters)).

get_openid_configuration(Config) ->
	#{response := [ {code, 200}, {content_type, _CT}, {payload, JsonPayload}] }
		= ?config(get_openid_configuration, Config),

	{ok, #oauth_provider{issuer = Issuer, token_endpoint = TokenEndPoint,
		jwks_uri = JwksURI} } =
		oauth2_client:get_openid_configuration(?config(issuer, Config)),

	?assertEqual(proplists:get_value(issuer, JsonPayload), Issuer),
	?assertEqual(proplists:get_value(token_endpoint, JsonPayload), TokenEndPoint),
	?assertEqual(proplists:get_value(jwks_uri, JsonPayload), JwksURI).


%%% HELPERS
build_issuer(Scheme) ->
	uri_string:recompose(#{scheme => Scheme,
												 host => "localhost",
												 port => rabbit_data_coercion:to_integer(?AUTH_PORT),
												 path => "/"}).

build_token_endpoint_uri(Scheme) ->
	uri_string:recompose(#{scheme => Scheme,
												 host => "localhost",
												 port => rabbit_data_coercion:to_integer(?AUTH_PORT),
												 path => "/token"}).

build_access_token_request(Request) ->
	#access_token_request {
	  client_id = proplists:get_value(?REQUEST_CLIENT_ID, Request),
	  client_secret = proplists:get_value(?REQUEST_CLIENT_SECRET, Request)
	}.
build_refresh_token_request(Request) ->
  #refresh_token_request{
    client_id = proplists:get_value(?REQUEST_CLIENT_ID, Request),
	  client_secret = proplists:get_value(?REQUEST_CLIENT_SECRET, Request),
	  refresh_token = proplists:get_value(?REQUEST_REFRESH_TOKEN, Request)
	}.
build_http_oauth_provider() ->
	#oauth_provider {
		issuer = build_issuer("http"),
	  token_endpoint = build_token_endpoint_uri("http")
	}.
build_https_oauth_provider(CaCertFile) ->
	#oauth_provider {
		issuer = build_issuer("https"),
	  token_endpoint = build_token_endpoint_uri("https"),
		ssl_options = ssl_options(verify_peer, false, CaCertFile)
	}.
oauth_provider_to_map(#oauth_provider{ issuer = Issuer, token_endpoint = TokenEndpoint,
	ssl_options = SslOptions}) ->
	#{ <<"issuer">> => Issuer, <<"token_endpoint">> => TokenEndpoint, <<"ssl_options">> => SslOptions}.

start_http_oauth_server(Port, Expectations) when is_list(Expectations) ->
	Dispatch = cowboy_router:compile([{'_',
		[{Path, oauth_http_mock, Expected} || #{request := #{path := Path}} = Expected <- Expectations ]
		}]),
	{ok, _} = cowboy:start_clear(
      mock_http_auth_listener,
			 [{port, Port}
			 ],
			 #{env => #{dispatch => Dispatch}});

start_http_oauth_server(Port, #{request := #{path := Path}} = Expected) ->
	Dispatch = cowboy_router:compile([{'_', [{Path, oauth_http_mock, Expected}]}]),
	{ok, _} = cowboy:start_clear(
      mock_http_auth_listener,
			 [{port, Port}
			 ],
			 #{env => #{dispatch => Dispatch}}).


start_https_oauth_server(Port, CertsDir, #{request := #{path := Path}} = Expected) ->
	Dispatch = cowboy_router:compile([{'_', [{Path, oauth_http_mock, Expected}]}]),
  {ok, _} = cowboy:start_tls(
      mock_http_auth_listener,
				[{port, Port},
				 {certfile, filename:join([CertsDir, "server", "cert.pem"])},
				 {keyfile, filename:join([CertsDir, "server", "key.pem"])}
				],
				#{env => #{dispatch => Dispatch}}).

stop_http_auth_server() ->
  cowboy:stop_listener(mock_http_auth_listener).

-spec ssl_options(ssl:verify_type(), boolean(), file:filename()) -> list().
ssl_options(PeerVerification, FailIfNoPeerCert, CaCertFile) ->
	[{verify, PeerVerification},
	  {depth, 10},
	  {fail_if_no_peer_cert, FailIfNoPeerCert},
	  {crl_check, false},
	  {crl_cache, {ssl_crl_cache, {internal, [{http, 10000}]}}},
		{cacertfile, CaCertFile}].
