-module(oauth2_client).
-export([get_access_token/2,
        refresh_access_token/2,
        get_openid_configuration/1,get_openid_configuration/2,get_openid_configuration/3]).

-include("oauth2_client.hrl").
-define(APP, auth_aouth2).

-spec get_access_token(oauth_provider_id() | oauth_provider(), access_token_request()) ->
  {ok, successful_access_token_response()} | {error, unsuccessful_access_token_response() | any()}.
get_access_token(OAuth2ProviderId, Request) when is_binary(OAuth2ProviderId) ->
  rabbit_log:debug("get_access_token OAuth2ProviderId:~p Request:~p env(rabbitmq_auth_backend_oauth2):~p",
    [OAuth2ProviderId, Request, application:get_all_env(rabbitmq_auth_backend_oauth2)]),
  get_access_token(lookup_oauth_provider_with_token_endpoint(OAuth2ProviderId), Request);

get_access_token(OAuthProvider, Request) ->
  rabbit_log:debug("get_access_token OAuthProvider:~p Request:~p", [OAuthProvider, Request]),
  URL = OAuthProvider#oauth_provider.token_endpoint,
  Header = [],
  Type = ?CONTENT_URLENCODED,
  Body = build_access_token_request_body(Request),
  HTTPOptions = get_ssl_options_if_any(OAuthProvider) ++
    get_timeout_of_default(Request#access_token_request.timeout),
  Options = [],
  rabbit_log:debug("get_access_token URL:~p using httpOptions: ~p", [URL, HTTPOptions]),
  Response = httpc:request(post, {URL, Header, Type, Body}, HTTPOptions, Options),
  ParsedResponse = parse_access_token_response(Response),
  rabbit_log:debug("get_access_token ParsedResponse:~p", [ParsedResponse]),
  ParsedResponse.

-spec refresh_access_token(oauth_provider(), refresh_token_request()) ->
  {ok, successful_access_token_response()} | {error, unsuccessful_access_token_response() | any()}.
refresh_access_token(OAuthProvider, Request) ->
  URL = OAuthProvider#oauth_provider.token_endpoint,
  Header = [],
  Type = ?CONTENT_URLENCODED,
  Body = build_refresh_token_request_body(Request),
  HTTPOptions = get_ssl_options_if_any(OAuthProvider) ++
    get_timeout_of_default(Request#refresh_token_request.timeout),
  Options = [],
  Response = httpc:request(post, {URL, Header, Type, Body}, HTTPOptions, Options),
  parse_access_token_response(Response).

-spec get_openid_configuration(uri_string:uri_string(), erlang:iodata() | <<>>, ssl:tls_option() | []) -> {ok, oauth_provider()} | {error, term()}.
get_openid_configuration(IssuerURI, OpenIdConfigurationPath, TLSOptions) ->
  URL = uri_string:resolve(OpenIdConfigurationPath, IssuerURI),
  Options = [],
  Response = httpc:request(get, {URL, []}, TLSOptions, Options),
  enrich_oauth_provider(parse_openid_configuration_response(Response), TLSOptions).

-spec get_openid_configuration(uri_string:uri_string(), ssl:tls_option() | []) ->  {ok, oauth_provider()} | {error, term()}.
get_openid_configuration(IssuerURI, TLSOptions) ->
  get_openid_configuration(IssuerURI, ?DEFAULT_OPENID_CONFIGURATION_PATH, TLSOptions).

-spec get_openid_configuration(uri_string:uri_string()) -> {ok, oauth_provider()} | {error, term()}.
get_openid_configuration(IssuerURI) ->
  get_openid_configuration(IssuerURI, ?DEFAULT_OPENID_CONFIGURATION_PATH, []).


%% HELPER functions

lookup_oauth_provider_with_token_endpoint(OAuth2ProviderId) ->
  Config = lookup_oauth_provider_config(OAuth2ProviderId),
  rabbit_log:debug("Found oauth_provider: ~p ", [Config]),
  OAuthProvider = case Config of
    {error,_} = Error -> throw(Error);
    _ -> map_to_oauth_provider(Config)
  end,
  rabbit_log:debug("Found oauth_provider: ~p ", [OAuthProvider]),
  case OAuthProvider#oauth_provider.token_endpoint of
    undefined -> case OAuthProvider#oauth_provider.issuer of
                  undefined -> {error, invalid_oauth_provider_config};
                  Issuer -> get_openid_configuration(Issuer, <<>>, [])
                end;
    _ -> OAuthProvider
  end.

lookup_oauth_provider_config(OAuth2ProviderId) ->
  case application:get_env(rabbitmq_auth_backend_oauth2, oauth_providers) of
    undefined -> {error, oauth_provider_not_found};
    {ok, MapOfProviders} when is_map(MapOfProviders) ->
        rabbit_log:debug("Map of providers: ~p ", [MapOfProviders]),
        case maps:get(OAuth2ProviderId, MapOfProviders, undefined) of
          undefined -> {error, oauth_provider_not_found};
          Value -> Value
        end;
    _ ->  {error, invalid_oauth_provider_configuration}
  end.

build_access_token_request_body(Request) ->
  uri_string:compose_query([
    grant_type_request_parameter(?CLIENT_CREDENTIALS_GRANT_TYPE),
    client_id_request_parameter(Request#access_token_request.client_id),
    client_secret_request_parameter(Request#access_token_request.client_secret)]
    ++ scope_request_parameter_or_default(Request#access_token_request.scope, [])).

build_refresh_token_request_body(Request) ->
  uri_string:compose_query([
    grant_type_request_parameter(?REFRESH_TOKEN_GRANT_TYPE),
    refresh_token_request_parameter(Request#refresh_token_request.refresh_token),
    client_id_request_parameter(Request#refresh_token_request.client_id),
    client_secret_request_parameter(Request#refresh_token_request.client_secret)]
     ++ scope_request_parameter_or_default(Request#refresh_token_request.scope, [])).

grant_type_request_parameter(Type) ->
  {?REQUEST_GRANT_TYPE, Type}.
client_id_request_parameter(Client_id) ->
  {?REQUEST_CLIENT_ID, binary_to_list(Client_id)}.
client_secret_request_parameter(Client_secret) ->
  {?REQUEST_CLIENT_SECRET, binary_to_list(Client_secret)}.
refresh_token_request_parameter(RefreshToken) ->
  {?REQUEST_REFRESH_TOKEN, RefreshToken}.
scope_request_parameter_or_default(Scope, Default) ->
  case Scope of
    undefined -> Default;
    <<>> -> Default;
    Scope -> [{?REQUEST_SCOPE, Scope}]
  end.

get_ssl_options_if_any(OAuthProvider) ->
  case OAuthProvider#oauth_provider.ssl_options of
    undefined -> [];
    Options ->  [{ssl, Options}]
  end.
get_timeout_of_default(Timeout) ->
  case Timeout of
    undefined -> [{timeout, ?DEFAULT_HTTP_TIMEOUT}];
    Timeout -> [{timeout, Timeout}]
  end.

is_json(?CONTENT_JSON) -> true;
is_json(_) -> false.

-spec decode_body(string(), string() | binary() | term()) -> 'false' | 'null' | 'true' |
                                                              binary() | [any()] | number() | map() | {error, term()}.

decode_body(_, []) -> [];
decode_body(?CONTENT_JSON, Body) ->
    case rabbit_json:try_decode(rabbit_data_coercion:to_binary(Body)) of
        {ok, Value} ->
            Value;
        {error, _} = Error  ->
            Error
    end;
decode_body(MimeType, Body) ->
    Items = string:split(MimeType, ";"),
    rabbit_log:debug("checking mimetype ~p -> ~p " , [MimeType, Items]),
    case lists:any(fun is_json/1, Items) of
      true -> decode_body(?CONTENT_JSON, Body);
      false -> {error, mime_type_is_not_json}
    end.


map_to_successful_access_token_response(Json) ->
  #successful_access_token_response{
    access_token=maps:get(?RESPONSE_ACCESS_TOKEN, Json),
    token_type=maps:get(?RESPONSE_TOKEN_TYPE, Json, undefined),
    refresh_token=maps:get(?RESPONSE_REFRESH_TOKEN, Json, undefined),
    expires_in=maps:get(?RESPONSE_EXPIRES_IN, Json, undefined)
  }.

map_to_unsuccessful_access_token_response(Json) ->
  #unsuccessful_access_token_response{
    error=maps:get(?RESPONSE_ERROR, Json),
    error_description=maps:get(?RESPONSE_ERROR_DESCRIPTION, Json, undefined)
  }.

map_to_oauth_provider(Json) when is_map(Json) ->
  rabbit_log:debug("map_to_oauth_provider map: ~p", [Json]),
  #oauth_provider{
    issuer=maps:get(?RESPONSE_ISSUER, Json),
    token_endpoint=maps:get(?RESPONSE_TOKEN_ENDPOINT, Json),
    authorization_endpoint=maps:get(?RESPONSE_AUTHORIZATION_ENDPOINT, Json, undefined),
    jwks_uri=maps:get(?RESPONSE_JWKS_URI, Json, undefined),
    ssl_options=maps:get(?RESPONSE_SSL_OPTIONS, Json, undefined)
  };
map_to_oauth_provider(PropList) when is_list(PropList) ->
  rabbit_log:debug("map_to_oauth_provider propList: ~p", [PropList]),
  #oauth_provider{
    issuer=proplists:get_value(issuer, PropList),
    token_endpoint=proplists:get_value(token_endpoint, PropList),
    authorization_endpoint=proplists:get_value(authorization_endpoint, PropList, undefined),
    jwks_uri=proplists:get_value(jwks_uri, PropList, undefined),
    ssl_options=proplists:get_value(ssl_options, PropList, undefined)
  }.

enrich_oauth_provider({ok, OAuthProvider}, TLSOptions) ->
  {ok, OAuthProvider#oauth_provider{ssl_options=TLSOptions}};
enrich_oauth_provider(Response, _) ->
  Response.

map_to_access_token_response(Code, Reason, Headers, Body) ->
  case decode_body(proplists:get_value("content-type", Headers, ?CONTENT_JSON), Body) of
    {error, {error, InternalError}} ->
      {error, InternalError};
    {error, _} = Error ->
      Error;
    Value ->
      case Code of
        200 -> {ok, map_to_successful_access_token_response(Value)};
        201 -> {ok, map_to_successful_access_token_response(Value)};
        204 -> {ok, []};
        400 -> {error, map_to_unsuccessful_access_token_response(Value)};
        401 -> {error, map_to_unsuccessful_access_token_response(Value)};
        _ ->   {error, Reason}
      end
  end.

map_response_to_oauth_provider(Code, Reason, Headers, Body) ->
  case decode_body(proplists:get_value("content-type", Headers, ?CONTENT_JSON), Body) of
    {error, {error, InternalError}} ->
      {error, InternalError};
    {error, _} = Error ->
      Error;
    Value ->
      case Code of
        200 -> {ok, map_to_oauth_provider(Value)};
        201 -> {ok, map_to_oauth_provider(Value)};
        _ ->   {error, Reason}
      end
  end.


parse_access_token_response({error, Reason}) ->
  {error, Reason};
parse_access_token_response({ok,{{_,Code,Reason}, Headers, Body}}) ->
  map_to_access_token_response(Code, Reason, Headers, Body).

parse_openid_configuration_response({error, Reason}) ->
  {error, Reason};
parse_openid_configuration_response({ok,{{_,Code,Reason}, Headers, Body}}) ->
  map_response_to_oauth_provider(Code, Reason, Headers, Body).
