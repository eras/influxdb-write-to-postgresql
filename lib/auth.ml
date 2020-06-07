(** Module for providing [@@deriving show] -compatibility *)
module Cryptokit =
struct
  include Cryptokit

  let pp_error formatter error =
    let msg = match error with
    | Wrong_key_size -> "Wrong_key_size"
    | Wrong_IV_size -> "Wrong_IV_size"
    | Wrong_data_length -> "Wrong_data_length"
    | Bad_padding -> "Bad_padding"
    | Output_buffer_overflow -> "Output_buffer_overflow"
    | Incompatible_block_size -> "Incompatible_block_size"
    | Number_too_long -> "Number_too_long"
    | Seed_too_short -> "Seed_too_short"
    | Message_too_long -> "Message_too_long"
    | Bad_encoding -> "Bad_encoding"
    | Compression_error _ -> "Compression_error"
    | No_entropy_source -> "No_entropy_source"
    | Entropy_source_closed -> "Entropy_source_closed"
    | Compression_not_supported -> "Compression_not_supported"
    in
    Format.pp_print_string formatter msg
end

type config = {
  users : Config.users;
}

type t = {
  config : config
}

type context = {
  allowed_users : string list option;
}

type request = {
  user : string option;
  password : string option;
  token : string option;
} [@@deriving show]
let _ = pp_request              (* ignore warning *)

type result =
  | AuthSuccess
  | AuthFailed
[@@deriving show]

let string_of_result = function
  | AuthSuccess -> "AuthSuccess"
  | AuthFailed -> "AuthFailed"

type error =
  | FailedToParseAuthorization
  | FailedToParseAuthorizationBasic
  | CryptokitError of Cryptokit.error
[@@deriving show]

exception Error of error

let string_of_error = function
  | FailedToParseAuthorization ->
    "Failed to parse authorization"
  | FailedToParseAuthorizationBasic ->
    "Failed to parse basic authorization"
  | CryptokitError error -> begin
      let open Cryptokit in
      match error with
      | Wrong_key_size -> "Wrong key size"
      | Wrong_IV_size -> "Wrong IV size"
      | Wrong_data_length -> "Wrong data length"
      | Bad_padding -> "Bad padding"
      | Output_buffer_overflow -> "Output buffer overflow"
      | Incompatible_block_size -> "Incompatible block size"
      | Number_too_long -> "Number too long"
      | Seed_too_short -> "Seed too short"
      | Message_too_long -> "Message too long"
      | Bad_encoding -> "Bad encoding"
      | Compression_error _ -> "Compression error"
      | No_entropy_source -> "No entropy source"
      | Entropy_source_closed -> "Entropy source closed"
      | Compression_not_supported -> "Compression not supported"
    end

let _ = Printexc.register_printer (function
    | Error error -> Some (string_of_error error)
    | _ -> None
  )


let create config =
  { config }

let auth_password_ok (_context : context) (pw : Config.password) request =
  match pw.type_, request with
  | Plain, { user = _; password = Some password; token = None; } ->
    if pw.password = password
    then AuthSuccess
    else AuthFailed
  | Plain, _ ->
    AuthFailed

let auth_token_ok (config : config) (token : string) =
  List.find_map (
    fun (user_name, config_user) ->
      if config_user.Config.token = Some token
      then Some (user_name, AuthSuccess)
      else None
  ) config.users

let permitted t ~(context : context) ~(request : request) =
  let config_user_info = Option.map (fun user -> List.assoc_opt user t.config.users) request.user in
  match context.allowed_users, request, config_user_info with
  | None, { user = None; password = None; token = None; }, _ ->
    (* If no authentication is provided and not configured, permit *)
    AuthSuccess
  | None, _, _ ->
    (* If authentication is provided but not configured, reject *)
    AuthFailed
  | Some allowed_users,
    ({ user = user; token = Some token; password = None; } as _request),
    _config_user_info -> begin
      match auth_token_ok t.config token with
      | Some (user', result) when List.mem user' allowed_users && (user == None || (Some user' == user)) ->
        result
      | _ -> AuthFailed
    end
  | Some allowed_users,
    ({ user = Some user; _ } as request),
    Some (Some { Config.password = Some password; _ }) when List.mem user allowed_users ->
    (* If authentication is provided and configured, then does it
       match? *)
    auth_password_ok context password request
  | Some _, _, _ ->
    (* Otherwise, reject *)
    AuthFailed

type authorization =
  | Basic of (string * string)
  | Token of string

let basic_char = [%sedlex.regexp? Sub(any, ('\x00'.. '\x1f' | '\x7f'))]

let parse_base64_user_pass content =
  let content =
    try
      let base64dec = Cryptokit.Base64.decode () in
      base64dec#put_string content;
      base64dec#finish;
      base64dec#get_string;
    with Cryptokit.Error error ->
      raise (Error (CryptokitError error))
  in
  let buf = Sedlexing.Utf8.from_string content in
  (* https://tools.ietf.org/html/rfc7617#section-2 *)
  (* https://tools.ietf.org/html/rfc5234#appendix-B.1 *)
  match%sedlex buf with
  | Star(Sub(basic_char, ':')) ->
    let user = Sedlexing.Utf8.lexeme buf in
    let password =
      match%sedlex buf with
      | ':', Star(basic_char) ->
        let str = Sedlexing.Utf8.lexeme buf in
        String.sub str 1 (String.length str - 1)
      | _ -> raise (Error (FailedToParseAuthorizationBasic))
    in
    (user, password)
  | _ -> raise (Error (FailedToParseAuthorizationBasic))

let parse_authorization str =
  let buf = Sedlexing.Utf8.from_string str in
  match%sedlex buf with
  | ('B' | 'b'), ('A' | 'a'), ('S' | 's'), ('I' | 'i'), ('C' | 'c'), ' ' ->
    let content =
      match%sedlex buf with
      | Plus any -> Basic (parse_base64_user_pass (Sedlexing.Utf8.lexeme buf))
      | _ -> raise (Error FailedToParseAuthorization)
    in
    content
  | ('T' | 't'), ('O' | 'o'), ('K' | 'k'), ('E' | 'e'), ('N' | 'n'), ' ' ->
    let content =
      match%sedlex buf with
      | Plus any -> Token (Sedlexing.Utf8.lexeme buf)
      | _ -> raise (Error FailedToParseAuthorization)
    in
    content
  | _ -> raise (Error FailedToParseAuthorization)

let request_of_header header =
  match Cohttp.Header.get header "Authorization" with
  | None ->
    {
      user = None;
      password = None;
      token = None;
    }
  | Some auth ->
    match parse_authorization auth with
    | Basic (user, password) ->
      {
        user = Some user;
        password = Some password;
        token = None;
      }
    | Token token ->
      {
        user = None;
        password = None;
        token = Some token;
      }

let permitted_header t ~context ~header =
  let request = request_of_header header in
  permitted t ~context ~request
