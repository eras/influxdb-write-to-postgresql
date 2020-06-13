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

type argon2_errorcode = Argon2.ErrorCodes.t
let pp_argon2_errorcode fmt error_code =
  Format.fprintf fmt "%s" (Argon2.ErrorCodes.message error_code)

module Argon2CacheKeyOrder : Map.OrderedType with type t = (string * string) =
struct
  type t = (string * string)
  let compare = compare
end

module Argon2Cache = Anycache.Make(Argon2CacheKeyOrder)(Anycache.Direct)

type t = {
  config : config;
  argon2_cache : (bool, argon2_errorcode) result Argon2Cache.t;
}

type context = {
  allowed_users : string list option;
}

type request = {
  user : string option;
  password : string option;
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
  | FailedToParseAuthorizationToken
  | FailedToParseAuthorizationBasic
  | CryptokitError of Cryptokit.error
  | Argon2Error of argon2_errorcode
[@@deriving show]

exception Error of error

let string_of_error = function
  | FailedToParseAuthorization ->
    "Failed to parse authorization"
  | FailedToParseAuthorizationBasic ->
    "Failed to parse basic authorization"
  | FailedToParseAuthorizationToken ->
    "Failed to parse token authorization"
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
  | Argon2Error error_code -> Argon2.ErrorCodes.message error_code

let _ = Printexc.register_printer (function
    | Error error -> Some (string_of_error error)
    | _ -> None
  )

let create config =
  { config;
    argon2_cache = Argon2Cache.create 1024 }

let argon2_lookup t =
  let lookup (encoded, password) =
    match Argon2.verify ~encoded ~pwd:password ~kind:Argon2.I with
    | Ok x -> Ok (Ok x)
    (* the next might be a dynamic error, so don't cache it (?!) *)
    | Error (Argon2.ErrorCodes.(THREAD_FAIL) as code) -> Error (Error (Argon2Error code))
    | Error x -> Ok (Error x)   (* normally we cache errors *)
  in
  fun ~encoded ~pwd ->
    match Argon2Cache.with_cache t.argon2_cache lookup (encoded, pwd) with
    | Ok x -> x
    | Error exn -> raise exn

let auth_password_ok t (_context : context) (pw : Config.password) request =
  match pw.type_, request with
  | Plain, { user = _; password = Some password } ->
    if pw.password = password
    then AuthSuccess
    else AuthFailed
  | Argon2,  { user = _; password = Some password } -> begin
      match argon2_lookup t ~encoded:pw.password ~pwd:password with
      | Ok true -> AuthSuccess
      | Ok false -> AuthFailed
      | Error Argon2.ErrorCodes.VERIFY_MISMATCH -> AuthFailed
      | Error error_code -> raise (Error (Argon2Error error_code))
    end
  | (Plain | Argon2), _ ->
    AuthFailed

let permitted t ~(context : context) ~(request : request) =
  let config_user_info = Option.map (fun user -> List.assoc_opt user t.config.users) request.user in
  match context.allowed_users, request, config_user_info with
  | None, { user = None; password = None }, _ ->
    (* If no authentication is provided and not configured, permit *)
    AuthSuccess
  | None, _, _ ->
    (* If authentication is provided but not configured, reject *)
    AuthFailed
  | Some allowed_users,
    ({ user = Some user; _ } as request),
    Some (Some { Config.password = Some password; _ }) when List.mem user allowed_users ->
    (* If authentication is provided and configured, then does it
       match? *)
    auth_password_ok t context password request
  | Some _, _, _ ->
    (* Otherwise, reject *)
    AuthFailed

type authorization =
  | Basic of (string * string)
  | Token of (string * string)

let basic_char = [%sedlex.regexp? Sub(any, ('\x00'.. '\x1f' | '\x7f'))]

(* Influxdb "token" is basic auth but without base64 🙄 *)
let parse_token ?(exn=FailedToParseAuthorizationToken) content =
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
      | _ -> raise (Error exn)
    in
    (user, password)
  | _ -> raise (Error exn)

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
  parse_token ~exn:FailedToParseAuthorizationBasic content

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
      | Plus any -> Token (parse_token (Sedlexing.Utf8.lexeme buf))
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
    }
  | Some auth ->
    match parse_authorization auth with
    | Basic (user, password) ->
      {
        user = Some user;
        password = Some password;
      }
    | Token (user, password) ->
      {
        user = Some user;
        password = Some password;
      }

let permitted_header t ~context ~header =
  let request = request_of_header header in
  permitted t ~context ~request
