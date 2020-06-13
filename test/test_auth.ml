open OUnit2
open Influxdb_write_to_postgresql

let case ~user ~password ~expect auth_request =
  let user, msg_user =
    match user with
    | `IncorrectUser -> Some "not test", "`IncorrectUser"
    | `CorrectUser -> Some "test1", "`CorrectUser"
    | `MissingUser -> None, "`MissingUser"
  in
  let password, msg_password =
    match password with
    | `IncorrectPassword -> Some "not password", "`IncorrectPassword"
    | `CorrectPassword -> Some "password1", "`CorrectPassword"
    | `MissingPassword -> None, "`MissingPassword"
  in
  let msg_expect =
    match expect with
    | Ok result -> "Ok " ^ Auth.show_result result
    | Error error -> "Error " ^ Auth.show_error error
  in
  let request =
    {
      Auth.user = user;
      password = password;
    }
  in
  let msg = Printf.sprintf "Grep for:\n%s, %s -> %s" msg_user msg_password msg_expect in
  let result =
    try Ok (auth_request ~request)
    with Auth.Error error -> Error error
  in
  let printer = function
    | Ok result -> "Ok " ^ Auth.show_result result
    | Error error -> "Err " ^ Auth.show_error error
  in
  assert_equal ~printer ~msg
    expect
    result

let flip f a b = f b a

let user_pass_config ?(password=(Config.Plain, "password1")) () =
  { Auth.users =
      let open Config in
      ["test1",
       {
         group = None;
         password = Some { type_ = fst password; password = snd password };
         expires = None;
       }]
  }

let user_password_driver auth_request =
  flip List.iter [`IncorrectUser; `CorrectUser] @@ fun user ->
  flip List.iter [`IncorrectPassword; `CorrectPassword] @@ fun password ->
  (* writing this way helps the compiler test all cases in a searchable manner :-o *)
  let expect = match user, password with
    | `IncorrectUser, `IncorrectPassword -> Ok Auth.AuthFailed
    | `IncorrectUser, `CorrectPassword -> Ok Auth.AuthFailed
    | `CorrectUser, `IncorrectPassword -> Ok Auth.AuthFailed
    | `CorrectUser, `CorrectPassword -> Ok Auth.AuthSuccess
  in
  case ~user ~password ~expect auth_request

let no_auth_driver auth_request =
  flip List.iter [`MissingUser; `IncorrectUser; `CorrectUser] @@ fun user ->
  flip List.iter [`MissingPassword; `IncorrectPassword; `CorrectPassword] @@ fun password ->
  (* writing this way helps the compiler test all cases in a searchable manner :-o *)
  let expect = match user, password with
    | `IncorrectUser, `IncorrectPassword -> Ok Auth.AuthFailed
    | `IncorrectUser, `CorrectPassword -> Ok Auth.AuthFailed
    | `CorrectUser, `IncorrectPassword -> Ok Auth.AuthFailed
    | `CorrectUser, `CorrectPassword -> Ok Auth.AuthFailed
    | `MissingUser, `IncorrectPassword -> Ok Auth.AuthFailed
    | `MissingUser, `CorrectPassword -> Ok Auth.AuthFailed
    | `CorrectUser, `MissingPassword -> Ok Auth.AuthFailed
    | `IncorrectUser, `MissingPassword -> Ok Auth.AuthFailed
    | `MissingUser, `MissingPassword -> Ok Auth.AuthSuccess
  in
  case ~user ~password ~expect auth_request

let testPlain _ctx =
  let auth = Auth.create (user_pass_config ()) in
  let context = { Auth.allowed_users = Some ["test1"; "test2"] } in
  user_password_driver @@ fun ~(request:Auth.request) ->
  Auth.permitted_exn auth ~context ~request

let testArgon2 _ctx =
  (* encrypted "password1" *)
  let password = Config.(Argon2, {|$argon2i$v=19$m=4096,t=3,p=1$aGVsbG93b3JsZA$T0FmZ4u+J2dXCUHmfCytd5L1dqOzbxlpYSyci7jCWaA|}) in
  let auth = Auth.create (user_pass_config ~password ()) in
  let context = { Auth.allowed_users = Some ["test1"; "test2"] } in
  user_password_driver @@ fun ~(request:Auth.request) ->
  Auth.permitted_exn auth ~context ~request

let basic_of_request (request : Auth.request) =
  let headers = Cohttp.Header.init () in
  match request with
  | { user = None; password = None; _; } ->
    (* Don't even add a header in this case *)
    headers
  | _ ->
    let base64enc = Cryptokit.Base64.encode_compact_pad () in
    Option.iter (fun user -> base64enc#put_string user) request.user;
    base64enc#put_string ":";
    Option.iter (fun password -> base64enc#put_string password) request.password;
    base64enc#finish;
    let base64 = base64enc#get_string in
    Cohttp.Header.add headers "Authorization" (Printf.sprintf "Basic %s" base64)

let testBasic _ctx =
  let auth = Auth.create (user_pass_config ()) in
  let context = { Auth.allowed_users = Some ["test1"; "test2"] } in
  user_password_driver @@ fun ~(request:Auth.request) ->
  let header = basic_of_request request in
  Auth.permitted_header_exn auth ~context ~header

let testToken _ctx =
  let token_of_request (request : Auth.request) =
    let headers = Cohttp.Header.init () in
    Cohttp.Header.add headers "Authorization" (Printf.sprintf "Token %s:%s"
                                                 (Option.value request.user ~default:"")
                                                 (Option.value request.password ~default:"")
                                              )
  in
  let auth = Auth.create (user_pass_config ()) in
  let context = { Auth.allowed_users = Some ["test1"; "test2"] } in
  user_password_driver @@ fun ~(request:Auth.request) ->
  let header = token_of_request request in
  Auth.permitted_header_exn auth ~context ~header

let testNoAuth _ctx =
  let auth = Auth.create (user_pass_config ()) in
  let context = { Auth.allowed_users = None } in
  no_auth_driver @@ fun ~(request:Auth.request) ->
  let header = basic_of_request request in
  Auth.permitted_header_exn auth ~context ~header

let suite = "Db_auth" >::: [
    "testPlain" >:: testPlain;
    "testArgon2" >:: testArgon2;
    "testBasic" >:: testBasic;
    "testToken" >:: testToken;
    "testNoAuth" >:: testNoAuth;
  ]
