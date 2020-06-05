open OUnit2
open Influxdb_write_to_postgresql

let testPlain _ctx =
  let config =
    { Auth.users =
        let open Config in
        ["test",
         {
           token = None;
           group = None;
           password = { type_ = Plain; password = "password" };
           expires = None;
         }]
    }
  in
  let auth = Auth.create config in
  let context =
    {
      Auth.allowed_users = Some ["test"; "test2"];
    }
  in
  let printer = function
    | Auth.AuthSuccess -> "AuthSuccess"
    | AuthFailed -> "AuthFailed"
  in
  let case ~user ~password ~expect =
    let user, msg_user =
      match user with
      | `Incorrect -> "not test", "`Incorrect"
      | `Correct -> "test", "`Correct"
    in
    let password, msg_password =
      match password with
      | `Incorrect -> "not password", "`Incorrect"
      | `Correct -> "password", "`Correct"
    in
    let msg_expect =
      match expect with
      | Auth.AuthSuccess -> "Auth.AuthSuccess"
      | Auth.AuthFailed -> "Auth.AuthFailed"
    in
    let request =
      {
        Auth.user = Some user;
        password = Some password;
        token = None;
      }
    in
    let msg = Printf.sprintf "%s, %s -> %s" msg_user msg_password msg_expect in
    assert_equal ~printer ~msg
      expect
      (Auth.permitted auth ~context ~request);
  in
  let flip f a b = f b a in
  flip List.iter [`Incorrect; `Correct] @@ fun user ->
  flip List.iter [`Incorrect; `Correct] @@ fun password ->
  (* writing this way helps the compiler test all cases in a searchable manner :-o *)
  let expect = match user, password with
    | `Incorrect, `Incorrect -> Auth.AuthFailed
    | `Incorrect, `Correct -> Auth.AuthFailed
    | `Correct, `Incorrect -> Auth.AuthFailed
    | `Correct, `Correct -> Auth.AuthSuccess
  in
  case ~user ~password ~expect

let suite = "Db_auth" >::: [
  "testPlain" >:: testPlain;
]
