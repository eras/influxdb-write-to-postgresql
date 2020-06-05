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
           password = { type_ = Plain; password = "helo" };
           expires = None;
         }]
    }
  in
  let auth = Auth.create config in
  let context =
    {
      Auth.allowed_users = Some ["test"; "test2"]
    }
  in
  let req user password =
    {
      Auth.user = Some user;
      password = Some password;
      token = None;
    }
  in
  assert_bool "case1: invalid user, non-matching password"
    (not (Auth.permitted auth ~context ~request:(req "not test" "dingding")));
  assert_bool "case1: invalid user, matching password"
    (not (Auth.permitted auth ~context ~request:(req "not test" "helo")));
  assert_bool "case1: valid user, non-matching password"
    (not (Auth.permitted auth ~context ~request:(req "test" "dingding")));
  assert_bool "case1: valid user, matching password"
    (Auth.permitted auth ~context ~request:(req "test" "helo"));
  assert_bool "case1: non-matching user, non-matching password"
    (not (Auth.permitted auth ~context ~request:(req "test2" "dingding")));
  assert_bool "case1: non-matching user, matching password"
    (not (Auth.permitted auth ~context ~request:(req "test2" "helo")))

let suite = "Db_auth" >::: [
  "testPlain" >:: testPlain;
]
