open OUnit2
open Influxdb_write_to_postgresql

let _testDump _ctx =
  let config = Config.({
      users = ["test", {
          token = None;
          group = None;
          password = "";
          expires = None;
        }];
      regexp_users = [".*", {
          group = None;
          token = None;
          password = "";
          expires = None;
        }];
      groups = [];
      databases = [("tidii", {
          db_name = "tadataa";
          db_host = "local";
          db_port = 4242;
          db_user = "asdf";
          db_password = "plop";
          time_column = None;
          tags_jsonb_column = None;
          tag_columns = None;
          fields_jsonb_column = None;
          field_columns = None;
        });
      ];
      regexp_databases = [];
    }) in
  Config.dump config

let testLoad _ctx =
  let _config = Config.load "config.example.yaml" in
  ()

let suite = "Db_spool" >::: [
  (* "_testDump" >:: _testDump; *)
  "testLoad" >:: testLoad;
]
