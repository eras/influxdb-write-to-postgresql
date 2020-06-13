open OUnit2
open Influxdb_write_to_postgresql

let _testDump _ctx =
  let config = Config.({
      users = ["test", {
          group = None;
          password = Some { type_ = Plain; password = "" };
          expires = None;
        }];
      regexp_users = [".*", {
          group = None;
          password = Some { type_ = Plain; password = "" };
          expires = None;
        }];
      groups = [];
      databases = [("tidii", {
          db_name = "tadataa";
          db_host = "local";
          db_port = 4242;
          db_user = "asdf";
          db_password = "plop";
          allowed_users = None;
          create_table = Some {
              regexp = regexp ".*";
              method_ = CreateTable;
            };
          time_column = None;
          tags_jsonb_column = None;
          tag_columns = None;
          fields_jsonb_column = None;
          field_columns = None;
        });
        ];
      regexp_databases = [];
      realm = "lala";
    }) in
  Config.dump_exn config

let testLoad _ctx =
  let _config = Config.load_exn "doc/config.example.yaml" in
  ()

let suite = "Db_config" >::: [
    (* "_testDump" >:: _testDump; *)
    "testLoad" >:: testLoad;
  ]
