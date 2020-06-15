open OUnit2
open Influxdb_write_to_postgresql

let testDump _ctx =
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
          time = TimestampTZPlusNanos { time_field = "time";
                                        nano_field = "nanos" };
          tags_jsonb_column = None;
          tag_columns = None;
          fields_jsonb_column = None;
          field_columns = None;
        });
        ];
      regexp_databases = [];
      realm = "lala";
    }) in
  assert_equal ~msg:"Comparing configurations"
    ~printer:[%derive.show: string]
    {|{"databases":[["tidii",{"create_table":{"method":"create_table","regexp":".*"},"db_host":"local","db_name":"tadataa","db_password":"plop","db_port":4242,"db_user":"asdf","fields_jsonb_column":null,"tags_jsonb_column":null,"time":{"tz+nanoseconds":{"nano_field":"nanos","time_field":"time"}}}]],"realm":"lala","regexp_users":[[".*",{"password":{"password":"","type":"plain"}}]],"users":[["test",{"password":{"password":"","type":"plain"}}]]}|}
    (Config.to_yojson config |> Yojson.Safe.sort |> Yojson.Safe.to_string)

let testLoad _ctx =
  let _config = Config.load_exn "doc/config.example.yaml" in
  ()

let suite = "Db_config" >::: [
    "testDump" >:: testDump;
    "testLoad" >:: testLoad;
  ]
