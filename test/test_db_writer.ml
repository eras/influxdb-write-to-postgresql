open OUnit2

open Influxdb_write_to_postgresql

let testCreate ctx =
  let schema = {|
CREATE TABLE meas(time timestamptz NOT NULL);
CREATE UNIQUE INDEX meas_time_idx ON meas(time);
|} in
  Test_utils.with_db_writer ctx ~schema @@ fun _ -> ()

let identity x = x

let testDbOfIdentifier _ctx =
  assert_equal ~printer:identity {|moi|} (Db_writer.Internal.db_of_identifier "moi");
  assert_equal ~printer:identity {|U&"Moi"|} (Db_writer.Internal.db_of_identifier "Moi");
  assert_equal ~printer:identity {|U&"moi\0020"|} (Db_writer.Internal.db_of_identifier "moi ");
  assert_equal ~printer:identity {|U&"tidii\2603"|} (Db_writer.Internal.db_of_identifier "tidii☃")

let testInsert ctx =
  let schema = {|
CREATE TABLE meas(time timestamptz NOT NULL,
                  moi1 text NOT NULL DEFAULT(''),
                  moi2 text NOT NULL DEFAULT(''));
CREATE UNIQUE INDEX meas_time_idx ON meas(time, moi1, moi2);
|} in
  Test_utils.with_db_writer ctx ~schema @@ fun { db; _ } ->
  let db = Lazy.force db in
  let meas =
    Lexer.make_measurement
      ~measurement:"meas"
      ~tags:[("moi1", "1");("moi2", "2")]
      ~fields:[("value", Lexer.Int 42L)]
      ~time:(Some 1590329952000000000L)
  in
  let query = Db_writer.Internal.insert_of_measurement db meas in
  assert_equal ~printer:identity {|INSERT INTO meas(time, moi1, moi2, value)
VALUES (to_timestamp($1), $2, $3, $4)
ON CONFLICT(time, moi1, moi2)
DO UPDATE SET value=excluded.value|} (fst query);
  assert_equal ~printer:(fun x -> Array.to_list x |> String.concat ",") (snd query) [|"1590329952"; "1"; "2"; "42"|]

let string_of_key_field_type (str, field_type) =
  str ^ " " ^ Db_writer.Internal.db_of_field_type field_type

let string_of_list f xs =
  String.concat ", " (List.map f xs)

let testCreateTable ctx =
  let schema = {|
CREATE TABLE meas(time timestamptz NOT NULL,
                  moi1 text NOT NULL DEFAULT(''),
                  moi2 text NOT NULL DEFAULT(''));
CREATE UNIQUE INDEX meas_time_idx ON meas(time, moi1, moi2);
|} in
  Test_utils.with_db_writer ctx ~schema @@ fun { db; _ } ->
  let db = Lazy.force db in
  let meas =
    Lexer.make_measurement
      ~measurement:"meas"
      ~tags:[("moi1", "");("moi2", "2")]
      ~fields:[("k_int", Lexer.Int 42L);
               ("k_float", Lexer.FloatNum 42.0);
               ("k_string", Lexer.String "42");
               ("k_bool", Lexer.Boolean true);]
      ~time:(Some 1590329952000000000L)
  in
  let { Db_writer.Internal.md_command = query; md_table_info = table_info; _ } =
    Db_writer.Internal.make_table_command db meas in
  assert_equal ~printer:identity
    {|CREATE TABLE meas (time timestamptz NOT NULL, moi1 text NOT NULL DEFAULT(''), moi2 text NOT NULL DEFAULT(''), k_int integer, k_float double precision, k_string text, k_bool boolean, PRIMARY KEY(time, moi1, moi2))|}
    query;
  assert_equal
    ~printer:(string_of_list string_of_key_field_type)
    (let open Db_writer.Internal in
     [("k_bool", FT_Boolean);
      ("k_float", FT_Float);
      ("k_int", FT_Int);
      ("k_string", FT_String);
      ("moi1", FT_String);
      ("moi2", FT_String);
      ("time", FT_Timestamptz);
     ])
    (table_info.fields |> Db_writer.Internal.FieldMap.to_seq |> List.of_seq |> List.sort compare)

let testCreateTableJson ctx =
  let schema = {|
CREATE TABLE meas(time timestamptz NOT NULL,
                  moi1 text NOT NULL DEFAULT(''),
                  moi2 text NOT NULL DEFAULT(''));
CREATE UNIQUE INDEX meas_time_idx ON meas(time, moi1, moi2);
|} in
  let make_config db_spec =
    { Db_writer.db_spec = Lazy.force db_spec;
      time_column = "time";
      tags_column = Some "tags";
      fields_column = Some "fields";
      create_table = None;
    }
  in
  Test_utils.with_db_writer ~make_config ctx ~schema @@ fun { db; _ } ->
  let db = Lazy.force db in
  let meas =
    Lexer.make_measurement
      ~measurement:"meas"
      ~tags:[("moi1", "");("moi2", "2")]
      ~fields:[("k_int", Lexer.Int 42L);
               ("k_float", Lexer.FloatNum 42.0);
               ("k_string", Lexer.String "42");
               ("k_bool", Lexer.Boolean true);]
      ~time:(Some 1590329952000000000L)
  in
  let { Db_writer.Internal.md_command = query; md_table_info = table_info; _ } =
    Db_writer.Internal.make_table_command db meas in
  assert_equal ~printer:identity
    {|CREATE TABLE meas (time timestamptz NOT NULL, tags jsonb NOT NULL DEFAULT('{}'), fields jsonb, PRIMARY KEY(time, tags))|}
    query;
  assert_equal
    ~printer:(string_of_list string_of_key_field_type)
    (let open Db_writer.Internal in
     [("fields", FT_Jsonb);
      ("tags", FT_Jsonb);
      ("time", FT_Timestamptz);
     ])
    (table_info.fields |> Db_writer.Internal.FieldMap.to_seq |> List.of_seq |> List.sort compare)

let testInsertNoTime ctx =
  let schema = {|
CREATE TABLE meas(time timestamptz NOT NULL, moi1 TEXT NOT NULL DEFAULT(''), moi2 TEXT NOT NULL DEFAULT(''));
CREATE UNIQUE INDEX meas_time_idx ON meas(time, moi1, moi2);
|} in
  Test_utils.with_db_writer ctx ~schema @@ fun { db; _ } ->
  let db = Lazy.force db in
  let meas =
    Lexer.make_measurement
      ~measurement:"meas"
      ~tags:[("moi1", "1");("moi2", "2")]
      ~fields:[("value", Lexer.Int 42L)]
      ~time:None
  in
  let query = Db_writer.Internal.insert_of_measurement db meas in
  assert_equal ~printer:identity {|INSERT INTO meas(time, moi1, moi2, value)
VALUES (CURRENT_TIMESTAMP, $1, $2, $3)
ON CONFLICT(time, moi1, moi2)
DO UPDATE SET value=excluded.value|} (fst query);
  assert_equal ~printer:(fun x -> Array.to_list x |> String.concat ",")
    [|"1"; "2"; "42"|]
    (snd query)

let testInsertJsonTags ctx =
  let schema = {|
CREATE TABLE meas(time timestamptz NOT NULL, tags jsonb NOT NULL);
CREATE UNIQUE INDEX meas_time_idx ON meas(time, tags);
|} in
  let make_config db_spec =
    { Db_writer.db_spec = Lazy.force db_spec;
      time_column = "time";
      tags_column = Some "tags";
      fields_column = None;
      create_table = None;
    }
  in
  Test_utils.with_db_writer ~make_config ctx ~schema @@ fun { db; _ } ->
  let db = Lazy.force db in
  let meas =
    Lexer.make_measurement
      ~measurement:"meas"
      ~tags:[("moi1", "1");("moi2", "2")]
      ~fields:[("value", Lexer.Int 42L)]
      ~time:(Some 1590329952000000000L)
  in
  let query = Db_writer.Internal.insert_of_measurement db meas in
  assert_equal ~printer:identity {|INSERT INTO meas(time, tags, value)
VALUES (to_timestamp($1), $2, $3)
ON CONFLICT(time, tags)
DO UPDATE SET value=excluded.value|} (fst query);
  assert_equal ~printer:(fun x -> Array.to_list x |> String.concat ",")
    [|"1590329952"; {|{"moi1":"1","moi2":"2"}|}; "42"|] (snd query)

let testInsertJsonFields ctx =
  let schema = {|
CREATE TABLE meas(time timestamptz NOT NULL, moi1 TEXT NOT NULL DEFAULT(''), moi2 TEXT NOT NULL DEFAULT(''), fields JSONB NOT NULL);
CREATE UNIQUE INDEX meas_time_idx ON meas(time, moi1, moi2);
|} in
  let make_config db_spec =
    { Db_writer.db_spec = Lazy.force db_spec;
      time_column = "time";
      tags_column = None;
      fields_column = Some "fields";
      create_table = None;
    }
  in
  Test_utils.with_db_writer ~make_config ctx ~schema @@ fun { db; _ } ->
  let db = Lazy.force db in
  let meas =
    Lexer.make_measurement
      ~measurement:"meas"
      ~tags:[("moi1", "1");("moi2", "2")]
      ~fields:[("value", Lexer.Int 42L)]
      ~time:(Some 1590329952000000000L)
  in
  let query = Db_writer.Internal.insert_of_measurement db meas in
  assert_equal ~printer:identity {|INSERT INTO meas(time, moi1, moi2, fields)
VALUES (to_timestamp($1), $2, $3, $4)
ON CONFLICT(time, moi1, moi2)
DO UPDATE SET fields=meas.fields||excluded.fields|} (fst query);
  assert_equal ~printer:(fun x -> Array.to_list x |> String.concat ",") [|"1590329952"; "1"; "2"; {|{"value":42}|}|]
    (snd query)

let testWriteBase ?(duplicate_write=false) ?json_tags ?json_fields ?make_config ?schema ctx =
  Test_utils.with_db_writer ?make_config ctx ?schema @@ fun { db; db_spec } ->
  let db = Lazy.force db in
  let meas =
    Lexer.make_measurement
      ~measurement:"meas"
      ~tags:[("moi1", "1");("moi2", "2")]
      ~fields:[("value", Lexer.Int 42L)]
      ~time:(Some 1590329952000000000L)
  in
  (try
     ignore (Db_writer.write db [meas]);
     if duplicate_write then ignore (Db_writer.write db [meas]);
     let direct = Db_writer.Internal.new_pg_connection (Lazy.force db_spec) in
     let tags =
       match json_tags with
       | None -> "moi1, moi2"
       | Some tags -> tags ^ "->>'moi1', " ^ tags ^ "->>'moi2'"
     in
     let fields =
       match json_fields with
       | None -> "value"
       | Some fields -> fields ^ "->>'value'"
     in
     let result = direct#exec ~expect:[Postgresql.Tuples_ok] ("SELECT extract(epoch from time), " ^ tags ^ ", " ^ fields ^ " FROM meas") in
     match result#get_all_lst with
     | [[time; moi1; moi2; value]] ->
       let time = float_of_string time in
       assert_equal ~printer:string_of_float 1590329952.0 time;
       assert_equal ~printer:identity "1" moi1;
       assert_equal ~printer:identity "2" moi2;
       assert_equal ~printer:identity "42" value
     | _ ->
       assert_failure "Unexpected results"
   with
   | Db_writer.Error error ->
     Printf.ksprintf assert_failure "Db_writer error: %s" (Db_writer.string_of_error error))

let testWrite ctx =
  let schema = {|
CREATE TABLE meas(time timestamptz NOT NULL, moi1 TEXT NOT NULL DEFAULT(''), moi2 TEXT NOT NULL DEFAULT(''));
CREATE UNIQUE INDEX meas_time_idx ON meas(time, moi1, moi2);
|} in
  testWriteBase ~schema ctx;
  testWriteBase ~schema ~duplicate_write:true ctx

let testWriteCreateTable1 ctx =
  let schema = None in
  let expected_exn =
    (* hackish? *)
    OUnitTest.OUnit_failure("Db_writer error: Cannot create value meas because creation of new tables not enabled")
  in
  assert_raises expected_exn (fun () -> testWriteBase ?schema ctx)

let testWriteCreateTable2 ctx =
  let schema = None in
  let make_config db_spec =
    { Db_writer.db_spec = Lazy.force db_spec;
      time_column = "time";
      tags_column = None;
      fields_column = None;
      create_table =
        let open Config in
        Some {
          regexp = Config.regexp ".*";
          method_ = CreateTable;
        }
    }
  in
  testWriteBase ~make_config ?schema ctx;
  testWriteBase ~make_config ?schema ~duplicate_write:true ctx

let testWriteCreateTable3 ctx =
  let schema = None in
  let make_config db_spec =
    { Db_writer.db_spec = Lazy.force db_spec;
      time_column = "time";
      tags_column = Some "tags";
      fields_column = None;
      create_table =
        let open Config in
        Some {
          regexp = Config.regexp ".*";
          method_ = CreateTable;
        }
    }
  in
  testWriteBase ~make_config ~json_tags:"tags" ?schema ctx;
  testWriteBase ~make_config ~json_tags:"tags" ?schema ~duplicate_write:true ctx

let testWriteCreateTable4 ctx =
  let schema = None in
  let make_config db_spec =
    { Db_writer.db_spec = Lazy.force db_spec;
      time_column = "time";
      tags_column = Some "tags";
      fields_column = Some "fields";
      create_table =
        let open Config in
        Some {
          regexp = Config.regexp ".*";
          method_ = CreateTable;
        }
    }
  in
  testWriteBase ~make_config ~json_tags:"tags" ~json_fields:"fields" ?schema ctx;
  testWriteBase ~make_config ~json_tags:"tags" ~json_fields:"fields" ?schema ~duplicate_write:true ctx

let testWriteMultiBase ?json_tags ?json_fields ?make_config ?schema ctx =
  Test_utils.with_db_writer ?make_config ctx ?schema @@ fun { db; db_spec } ->
  let db = Lazy.force db in
  let test_sequence label input all_reference_content =
    (try
       ignore (Db_writer.write db input);
       let direct = Db_writer.Internal.new_pg_connection (Lazy.force db_spec) in
       let tags =
         match json_tags with
         | None -> "moi1, moi2"
         | Some tags -> tags ^ "->>'moi1', " ^ tags ^ "->>'moi2'"
       in
       let fields =
         match json_fields with
         | None -> "value"
         | Some fields -> fields ^ "->>'value'"
       in
       let query = "SELECT extract(epoch from time), " ^ tags ^ ", " ^ fields ^ " FROM meas" in
       let result = direct#exec ~expect:[Postgresql.Tuples_ok] query in
       assert_equal ~printer:string_of_int (List.length all_reference_content) (List.length result#get_all_lst);
       List.combine (List.sort compare result#get_all_lst) (List.sort compare all_reference_content) |> List.iter @@ function
       | ([time; moi1; moi2; value], (time', moi1', moi2', value')) ->
         let time = float_of_string time in
         assert_equal ~printer:string_of_float time' time;
         assert_equal ~printer:identity moi1' moi1;
         assert_equal ~printer:identity moi2' moi2;
         assert_equal ~printer:identity value' value
       | _ ->
         assert_failure "Unexpected columns"
     with
     | Db_writer.Error error ->
       Printf.ksprintf assert_failure "Db_writer error in sequence %s: %s" label (Db_writer.string_of_error error))
  in
  test_sequence "1"
    [Lexer.make_measurement
       ~measurement:"meas"
       ~tags:[("moi1", "1");("moi2", "2")]
       ~fields:[("value", Lexer.Int 42L)]
       ~time:(Some 1590329952000000000L);
     Lexer.make_measurement
       ~measurement:"meas"
       ~tags:[("moi1", "1");("moi2", "2")]
       ~fields:[("value", Lexer.Int 42L)]
       ~time:(Some 1590329952000000000L)]
    [(1590329952.0, "1", "2", "42")];
  test_sequence "2"
    [Lexer.make_measurement
       ~measurement:"meas"
       ~tags:[("moi1", "1")]
       ~fields:[("value", Lexer.Int 43L)]
       ~time:(Some 1590329952000000000L);
     Lexer.make_measurement
       ~measurement:"meas"
       ~tags:[("moi2", "2")]
       ~fields:[("value", Lexer.Int 44L)]
       ~time:(Some 1590329952000000000L)]
    [(1590329952.0, "1", "2", "42");
     (1590329952.0, "1", "",  "43");
     (1590329952.0, "",  "2", "44");
    ]

let testWriteMulti1 ctx =
  let schema = {|
CREATE TABLE meas(time timestamptz NOT NULL, moi1 TEXT NOT NULL DEFAULT(''), moi2 TEXT NOT NULL DEFAULT(''));
CREATE UNIQUE INDEX meas_time_idx ON meas(time, moi1, moi2);
|} in
  testWriteMultiBase ~schema ctx

let testWriteMulti2 ctx =
  let schema = None in
  let make_config db_spec =
    { Db_writer.db_spec = Lazy.force db_spec;
      time_column = "time";
      tags_column = None;
      fields_column = None;
      create_table =
        let open Config in
        Some {
          regexp = Config.regexp ".*";
          method_ = CreateTable;
        }
    }
  in
  testWriteMultiBase ~make_config ?schema ctx

let testWriteMulti3 ctx =
  let schema = None in
  let make_config db_spec =
    { Db_writer.db_spec = Lazy.force db_spec;
      time_column = "time";
      tags_column = Some "tags";
      fields_column = None;
      create_table =
        let open Config in
        Some {
          regexp = Config.regexp ".*";
          method_ = CreateTable;
        }
    }
  in
  testWriteMultiBase ~make_config ~json_tags:"tags" ?schema ctx

let testWriteMulti4 ctx =
  let schema = None in
  let make_config db_spec =
    { Db_writer.db_spec = Lazy.force db_spec;
      time_column = "time";
      tags_column = Some "tags";
      fields_column = Some "fields";
      create_table =
        let open Config in
        Some {
          regexp = Config.regexp ".*";
          method_ = CreateTable;
        }
    }
  in
  testWriteMultiBase ~make_config ~json_tags:"tags" ~json_fields:"fields" ?schema ctx

let testWriteNoTime ctx =
  let schema = {|
CREATE TABLE meas(time timestamptz NOT NULL, moi1 text NOT NULL DEFAULT(''), moi2 text NOT NULL DEFAULT(''));
CREATE UNIQUE INDEX meas_time_idx ON meas(time, moi1, moi2);
|} in
  Test_utils.with_db_writer ctx ~schema @@ fun { db; db_spec } ->
  let db = Lazy.force db in
  let meas =
    Lexer.make_measurement
      ~measurement:"meas"
      ~tags:[("moi1", "1");("moi2", "2")]
      ~fields:[("value", Lexer.Int 42L)]
      ~time:None
  in
  (try
     ignore (Db_writer.write db [meas]);
     let direct = Db_writer.Internal.new_pg_connection (Lazy.force db_spec) in
     let result = direct#exec ~expect:[Postgresql.Tuples_ok] "SELECT extract(epoch from now() - time), moi1, moi2, value FROM meas" in
     match result#get_all_lst with
     | [[delta; moi1; moi2; value]] ->
       let delta = float_of_string delta in
       assert_bool "Time delta is negative" (delta >= 0.0);
       assert_bool "Time delta is too big" (delta <= 10.0);
       assert_equal ~printer:identity "1" moi1;
       assert_equal ~printer:identity "2" moi2;
       assert_equal ~printer:identity "42" value
     | _ ->
       assert_failure "Unexpected results"
   with
   | Db_writer.Error error ->
     Printf.ksprintf assert_failure "Db_writer error: %s" (Db_writer.string_of_error error))

let testWriteJsonTags ctx =
  let schema = {|
CREATE TABLE meas(time timestamptz NOT NULL, tags jsonb NOT NULL);
CREATE UNIQUE INDEX meas_time_idx ON meas(time, tags);
|} in
  let make_config db_spec =
    { Db_writer.db_spec = Lazy.force db_spec;
      time_column = "time";
      tags_column = Some "tags";
      fields_column = None;
      create_table = None;
    }
  in
  Test_utils.with_db_writer ~make_config ctx ~schema @@ fun { db; db_spec } ->
  let db = Lazy.force db in
  let meas =
    Lexer.make_measurement
      ~measurement:"meas"
      ~tags:[("moi1", "1");("moi2", "2")]
      ~fields:[("value", Lexer.Int 42L)]
      ~time:(Some 1590329952000000000L)
  in
  (try
     ignore (Db_writer.write db [meas]);
     let direct = Db_writer.Internal.new_pg_connection (Lazy.force db_spec) in
     let result = direct#exec ~expect:[Postgresql.Tuples_ok] "SELECT extract(epoch from time), tags->>'moi1', tags->>'moi2', value FROM meas" in
     match result#get_all_lst with
     | [[time; moi1; moi2; value]] ->
       let time = float_of_string time in
       assert_equal ~printer:string_of_float 1590329952.0 time;
       assert_equal ~printer:identity "1" moi1;
       assert_equal ~printer:identity "2" moi2;
       assert_equal ~printer:identity "42" value
     | _ ->
       assert_failure "Unexpected results"
   with
   | Db_writer.Error error ->
     Printf.ksprintf assert_failure "Db_writer error: %s" (Db_writer.string_of_error error))

let testWriteWriteJsonFields ctx =
  let schema = {|
CREATE TABLE meas(time timestamptz NOT NULL, moi1 TEXT NOT NULL DEFAULT(''), moi2 TEXT NOT NULL DEFAULT(''), fields jsonb NOT NULL);
CREATE UNIQUE INDEX meas_time_idx ON meas(time, moi1, moi2);
|} in
  let make_config db_spec =
    { Db_writer.db_spec = Lazy.force db_spec;
      time_column = "time";
      tags_column = None;
      fields_column = Some "fields";
      create_table = None;
    }
  in
  Test_utils.with_db_writer ~make_config ctx ~schema @@ fun { db; db_spec } ->
  let db = Lazy.force db in
  let meas =
    Lexer.make_measurement
      ~measurement:"meas"
      ~tags:[("moi1", "1");("moi2", "2")]
      ~fields:[("value", Lexer.Int 42L)]
      ~time:(Some 1590329952000000000L)
  in
  (try
     ignore (Db_writer.write db [meas]);
     let direct = Db_writer.Internal.new_pg_connection (Lazy.force db_spec) in
     let result = direct#exec ~expect:[Postgresql.Tuples_ok] "SELECT extract(epoch from time), moi1, moi2, fields->>'value' FROM meas" in
     match result#get_all_lst with
     | [[time; moi1; moi2; value]] ->
       let time = float_of_string time in
       assert_equal ~printer:string_of_float 1590329952.0 time;
       assert_equal ~printer:identity "1" moi1;
       assert_equal ~printer:identity "2" moi2;
       assert_equal ~printer:identity "42" value
     | _ ->
       assert_failure "Unexpected results"
   with
   | Db_writer.Error error ->
     Printf.ksprintf assert_failure "Db_writer error: %s" (Db_writer.string_of_error error))

let suite = "Db_writer" >::: [
  "testCreate" >:: testCreate;
  "testDbOfIdentifier" >:: testDbOfIdentifier;
  "testInsert" >:: testInsert;
  "testInsertNoTime" >:: testInsertNoTime;
  "testInsertJsonTags" >:: testInsertJsonTags;
  "testInsertJsonFields" >:: testInsertJsonFields;
  "testCreateTable" >:: testCreateTable;
  "testCreateTableJson" >:: testCreateTableJson;
  "testWrite" >:: testWrite;
  "testWriteCreateTable1" >:: testWriteCreateTable1;
  "testWriteCreateTable2" >:: testWriteCreateTable2;
  "testWriteCreateTable3" >:: testWriteCreateTable3;
  "testWriteCreateTable4" >:: testWriteCreateTable4;
  "testWriteNoTime" >:: testWriteNoTime;
  "testWriteJsonTags" >:: testWriteJsonTags;
  "testWriteWriteJsonFields" >:: testWriteWriteJsonFields;
  "testWriteMulti1" >:: testWriteMulti1;
  "testWriteMulti2" >:: testWriteMulti2;
  "testWriteMulti3" >:: testWriteMulti3;
  "testWriteMulti4" >:: testWriteMulti4;
]
