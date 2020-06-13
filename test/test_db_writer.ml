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
  assert_equal ~printer:identity {|moi0|} (Db_writer.Internal.db_of_identifier "moi0");
  assert_equal ~printer:identity {|U&"0moi"|} (Db_writer.Internal.db_of_identifier "0moi");
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
    Influxdb_lexer.make_measurement
      ~measurement:"meas"
      ~tags:[("moi1", "1");("moi2", "2")]
      ~fields:[("value", Influxdb_lexer.Int 42L)]
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
    Influxdb_lexer.make_measurement
      ~measurement:"meas"
      ~tags:[("moi1", "");("moi2", "2")]
      ~fields:[("k_int", Influxdb_lexer.Int 42L);
               ("k_float", Influxdb_lexer.FloatNum 42.0);
               ("k_string", Influxdb_lexer.String "42");
               ("k_bool", Influxdb_lexer.Boolean true);]
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
    Influxdb_lexer.make_measurement
      ~measurement:"meas"
      ~tags:[("moi1", "");("moi2", "2")]
      ~fields:[("k_int", Influxdb_lexer.Int 42L);
               ("k_float", Influxdb_lexer.FloatNum 42.0);
               ("k_string", Influxdb_lexer.String "42");
               ("k_bool", Influxdb_lexer.Boolean true);]
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
    Influxdb_lexer.make_measurement
      ~measurement:"meas"
      ~tags:[("moi1", "1");("moi2", "2")]
      ~fields:[("value", Influxdb_lexer.Int 42L)]
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
    Influxdb_lexer.make_measurement
      ~measurement:"meas"
      ~tags:[("moi1", "1");("moi2", "2")]
      ~fields:[("value", Influxdb_lexer.Int 42L)]
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
    Influxdb_lexer.make_measurement
      ~measurement:"meas"
      ~tags:[("moi1", "1");("moi2", "2")]
      ~fields:[("value", Influxdb_lexer.Int 42L)]
      ~time:(Some 1590329952000000000L)
  in
  let query = Db_writer.Internal.insert_of_measurement db meas in
  assert_equal ~printer:identity {|INSERT INTO meas(time, moi1, moi2, fields)
VALUES (to_timestamp($1), $2, $3, $4)
ON CONFLICT(time, moi1, moi2)
DO UPDATE SET fields=meas.fields||excluded.fields|} (fst query);
  assert_equal ~printer:(fun x -> Array.to_list x |> String.concat ",") [|"1590329952"; "1"; "2"; {|{"value":42}|}|]
    (snd query)

let assert_tables db expect =
  let module DI = Db_writer.Internal in
  let sort xs = List.sort compare xs in
  let tables =
    Db_writer.Internal.query_database_info db
    |> Hashtbl.to_seq
    |> List.of_seq
    |> Common.map_snd (fun x -> x.DI.fields |> DI.FieldMap.to_seq |> Seq.map fst |> List.of_seq |> sort)
    |> sort in
  assert_equal ~printer:[%derive.show: (string * string list) list]
    (expect |> Common.map_snd (sort) |> sort)
    tables

let tags_fields json_tags json_fields =
  let tags =
    match json_tags with
    | None -> "moi1, moi2", ["moi1"; "moi2"]
    | Some tags -> tags ^ "->>'moi1', " ^ tags ^ "->>'moi2'", [tags]
  in
  let fields =
    match json_fields with
    | None -> "value", ["value"]
    | Some fields -> fields ^ "->>'value'", [fields]
  in
  (tags, fields)

let testWriteBase ?(duplicate_write=false) ?json_tags ?json_fields ?make_config ?schema ctx =
  Test_utils.with_db_writer ?make_config ctx ?schema @@ fun { db; db_spec } ->
  let db = Lazy.force db in
  let meas =
    Influxdb_lexer.make_measurement
      ~measurement:"meas"
      ~tags:[("moi1", "1");("moi2", "2")]
      ~fields:[("value", Influxdb_lexer.Int 42L)]
      ~time:(Some 1590329952000000000L)
  in
  (try
     ignore (Db_writer.write db [meas]);
     if duplicate_write then ignore (Db_writer.write db [meas]);
     let direct = Db_writer.Internal.new_pg_connection (Lazy.force db_spec) in
     let (tags, tag_fields), (fields, fields_fields) = tags_fields json_tags json_fields in
     assert_tables direct ["meas", ["time"] @ tag_fields @ fields_fields];
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

let base_test_sequence (test_sequence : string ->
                        Influxdb_lexer.measurement list ->
                        (float * string * string * string) list -> unit
                       ) =
  test_sequence "1"
    [Influxdb_lexer.make_measurement
       ~measurement:"meas"
       ~tags:[("moi1", "1");("moi2", "2")]
       ~fields:[("value", Influxdb_lexer.Int 42L)]
       ~time:(Some 1590329952000000000L);
     Influxdb_lexer.make_measurement
       ~measurement:"meas"
       ~tags:[("moi1", "1");("moi2", "2")]
       ~fields:[("value", Influxdb_lexer.Int 45L)]
       ~time:(Some 1590329952000000000L)]
    [(1590329952.0, "1", "2", "45")];
  test_sequence "2"
    [Influxdb_lexer.make_measurement
       ~measurement:"meas"
       ~tags:[("moi1", "1")]
       ~fields:[("value", Influxdb_lexer.Int 43L)]
       ~time:(Some 1590329952000000000L);
     Influxdb_lexer.make_measurement
       ~measurement:"meas"
       ~tags:[("moi2", "2")]
       ~fields:[("value", Influxdb_lexer.Int 44L)]
       ~time:(Some 1590329952000000000L)]
    [(1590329952.0, "1", "2", "45");
     (1590329952.0, "1", "",  "43");
     (1590329952.0, "",  "2", "44");
    ]

let testWriteMultiBase ?(test_sequence=base_test_sequence) ?json_tags ?json_fields ?make_config ?schema ctx =
  Test_utils.with_db_writer ?make_config ctx ?schema @@ fun { db; db_spec } ->
  let db = Lazy.force db in
  let test_sequence_op label input all_reference_content =
    (try
       ignore (Db_writer.write db input);
       let direct = Db_writer.Internal.new_pg_connection (Lazy.force db_spec) in
       let (tags, tag_fields), (fields, fields_fields) = tags_fields json_tags json_fields in
       assert_tables direct ["meas", ["time"] @ tag_fields @ fields_fields];
       let query = "SELECT extract(epoch from time), " ^ tags ^ ", " ^ fields ^ " FROM meas" in
       let result = direct#exec ~expect:[Postgresql.Tuples_ok] query in
       assert_equal ~msg:"Number of results" ~printer:string_of_int (List.length all_reference_content) (List.length result#get_all_lst);
       List.combine (List.sort compare result#get_all_lst) (List.sort compare all_reference_content) |> CCList.iteri @@ fun idx -> function
       | ([time; moi1; moi2; value], (time', moi1', moi2', value')) ->
         let time = float_of_string time in
         let label x = "Comparing " ^ x ^ " at " ^ string_of_int idx in
         assert_equal ~msg:(label "time") ~printer:string_of_float time' time;
         assert_equal ~msg:(label "moi1") ~printer:identity moi1' moi1;
         assert_equal ~msg:(label "moi2") ~printer:identity moi2' moi2;
         assert_equal ~msg:(label "value") ~printer:identity value' value
       | _ ->
         assert_failure "Unexpected columns"
     with
     | Db_writer.Error error ->
       Printf.ksprintf assert_failure "Db_writer error in sequence %s: %s" label (Db_writer.string_of_error error))
  in
  test_sequence test_sequence_op

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

let testWriteMultiMany ctx =
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
  let test_sequence test_sequence =
    let data =
      CCList.(0 -- 100000) |> List.map @@ fun i ->
      (Influxdb_lexer.make_measurement
         ~measurement:"meas"
         ~tags:[("moi1", "1");("moi2", string_of_int i)]
         ~fields:[("value", Influxdb_lexer.Int (Int64.of_int (2 * i)))]
         ~time:(Some Int64.(add 1590329952000000000L (mul 1000000000L (of_int i)))),
       (1590329952.0 +. float_of_int i, "1", string_of_int i, string_of_int (2 * i)))
    in
    let measurements, expected = List.split data in
    test_sequence "1"
      measurements
      expected
  in
  testWriteMultiBase ~test_sequence ~make_config ~json_tags:"tags" ~json_fields:"fields" ?schema ctx

let testWriteMultiCoalesce ctx =
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
  let test_sequence test_sequence =
    test_sequence "1"
      [(Influxdb_lexer.make_measurement
          ~measurement:"meas"
          ~tags:[("moi1", "1.1");("moi2", "2.1")]
          ~fields:[("value", Influxdb_lexer.Int 10L)]
          ~time:(Some 1590329952000000000L));
       (Influxdb_lexer.make_measurement
          ~measurement:"meas"
          ~tags:[("moi1", "1.1");("moi2", "2.1")]
          ~fields:[("value", Influxdb_lexer.Int 11L)]
          ~time:(Some 1590329952000000000L))]
      [(1590329952.0, "1.1", "2.1", "11")];
    test_sequence "2"
      [(Influxdb_lexer.make_measurement
          ~measurement:"meas"
          ~tags:[("moi1", "1.2");("moi2", "2.2")]
          ~fields:[("value", Influxdb_lexer.Int 12L)]
          ~time:(Some 1590329952000000000L));
       (Influxdb_lexer.make_measurement
          ~measurement:"meas"
          ~tags:[("moi1", "1.2");("moi2", "2.3")]
          ~fields:[("value", Influxdb_lexer.Int 13L)]
          ~time:(Some 1590329952000000001L))]
      [(1590329952.0, "1.1", "2.1", "11");
       (1590329952.0, "1.2", "2.2", "12");
       (1590329952.0, "1.2", "2.3", "13");
      ];
    test_sequence "3"
      [(Influxdb_lexer.make_measurement
          ~measurement:"meas"
          ~tags:[("moi1", "1.3");("moi2", "2.4")]
          ~fields:[("value", Influxdb_lexer.Int 14L)]
          ~time:(Some 1590329953000000000L));
       (Influxdb_lexer.make_measurement
          ~measurement:"meas"
          ~tags:[("moi1", "1.4");("moi2", "2.5")]
          ~fields:[("value", Influxdb_lexer.Int 15L)]
          ~time:(Some 1590329954000000001L))]
      [(1590329952.0, "1.1", "2.1", "11");
       (1590329952.0, "1.2", "2.2", "12");
       (1590329952.0, "1.2", "2.3", "13");
       (1590329953.0, "1.3", "2.4", "14");
       (1590329954.0, "1.4", "2.5", "15");
      ];
  in
  testWriteMultiBase ~test_sequence ~make_config ~json_tags:"tags" ~json_fields:"fields" ?schema ctx

let testWriteNoTime ctx =
  let schema = {|
CREATE TABLE meas(time timestamptz NOT NULL, moi1 text NOT NULL DEFAULT(''), moi2 text NOT NULL DEFAULT(''));
CREATE UNIQUE INDEX meas_time_idx ON meas(time, moi1, moi2);
|} in
  Test_utils.with_db_writer ctx ~schema @@ fun { db; db_spec } ->
  let db = Lazy.force db in
  let meas =
    Influxdb_lexer.make_measurement
      ~measurement:"meas"
      ~tags:[("moi1", "1");("moi2", "2")]
      ~fields:[("value", Influxdb_lexer.Int 42L)]
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
    Influxdb_lexer.make_measurement
      ~measurement:"meas"
      ~tags:[("moi1", "1");("moi2", "2")]
      ~fields:[("value", Influxdb_lexer.Int 42L)]
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
    Influxdb_lexer.make_measurement
      ~measurement:"meas"
      ~tags:[("moi1", "1");("moi2", "2")]
      ~fields:[("value", Influxdb_lexer.Int 42L)]
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
    "testWriteMultiMany" >:: testWriteMultiMany;
    "testWriteMultiCoalesce" >:: testWriteMultiCoalesce;
  ]
