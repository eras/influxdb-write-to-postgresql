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
CREATE TABLE meas(time timestamptz NOT NULL);
CREATE UNIQUE INDEX meas_time_idx ON meas(time);
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
  let make_config conninfo =
    { Db_writer.conninfo = Lazy.force conninfo;
      time_field = "time";
      tags_column = Some "tags"; }
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
    [|"1590329952"; {|{"moi1":"1","moi2":"2"}|}; "42"|]
    (snd query)

let testWrite ctx =
  let schema = {|
CREATE TABLE meas(time timestamptz NOT NULL, moi1 TEXT NOT NULL DEFAULT(''), moi2 TEXT NOT NULL DEFAULT(''));
CREATE UNIQUE INDEX meas_time_idx ON meas(time, moi1, moi2);
|} in
  Test_utils.with_db_writer ctx ~schema @@ fun { db; conninfo } ->
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
     let direct = new Postgresql.connection ~conninfo:(Lazy.force conninfo) () in
     let result = direct#exec ~expect:[Postgresql.Tuples_ok] "SELECT extract(epoch from time), moi1, moi2, value FROM meas" in
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

let testWriteNoTime ctx =
  let schema = {|
CREATE TABLE meas(time timestamptz NOT NULL, moi1 text NOT NULL DEFAULT(''), moi2 text NOT NULL DEFAULT(''));
CREATE UNIQUE INDEX meas_time_idx ON meas(time, moi1, moi2);
|} in
  Test_utils.with_db_writer ctx ~schema @@ fun { db; conninfo } ->
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
     let direct = new Postgresql.connection ~conninfo:(Lazy.force conninfo) () in
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
  let make_config conninfo =
    { Db_writer.conninfo = Lazy.force conninfo;
      time_field = "time";
      tags_column = Some "tags"; }
  in
  Test_utils.with_db_writer ~make_config ctx ~schema @@ fun { db; conninfo } ->
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
     let direct = new Postgresql.connection ~conninfo:(Lazy.force conninfo) () in
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

let suite = "Db_writer" >::: [
  "testCreate" >:: testCreate;
  "testDbOfIdentifier" >:: testDbOfIdentifier;
  "testInsert" >:: testInsert;
  "testInsertNoTime" >:: testInsertNoTime;
  "testInsertJsonTags" >:: testInsertJsonTags;
  "testWrite" >:: testWrite;
  "testWriteNoTime" >:: testWriteNoTime;
  "testWriteJsonTags" >:: testWriteJsonTags;
]
