open OUnit2

open Influxdb_write_to_postgresql

let testCreate ctx =
  Test_utils.with_db_writer ctx @@ fun _ -> ()

let identity x = x

let testDbOfIdentifier _ctx =
  assert_equal ~printer:identity {|moi|} (Db_writer.Internal.db_of_identifier "moi");
  assert_equal ~printer:identity {|U&"moi\0020"|} (Db_writer.Internal.db_of_identifier "moi ");
  assert_equal ~printer:identity {|U&"tidii\2603"|} (Db_writer.Internal.db_of_identifier "tidii☃")

let testInsert ctx =
  Test_utils.with_db_writer ctx @@ fun { db; _ } ->
  let db = Lazy.force db in
  let meas = {
    Lexer.measurement = "meas";
    tags = [("moi1", "1");("moi2", "2")];
    fields = [];
    time = Some 1590329952000000000L;
  } in
  let query = Db_writer.Internal.insert_of_measurement db meas in
  assert_equal ~printer:identity {|INSERT INTO meas(time, moi1, moi2) VALUES (to_timestamp($1),$2,$3) ON CONFLICT(time) DO UPDATE SET moi1=excluded.moi1, moi2=excluded.moi2|} (fst query);
  assert_equal (snd query) [|"1590329952"; "1"; "2"|]

let testInsertNoTime ctx =
  Test_utils.with_db_writer ctx @@ fun { db; _ } ->
  let db = Lazy.force db in
  let meas = {
    Lexer.measurement = "meas";
    tags = [("moi1", "1");("moi2", "2")];
    fields = [];
    time = None;
  } in
  let query = Db_writer.Internal.insert_of_measurement db meas in
  assert_equal ~printer:identity {|INSERT INTO meas(time, moi1, moi2) VALUES (CURRENT_TIMESTAMP,$1,$2) ON CONFLICT(time) DO UPDATE SET moi1=excluded.moi1, moi2=excluded.moi2|} (fst query);
  assert_equal (snd query) [|"1"; "2"|]

let testWrite ctx =
  Test_utils.with_db_writer ctx @@ fun { db; _ } ->
  let db = Lazy.force db in
  let meas = {
    Lexer.measurement = "meas";
    tags = [("moi1", "1");("moi2", "2")];
    fields = [];
    time = Some 1590329952000000000L;
  } in
  (try
     ignore (Db_writer.write db [meas]);
   with
   | Db_writer.Error error ->
     Printf.fprintf stderr "Db_writer error: %s\n%!" (Db_writer.string_of_error error))

let testWriteNoTime ctx =
  Test_utils.with_db_writer ctx @@ fun { db; _ } ->
  let db = Lazy.force db in
  let meas = {
    Lexer.measurement = "meas";
    tags = [("moi1", "1");("moi2", "2")];
    fields = [];
    time = None;
  } in
  (try
     ignore (Db_writer.write db [meas]);
   with
   | Db_writer.Error error ->
     Printf.fprintf stderr "Db_writer error: %s\n%!" (Db_writer.string_of_error error))

let suite = "Db_writer" >::: [
  "testCreate" >:: testCreate;
  "testDbOfIdentifier" >:: testDbOfIdentifier;
  "testInsert" >:: testInsert;
  "testInsertNoTime" >:: testInsertNoTime;
  "testWrite" >:: testWrite;
  "testWriteNoTime" >:: testWriteNoTime;
]
