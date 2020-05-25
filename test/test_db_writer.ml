open OUnit2

module Lexer = Influxdb_write_to_postgresql.Lexer
module Db_writer = Influxdb_write_to_postgresql.Db_writer

let make_db _ctx =
  Db_writer.create ~conninfo:"service=dbwriter_test"

let testCreate ctx =
  let db = make_db ctx in
  Db_writer.close db

let testDbOfIdentifier ctx =
  let db = make_db ctx in
  let s = Db_writer.Internal.db_of_identifier "moi" in
  assert_equal s {|"moi"|};
  Db_writer.close db

let testInsert ctx =
  let db = make_db ctx in
  let meas = {
    Lexer.measurement = "meas";
    tags = [("moi1", "1");("moi2", "2")];
    fields = [];
    time = Some 1590329952000000000L;
  } in
  let query = Db_writer.Internal.insert_of_measurement db meas in
  (* Printf.fprintf stderr "query: %s\n%!" (fst query); *)
  assert_equal (fst query) {|INSERT INTO "meas"("time", "moi1", "moi2") VALUES (to_timestamp($1),$2,$3)|};
  assert_equal (snd query) [|"1590329952"; "1"; "2"|];
  Db_writer.close db

let testInsertNoTime ctx =
  let db = make_db ctx in
  let meas = {
    Lexer.measurement = "meas";
    tags = [("moi1", "1");("moi2", "2")];
    fields = [];
    time = None;
  } in
  let query = Db_writer.Internal.insert_of_measurement db meas in
  (* Printf.fprintf stderr "query: %s\n%!" (fst query); *)
  assert_equal (fst query) {|INSERT INTO "meas"("time", "moi1", "moi2") VALUES (CURRENT_TIMESTAMP,$1,$2)|};
  assert_equal (snd query) [|"1"; "2"|];
  Db_writer.close db

let testWrite ctx =
  let db = make_db ctx in
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
     Printf.fprintf stderr "Db_writer error: %s\n%!" (Db_writer.string_of_error error));
  Db_writer.close db

let testWriteNoTime ctx =
  let db = make_db ctx in
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
     Printf.fprintf stderr "Db_writer error: %s\n%!" (Db_writer.string_of_error error));
  Db_writer.close db

let suite = "Db_writer" >::: [
  "testCreate" >:: testCreate;
  "testDbOfIdentifier" >:: testDbOfIdentifier;
  "testInsert" >:: testInsert;
  "testInsertNoTime" >:: testInsertNoTime;
  "testWrite" >:: testWrite;
  "testWriteNoTime" >:: testWriteNoTime;
]
