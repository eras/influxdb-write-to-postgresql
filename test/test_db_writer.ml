open OUnit2

module Lexer = Influxdb_write_to_postgresql.Lexer
module Db_writer = Influxdb_write_to_postgresql.Db_writer

let testCreate _ctx =
  let db = Db_writer.create () in
  Db_writer.close db

let testDbOfIdentifier _ctx =
  let db = Db_writer.create () in
  let s = Db_writer.db_of_identifier db "moi" in
  assert_equal s {|"moi"|};
  Db_writer.close db

let testInsert _ctx =
  let db = Db_writer.create () in
  let meas = {
    Lexer.measurement = "meas";
    tags = [("moi1", "1");("moi2", "2")];
    fields = [];
    time = 1590329952000000000L;
  } in
  let query = Db_writer.insert_of_measurement db meas in
  (* Printf.fprintf stderr "query: %s\n%!" (fst query); *)
  assert_equal (fst query) {|INSERT INTO "meas"("time", "moi1", "moi2") VALUES (to_timestamp($1),$2,$3)|};
  assert_equal (snd query) [|"1590329952"; "1"; "2"|];
  Db_writer.close db

let testWrite _ctx =
  let db = Db_writer.create () in
  let meas = {
    Lexer.measurement = "meas";
    tags = [("moi1", "1");("moi2", "2")];
    fields = [];
    time = 1590329952000000000L;
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
  "testWrite" >:: testWrite;
]
