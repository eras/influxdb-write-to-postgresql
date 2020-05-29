open OUnit2

open Influxdb_write_to_postgresql

let testCreate ctx =
  Test_utils.with_db_writer ctx @@ fun _ -> ()

let identity x = x

let testDbOfIdentifier _ctx =
  assert_equal ~printer:identity {|moi|} (Db_writer.Internal.db_of_identifier "moi");
  assert_equal ~printer:identity {|U&"Moi"|} (Db_writer.Internal.db_of_identifier "Moi");
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
  Test_utils.with_db_writer ctx @@ fun { db; conninfo } ->
  let db = Lazy.force db in
  let meas = {
    Lexer.measurement = "meas";
    tags = [("moi1", "1");("moi2", "2")];
    fields = [];
    time = Some 1590329952000000000L;
  } in
  (try
     ignore (Db_writer.write db [meas]);
     let direct = new Postgresql.connection ~conninfo:(Lazy.force conninfo) () in
     let result = direct#exec ~expect:[Postgresql.Tuples_ok] "SELECT extract(epoch from time), moi1, moi2 FROM meas" in
     match result#get_all_lst with
     | [[time; moi1; moi2]] ->
       let time = float_of_string time in
       assert_equal ~printer:string_of_float 1590329952.0 time;
       assert_equal ~printer:identity "1" moi1;
       assert_equal ~printer:identity "2" moi2
     | _ ->
       assert_failure "Unexpected results"
   with
   | Db_writer.Error error ->
     Printf.fprintf stderr "Db_writer error: %s\n%!" (Db_writer.string_of_error error))

let testWriteNoTime ctx =
  Test_utils.with_db_writer ctx @@ fun { db; conninfo } ->
  let db = Lazy.force db in
  let meas = {
    Lexer.measurement = "meas";
    tags = [("moi1", "1");("moi2", "2")];
    fields = [];
    time = None;
  } in
  (try
     ignore (Db_writer.write db [meas]);
     let direct = new Postgresql.connection ~conninfo:(Lazy.force conninfo) () in
     let result = direct#exec ~expect:[Postgresql.Tuples_ok] "SELECT extract(epoch from now() - time), moi1, moi2 FROM meas" in
     match result#get_all_lst with
     | [[delta; moi1; moi2]] ->
       let delta = float_of_string delta in
       assert_bool "Time delta is negative" (delta >= 0.0);
       assert_bool "Time delta is too big" (delta <= 10.0);
       assert_equal ~printer:identity "1" moi1;
       assert_equal ~printer:identity "2" moi2
     | _ ->
       assert_failure "Unexpected results"
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
