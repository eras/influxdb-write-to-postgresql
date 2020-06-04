open OUnit2
open Influxdb_write_to_postgresql

let testGet ctx =
  Test_utils.with_conninfo ctx @@ fun { Test_utils.db_spec; _ } ->
  let db_spec = Lazy.force db_spec in
  let schema = {|
CREATE TABLE meas(time timestamptz NOT NULL);
CREATE UNIQUE INDEX meas_time_dx ON meas(time);
|} in
  let db_spec = Test_utils.create_new_database ~schema db_spec in
  let db_config = {
    Db_writer.db_spec;
    time_column = "time";
    tags_column = None;
    fields_column = None;
    create_table = None;
  } in
  let config = { Db_spool.databases = [("default", db_config)] } in
  let spool = Db_spool.create config in
  (match Db_spool.db spool "default" with
   | None -> assert_failure "Expected to get a database (default) but did not (1)";
  | Some db -> db.release ());
  (match Db_spool.db spool "default" with
   | None -> assert_failure "Expected to get a database (default) but did not (2)";
  | Some db -> db.release ());
  (match Db_spool.db spool "not_there" with
   | None -> ()
   | Some db ->
     db.release ();
     assert_failure "Did not expect to get a database (not_there) but got one";
     )

let suite = "Db_spool" >::: [
  "testGet" >:: testGet;
  (* "testDb" >:: testDb; *)
]
