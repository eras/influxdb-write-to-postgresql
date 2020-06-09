open OUnit2

let suite =
  "test" >::: [
    Test_influxdb_lexer.suite;
    Test_db_writer.suite;
    Test_db_spool.suite;
    Test_sql.suite;
    Test_config.suite;
    Test_auth.suite;
    Test_iw2pg.suite;
  ]

let () =
  run_test_tt_main suite
