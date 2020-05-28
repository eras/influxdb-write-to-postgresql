open OUnit2

let suite =
  "test" >::: [
    Test_lexer.suite;
    Test_db_writer.suite;
    Test_db_spool.suite;
  ]

let () =
  run_test_tt_main suite
