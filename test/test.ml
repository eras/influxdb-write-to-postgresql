open OUnit2

let suite =
  "test" >::: [
    Test_lexer.suite;
    Test_db_writer.suite
  ]

let () =
  run_test_tt_main suite
