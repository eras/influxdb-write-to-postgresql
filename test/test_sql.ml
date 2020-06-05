open OUnit2
open Influxdb_write_to_postgresql

let testLexCreateUniqueIndex _ctx =
  let statement = {|CREATE UNIQUE INDEX meas_time_idx ON public.meas USING btree ("time", moi1, moi2) WHERE moi1 < 500|} in
  let sedlexing = Sedlexing.Utf8.from_string statement in

  let printer = Sql_lexer.string_of_token in
  let lex () = Sql_lexer.lex sedlexing in
  let open Sql_parser in
  assert_equal ~printer CREATE (lex ());
  assert_equal ~printer UNIQUE (lex ());
  assert_equal ~printer INDEX (lex ());
  assert_equal ~printer (IDENT "meas_time_idx") (lex ());
  assert_equal ~printer ON (lex ());
  assert_equal ~printer (IDENT "public") (lex ());
  assert_equal ~printer DOT (lex ());
  assert_equal ~printer (IDENT "meas") (lex ());
  assert_equal ~printer USING (lex ());
  assert_equal ~printer (IDENT "btree") (lex ());
  assert_equal ~printer LPAREN (lex ());
  assert_equal ~printer (IDENT "time") (lex ());
  assert_equal ~printer COMMA (lex ());
  assert_equal ~printer (IDENT "moi1") (lex ());
  assert_equal ~printer COMMA (lex ());
  assert_equal ~printer (IDENT "moi2") (lex ());
  assert_equal ~printer RPAREN (lex ());
  assert_equal ~printer WHERE (lex ());
  assert_equal ~printer (IDENT "moi1") (lex ());
  assert_equal ~printer RELOP_LT (lex ());
  assert_equal ~printer (VALUE (Sql_types.V_Integer 500L)) (lex ())

let testParseCreateUniqueIndex _ctx =
  let statement = {|CREATE UNIQUE INDEX meas_time_idx ON public.meas USING btree ("time", moi1, moi2)|} in
  let parsed =
    Sedlex_menhir.sedlex_with_menhir
      Sql_lexer.lex_menhir
      Sql_parser.statement
      (Sedlex_menhir.create_lexbuf (Sedlexing.Utf8.from_string statement))
  in
  let open Sql_types in
  assert_equal
    (Sql_types.CreateIndex {
        index     = "meas_time_idx";
        unique    = true;
        table     = "public.meas";
        algorithm = Some "btree";
        fields    = [`Column "time"; `Column "moi1"; `Column "moi2"];
        where     = None;
      })
    parsed

let testParseCreateUniqueIndexFunctional _ctx =
  let statement = {|CREATE UNIQUE INDEX meas_time_idx ON public.meas USING btree ("time", (wrappersies(moi1)), moi2)|} in
  let parsed =
    try
      Sedlex_menhir.sedlex_with_menhir
        Sql_lexer.lex_menhir
        Sql_parser.statement
        (Sedlex_menhir.create_lexbuf (Sedlexing.Utf8.from_string statement))
    with Sql_types.Error error ->
      Printf.ksprintf assert_failure "Sql parsing error: %s" (Sql_types.string_of_error error)
  in
  let open Sql_types in
  assert_equal
    (Sql_types.CreateIndex {
        index     = "meas_time_idx";
        unique    = true;
        table     = "public.meas";
        algorithm = Some "btree";
        fields    = [`Column "time"; `Expression (Sql_types.(E_FunCall ("wrappersies", [E_Identifier "moi1"]))); `Column "moi2"];
        where     = None;
      })
    parsed

let testParseCreateUniqueIndexWhere _ctx =
  let statement = {|CREATE UNIQUE INDEX meas_time_idx ON public.meas USING btree ("time", moi1) WHERE moi1 < 500|} in
  let parsed =
    let lexbuf = Sedlex_menhir.create_lexbuf (Sedlexing.Utf8.from_string statement) in
    try
      Sedlex_menhir.sedlex_with_menhir
        Sql_lexer.lex_menhir
        Sql_parser.statement
        lexbuf
    with
    |Sql_types.Error error ->
      let (p0, p1) = Sedlexing.loc lexbuf.stream in
      Printf.ksprintf assert_failure "Sql parsing error at offset %d-%d: %s"
        p0 p1 (Sql_types.string_of_error error)
    | Sql_parser.Error ->
      let (p0, p1) = Sedlexing.loc lexbuf.stream in
      Printf.ksprintf assert_failure "Sql parsing error at offset %d-%d"
        p0 p1
  in
  let open Sql_types in
  assert_equal
    (Sql_types.CreateIndex {
        index     = "meas_time_idx";
        unique    = true;
        table     = "public.meas";
        algorithm = Some "btree";
        fields    = [`Column "time"; `Column "moi1"];
        where     = Some (Sql_types.(E_RelOp (E_Identifier "moi1", "<", E_Literal (V_Integer 500L))))
      })
    parsed

let suite = "Db_sql" >::: [
  "testLexCreateUniqueIndex" >:: testLexCreateUniqueIndex;
  "testParseCreateUniqueIndex" >:: testParseCreateUniqueIndex;
  "testParseCreateUniqueIndexFunctional" >:: testParseCreateUniqueIndexFunctional;
  "testParseCreateUniqueIndexWhere" >:: testParseCreateUniqueIndexWhere;
]
