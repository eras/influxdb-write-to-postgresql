include Sql_types
include Sql_parser

let parse_sql str =
  let lexbuf = Sedlex_menhir.create_lexbuf (Sedlexing.Utf8.from_string str) in
  try
    Sedlex_menhir.sedlex_with_menhir
      Sql_lexer.lex_menhir
      Sql_parser.statement
      lexbuf
  with
  | Sql_parser.Error ->
    let at = Sedlexing.loc lexbuf.stream in
    raise (Sql_types.Error (ParseError at))
