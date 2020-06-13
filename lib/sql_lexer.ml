open Sedlex_menhir
open Sql_parser

let string_of_token = function
  | ASC -> "ASC"
  | COLLATE -> "COLLATE"
  | COMMA -> "COMMA"
  | CONCURRENTLY -> "CONCURRENTLY"
  | CREATE -> "CREATE"
  | DESC -> "DESC"
  | DOT -> "."
  | EXISTS -> "EXISTS"
  | FIRST -> "FIRST"
  | IDENT string -> string
  | IF -> "IF"
  | INCLUDE -> "INCLUDE"
  | INDEX -> "INDEX"
  | LAST -> "LAST"
  | LPAREN -> "("
  | NOT -> "NOT"
  | NULLS -> "NULLS"
  | ON -> "ON"
  | ONLY -> "ONLY"
  | RPAREN -> ")"
  | TABLESPACE -> "TABLESPACE"
  | UNIQUE -> "UNIQUE"
  | USING -> "USING"
  | VALUE value -> Sql_types.string_of_value value
  | WHERE -> "WHERE"
  | WITH -> "WITH"
  | RELOP_LT -> "<"
  | RELOP_LTE -> "<="
  | RELOP_GT -> ">"
  | RELOP_GTE -> ">="
  | EQUAL -> "="
  | RELOP_NE -> "<>"
  | BINOP str -> str
  | CAST -> "::"
  | END -> ";"

type error =
  | IdentifierNotClosed
  | UnexpectedLexingInput

exception Error of error

let ident_first = [%sedlex.regexp? 'a'..'z' | 'A'..'Z' | '_' ]
let ident_rest = [%sedlex.regexp? ident_first | '0'..'9' ]

let integer_first = [%sedlex.regexp? '0'..'9' ]
let integer_rest = [%sedlex.regexp? '0'..'9' ]

let binop = [%sedlex.regexp? Plus('+' | '-' | '*' | '/' | '<' | '>' | '=' | '~' | '!' | '@' | '#' | '%' | '^' | '&' | '|' | '`' | '?') ]

let rec lex_exn buf =
  match%sedlex buf with
  | ('C' | 'c'), ('R' | 'r'), ('E' | 'e'), ('A' | 'a'), ('T' | 't'), ('E' | 'e') -> CREATE
  | ('U' | 'u'), ('N' | 'n'), ('I' | 'i'), ('Q' | 'q'), ('U' | 'u'), ('E' | 'e') -> UNIQUE
  | ('I' | 'i'), ('N' | 'n'), ('D' | 'd'), ('E' | 'e'), ('X' | 'x') -> INDEX
  | ('O' | 'o'), ('N' | 'n') -> ON
  | ('U' | 'u'), ('S' | 's'), ('I' | 'i'), ('N' | 'n'), ('G' | 'g') -> USING
  | ("C" | "c"), ("O" | "o"), ("N" | "n"), ("C" | "c"), ("U" | "u"), ("R" | "r"), ("R" | "r"), ("E" | "e"), ("N" | "n"), ("T" | "t"), ("L" | "l"), ("Y" | "y") -> CONCURRENTLY
  | ("O" | "o"), ("N" | "n"), ("L" | "l"), ("Y" | "y") -> ONLY
  | ("C" | "c"), ("O" | "o"), ("L" | "l"), ("L" | "l"), ("A" | "a"), ("T" | "t"), ("E" | "e") -> COLLATE
  | ("A" | "a"), ("S" | "s"), ("C" | "c") -> ASC
  | ("D" | "d"), ("E" | "e"), ("S" | "s"), ("C" | "c") -> DESC
  | ("N" | "n"), ("U" | "u"), ("L" | "l"), ("L" | "l"), ("S" | "s") -> NULLS
  | ("F" | "f"), ("I" | "i"), ("R" | "r"), ("S" | "s"), ("T" | "t") -> FIRST
  | ("L" | "l"), ("A" | "a"), ("S" | "s"), ("T" | "t") -> LAST
  | ("I" | "i"), ("N" | "n"), ("C" | "c"), ("L" | "l"), ("U" | "u"), ("D" | "d"), ("E" | "e") -> INCLUDE
  | ("W" | "w"), ("I" | "i"), ("T" | "t"), ("H" | "h") -> WITH
  | ("T" | "t"), ("A" | "a"), ("B" | "b"), ("L" | "l"), ("E" | "e"), ("S" | "s"), ("P" | "p"), ("A" | "a"), ("C" | "c"), ("E" | "e") -> TABLESPACE
  | ("W" | "w"), ("H" | "h"), ("E" | "e"), ("R" | "r"), ("E" | "e") -> WHERE
  | '(' -> LPAREN
  | ')' -> RPAREN
  | ',' -> COMMA
  | "<=" -> RELOP_LTE
  | ">=" -> RELOP_GTE
  | "<>" -> RELOP_NE
  | "<" -> RELOP_LT
  | ">" -> RELOP_GT
  | "=" -> EQUAL
  | binop -> BINOP (Sedlexing.Utf8.lexeme buf)
  | ' ' | '\t' -> lex_exn buf
  | '.' -> DOT
  | integer_first, Star integer_rest -> VALUE (Sql_types.V_Integer (Int64.of_string (Sedlexing.Utf8.lexeme buf)))
  | ident_first, Star ident_rest -> IDENT (Sedlexing.Utf8.lexeme buf)
  | '"' ->
    let buffer = Buffer.create 1024 in
    let rec scan () =
      match%sedlex buf with
      | '\\' ->
        Buffer.add_string buffer (Sedlexing.Utf8.lexeme buf);
        let skip () =
          match%sedlex buf with
          | any ->
            Buffer.add_string buffer (Sedlexing.Utf8.lexeme buf);
            scan ()
          | _ -> raise (Error IdentifierNotClosed)
        in
        skip ()
      | '"' ->
        IDENT (Buffer.contents buffer)
      | any ->
        Buffer.add_string buffer (Sedlexing.Utf8.lexeme buf);
        scan ()
      | _ -> raise (Error IdentifierNotClosed)
    in
    scan ()
  | _ ->
    (* TODO: Raise error if there is still data in buffer *)
    END

let lex_menhir_exn lexbuf =
  lex_exn lexbuf.stream
