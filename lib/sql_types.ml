type value =
  | V_Integer of Int64.t
  | V_String of string

type expression =
  | E_Literal of value
  | E_Identifier of string
  | E_FunCall of string * expression list
  | E_RelOp of expression * string * expression
  | E_Cast of expression * string

type index_field =
  [`Column of string
  |`Expression of expression]

type create_index = {
  index: string;
  unique: bool;
  table: string;
  algorithm: string option;
  fields: index_field list;
  where: expression option;
}

type error =
  | ParseError of ((int * int) * string)

let string_of_error = function
  | ParseError ((p0, p1), statement) -> Printf.sprintf "Parse error at %d-%d: %s" p0 p1 statement

exception Error of error

let _ = Printexc.register_printer (function
    | Error error -> Some (string_of_error error)
    | _ -> None
  )

let db_quoted_of_value = function
  | V_Integer x -> Db_quoted.(!(Int64.to_string x))
  | V_String _x -> failwith "code to quote string values to SQL not implemented"

let string_of_value = function
  | V_Integer x -> Int64.to_string x
  | V_String x -> x

type statement =
  | CreateIndex of create_index

let rec db_quoted_of_expression ?(db_of_identifier_exn=Db_quoted.id_exn) = fun x ->
  let open Db_quoted in
  let (!!) x = !"(" ^ x ^ !")" in
  try
    match x with
    | E_Literal value -> db_quoted_of_value value
    | E_Identifier string -> db_of_identifier_exn string
    | E_FunCall (func, expressions) ->
      db_of_identifier_exn func ^ !"(" ^ concat !", " (List.map db_quoted_of_expression expressions) ^ !")"
    | E_RelOp (a, relop, b) ->
      !!(db_quoted_of_expression a) ^ !" " ^ !relop ^ !" " ^ !!(db_quoted_of_expression b)
    | E_Cast (a, b) ->
      !!(db_quoted_of_expression a) ^ !"::" ^ db_of_identifier_exn b
  with exn ->
    (* TODO: bit on the dangerous side? *)
    !"--- " ^ !(CCString.replace ~which:`All ~sub:"\n" ~by:" " (Printexc.to_string exn)) ^ !"\n"
