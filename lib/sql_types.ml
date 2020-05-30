type value =
  | V_Integer of Int64.t
  | V_String of string

type expression =
  | E_Literal of value
  | E_Identifier of string
  | E_FunCall of string * expression list
  | E_RelOp of expression * string * expression

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

let string_of_value = function
  | V_Integer x -> Int64.to_string x
  | V_String x -> x

type statement =
  | CreateIndex of create_index
