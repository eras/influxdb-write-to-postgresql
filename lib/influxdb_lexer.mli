type value =
  | String of string
  | Int of int64
  | FloatNum of float
  | Boolean of bool
[@@deriving show]

type measurement = private {
  measurement: string;
  tags: (string * string) list;
  fields: (string * value) list;
  time: int64 option;
}

val make_measurement :
  measurement:string ->
  tags:(string * string) list ->
  fields:(string * value) list ->
  time:int64 option ->
  measurement

type error_info = Parse_error

type error = {
  info: error_info;
  message: string;
}

exception Error of error

val string_of_error : error -> string

val line : Sedlexing.lexbuf -> measurement
val lines : Sedlexing.lexbuf -> measurement list

val string_of_tag : string * string -> string
val string_of_field : string * value -> string
val string_of_measurement : measurement -> string
  
