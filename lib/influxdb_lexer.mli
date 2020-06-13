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
} [@@deriving show]

val make_measurement :
  measurement:string ->
  tags:(string * string) list ->
  fields:(string * value) list ->
  time:int64 option ->
  measurement

(** [combine_fields a] returns a measurement which contains the fields
   of both a and b (not tags nor time), values in b overwriting values
   in a *)
val combine_fields : measurement -> measurement -> measurement

(** [update_timestamp epoch_time measurement] ensure the time field of
    the measurement is set to the given epoch time value should it be None;
    otherwise the measurement is returned as-is. *)
val fill_missing_timestamp : float -> measurement -> measurement

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
  
