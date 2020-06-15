type error =
  | MalformedUTF8

exception Error of error

module Types =
struct
  type t =
    | FT_String
    | FT_Int
    | FT_Float
    | FT_Boolean
    | FT_Jsonb
    | FT_Timestamptz
    | FT_Unknown of string

  let db_of_field_type = function
    | FT_Int         -> "integer"
    | FT_Float       -> "double precision"
    | FT_String      -> "text"
    | FT_Boolean     -> "boolean"
    | FT_Jsonb       -> "jsonb"
    | FT_Timestamptz -> "timestamptz"
    | FT_Unknown str -> str
end

let string_of_error = function
  | MalformedUTF8 -> "MalformedUTF8"

type t = string

let (!) x = x

let (^) a b = a ^ b

let id_exn str =
  let out = Buffer.create (String.length str) in
  Buffer.add_string out "U&\"";
  let decoder = Uutf.decoder ~encoding:`UTF_8 (`String str) in
  let rec loop ~first ~any_special =
    match Uutf.decode decoder with
    | `Await -> assert false
    | `Uchar x when x == Uchar.of_char '\\' || x == Uchar.of_char '"' ->
      Buffer.add_char out '\\';
      Buffer.add_char out (Uchar.to_char x);
      loop ~first:false ~any_special:true
    | `Uchar x when Common.is_unquoted_ascii ~first x ->
      Buffer.add_char out (Uchar.to_char x);
      loop ~first:false ~any_special
    | `Uchar x when Common.is_unescaped_ascii x ->
      Buffer.add_char out (Uchar.to_char x);
      loop ~first:false ~any_special:true
    | `Uchar x when Uchar.to_int x < (1 lsl 16) ->
      Printf.ksprintf (Buffer.add_string out) "\\%04x" (Uchar.to_int x);
      loop ~first:false ~any_special:true
    | `Uchar x when Uchar.to_int x < (1 lsl 24) ->
      Printf.ksprintf (Buffer.add_string out) "\\+%06x" (Uchar.to_int x);
      loop ~first:false ~any_special:true
    | `Uchar _ | `Malformed _ ->
      raise (Error MalformedUTF8)
    | `End when any_special ->
      Buffer.add_char out '"';
      Buffer.contents out
    | `End ->
      str (* return original identifier as nothing special was done *)
  in
  loop ~first:true ~any_special:false

let id_type = Types.db_of_field_type

let concat = String.concat

let not_null x = x ^ " NOT NULL"

let default value x = x ^ " DEFAULT(" ^ value ^ ")"

let placeholder x = Printf.sprintf "$%d" x

let to_string x = x
