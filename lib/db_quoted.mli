type error =
  | MalformedUTF8

exception Error of error

val string_of_error : error -> string

module Types :
sig
  type t =
    | FT_String
    | FT_Int
    | FT_Float
    | FT_Boolean
    | FT_Jsonb
    | FT_Timestamptz
    | FT_Unknown of string
end

(** This contains only data that is suitable for sending to database *)
type t = private string

(** !"Foo" convert "Foo" int a database literal; please use this only for string literals *)
val (!) : string -> t

(** !"hello" ^ !"world" concatenates two database strings *)
val (^) : t -> t -> t

(** Quotes a database identifier *)
val id_exn : string -> t

(** Quoted version of a database type *)
val id_type : Types.t -> t

(** Concatenate database strings *)
val concat : t -> t list -> t

(** Appends "NOT NULL" *)
val not_null : t -> t

(** Appends " DEFAULT(x)" *)
val default : t -> t -> t

(** $n *)
val placeholder : int -> t

(** Alternative to (t :> string) when it's more convenient *)
val to_string : t -> string
