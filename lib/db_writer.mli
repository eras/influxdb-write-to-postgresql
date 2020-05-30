module Pg = Postgresql

type quote_mode = QuoteAlways

type t
type query = string
type error =
  | PgError of (Pg.error * query option)
  | MalformedUTF8
  | CannotAddTags of string list

exception Error of error

type config = {
  conninfo : string;
  time_field : string;
  tags_column: string option;   (* using tags column? then this is its name *)
  fields_column: string option;   (* using fields column? then this is its name *)
}

(** [string_of_error error] converts the error to a string *)
val string_of_error : error -> string

(** [create conninfo] connects to the given database; can raise PgError if it fails *)
val create : config -> t

(** [close t] disconnects from the database. Can raise. *)
val close : t -> unit

(** [reconnect t] disconnects and then reconnectfrom the database. Can raise. *)
val reconnect : t -> unit

(** [write t measurements] writes measurements to the database, all in one transaction.

    Can raise PgError *)
val write : t -> Lexer.measurement list -> unit

val is_unquoted_ascii : Uchar.t -> bool

(** exposed for unit testing *)
module Internal: sig
  val db_of_identifier : string -> string
  val insert_of_measurement : t -> Lexer.measurement -> string * string array
end
