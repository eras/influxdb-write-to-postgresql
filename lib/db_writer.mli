module Pg = Postgresql

type quote_mode = QuoteAlways

type t
type error =
  | PgError of Pg.error

exception Error of error

(** [string_of_error error] converts the error to a string *)
val string_of_error : error -> string

(** [create conninfo] connects to the given database; can raise PgError if it fails *)
val create : conninfo:string -> t

(** [close t] disconnects from the database. Can raise. *)
val close : t -> unit

(** [write t measurements] writes measurements to the database, all in one transaction.

    Can raise PgError *)
val write : t -> Lexer.measurement list -> unit

(** exposed for unit testing *)
module Internal: sig
  val db_of_identifier : string -> string
  val insert_of_measurement : t -> Lexer.measurement -> string * string array
end
