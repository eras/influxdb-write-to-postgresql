module Pg = Postgresql

type quote_mode = QuoteAlways

type t
type query = string
type 'a with_reason = {
  reason: string;
  value: 'a;
}
type error =
  | PgError of (Pg.error * query option)
  | MalformedUTF8
  | CannotAddTags of string list
  | CannotCreateTable of string with_reason
  | NoPrimaryIndexFound of string

exception Error of error

type db_info = {
  db_host: string;
  db_port: int;
  db_user: string;
  db_password: string;
  db_name: string;
}

type db_spec =
  | DbInfo of db_info
  | DbConnInfo of string

type config = {
  db_spec : db_spec;
  create_table : Config.create_table option;
  time_column : string;
  tags_column: string option;   (* using tags column? then this is its name *)
  fields_column: string option;   (* using fields column? then this is its name *)
}

(** [string_of_error error] converts the error to a string *)
val string_of_error : error -> string

(** [create db_spec] connects to the given database; can raise PgError if it fails *)
val create : config -> t

(** [close t] disconnects from the database. Can raise. *)
val close : t -> unit

(** [reconnect t] disconnects and then reconnectfrom the database. Can raise. *)
val reconnect : t -> unit

(** [write t measurements] writes measurements to the database, all in one transaction.

    Can raise PgError *)
val write : t -> Lexer.measurement list -> unit

val db_spec_of_database : Config.database -> db_spec

val db_config_of_database : Config.database -> config


(** exposed for unit testing *)
module Internal: sig
  module FieldMap: Map.S with type key = string
  module TableMap: Map.S with type key = string

  type field_type =
    | FT_String
    | FT_Int
    | FT_Float
    | FT_Boolean
    | FT_Jsonb
    | FT_Timestamptz
    | FT_Unknown of string

  type table_info = {
    fields: field_type FieldMap.t
  }

  val new_pg_connection : db_spec -> Pg.connection
  val db_of_identifier : string -> string
  val db_of_field_type : field_type -> string
  val insert_of_measurement : t -> Lexer.measurement -> string * string array

  type made_table = {
    md_command : string;
    md_table_info : table_info;
    md_update_pks : string list list TableMap.t -> string list list TableMap.t;
  }
  val make_table_command : t -> Lexer.measurement -> made_table
end
