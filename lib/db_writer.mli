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

(** [create_exn db_spec] connects to the given database; can raise PgError if it fails *)
val create_exn : config -> t

(** [close t] disconnects from the database. Can raise. *)
val close : t -> unit

(** [reconnect t] disconnects and then reconnectfrom the database. Can raise. *)
val reconnect_exn : t -> unit

(** [write_exn t measurements] writes measurements to the database, all in one transaction.

    Can raise PgError *)
val write_exn : t -> Influxdb_lexer.measurement list -> unit

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

  type table_name = string

  type database_info = (table_name, table_info) Hashtbl.t

  val new_pg_connection_exn : db_spec -> Pg.connection
  val db_of_identifier_exn : string -> string
  val db_of_field_type : field_type -> string

  (** [insert_of_measurement ?measurements t reference_measurement] generates the INSERT command for inserting the
      reference_measurement (or if measurements is provided, the measurements) as well as the required parameters for the
      command.

      If measurements is provided, it must be similar to reference_measurement in the sense that it they must have the
      same tags and the same fields and time field must exist or not exist for all of them, no mixing.
  *)
  val insert_of_measurement_exn : ?measurements:Influxdb_lexer.measurement list -> t -> Influxdb_lexer.measurement -> string * string array
  val query_database_info_exn : Pg.connection -> database_info

  type made_table = {
    md_command : string;
    md_table_info : table_info;
    md_update_pks : string list list TableMap.t -> string list list TableMap.t;
  }
  val make_table_command_exn : t -> Influxdb_lexer.measurement -> made_table
end
