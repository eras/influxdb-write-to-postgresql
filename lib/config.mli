module TableMap: Map.S with type key = string

type error = CannotParseConfig of string | CannotProcessConfig of string
exception Error of error
val string_of_error : error -> string

(* type matches_exact = string
 *
 * type matches_regexp = Regexp of matches_exact *)

type group = { expires : float option; }

type user = {
  token : string option;
  password : string;
  group : string option;
  expires : float option;
}

type database = {
  db_name : string;
  db_host : string;
  db_port : int;
  db_user : string;
  db_password : string;
  time_column : string option;
  tags_jsonb_column : string option;
  tag_columns : string list option;
  fields_jsonb_column : string option;
  field_columns : string list option;
}

val db_spec_of_database : database -> Db_writer.db_spec

type t = {
  users : (string * user) list;
  regexp_users : (string * user) list;
  groups : (string * group) list;
  databases : (string * database) list;
  regexp_databases : (string * database) list;
}

(* val to_yojson : t -> Yojson.Safe.t
 * val of_yojson : Yojson.Safe.t -> t Ppx_deriving_yojson_runtime.error_or
 * val parse : Yaml.value -> t *)

val dump : t -> unit
val load : string -> t
