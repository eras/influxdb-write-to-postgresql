module TableMap = Map.Make(String)

type error =
  | CannotParseConfig of string
  | CannotProcessConfig of string

let string_of_error = function
  | CannotParseConfig str -> "Cannot parse yaml: " ^ str
  | CannotProcessConfig str -> "Cannot process configuration: " ^ str

exception Error of error

let _ = Printexc.register_printer (function
    | Error error -> Some (string_of_error error)
    | _ -> None
  )

(* type matches_exact = string [@@deriving yojson]
 * type matches_regexp = Regexp of string *)
(* let matches_regexp_of_yojson = function
 *   | `String str -> Ok (Regexp str)
 *   | _ -> Error "Cannot parse regexp for user"
 * let matches_regexp_to_yojson = function
 *   | Regexp str -> `String str *)

type group = {
  expires: (float option [@default None]);
} [@@deriving yojson]

type user = {
  token: (string option [@default None]);
  password: string;
  group: (string option [@default None]);
  expires: (float option [@default None]);
} [@@deriving yojson]

type database = {
  db_name: string;

  db_host: string;
  db_port: int;

  db_user: string;
  db_password: string;

  time_column: (string option [@default None]);
  tags_jsonb_column: (string option [@default None]);

  tag_columns: (string list option [@default None]);

  fields_jsonb_column: (string option [@default None]);
  field_columns: (string list option [@default None]);
} [@@deriving yojson]

let db_spec_of_database : database -> Db_writer.db_spec =
  fun { db_name; db_host; db_port; db_user; db_password; _ } ->
  Db_writer.DbInfo {
    Db_writer.db_host;
    db_port;
    db_user;
    db_password;
    db_name;
  }

let db_config_of_database : database -> Db_writer.config =
  fun ({ time_column; tags_jsonb_column; fields_jsonb_column; _ } as database) ->
  let db_spec = db_spec_of_database database in
  { Db_writer.db_spec;
    time_column = Option.value time_column ~default:"time";
    tags_column = tags_jsonb_column;
    fields_column = fields_jsonb_column; }

let associative_of_yojson
    (of_yojson: Yojson.Safe.t -> ('result, string) result)
    (xs: Yojson.Safe.t) :
  ((string * 'result) list, string) result =
  match xs with
  | `Assoc (xs) ->
    let rec map xs : (_, _) result =
      match xs with
      | ((k, v)::rest) ->
        (match of_yojson v, lazy (map rest) with
         | Ok x, lazy (Ok rest) -> Ok ((k, x)::rest)
         | Error x, _ -> Error x
         | _, lazy (Error x) -> Error x)
      | [] -> Ok []
    in
    map xs
  | x ->
    Error ("Unexpected value: " ^ Yojson.Safe.to_string x)

let _option_of_yojson of_yojson yojson =
  match yojson with
  | `Null -> Ok None
  | other ->
    match of_yojson other with
    | Ok x -> Ok (Some x)
    | Error x -> Error x

let list_or_empty_of_yojson of_yojson yojson =
  match yojson with
  | `Null -> Ok []
  | other ->
    match of_yojson other with
    | Ok x -> Ok x
    | Error x -> Error x

type t = {
  users : (string * user) list [@default []] [@of_yojson (associative_of_yojson user_of_yojson)];
  regexp_users : ((string * user) list[@default []]  [@of_yojson (list_or_empty_of_yojson (associative_of_yojson user_of_yojson))]);

  groups: ((string * group) list [@default []] [@of_yojson (list_or_empty_of_yojson (associative_of_yojson group_of_yojson))]);

  databases: (string * database) list [@of_yojson (associative_of_yojson database_of_yojson)];
  regexp_databases: (string * database) list [@default []] [@of_yojson (associative_of_yojson database_of_yojson)];
} [@@deriving yojson]

let rec yojson_of_yaml : Yaml.value -> _ = function
  | `Null -> `Null
  | `Bool x -> `Bool x
  | `Float x ->
    (* I wonder how reliable this is.. *)
    if mod_float x 1.0 = 0.0
    then `Int (int_of_float x)
    else `Float x
  | `String x -> `String x
  | `O kv -> `Assoc (Common.map_snd yojson_of_yaml kv)
  | `A xs -> `List (List.map yojson_of_yaml xs)

let rec yaml_of_yojson : _ -> Yaml.value = function
  | `Null -> `Null
  | `Bool x -> `Bool x
  | `Float x -> `Float x
  | `String x -> `String x
  | `Assoc kv -> `O (Common.map_snd yaml_of_yojson kv)
  | `List xs -> `A (List.map yaml_of_yojson xs)
  | `Int x -> `Float (float_of_int x)
  | `Intlit x -> `Float (float_of_string x)
  | `Tuple _ -> failwith "Tuple->Yaml conversion not supported"
  | `Variant _ -> failwith "Variant->Yaml conversion not supported"

let dump t =
  let yaml = to_yojson t |> yaml_of_yojson in
  match Yaml.to_string yaml with
  | Ok str -> output_string stdout str
  | Error (`Msg message) -> raise (Error (CannotProcessConfig message))

let parse yaml =
  let yojson = yaml |> yojson_of_yaml in
  (* Printf.printf "yojson: %s\n%!" (Yojson.Safe.to_string yojson); *)
  match yojson |> of_yojson with
  | Ok config -> config
  | Error message -> raise (Error (CannotProcessConfig message))

let load file =
  match Yaml_unix.of_file (Fpath.(v file)) with
  | Ok yaml -> parse yaml
  | Error (`Msg message) -> raise (Error (CannotParseConfig message))
