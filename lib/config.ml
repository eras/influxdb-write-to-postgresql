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
 *   | _ -> Error "Cannot parse_exn regexp for user"
 * let matches_regexp_to_yojson = function
 *   | Regexp str -> `String str *)

type group = {
  expires: (float option [@default None]);
} [@@deriving yojson]

type password_type =
  | Plain
  | Argon2
[@@deriving show]

(* don't use derived yojson handling; it puts things inside an array *)
let password_type_of_yojson = function
  | `String "plain" -> Ok Plain
  | `String "argon2" -> Ok Argon2
  | _ -> Stdlib.Error "Invalid password type"

let password_type_to_yojson = function
  | Plain -> `String "plain"
  | Argon2 -> `String "argon2"

type password = {
  type_: password_type [@key "type"];
  password: string;
} [@@deriving yojson, show]

type user = {
  password: password option [@default None];
  group: (string option [@default None]);
  expires: (float option [@default None]);
} [@@deriving yojson, show]
let _ = pp_user

type create_table_method =
  | CreateTable
  | CreateHyperTable

(* don't use derived yojson handling; it puts things inside an array *)
let create_table_method_of_yojson = function
  | `String "create_table" -> Ok CreateTable
  | `String "create_hypertable" -> Ok CreateHyperTable
  | _ -> Error "Expected create_table or create_hypertable"

let create_table_method_to_yojson = function
  | CreateTable -> `String "create_table"
  | CreateHyperTable -> `String "create_hypertable"

type regexp = Regexp of string * Re.re

let matches : regexp -> string -> bool =
  fun (Regexp (_, re)) str ->
  Re.exec_opt re str <> None

let regexp str = Regexp (str, Re.Perl.re str |> Re.compile)

let regexp_of_yojson =
  let rere = Re.Perl.re "^/(.*)/$" |> Re.compile in
  fun (yojson: Yojson.Safe.t) ->
    match yojson with
    | `String str -> begin
        match Re.exec_opt rere str with
        | None -> Stdlib.Error "Expect regexp of form /regexp/"
        | Some group ->
          let groups = Re.Group.all group in
          try Ok (Regexp (str, Re.Perl.re groups.(1) |> Re.compile))
          with
          | Re.Perl.Parse_error -> Stdlib.Error "Failed to parse regexp"
          | Re.Perl.Not_supported -> Stdlib.Error "Regexp feature not supported"
      end
    | _ -> Stdlib.Error "Expected regexp"

let regexp_to_yojson (Regexp (original, _)) = `String original

type create_table = {
  regexp: regexp;
  method_: create_table_method [@key "method"]
} [@@deriving yojson]

type database = {
  db_name: string;

  db_host: string;
  db_port: int;

  db_user: string;
  db_password: string;

  allowed_users: (string list option [@default None]);

  create_table: (create_table option [@default Some { regexp = regexp ".+"; method_ = CreateTable }]);

  time_column: (string option [@default Some "time"]);
  tags_jsonb_column: (string option [@default Some "tags"]);

  tag_columns: (string list option [@default None]);

  fields_jsonb_column: (string option [@default Some "fields"]);
  field_columns: (string list option [@default None]);
} [@@deriving yojson]

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

type users = (string * user) list [@@deriving yojson]
let users_of_yojson = associative_of_yojson user_of_yojson

type regexp_users = (string * user) list [@@deriving yojson]
let regexp_users_of_yojson = associative_of_yojson user_of_yojson

type groups = (string * group) list [@@deriving yojson]
let groups_of_yojson = list_or_empty_of_yojson (associative_of_yojson group_of_yojson)

type t = {
  users : users [@default []];
  regexp_users : regexp_users [@default []];

  groups: groups [@default []];

  realm : string [@default "IW2PG"];

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

let dump_exn t =
  let yaml = to_yojson t |> Common.yaml_of_yojson in
  match Yaml.to_string yaml with
  | Ok str -> output_string stdout str
  | Error (`Msg message) -> raise (Error (CannotProcessConfig message))

let parse_exn yaml =
  let yojson = yaml |> yojson_of_yaml in
  (* Printf.printf "yojson: %s\n%!" (Yojson.Safe.to_string yojson); *)
  match yojson |> of_yojson with
  | Ok config -> config
  | Error message -> raise (Error (CannotProcessConfig message))

let load_exn file =
  match Yaml_unix.of_file (Fpath.(v file)) with
  | Ok yaml -> parse_exn yaml
  | Error (`Msg message) -> raise (Error (CannotParseConfig message))
