module Pg = Postgresql

type quote_mode = QuoteAlways

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
  time_column : string;
  tags_column: string option;   (* using tags column? then this is its name *)
  fields_column: string option;   (* using fields column? then this is its name *)
}

module Internal0 =
struct
  module FieldMap = Map.Make(struct type t = string let compare = compare end)
  module TableMap = Map.Make(struct type t = string let compare = compare end)

  type field_type =
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

  let not_null x = x ^ " NOT NULL"
  let db_concat = String.concat ", "

  let field_type_of_db = function
    | "integer"           -> FT_Int
    | "double precision"  -> FT_Float
    | "numeric"           -> FT_Float
    | "text" | "varchar"  -> FT_String
    | "boolean"           -> FT_Boolean
    | "jsonb"             -> FT_Jsonb
    | "timestamptz"       -> FT_Timestamptz
    | name                -> FT_Unknown name

  let field_type_of_value = function
    | Lexer.String _   -> FT_String
    | Lexer.Int _      -> FT_Int
    | Lexer.FloatNum _ -> FT_Float
    | Lexer.Boolean _  -> FT_Boolean

  type table_name = string

  type table_info = {
    fields: field_type FieldMap.t
  }
end

open Internal0


let map_fields f table_info = { fields = f table_info.fields }

type database_info = (table_name, table_info) Hashtbl.t

type t = {
  mutable db: Pg.connection; (* mutable for reconnecting *)
  quote_mode: quote_mode;
  quoted_time_field: string;
  subsecond_time_field: bool;
  config: config;
  mutable database_info: database_info;
  indices: string list list TableMap.t;
}

let create_database_info () : database_info = Hashtbl.create 10

type query = string

type error =
  | PgError of (Pg.error * query option)
  | MalformedUTF8
  | CannotAddTags of string list
  | NoPrimaryIndexFound of string

exception Error of error

(* backwards compatibility; Option was introduced in OCaml 4.08 *)
module Option :
sig
  val value : 'a option -> default:'a -> 'a
end =
struct
  let value x ~default =
    match x with
    | None -> default
    | Some x -> x
end

module Internal =
struct
  include Internal0

  let db_of_identifier x =
    try Common.db_of_identifier x
    with Common.Error Common.MalformedUTF8 ->
      raise (Error MalformedUTF8)

  let db_tags (meas : Lexer.measurement) =
    List.map fst meas.tags |> List.map db_of_identifier

  let db_fields (meas : Lexer.measurement) =
    List.map fst meas.fields |> List.map db_of_identifier

  let db_raw_of_value =
    let open Lexer in
    function
    | String x -> x
    | Int x -> Int64.to_string x
    | FloatNum x -> Printf.sprintf "%f" x
    | Boolean true -> "true"
    | Boolean false -> "false"

  let db_insert_tag_values t (meas : Lexer.measurement) =
    match t.config.tags_column with
    | None ->
      meas.tags |> List.map (fun (_, value) -> db_raw_of_value (String value))
    | Some _ ->
      [`Assoc (
          meas.tags |>
          List.map (
            fun (name, value) ->
              (name, `String value)
          )
        ) |> Yojson.Basic.to_string]

  let json_of_value : Lexer.value -> Yojson.t = function
    | Lexer.String x -> `String x
    | Lexer.Int x -> `Intlit (Int64.to_string x)
    | Lexer.FloatNum x -> `Float x
    | Lexer.Boolean x -> `Bool x

  let db_insert_field_values t (meas : Lexer.measurement) =
    match t.config.fields_column with
    | None ->
      meas.fields |> List.map (fun (_, field) -> db_raw_of_value field)
    | Some _ ->
      [`Assoc (
          meas.fields |>
          List.map (
            fun (name, value) ->
              (name, json_of_value value)
          )
        ) |> Yojson.to_string]

  let map_first f els =
    match els with
    | x::els -> f x::els
    | els -> els

  let db_names_of_tags t meas=
    match t.config.tags_column with
    | None -> db_tags meas
    | Some name -> [db_of_identifier name]

  let db_names_of_fields t meas =
    match t.config.fields_column with
    | None -> db_fields meas
    | Some name -> [db_of_identifier name]

  let db_insert_values t (meas : Lexer.measurement) =
    let with_enumerate first els =
      let (result, next) =
        (List.fold_left (
            fun (xs, n) element ->
              (((n, element)::xs), succ n)
          ) ([], first) els)
      in
      (List.rev result, next)
    in
    let tags =
      let time =
        match meas.time with
        | None -> []
        | Some _ -> ["time"]
      in
      List.append
        time
        (db_names_of_tags t meas)
    in
    (* actual values are ignored, only the number of them matters *)
    List.concat [tags; db_names_of_fields t meas]
    |> with_enumerate 1 |> fst |> Common.map_fst (Printf.sprintf "$%d")
    |> List.map fst
    |>
    match meas.time with
    | None -> (fun xs -> "CURRENT_TIMESTAMP"::xs)
    | Some _ -> map_first (fun x -> Printf.sprintf "to_timestamp(%s)" x)

  let db_insert_fields t meas =
    let tags =
      match t.config.tags_column with
      | None -> db_tags meas
      | Some tags -> [db_of_identifier tags]
    in
    let fields =
      match t.config.fields_column with
      | None -> db_fields meas
      | Some fields -> [db_of_identifier fields]
    in
    List.concat [[t.quoted_time_field]; tags; fields]

  let db_update_set t meas =
    match t.config.fields_column with
    | None ->
      db_concat (List.concat [db_fields meas] |>
                 List.map @@ fun field ->
                 field ^ "=" ^ "excluded." ^ field)
    | Some fields ->
      db_of_identifier fields ^ "=" ^
      db_of_identifier meas.measurement ^ "." ^ db_of_identifier fields ^ "||" ^
      "excluded." ^ db_of_identifier fields

  let conflict_tags t (meas : Lexer.measurement) =
    match TableMap.find_opt meas.measurement t.indices with
    | None | Some [] -> raise (Error (NoPrimaryIndexFound meas.measurement))
    | Some (index::_rest) -> index

  let insert_of_measurement t (meas : Lexer.measurement) =
    let query =
      "INSERT INTO " ^ db_of_identifier meas.measurement ^
      "(" ^ db_concat (db_insert_fields t meas) ^ ")" ^
      "\nVALUES (" ^ db_concat (db_insert_values t meas) ^ ")" ^
      "\nON CONFLICT(" ^ db_concat (conflict_tags t meas) ^ ")" ^
      "\nDO UPDATE SET " ^ db_update_set t meas
    in
    let params = db_insert_tag_values t meas @ db_insert_field_values t meas in
    let time =
      match meas.time with
      | None -> []
      | Some x ->
        [Printf.sprintf "%s" (
            (* TODO: what about negative values? Check that 'rem' works as expected *)
            if t.subsecond_time_field
            then Printf.sprintf "%Ld.%09Ld" (Int64.div x 1000000000L) (Int64.rem x 1000000000L)
            else Printf.sprintf "%Ld" (Int64.div x 1000000000L)
          )]
    in
    (query, time @ params |> Array.of_list)

  let query_database_info (db: Pg.connection) =
    let result = db#exec ~expect:[Pg.Tuples_ok] "SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS" in
    let database_info = create_database_info () in
    let () = result#get_all_lst |> List.iter @@ function
    | [table_name; column_name; data_type] ->
      Hashtbl.find_opt database_info table_name
      |> Option.value ~default:{ fields = FieldMap.empty }
      |> map_fields (FieldMap.add column_name (field_type_of_db data_type))
      |> Hashtbl.add database_info table_name
    | _ -> assert false
    in
    database_info

  let primary_keys_of_index = function
    | Sql.CreateIndex {
        unique = true;
        fields;
        _;
      } ->
      Some (fields |> List.map (function
          | `Column field -> field
          | `Expression expression -> Sql.string_of_expression ~db_of_identifier expression
        ))
    | Sql.CreateIndex {
        unique = false;
        _;
      } -> None

  let query_indices (db : Pg.connection) =
    let result = db#exec ~expect:[Pg.Tuples_ok] {|SELECT tablename, indexdef FROM pg_indexes WHERE schemaname='public' order by tablename|} in
    result#get_all_lst |> List.map (function
        | [table_name; index_definition] -> (table_name, Sql.parse_sql index_definition)
        | _ -> assert false)
    |> Common.map_snd primary_keys_of_index
    |> CCList.group_succ ~eq:( = )
    |> List.map (fun xs ->
        let tablename = fst (List.hd xs) in
        (tablename, List.filter_map snd xs)
      )
    |> List.to_seq

  let new_pg_connection = function
    | DbInfo { db_host; db_port; db_user; db_password; db_name } ->
      new Pg.connection ~host:db_host ~port:(string_of_int db_port) ~user:db_user ~password:db_password ~dbname:db_name ()
    | DbConnInfo conninfo -> new Pg.connection ~conninfo ()

  let db_fields_and_types (measurement : Lexer.measurement) =
    List.map (fun (name, value) -> (name, field_type_of_value value)) measurement.fields

  let db_tags_and_types (measurement : Lexer.measurement) =
    List.map (fun (name, _) -> (name, FT_String)) measurement.tags

  type made_table = {
    md_command : string;
    md_table_info : table_info;
  }

  let make_table_command (t : t) (measurement : Lexer.measurement) : made_table =
    let table_name = measurement.measurement in
    let dbify (name, type_) = (db_of_identifier name, db_of_field_type type_) in
    let field_columns =
      (match t.config.fields_column with
       | None -> db_fields_and_types measurement
       | Some field -> [(field, FT_Jsonb)])
    in
    let db_field_columns = field_columns |> List.map dbify in
    let pk_columns =
      (db_of_identifier t.config.time_column, FT_Timestamptz)::
      (match t.config.tags_column with
       | None -> (db_tags_and_types measurement)
       | Some tags -> [(tags, FT_Jsonb)])
    in
    let db_pk_columns = pk_columns |> List.map dbify |>  Common.map_snd not_null in
    let join (a, b) = a ^ " " ^ b in
    let command =
      "CREATE TABLE " ^ db_of_identifier table_name ^
      " (" ^ db_concat (((db_pk_columns @ db_field_columns) |> List.map join) @
                          ["PRIMARY KEY(" ^ db_concat (List.map fst db_pk_columns) ^ ")"]) ^ ")"
    in
    let table_info = { fields = (pk_columns @ field_columns) |> List.to_seq |> FieldMap.of_seq } in
    { md_command = command;
      md_table_info = table_info }
end

open Internal

let db_spec_of_database : Config.database -> db_spec =
  fun { db_name; db_host; db_port; db_user; db_password; _ } ->
  DbInfo {
    db_host;
    db_port;
    db_user;
    db_password;
    db_name;
  }

let db_config_of_database : Config.database -> config =
  fun ({ Config.time_column; tags_jsonb_column; fields_jsonb_column; create_table; _ } as database) ->
  let db_spec = db_spec_of_database database in
  { db_spec;
    time_column = Option.value time_column ~default:"time";
    tags_column = tags_jsonb_column;
    fields_column = fields_jsonb_column;
    create_table; }

let create (config : config) =
  try
    let db = new_pg_connection config.db_spec in
    let quote_mode = QuoteAlways in
    let quoted_time_field = db_of_identifier (config.time_column) in
    let subsecond_time_field = false in
    let database_info = query_database_info db in
    let indices = query_indices db |> TableMap.of_seq in
    { db; quote_mode; quoted_time_field; subsecond_time_field;
      database_info; indices;
      config }
  with Pg.Error error ->
    raise (Error (PgError (error, None)))

let close t =
  t.db#finish

let reconnect t =
  ( try close t
    with _ -> (* eat *) () );
  t.db <- new_pg_connection t.config.db_spec

let string_of_error error =
  match error with
  | PgError (error, None) -> Pg.string_of_error error
  | PgError (error, Some query) -> Pg.string_of_error error ^ " for " ^ query
  | MalformedUTF8 -> "Malformed UTF8"
  | CannotAddTags tags -> "Cannot add tags " ^ db_concat (List.map db_of_identifier tags)
  | NoPrimaryIndexFound table -> "No primary index found for table " ^ table

let _ = Printexc.register_printer (function
    | Error error -> Some (string_of_error error)
    | _ -> None
  )

let update hashtbl default key f =
  match Hashtbl.find_opt hashtbl key with
  | None -> Hashtbl.add hashtbl key (f default)
  | Some value -> Hashtbl.add hashtbl key (f value)

(** Ensure database has the columns we need *)
let check_and_update_columns ~kind t table_name values =
  let missing_columns, new_columns =
    List.fold_left
      (fun (to_create, table_info) (field_name, field_type) ->
         if FieldMap.mem field_name table_info
         then (to_create, table_info)
         else ((field_name, field_type)::to_create, FieldMap.add field_name field_type table_info)
      )
      ([],
       try (Hashtbl.find t.database_info table_name).fields
       with Not_found -> FieldMap.empty)
      values
  in
  match missing_columns, kind, t.config.tags_column with
  | [], _, _ -> ()
  | missing_columns, `Fields, _ ->
    update t.database_info { fields = FieldMap.empty } table_name (fun _table_info ->
        { fields = new_columns }
      );
    missing_columns |> List.iter @@ fun (field_name, field_type) ->
    ignore (t.db#exec ~expect:[Pg.Command_ok]
              (Printf.sprintf "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s"
                 (db_of_identifier table_name)
                 (db_of_identifier field_name)
                 (db_of_field_type field_type)
              )
           )
  | missing_columns, `Tags, None ->
    raise (Error (CannotAddTags (List.map fst missing_columns)))
  | _, `Tags, Some _ ->
    () (* these are inside a json and will be added dynamically *)

let check_and_update_tables (t : t) (measurement : Lexer.measurement) =
  let table_name = measurement.measurement in
  if Hashtbl.mem t.database_info table_name then
    ()
  else
    let made_table = make_table_command t measurement in
    Hashtbl.add t.database_info table_name made_table.md_table_info;
    ignore (t.db#exec ~expect:[Pg.Command_ok] made_table.md_query)

let write t (measurements: Lexer.measurement list) =
  try
    ignore (t.db#exec ~expect:[Pg.Command_ok] "BEGIN TRANSACTION");
    (* TODO: group requests by their parameters and use multi-value inserts *)
    List.iter (
      fun measurement -> 
        let (query, params) = insert_of_measurement t measurement in
        let field_types = db_fields_and_types measurement in
        let tag_types = db_tags_and_types measurement in
        let () = check_and_update_tables t measurement in
        let () = check_and_update_columns ~kind:`Tags t measurement.measurement tag_types in
        let () = check_and_update_columns ~kind:`Fields t measurement.measurement field_types in
        try
          ignore (t.db#exec ~params ~expect:[Pg.Command_ok] query);
        with Pg.Error error ->
          (try ignore (t.db#exec ~expect:[Pg.Command_ok] "ROLLBACK");
           with Pg.Error _ -> (* ignore *) ());
          raise (Error (PgError (error, Some query)))
    ) measurements;
    ignore (t.db#exec ~expect:[Pg.Command_ok] "COMMIT");
  with Pg.Error error ->
    (try ignore (t.db#exec ~expect:[Pg.Command_ok] "ROLLBACK");
     with Pg.Error _ -> (* ignore *) ());
    raise (Error (PgError (error, None)))
