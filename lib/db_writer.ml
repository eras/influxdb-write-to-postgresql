module Pg = Postgresql

type quote_mode = QuoteAlways

module FieldMap = Map.Make(struct type t = string let compare = compare end)
module TableMap = Map.Make(struct type t = string let compare = compare end)

type config = {
  conninfo : string;
  time_field : string;
  tags_column: string option;   (* using tags column? then this is its name *)
  fields_column: string option;   (* using fields column? then this is its name *)
}

type field_type =
  | FT_String
  | FT_Int
  | FT_Float
  | FT_Boolean
  | FT_Unknown of string

let db_of_field_type = function
  | FT_Int         -> "integer"
  | FT_Float       -> "double prescision"
  | FT_String      -> "text"
  | FT_Boolean     -> "boolean"
  | FT_Unknown str -> str

let field_type_of_db = function
  | "integer"           -> FT_Int
  | "double prescision" -> FT_Float
  | "numeric"           -> FT_Float
  | "text" | "varchar"  -> FT_String
  | "boolean"           -> FT_Boolean
  | name                -> FT_Unknown name

let field_type_of_value = function
  | Lexer.String _   -> FT_String
  | Lexer.Int _      -> FT_Int
  | Lexer.FloatNum _ -> FT_Float
  | Lexer.Boolean _  -> FT_Boolean

type table_name = string

type column_info = (table_name, field_type FieldMap.t) Hashtbl.t

type t = {
  mutable db: Pg.connection; (* mutable for reconnecting *)
  quote_mode: quote_mode;
  quoted_time_field: string;
  subsecond_time_field: bool;
  config: config;
  mutable known_columns: column_info;
  indices: string list list TableMap.t;
}

let create_column_info () = Hashtbl.create 10

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
      String.concat ", " (List.concat [db_fields meas] |>
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
      "(" ^ String.concat ", " (db_insert_fields t meas) ^ ")" ^
      "\nVALUES (" ^ String.concat ", " (db_insert_values t meas) ^ ")" ^
      "\nON CONFLICT(" ^ String.concat ", " (conflict_tags t meas) ^ ")" ^
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

  let query_column_info (db: Pg.connection) =
    let result = db#exec ~expect:[Pg.Tuples_ok] "SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS" in
    let column_info = create_column_info () in
    let () = result#get_all_lst |> List.iter @@ function
    | [table_name; column_name; data_type] ->
      Hashtbl.find_opt column_info table_name
      |> Option.value ~default:FieldMap.empty
      |> FieldMap.add column_name (field_type_of_db data_type)
      |> Hashtbl.add column_info table_name
    | _ -> assert false
    in
    column_info

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
end

open Internal

let create (config : config) =
  try
    let db = new Pg.connection ~conninfo:config.conninfo () in
    let quote_mode = QuoteAlways in
    let quoted_time_field = db_of_identifier (config.time_field) in
    let subsecond_time_field = false in
    let known_columns = query_column_info db in
    let indices = query_indices db |> TableMap.of_seq in
    { db; quote_mode; quoted_time_field; subsecond_time_field;
      known_columns; indices;
      config }
  with Pg.Error error ->
    raise (Error (PgError (error, None)))

let close t =
  t.db#finish

let reconnect t =
  ( try close t
    with _ -> (* eat *) () );
  t.db <- new Pg.connection ~conninfo:t.config.conninfo ()

let string_of_error error =
  match error with
  | PgError (error, None) -> Pg.string_of_error error
  | PgError (error, Some query) -> Pg.string_of_error error ^ " for " ^ query
  | MalformedUTF8 -> "Malformed UTF8"
  | CannotAddTags tags -> "Cannot add tags " ^ String.concat ", " (List.map db_of_identifier tags)
  | NoPrimaryIndexFound table -> "No primary index found for table " ^ table

let _ = Printexc.register_printer (function
    | Error error -> Some (string_of_error error)
    | _ -> None
  )

(** Ensure database has the columns we need *)
let check_and_update_columns ~kind t table_name values =
  let missing_columns, new_columns =
    List.fold_left
      (fun (to_create, known_columns) (field_name, field_type) ->
         if FieldMap.mem field_name known_columns
         then (to_create, known_columns)
         else ((field_name, field_type)::to_create, FieldMap.add field_name field_type known_columns)
      )
      ([],
       try Hashtbl.find t.known_columns table_name
       with Not_found -> FieldMap.empty)
      values
  in
  match missing_columns, kind, t.config.tags_column with
  | [], _, _ -> ()
  | missing_columns, `Fields, _ ->
    Hashtbl.add t.known_columns table_name new_columns;
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

let write t (measurements: Lexer.measurement list) =
  try
    ignore (t.db#exec ~expect:[Pg.Command_ok] "BEGIN TRANSACTION");
    (* TODO: group requests by their parameters and use multi-value inserts *)
    List.iter (
      fun measurement -> 
        let (query, params) = insert_of_measurement t measurement in
        let field_types = List.map (fun (name, value) -> (name, field_type_of_value value)) measurement.fields in
        let tag_types = List.map (fun (name, _) -> (name, FT_String)) measurement.tags in
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
