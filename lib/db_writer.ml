module Pg = Postgresql

type quote_mode = QuoteAlways

module FieldMap = Map.Make(struct type t = string let compare = compare end)

type config = {
  conninfo : string;
  time_field : string;
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
}

let create_column_info () = Hashtbl.create 10

type error =
  | PgError of Pg.error
  | MalformedUTF8

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

let is_unquoted_ascii x =
  let i = Uchar.to_int x in
  if i >= 1 && i <= 127 then
    let c = Uchar.to_char x in
    (c >= 'a' && c <= 'z')
    || (c >= '0' && c <= '9')
    || (c == '_')
  else
    false

let is_unescaped_ascii x =
  let i = Uchar.to_int x in
  if i >= 1 && i <= 127 then
    let c = Uchar.to_char x in
    (c >= 'a' && c <= 'z')
    || (c >= 'A' && c <= 'Z')
    || (c >= '0' && c <= '9')
    || (c == '_')
  else
    false

module Internal =
struct
  let db_of_identifier str =
    let out = Buffer.create (String.length str) in
    Buffer.add_string out "U&\"";
    let decoder = Uutf.decoder ~encoding:`UTF_8 (`String str) in
    let any_special = ref false in
    let rec loop () =
      match Uutf.decode decoder with
      | `Await -> assert false
      | `Uchar x when x == Uchar.of_char '\\' || x == Uchar.of_char '"' ->
        any_special := true;
        Buffer.add_char out '\\';
        Buffer.add_char out (Uchar.to_char x);
        loop ()
      | `Uchar x when is_unquoted_ascii x ->
        Buffer.add_char out (Uchar.to_char x);
        loop ()
      | `Uchar x when is_unescaped_ascii x ->
        any_special := true;
        Buffer.add_char out (Uchar.to_char x);
        loop ()
      | `Uchar x when Uchar.to_int x < (1 lsl 16) ->
        any_special := true;
        Printf.ksprintf (Buffer.add_string out) "\\%04x" (Uchar.to_int x);
        loop ()
      | `Uchar x when Uchar.to_int x < (1 lsl 24) ->
        any_special := true;
        Printf.ksprintf (Buffer.add_string out) "\\+%06x" (Uchar.to_int x);
        loop ()
      | `Uchar _ | `Malformed _ ->
        any_special := true;
        raise (Error MalformedUTF8)
      | `End when !any_special ->
        Buffer.add_char out '"';
        Buffer.contents out
      | `End ->
        str (* return original identifier as nothing special was done *)
    in
    loop ()

  let db_fields (meas : Lexer.measurement) =
    List.concat [List.map fst meas.tags |> List.map db_of_identifier;
                 List.map fst meas.fields |> List.map db_of_identifier]

  let db_raw_of_value =
    let open Lexer in
    function
    | String x -> x
    | Int x -> Int64.to_string x
    | FloatNum x -> Printf.sprintf "%f" x
    | Boolean true -> "true"
    | Boolean false -> "false"

  let db_raw_values (meas : Lexer.measurement) =
    List.concat [List.map (fun (_, value) -> db_raw_of_value (String value)) meas.tags;
                 List.map (fun (_, field) -> db_raw_of_value field) meas.fields]

  let map_first f els =
    match els with
    | x::els -> f x::els
    | els -> els

  let db_placeholders (meas : Lexer.measurement) =
    let enumerate els =
      List.rev (fst (List.fold_left (fun (xs, n) _ -> ((n::xs), succ n)) ([], 1) els))
    in
    List.concat [
      (match meas.time with
       | None -> []
       | Some _ -> ["time"]);
      List.map fst meas.tags;
      List.map fst meas.fields
    ]
    |> enumerate
    |> List.map (Printf.sprintf "$%d")
    |>
    match meas.time with
    | None -> (fun xs -> "CURRENT_TIMESTAMP"::xs)
    | Some _ -> map_first (fun x -> Printf.sprintf "to_timestamp(%s)" x)

  let insert_of_measurement t (meas : Lexer.measurement) =
    let query =
      "INSERT INTO " ^ db_of_identifier meas.measurement ^
      "(" ^ String.concat ", " (t.quoted_time_field::db_fields meas) ^ ")" ^
      " VALUES (" ^ String.concat "," (db_placeholders meas) ^ ")" ^
      " ON CONFLICT(" ^ t.quoted_time_field ^ ") DO UPDATE SET " ^
      String.concat ", " (db_fields meas |> List.map @@ fun field ->
                          field ^ "=" ^ "excluded." ^ field)
    in
    let params = db_raw_values meas in
    let params =
      match meas.time with
      | None -> params
      | Some x ->
        Printf.sprintf "%s" (
          (* TODO: what about negative values? Check that 'rem' works as expected *)
          if t.subsecond_time_field
          then Printf.sprintf "%Ld.%09Ld" (Int64.div x 1000000000L) (Int64.rem x 1000000000L)
          else Printf.sprintf "%Ld" (Int64.div x 1000000000L)
        )::params
    in
    (query, params |> Array.of_list)

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
end

open Internal

let create (config : config) =
  try
    let db = new Pg.connection ~conninfo:config.conninfo () in
    let quote_mode = QuoteAlways in
    let quoted_time_field = db_of_identifier (config.time_field) in
    let subsecond_time_field = false in
    let known_columns = query_column_info db in
    { db; quote_mode; quoted_time_field; subsecond_time_field;
      known_columns;
      config }
  with Pg.Error error ->
    raise (Error (PgError error))

let close t =
  t.db#finish

let reconnect t =
  ( try close t
    with _ -> (* eat *) () );
  t.db <- new Pg.connection ~conninfo:t.config.conninfo ()

let string_of_error error =
  match error with
  | PgError error -> Pg.string_of_error error
  | MalformedUTF8 -> "Malformed UTF8"

(** Ensure database has the columns we need *)
let update_columns t table_name values =
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
  match missing_columns with
  | [] -> ()
  | missing_columns ->
    Hashtbl.add t.known_columns table_name new_columns;
    missing_columns |> List.iter @@ fun (field_name, field_type) ->
    ignore (t.db#exec ~expect:[Pg.Command_ok]
              (Printf.sprintf "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s"
                 (db_of_identifier table_name)
                 (db_of_identifier field_name)
                 (db_of_field_type field_type)
              )
           )

let write t (measurements: Lexer.measurement list) =
  try
    ignore (t.db#exec ~expect:[Pg.Command_ok] "BEGIN TRANSACTION");
    (* TODO: group requests by their parameters and use multi-value inserts *)
    List.iter (
      fun measurement -> 
        let (query, params) = insert_of_measurement t measurement in
        let columns_types =
          let tags = List.map (fun (name, _) -> (name, FT_String)) measurement.tags in
          let fields = List.map (fun (name, value) -> (name, field_type_of_value value)) measurement.fields in
          tags @ fields
        in
        let () = update_columns t measurement.measurement columns_types in
        ignore (t.db#exec ~params ~expect:[Pg.Command_ok] query);
    ) measurements;
    ignore (t.db#exec ~expect:[Pg.Command_ok] "COMMIT");
  with Pg.Error error ->
    (try ignore (t.db#exec ~expect:[Pg.Command_ok] "ROLLBACK");
     with Pg.Error _ -> (* ignore *) ());
    raise (Error (PgError error))
