module Log = (val Logs.src_log Logging.db_writer_src : Logs.LOG)

module Pg = Postgresql

let db_exec (db: Pg.connection) ?params ?expect query =
  let (time, res) =
    Common.time
      (Common.valuefy (fun () -> db#exec ?expect ?params query))
      ()
  in
  Log.debug (fun m ->
      m "Exec %s with %s took %.1f ms"
        ([%derive.show: string] query)
        (match params with
         | None -> "no parameters"
         | Some params -> [%derive.show: string array] params)
        (Mtime.Span.(time |> to_ms))
    );
  Common.unvaluefy res

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
  create_table : Config.create_table option;
  time_column : string;
  tags_column: string option;   (* using tags column? then this is its name *)
  fields_column: string option;   (* using fields column? then this is its name *)
}

module Internal0 =
struct
  module FieldMap = Map.Make(String)
  module TableMap = Map.Make(String)

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
  let default value x = x ^ " DEFAULT(" ^ value ^ ")"
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
    | Influxdb_lexer.String _   -> FT_String
    | Influxdb_lexer.Int _      -> FT_Int
    | Influxdb_lexer.FloatNum _ -> FT_Float
    | Influxdb_lexer.Boolean _  -> FT_Boolean

  type table_name = string

  type table_info = {
    fields: field_type FieldMap.t
  }

  type database_info = (table_name, table_info) Hashtbl.t

  let create_database_info () : database_info = Hashtbl.create 10
end

open Internal0


let map_fields f table_info = { fields = f table_info.fields }

type t = {
  mutable db: Pg.connection; (* mutable for reconnecting *)
  quote_mode: quote_mode;
  quoted_time_field: string;
  subsecond_time_field: bool;
  config: config;
  mutable database_info: database_info;
  mutable indices: string list list TableMap.t;
}

type timestamp_method =
  | TS_CallTimestamp
  | TS_StringTimestamp [@@warning "-37"] (* warning about not being used for values *)

let timestamp_method = TS_CallTimestamp

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

module Internal =
struct
  include Internal0

  let db_of_identifier x =
    try Common.db_of_identifier x
    with Common.Error Common.MalformedUTF8 ->
      raise (Error MalformedUTF8)

  let db_tags (meas : Influxdb_lexer.measurement) =
    List.map fst meas.tags |> List.map db_of_identifier

  let db_fields (meas : Influxdb_lexer.measurement) =
    List.map fst meas.fields |> List.map db_of_identifier

  let db_raw_of_value =
    let open Influxdb_lexer in
    function
    | String x -> x
    | Int x -> Int64.to_string x
    | FloatNum x -> Printf.sprintf "%f" x
    | Boolean true -> "true"
    | Boolean false -> "false"

  let db_insert_tag_values t (meas : Influxdb_lexer.measurement) =
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

  let json_of_value : Influxdb_lexer.value -> Yojson.t = function
    | Influxdb_lexer.String x -> `String x
    | Influxdb_lexer.Int x -> `Intlit (Int64.to_string x)
    | Influxdb_lexer.FloatNum x -> `Float x
    | Influxdb_lexer.Boolean x -> `Bool x

  let db_insert_field_values t (meas : Influxdb_lexer.measurement) =
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

  let db_insert_value_placeholders ?(next_placeholder=1) t (meas : Influxdb_lexer.measurement) =
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
    |> with_enumerate next_placeholder
    |> fun (placeholder, next_placeholder) ->
    placeholder
    |> Common.map_fst (Printf.sprintf "$%d")
    |> List.map fst
    |>
    match meas.time with
    | None -> fun xs -> ("CURRENT_TIMESTAMP"::xs, next_placeholder)
    | Some _ -> fun xs -> (map_first (fun x -> Printf.sprintf
                                         (match timestamp_method with
                                          | TS_StringTimestamp -> "%s"
                                          | TS_CallTimestamp -> "to_timestamp(%s)") x) xs), next_placeholder

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

  let conflict_tags t (meas : Influxdb_lexer.measurement) =
    match TableMap.find_opt meas.measurement t.indices with
    | None | Some [] -> raise (Error (NoPrimaryIndexFound meas.measurement))
    | Some (index::_rest) -> index

  let insert_of_measurement ?(measurements:Influxdb_lexer.measurement list option) t (reference : Influxdb_lexer.measurement) =
    let db_values_placeholders ?next_placeholder meas =
      let (placeholders, next) = db_insert_value_placeholders ?next_placeholder t meas in
      ("(" ^ db_concat placeholders ^ ")", next)
    in
    let values =
      match measurements with
      | None -> [reference]
      | Some values -> values
    in
    let values_placeholders =
      List.fold_left
        (fun (next_placeholder, fields) meas ->
           let (placeholders, next_placeholder) = db_values_placeholders ~next_placeholder meas in
           (next_placeholder, placeholders::fields)
        )
        (1, [])
        values |> snd |> List.rev |> String.concat ", "
    in
    let query =
      "INSERT INTO " ^ db_of_identifier reference.measurement ^
      "(" ^ db_concat (db_insert_fields t reference) ^ ")" ^
      "\nVALUES " ^ values_placeholders ^
      "\nON CONFLICT(" ^ db_concat (conflict_tags t reference) ^ ")" ^
      "\nDO UPDATE SET " ^ db_update_set t reference
    in
    let time (meas : Influxdb_lexer.measurement) =
      match meas.time with
      | None -> []
      | Some epoch_time ->
        [Printf.sprintf "%s" (
            let seconds = Int64.div epoch_time 1000000000L in
            let microseconds = Int64.(div (rem epoch_time 1000000000L) 1000L) in
            match timestamp_method with
            | TS_StringTimestamp ->
              let date_str =
                let netdate_t_utc = Netdate.create ~zone:0 (Int64.to_float seconds) in
                Netdate.format netdate_t_utc ~fmt:"%Y-%m-%d %H:%M:%S"
              in
              (* TODO: what about negative values? Check that 'rem' works as
                 expected *)
              (* TODO: PostgreSQL only supports microseconds, so this can cause
                 data loss until the overflow precision finds a storage *)
              let microseconds = Int64.(div (rem epoch_time 1000000000L) 1000L) in
              if t.subsecond_time_field
              then Printf.sprintf "%s.%06Ld UTC" date_str microseconds
              else Printf.sprintf "%s UTC" date_str
            | TS_CallTimestamp ->
              if t.subsecond_time_field
              then Printf.sprintf "%Ld.%06Ld" seconds microseconds
              else Printf.sprintf "%Ld" seconds
          )]
    in
    let params =
      values
      |> List.map (fun meas -> time meas @ db_insert_tag_values t meas @ db_insert_field_values t meas)
      |> List.concat
    in
    (query, params |> Array.of_list)

  let query_database_info (db: Pg.connection) =
    let result = db#exec ~expect:[Pg.Tuples_ok] "SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema='public'" in
    let database_info = create_database_info () in
    let () = result#get_all_lst |> List.iter @@ function
      | [table_name; column_name; data_type] ->
        Hashtbl.find_opt database_info table_name
        |> Option.value ~default:{ fields = FieldMap.empty }
        |> map_fields (FieldMap.add column_name (field_type_of_db data_type))
        |> Hashtbl.replace database_info table_name
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
      Logs.info (fun m ->
          m "Connecting %s:%d user %s database %s"
            ([%derive.show: string] db_host)
            db_port
            ([%derive.show: string] db_user)
            ([%derive.show: string] db_name)
        );
      new Pg.connection ~host:db_host ~port:(string_of_int db_port) ~user:db_user ~password:db_password ~dbname:db_name ()
    | DbConnInfo conninfo ->
      Logs.info (fun m ->
          m "Connecting conninfo %s"
            ([%derive.show: string] conninfo)
        );
      new Pg.connection ~conninfo ()

  let db_fields_and_types (measurement : Influxdb_lexer.measurement) =
    List.map (fun (name, value) -> (name, field_type_of_value value)) measurement.fields

  let db_tags_and_types (measurement : Influxdb_lexer.measurement) =
    List.map (fun (name, _) -> (name, FT_String)) measurement.tags

  type made_table = {
    md_command : string;
    md_table_info : table_info;
    md_update_pks : string list list TableMap.t -> string list list TableMap.t;
  }

  let make_table_command (t : t) (measurement : Influxdb_lexer.measurement) : made_table =
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
    let db_pk_columns =
      let identity x = x in
      let map2_zip f g x = (f x, g x) in
      let snd_lens f (a, b) = (a, f b) in
      pk_columns |> List.map (map2_zip identity dbify) |>
      List.map (snd_lens (snd_lens not_null)) |>
      Common.map_rest
        snd
        (fun ((_name, field_type), (db_name, db_type)) ->
           (db_name, default (
               match field_type with
               | FT_Jsonb -> "'{}'"
               | FT_String -> "''"
               | ft -> failwith ("Unexpected field type " ^ db_of_field_type ft)
             ) db_type)
        ) in
    let join (a, b) = a ^ " " ^ b in
    let command =
      "CREATE TABLE " ^ db_of_identifier table_name ^
      " (" ^ db_concat (((db_pk_columns @ db_field_columns) |> List.map join) @
                        ["PRIMARY KEY(" ^ db_concat (List.map fst db_pk_columns) ^ ")"]) ^ ")"
    in
    let table_info = { fields = (pk_columns @ field_columns) |> List.to_seq |> FieldMap.of_seq } in
    { md_command = command;
      md_table_info = table_info;
      md_update_pks = TableMap.add table_name [pk_columns |> List.map fst] }
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
  | CannotCreateTable { reason; value } -> "Cannot create value " ^ db_of_identifier value ^ " because " ^ reason
  | NoPrimaryIndexFound table -> "No primary index found for table " ^ table

let _ = Printexc.register_printer (function
    | Error error -> Some (string_of_error error)
    | _ -> None
  )

let _ = Printexc.register_printer (function
    | Pg.Error error -> Some (Pg.string_of_error error)
    | _ -> None
  )

let update hashtbl default key f =
  match Hashtbl.find_opt hashtbl key with
  | None -> Hashtbl.replace hashtbl key (f default)
  | Some value -> Hashtbl.replace hashtbl key (f value)

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
  let update_missing_columns missing_columns =
    update t.database_info { fields = FieldMap.empty } table_name (fun _table_info ->
        { fields = new_columns }
      );
    missing_columns |> List.iter @@ fun (field_name, field_type) ->
    ignore (db_exec t.db ~expect:[Pg.Command_ok]
              (Printf.sprintf "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s"
                 (db_of_identifier table_name)
                 (db_of_identifier field_name)
                 (db_of_field_type field_type)
              )
           )
  in
  match missing_columns, kind, t.config.tags_column, t.config.fields_column with
  | [], _, _, _ -> ()
  | missing_columns, `Fields, _, Some fields_column ->
    missing_columns
    |> List.filter (fun (field, _) -> field == fields_column)
    |> update_missing_columns
  | missing_columns, `Fields, _, None ->
    update_missing_columns missing_columns
  | missing_columns, `Tags, None, _ ->
    raise (Error (CannotAddTags (List.map fst missing_columns)))
  | _, `Tags, Some _, _ ->
    () (* these are inside a json and will be added dynamically *)

let check_and_update_tables (t : t) (measurement : Influxdb_lexer.measurement) =
  let table_name = measurement.measurement in
  if Hashtbl.mem t.database_info table_name then
    ()
  else
    let make_table () =
      let made_table = make_table_command t measurement in
      ignore (db_exec t.db ~expect:[Pg.Command_ok] made_table.md_command);
      t.indices <- made_table.md_update_pks t.indices;
      Hashtbl.replace t.database_info table_name made_table.md_table_info
    in
    let make_hypertable () =
      let command = "SELECT create_hypertable($1, $2)" in
      let params = [|table_name; t.config.time_column|] in
      ignore (db_exec t.db ~expect:[Pg.Tuples_ok] ~params command)
    in
    match t.config.create_table with
    | Some create_table when Config.matches create_table.regexp table_name -> begin
        match create_table.method_ with
        | Config.CreateTable ->
          make_table ()
        | Config.CreateHyperTable ->
          make_table ();
          make_hypertable ()
      end
    | None -> raise (Error (CannotCreateTable { value = table_name;
                                                reason = "creation of new tables not enabled" }))
    | Some _ -> raise (Error (CannotCreateTable { value = table_name;
                                                  reason = "table does not match the regexp" }))

(** [split_longer_lists n xs] splits xs into a list of one or most lists, where each sublist is at most n elements
    long. *)
let split_longer_lists (limit : int) (xs : 'a list) : 'a list list =
  assert (limit > 0);
  let (_, collect, part) =
    List.fold_left
      (fun (n, collect, part) x ->
         if n < limit
         then (n + 1, collect, x::part)
         else (1, List.rev part::collect, [x])
      )
      (0, [], [])
      xs
  in
  if part = [] then
    List.rev collect
  else
    List.rev (List.rev part::collect)

let group_measurements t measurements =
  (* Consider two measurements groupable if they
     1) affect the same table
     2) have the same tags
     3) have the same fields *)
  let now = Unix.time () in
  let eq (a : Influxdb_lexer.measurement) (b : Influxdb_lexer.measurement) : bool =
    a.measurement = b.measurement
    && List.map fst a.tags = List.map fst b.tags
    && List.map fst a.fields = List.map fst b.fields
  in
  (* But a single insert cannot have multiple rows doing ON CONFLICT DO SET UPDATE..  So we go through the data and
     if a row has the same time (TODO: convert to timestamp here for exact matching?)+tags, we update the previous
     entry and remove the later one.  *)
  let coalesce_updates (measurements : Influxdb_lexer.measurement list) =
    let key_to_index = Hashtbl.create (List.length measurements) in
    let index_to_measurement = Hashtbl.create (List.length measurements) in
    let _ = measurements |> CCList.iteri @@ fun index (measurement : Influxdb_lexer.measurement) ->
      (* time field needs to be updated here so we can detect collisions between
         rows that don't specify timestamp and rows that do, and the timestamp
         happens to match
      *)
      let measurement = Influxdb_lexer.fill_missing_timestamp now measurement in
      Hashtbl.replace index_to_measurement index (ref measurement);
      let precision_divider =
        match t.subsecond_time_field with
        | false -> 1000000000L  (* second precision *)
        (* TODO: PostgreSQL only supports microsecond precision, so we use that precision here as well. *)
        | true -> 1000L         (* microsecond precision *)
      in
      (* always set due to previous call to fill_missing_timestamp *)
      let time_key = Int64.(div (Option.get measurement.time) precision_divider) in
      (* Printf.printf "time_key=%Ld\n%!" time_key; *)
      let key = (time_key, List.sort compare measurement.tags) in
      match Hashtbl.find_opt key_to_index key with
      | None -> Hashtbl.add key_to_index key index;
      | Some meas_idx ->
        (* Printf.printf "Foundsies!\n%!"; *)
        let earlier = Hashtbl.find index_to_measurement meas_idx in
        (* So keys are the same, we need to merge values and remove this entry *)
        earlier := Influxdb_lexer.combine_fields !earlier measurement;
        Hashtbl.remove index_to_measurement index;
    in
    index_to_measurement
    |> Hashtbl.to_seq
    |> List.of_seq
    |> List.sort compare
    |> List.map (fun (_, x) -> !x)
  in
  CCList.group_succ ~eq measurements
  (* split so that no segment results in too many parameters e*)
  |> List.map (fun xs ->
      let meas : Influxdb_lexer.measurement = List.hd xs in
      (* estimate the number of parameters per row *)
      let num_fields = List.length meas.tags + List.length meas.fields + 1 in
      (* and keep the number of total parameters in the query < 30000 *)
      split_longer_lists (30000 / num_fields) xs
    ) |> List.concat
  (* ensure no two rows referring to the same row exist in one batch *)
  |> List.map coalesce_updates
  (* if something resulted in empty segment, remove them. though nothing should.. *)
  |> List.filter ((<>) [])

let write t (measurements: Influxdb_lexer.measurement list) =
  try
    ignore (db_exec t.db ~expect:[Pg.Command_ok] "BEGIN TRANSACTION");
    let grouped = group_measurements t measurements in
    (* let grouped = [measurements] in *)
    List.iter (
      fun measurements ->
        assert(measurements <> []);
        let reference = List.hd measurements in
        Log.debug (fun m ->
            m "Measurements: %s" ([%derive.show: Influxdb_lexer.measurement list] measurements)
          );
        let () = check_and_update_tables t reference in
        let (query, params) = insert_of_measurement ~measurements t reference in
        let field_types = db_fields_and_types reference in
        let tag_types = db_tags_and_types reference in
        let () = check_and_update_columns ~kind:`Tags t reference.measurement tag_types in
        let () = check_and_update_columns ~kind:`Fields t reference.measurement field_types in
        try
          ignore (db_exec t.db ~params ~expect:[Pg.Command_ok] query);
        with Pg.Error error ->
          (try ignore (db_exec t.db ~expect:[Pg.Command_ok] "ROLLBACK");
           with Pg.Error _ -> (* ignore *) ());
          raise (Error (PgError (error, Some query)))
    ) grouped;
    ignore (db_exec t.db ~expect:[Pg.Command_ok] "COMMIT");
  with Pg.Error error ->
    (try ignore (db_exec t.db ~expect:[Pg.Command_ok] "ROLLBACK");
     with Pg.Error _ -> (* ignore *) ());
    raise (Error (PgError (error, None)))
