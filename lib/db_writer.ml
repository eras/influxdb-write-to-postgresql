module Log = (val Logs.src_log Logging.db_writer_src : Logs.LOG)

module Pg = Postgresql

let db_exec_exn (db: Pg.connection) ?params ?expect query =
  let (time_exn, res) =
    Common.time_exn
      (Common.valuefy (fun () -> db#exec ?expect ?params query))
      ()
  in
  Log.debug (fun m ->
      m "Exec %s with %s took %.1f ms"
        ([%derive.show: string] query)
        (match params with
         | None -> "no parameters"
         | Some params -> [%derive.show: string array] params)
        (Mtime.Span.(time_exn |> to_ms))
    );
  Common.unvaluefy_exn res

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
  time_method : Config.time_method;
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

module Precision : sig
  type t = private Precision of int64
  (* val seconds : t *)
  (* val milliseconds : t *)
  (* val microseconds : t *)
  (* val nanoseconds : t *)
  val of_time_method : Config.time_method -> t
end = struct
  type t = Precision of int64
  let seconds = Precision 1L
  let milliseconds = Precision 1_000L
  let microseconds = Precision 1_000_000L
  let nanoseconds = Precision 1_000_000_000L

  let of_time_method = function
    | Config.TimestampTZ _ -> seconds
    | Config.TimestampTZ3 _ -> milliseconds
    | Config.TimestampTZ6 _ -> microseconds
    | Config.TimestampTZ9 _ -> nanoseconds
    | Config.TimestampTZPlusNanos _ -> nanoseconds
end

type t = {
  mutable db: Pg.connection; (* mutable for reconnecting *)
  quote_mode: quote_mode;
  time_fields: string list;
  time_precision: Precision.t;
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

  let db_of_identifier_exn x =
    try Common.db_of_identifier_exn x
    with Common.Error Common.MalformedUTF8 ->
      raise (Error MalformedUTF8)

  let db_tags_exn (meas : Influxdb_lexer.measurement) =
    List.map fst meas.tags |> List.map db_of_identifier_exn

  let db_fields_exn (meas : Influxdb_lexer.measurement) =
    List.map fst meas.fields |> List.map db_of_identifier_exn

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

  let map_timestamp t els =
    let map_timefield placeholder =
      Printf.sprintf
        (match timestamp_method with
         | TS_StringTimestamp -> "%s"
         | TS_CallTimestamp -> "to_timestamp(%s)") placeholder
    in
    let aux els =
      match els, List.length t.time_fields with
      | ph::els, 1 -> map_timefield ph::els
      | ph1::ph2::els, 2 -> map_timefield ph1::ph2::els
      | els, _ -> els
    in
    aux els

  let make_timestamp t xs =
    (match t.time_fields with
     | [_] -> ["CURRENT_TIMESTAMP"]
     | [_; _] -> ["CURRENT_TIMESTAMP"; "0"]
     | _ -> assert false) @ xs

  let db_names_of_tags_exn t meas=
    match t.config.tags_column with
    | None -> db_tags_exn meas
    | Some name -> [db_of_identifier_exn name]

  let db_names_of_fields_exn t meas =
    match t.config.fields_column with
    | None -> db_fields_exn meas
    | Some name -> [db_of_identifier_exn name]

  let db_insert_value_placeholders_exn ?(next_placeholder=1) t (meas : Influxdb_lexer.measurement) =
    let with_enumerate first els =
      let (result, next) =
        (List.fold_left (
            fun (xs, n) element ->
              (((n, element)::xs), succ n)
          ) ([], first) els)
      in
      (List.rev result, next)
    in
    let time_and_tags =
      let time =
        match meas.time with
        | None -> []
        | Some _ -> t.time_fields
      in
      time @ db_names_of_tags_exn t meas
    in
    (* actual values are ignored, only the number of them matters *)
    List.concat [time_and_tags; db_names_of_fields_exn t meas]
    |> with_enumerate next_placeholder
    |> fun (placeholder, next_placeholder) ->
    placeholder
    |> Common.map_fst (Printf.sprintf "$%d")
    |> List.map fst
    |> fun xs ->
    match meas.time with
    | None -> (make_timestamp t xs, next_placeholder)
    | Some _ -> (map_timestamp t xs, next_placeholder)

  let db_insert_fields_exn t meas =
    let tags =
      match t.config.tags_column with
      | None -> db_tags_exn meas
      | Some tags -> [db_of_identifier_exn tags]
    in
    let fields =
      match t.config.fields_column with
      | None -> db_fields_exn meas
      | Some fields -> [db_of_identifier_exn fields]
    in
    List.concat [List.map db_of_identifier_exn t.time_fields; tags; fields]

  let db_update_set_exn t meas =
    match t.config.fields_column with
    | None ->
      db_concat (List.concat [db_fields_exn meas] |>
                 List.map @@ fun field ->
                 field ^ "=" ^ "excluded." ^ field)
    | Some fields ->
      db_of_identifier_exn fields ^ "=" ^
      db_of_identifier_exn meas.measurement ^ "." ^ db_of_identifier_exn fields ^ "||" ^
      "excluded." ^ db_of_identifier_exn fields

  let conflict_tags_exn t (meas : Influxdb_lexer.measurement) =
    match TableMap.find_opt meas.measurement t.indices with
    | None | Some [] -> raise (Error (NoPrimaryIndexFound meas.measurement))
    | Some (index::_rest) -> index

  let insert_of_measurement_exn ?(measurements:Influxdb_lexer.measurement list option) t (reference : Influxdb_lexer.measurement) =
    let db_values_placeholders ?next_placeholder meas =
      let (placeholders, next) = db_insert_value_placeholders_exn ?next_placeholder t meas in
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
      "INSERT INTO " ^ db_of_identifier_exn reference.measurement ^
      "(" ^ db_concat (db_insert_fields_exn t reference) ^ ")" ^
      "\nVALUES " ^ values_placeholders ^
      "\nON CONFLICT(" ^ db_concat (conflict_tags_exn t reference) ^ ")" ^
      "\nDO UPDATE SET " ^ db_update_set_exn t reference
    in
    let time (meas : Influxdb_lexer.measurement) =
      let fields base nanoseconds =
        match timestamp_method, t.time_fields with
        | TS_StringTimestamp, [_] -> [Printf.sprintf "%s.%09Ld UTC" base nanoseconds]
        | TS_StringTimestamp, [_; _] -> [Printf.sprintf "%s UTC" base;
                                         Printf.sprintf "%Ld" nanoseconds]
        | TS_CallTimestamp, [_] -> [Printf.sprintf "%s.%09Ld" base nanoseconds]
        | TS_CallTimestamp, [_; _] -> [base;
                                       Printf.sprintf "%Ld" nanoseconds]
        | (TS_StringTimestamp | TS_CallTimestamp), _ -> assert false
      in
      match meas.time with
      | None -> []
      | Some epoch_time -> begin
        let seconds = Int64.div epoch_time 1000000000L in
        let nanoseconds = Int64.(rem epoch_time 1000000000L) in
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
          fields date_str nanoseconds
        | TS_CallTimestamp ->
          fields (Int64.to_string seconds) nanoseconds
      end
    in
    let params =
      values
      |> List.map (fun meas -> time meas @ db_insert_tag_values t meas @ db_insert_field_values t meas)
      |> List.concat
    in
    (query, params |> Array.of_list)

  let query_database_info_exn (db: Pg.connection) =
    let result = db_exec_exn db ~expect:[Pg.Tuples_ok] "SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema='public'" in
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
          | `Expression expression -> Sql.string_of_expression ~db_of_identifier_exn expression
        ))
    | Sql.CreateIndex {
        unique = false;
        _;
      } -> None

  let query_indices_exn (db : Pg.connection) =
    let result = db#exec ~expect:[Pg.Tuples_ok] {|SELECT tablename, indexdef FROM pg_indexes WHERE schemaname='public' order by tablename|} in
    result#get_all_lst |> List.map (function
        | [table_name; index_definition] -> (table_name, Sql.parse_sql_exn index_definition)
        | _ -> assert false)
    |> Common.map_snd primary_keys_of_index
    |> CCList.group_succ ~eq:( = )
    |> List.map (fun xs ->
        let tablename = fst (List.hd xs) in
        (tablename, List.filter_map snd xs)
      )
    |> List.to_seq

  let new_pg_connection_exn = function
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

  let make_table_command_exn (t : t) (measurement : Influxdb_lexer.measurement) : made_table =
    let table_name = measurement.measurement in
    let dbify (name, type_) = (db_of_identifier_exn name, db_of_field_type type_) in
    let field_columns =
      (match t.config.fields_column with
       | None -> db_fields_and_types measurement
       | Some field -> [(field, FT_Jsonb)])
    in
    let db_field_columns = field_columns |> List.map dbify in
    let pk_columns =
      (match List.map db_of_identifier_exn t.time_fields with
       | [timestamp] -> [(timestamp, FT_Timestamptz)]
       | [timestamp; nanos] -> [(timestamp, FT_Timestamptz);
                                (nanos, FT_Int)]
       | _ -> assert false) @
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
      (* TODO: not very pretty. perhaps some more general concept about fields would help here. *)
      (match t.time_fields with
       | [_] -> Common.map_rest
       | [_; _] ->
         let map_rest2 map_heads map_rest xs =
           map_heads (List.hd xs)::Common.map_rest map_heads map_rest (List.tl xs)
         in
         map_rest2
       | _ -> failwith "Expected exactly one or two time fields"
      )
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
      "CREATE TABLE " ^ db_of_identifier_exn table_name ^
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
  fun ({ Config.time; tags_jsonb_column; fields_jsonb_column; create_table; _ } as database) ->
  let db_spec = db_spec_of_database database in
  { db_spec;
    time_method = time;
    tags_column = tags_jsonb_column;
    fields_column = fields_jsonb_column;
    create_table; }

let time_columns_of_time_method = function
  | Config.TimestampTZ { time_field }
  | Config.TimestampTZ3 { time_field }
  | Config.TimestampTZ6 { time_field }
  | Config.TimestampTZ9 { time_field } -> [time_field]
  | Config.TimestampTZPlusNanos { time_field; nano_field } -> [time_field; nano_field]

let create_exn (config : config) =
  try
    let db = new_pg_connection_exn config.db_spec in
    let quote_mode = QuoteAlways in
    let time_fields = time_columns_of_time_method config.time_method in
    let time_precision = Precision.of_time_method config.time_method in
    let database_info = query_database_info_exn db in
    let indices = query_indices_exn db |> TableMap.of_seq in
    { db; quote_mode; time_fields;
      time_precision; database_info; indices;
      config }
  with Pg.Error error ->
    raise (Error (PgError (error, None)))

let close t =
  t.db#finish

let reconnect_exn t =
  ( try close t
    with _ -> (* eat *) () );
  t.db <- new_pg_connection_exn t.config.db_spec

let string_of_error error =
  match error with
  | PgError (error, None) -> Pg.string_of_error error
  | PgError (error, Some query) -> Pg.string_of_error error ^ " for " ^ query
  | MalformedUTF8 -> "Malformed UTF8"
  | CannotAddTags tags -> "Cannot add tags " ^ db_concat (List.map (fun x -> try db_of_identifier_exn x with Error MalformedUTF8 -> "*malformed utf8*") tags)
  | CannotCreateTable { reason; value } -> "Cannot create value " ^ (try db_of_identifier_exn value with Error MalformedUTF8 -> "*malformed utf8*") ^ " because " ^ reason
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
let check_and_update_columns_exn ~kind t table_name values =
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
  let update_missing_columns_exn missing_columns =
    update t.database_info { fields = FieldMap.empty } table_name (fun _table_info ->
        { fields = new_columns }
      );
    missing_columns |> List.iter @@ fun (field_name, field_type) ->
    ignore (db_exec_exn t.db ~expect:[Pg.Command_ok]
              (Printf.sprintf "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s"
                 (db_of_identifier_exn table_name)
                 (db_of_identifier_exn field_name)
                 (db_of_field_type field_type)
              )
           )
  in
  match missing_columns, kind, t.config.tags_column, t.config.fields_column with
  | [], _, _, _ -> ()
  | missing_columns, `Fields, _, Some fields_column ->
    missing_columns
    |> List.filter (fun (field, _) -> field == fields_column)
    |> update_missing_columns_exn
  | missing_columns, `Fields, _, None ->
    update_missing_columns_exn missing_columns
  | missing_columns, `Tags, None, _ ->
    raise (Error (CannotAddTags (List.map fst missing_columns)))
  | _, `Tags, Some _, _ ->
    () (* these are inside a json and will be added dynamically *)

let check_and_update_tables_exn (t : t) (measurement : Influxdb_lexer.measurement) =
  let table_name = measurement.measurement in
  if Hashtbl.mem t.database_info table_name then
    ()
  else
    let make_table () =
      let made_table = make_table_command_exn t measurement in
      ignore (db_exec_exn t.db ~expect:[Pg.Command_ok] made_table.md_command);
      t.indices <- made_table.md_update_pks t.indices;
      Hashtbl.replace t.database_info table_name made_table.md_table_info
    in
    let make_hypertable () =
      let command = "SELECT create_hypertable($1, $2)" in
      let params = table_name::t.time_fields |> Array.of_list in
      ignore (db_exec_exn t.db ~expect:[Pg.Tuples_ok] ~params command)
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
      let Precision.Precision precision_divider = t.time_precision in
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

let write_exn t (measurements: Influxdb_lexer.measurement list) =
  try
    ignore (db_exec_exn t.db ~expect:[Pg.Command_ok] "BEGIN TRANSACTION");
    let grouped = group_measurements t measurements in
    (* let grouped = [measurements] in *)
    List.iter (
      fun measurements ->
        assert(measurements <> []);
        let reference = List.hd measurements in
        Log.debug (fun m ->
            m "Measurements: %s" ([%derive.show: Influxdb_lexer.measurement list] measurements)
          );
        let () = check_and_update_tables_exn t reference in
        let (query, params) = insert_of_measurement_exn ~measurements t reference in
        let field_types = db_fields_and_types reference in
        let tag_types = db_tags_and_types reference in
        let () = check_and_update_columns_exn ~kind:`Tags t reference.measurement tag_types in
        let () = check_and_update_columns_exn ~kind:`Fields t reference.measurement field_types in
        try
          ignore (db_exec_exn t.db ~params ~expect:[Pg.Command_ok] query);
        with Pg.Error error ->
          (try ignore (db_exec_exn t.db ~expect:[Pg.Command_ok] "ROLLBACK");
           with Pg.Error _ -> (* ignore *) ());
          raise (Error (PgError (error, Some query)))
    ) grouped;
    ignore (db_exec_exn t.db ~expect:[Pg.Command_ok] "COMMIT");
  with Pg.Error error ->
    (try ignore (db_exec_exn t.db ~expect:[Pg.Command_ok] "ROLLBACK");
     with Pg.Error _ -> (* ignore *) ());
    raise (Error (PgError (error, None)))
