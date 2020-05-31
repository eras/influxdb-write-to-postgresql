type config = {
  databases: (string * Db_writer.db_spec) list;
}

module DatabaseNameMap = Map.Make(struct type t = string let compare = compare end)

type db_status = {
  db_spec: Db_writer.db_spec;
  mutable available: Db_writer.t list;
}

type t = {
  config: config;
  mutable dbs: db_status DatabaseNameMap.t;
}

type db_info = {
  db: Db_writer.t;
  release: unit -> unit;
}

type error = Invalid_database_name of string

exception Error of error

let validate_char ch =
  if not (Common.is_unquoted_ascii (Uchar.of_char ch)) then
    raise (Error (Invalid_database_name ""))

let validate_name name =
  try String.iter validate_char name; name
  with Error (Invalid_database_name _) ->
    raise (Error (Invalid_database_name name))

let create config =
  List.iter (fun (name, _) -> ignore (validate_name name)) config.databases;
  let dbs =
    List.to_seq config.databases
    |> Seq.map (fun (name, db_spec) ->
        (name, { db_spec; available = [] })
      )
    |> DatabaseNameMap.of_seq in
  { config; dbs }

let release db_status db () =
  db_status.available <- db::db_status.available

let db t name =
  match DatabaseNameMap.find name t.dbs with
  | exception Not_found -> None
  | db_status ->
    match db_status.available with
    | [] ->
      let config = {
        Db_writer.db_spec = db_status.db_spec;
        time_field = "time";
        tags_column = None;
        fields_column = None;
      } in
      let db = Db_writer.create config in
      let release = release db_status db in
      Some { db; release }
    | db::xs ->
      db_status.available <- xs;
      let release = release db_status db in
      Some { db; release }
