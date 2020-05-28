type conninfo = string

type config = {
  databases: (string * conninfo) list;
}

module DatabaseNameMap = Map.Make(struct type t = string let compare = compare end)

type t = {
  config: config;
  mutable dbs: conninfo DatabaseNameMap.t;
}

type db_info = {
  db: Db_writer.t;
  release: unit -> unit;
}

type error = Invalid_database_name of string

exception Error of error

let validate_char ch =
  if not (Db_writer.is_unquoted_ascii (Uchar.of_char ch)) then
    raise (Error (Invalid_database_name ""))

let validate_name name =
  try String.iter validate_char name; name
  with Error (Invalid_database_name _) ->
    raise (Error (Invalid_database_name name))

let create config =
  List.iter (fun (name, _) -> ignore (validate_name name)) config.databases;
  let dbs = List.to_seq config.databases |> DatabaseNameMap.of_seq in
  { config; dbs }

let db t name =
  match DatabaseNameMap.find name t.dbs with
  | exception Not_found -> None
  | conninfo ->
    let config = {
      Db_writer.conninfo;
      time_field = "time"
    } in
    let db = Db_writer.create config in
    let release = ignore in
    Some { db; release }
