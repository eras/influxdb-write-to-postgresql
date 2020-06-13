type config = {
  databases: (string * Db_writer.config) list;
}

module DatabaseNameMap = Map.Make(struct type t = string let compare = compare end)

type db_status = {
  db_config: Db_writer.config;
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

let validate_exn cond =
  if not cond then
    raise (Error (Invalid_database_name ""))

let validate_char_exn ~first ch =
  validate_exn (Common.is_unquoted_ascii ~first (Uchar.of_char ch))

let validate_name_exn name =
  try
    validate_exn (String.length name > 0);
    validate_char_exn ~first:false name.[0];
    String.iter (validate_char_exn ~first:false) name;
    name
  with Error (Invalid_database_name _) ->
    raise (Error (Invalid_database_name name))

let create_exn config =
  List.iter (fun (name, _) -> ignore (validate_name_exn name)) config.databases;
  let dbs =
    List.to_seq config.databases
    |> Seq.map (fun (name, db_config) ->
        (name, { db_config; available = [] })
      )
    |> DatabaseNameMap.of_seq in
  { config; dbs }

let release db_status db () =
  db_status.available <- db::db_status.available

let db_exn t name =
  match DatabaseNameMap.find name t.dbs with
  | exception Not_found -> None
  | db_status ->
    match db_status.available with
    | [] ->
      let db = Db_writer.create_exn db_status.db_config in
      let release = release db_status db in
      Some { db; release }
    | db::xs ->
      db_status.available <- xs;
      let release = release db_status db in
      Some { db; release }
