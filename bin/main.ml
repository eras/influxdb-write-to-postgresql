open Lwt
open Cohttp
open Cohttp_lwt_unix
module Lexer = Influxdb_write_to_postgresql.Lexer
module Db_writer = Influxdb_write_to_postgresql.Db_writer
module Db_spool = Influxdb_write_to_postgresql.Db_spool

let conninfo_env_name = "IWTP_CONNINFO"

let handle_request body db =
  body |> Cohttp_lwt.Body.to_string >|= fun body ->
  let rec try_write retries =
    try
      let measurements = Lexer.lines (Sedlexing.Utf8.from_string body) in
      Db_writer.write db measurements;
      `Ok measurements
    with
    | Lexer.Error error ->
      `Error ("lexer error: " ^ Lexer.string_of_error error)
    | Db_writer.Error (Db_writer.PgError (Postgresql.Connection_failure message, None)) ->
      if retries > 0 then (
        Db_writer.reconnect db;
        try_write (retries - 1)
      )
      else
        `Error ("db connection error: " ^ message)
    | Db_writer.Error error ->
      `Error ("db error: " ^ Db_writer.string_of_error error)
  in
  let results = try_write 3 in
  match results with
  | `Ok _results -> "OK"
  | `Error error -> error

let server =
  let db_spec =
    try Db_writer.DbConnInfo (Unix.getenv conninfo_env_name)
    with Not_found ->
      Printf.eprintf "Environment variable %s not provided, exiting\n" conninfo_env_name;
      exit 1
  in
  let db_spool = Db_spool.create { Db_spool.databases = [("default", db_spec)] } in
  let callback _conn req body =
    let _uri = req |> Cohttp.Request.uri |> Uri.to_string in
    let query = req |> Cohttp.Request.uri |> Uri.query in
    let _meth = req |> Request.meth |> Code.string_of_method in
    let _headers = req |> Request.headers |> Header.to_string in
    let db_info_or_error =
      match List.assoc "db" query with
      | [] | exception Not_found ->
        `Error "Query is missing database name"
      | [db_name] -> begin
          match Db_spool.db db_spool db_name with
          | None -> `Error "Unable to get database instance"
          | Some db -> `Db db
        end
      | _ ->
        `Error "Query is giving >1 database"
    in
    (match db_info_or_error with
     | `Db db_info ->
       let db = db_info.Db_spool.db in
       Lwt.finalize
         (fun () -> handle_request body db)
         (fun () -> db_info.Db_spool.release (); return ())
     | `Error error -> return error
    )
    >>= (fun body -> Server.respond_string ~status:`OK ~body ())
  in
  Server.create ~mode:(`TCP (`Port 8080)) (Server.make ~callback ())

let () = ignore (Lwt_main.run server)
