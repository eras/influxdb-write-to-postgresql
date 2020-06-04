open Lwt
open Cohttp
open Cohttp_lwt_unix
open Influxdb_write_to_postgresql

type prog_config = {
  listen_port: int;
  config_file: string;
}

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

let server prog_config =
  let config = Config.load prog_config.config_file in
  let databases =
    config.databases |> Common.map_snd Db_writer.db_config_of_database
  in
  let _users =
    if config.users <> [] then
      failwith "Users not yet supported"
  in
  let _regexp_users =
    if config.regexp_users <> [] then
      failwith "Regexp users not yet supported"
  in
  let _groups =
    if config.groups <> [] then
      failwith "Groups not yet supported"
  in
  let _regexp_databases =
    if config.regexp_databases <> [] then
      failwith "Regexp databases not yet supported"
  in
  let db_spool = Db_spool.create { Db_spool.databases } in
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
  Server.create ~mode:(`TCP (`Port prog_config.listen_port)) (Server.make ~callback ())

let iw2pg prog_config = ignore (Lwt_main.run (server prog_config))

let main () =
  let _ = Hashtbl.randomize () in
  let open Cmdliner in
  let open Cmdargs in
  let wrap_to_prog_config listen_port config_file =
    iw2pg { listen_port; config_file }
  in
  let iw2pg_t = Term.(const wrap_to_prog_config $ port $ config_file) in
  Term.exit @@ Term.eval (iw2pg_t, Term.info "iw2pg")

let _ = main ()
