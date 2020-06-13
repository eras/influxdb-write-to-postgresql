open Lwt
module C = Cohttp
open Cohttp_lwt_unix

let handle_write_request_exn body db =
  body |> Cohttp_lwt.Body.to_string >|= fun body ->
  let rec try_write_exn retries =
    try
      let measurements = Influxdb_lexer.lines_exn (Sedlexing.Utf8.from_string body) in
      Db_writer.write_exn db measurements;
      `Ok measurements
    with
    | Influxdb_lexer.Error error ->
      `Error ("lexer error: " ^ Influxdb_lexer.string_of_error error)
    | Db_writer.Error (Db_writer.PgError (Postgresql.Connection_failure message, None)) ->
      if retries > 0 then (
        Db_writer.reconnect_exn db; (* TODO: handle exception here *)
        try_write_exn (retries - 1)
      )
      else
        `Error ("db connection error: " ^ message)
    | Db_writer.Error error ->
      `Error ("db error: " ^ Db_writer.string_of_error error)
  in
  let results = try_write_exn 3 in
  match results with
  | `Ok _results -> (`No_content, None, "")
  | `Error error -> (`Internal_server_error, None, error)

let handle_write_setup_exn { Requests.auth; config; db_spool } req =
  let query = req |> Cohttp.Request.uri |> Uri.query in
  let _meth = req |> Request.meth |> C.Code.string_of_method in
  (* the evaluation is postponed as this check can just all be here *)
  let db_config, db_info_or_error =
    match List.assoc "db" query with
    | [] | exception Not_found ->
      Error "Missing database argument.", fun () -> `Error "Query is missing database name"
    | [db_name] -> begin
        List.assoc_opt db_name config.databases |> Option.to_result ~none:"No such database.",
        fun () ->
          match Db_spool.db_exn db_spool db_name with
          | Some db  -> `Db (db, config)
          | _ -> `Error "Unable to get database instance"
      end
    | _ ->
      Error "Too many such dbs", fun () -> `Error "Query is giving >1 database"
  in
  let db_info_or_error =
    let authorized =
      let header : C.Header.t = req |> Request.headers in
      match db_config with
      | Error error ->
        Error error
      | Ok db_config ->
        let context = {
          Auth.allowed_users = db_config.allowed_users;
        } in
        (* this also handles the case of allowed_users = None *)
        Ok (Auth.permitted_header_exn auth ~context ~header)
    in
    match authorized with
    | Ok Auth.AuthSuccess -> db_info_or_error
    | Ok _ -> fun () -> `AuthError ""
    | Error db_error -> fun () -> `AuthError db_error
  in
  db_info_or_error

let handle (environment : Requests.request_environment) req body =
  let db_info_or_error = handle_write_setup_exn environment req in
  (match db_info_or_error () with
   | `Db (db_info, _) ->
     let db = db_info.Db_spool.db in
     Lwt.finalize
       (fun () -> handle_write_request_exn body db)
       (fun () -> db_info.Db_spool.release (); return ())
   | `Error error -> return (`OK, None, error)
   | `AuthError error ->
     let header = Cohttp.Header.init () in
     let header = Cohttp.Header.add header "WWW-Authenticate" ("Basic realm=\"" ^ environment.config.realm ^"\"") in
     return (`Unauthorized, Some header, "Unauthorized. " ^ error)
  )
