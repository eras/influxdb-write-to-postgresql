open Lwt
module C = Cohttp
open Cohttp_lwt_unix

type prog_config = {
  listen_at: Conduit_lwt_unix.server;
  config_file: string;
}

let handle_request (environment : Requests.request_environment) req body =
  let uri = req |> Cohttp.Request.uri in
  match uri |> Uri.path with
  | "/write" -> Request_write.handle environment req body
  | _ -> return (`Not_found, None, "Not found.")

let make_environment prog_config : Requests.request_environment =
  let config = Config.load prog_config.config_file in
  let auth =
    let { Config.users; _ } = config in
    Auth.create { Auth.users } in
  let databases =
    config.databases |> Common.map_snd Db_writer.db_config_of_database
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
  { auth; config; db_spool; }

let server prog_config =
  let environment = make_environment prog_config in
  let callback _conn req body =
    handle_request environment req body
    >>= fun (status, headers, body) ->
    Server.respond_string ~status ~body () >>= fun (response, body) ->
    let response = match headers with
      | None -> response
      | Some headers -> { response with headers = headers }
    in
    return (response, body)
  in
  (* (`TCP (`Port prog_config.listen_at)) *)
  Server.create ~mode:prog_config.listen_at (Server.make ~callback ())

let iw2pg prog_config = ignore (Lwt_main.run (server prog_config))

let main () =
  let _ = Hashtbl.randomize () in
  let open Cmdliner in
  let open Cmdargs in
  let wrap_to_prog_config listen_at config_file =
    iw2pg { listen_at; config_file }
  in
  let version = Version.version in
  let iw2pg_t = Term.(const wrap_to_prog_config $ listen_at $ config_file) in
  Term.exit @@ Term.eval (iw2pg_t, Term.info ~version "iw2pg")
