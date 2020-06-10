open Lwt
module C = Cohttp
open Cohttp_lwt_unix

module Log = (val Logs.src_log Logging.iw2pg_src : Logs.LOG)

type prog_config = {
  listen_at: Conduit_lwt_unix.server;
  config_file: string;
}

let handle_request (environment : Requests.request_environment) req body =
  let uri = req |> Cohttp.Request.uri in
  match req.meth, uri |> Uri.path with
  | `POST, "/write" -> Request_write.handle environment req body
  | _, "/write" -> return (`Method_not_allowed, None, "Method not supported.")
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

let string_of_server = function
  | `TLS _server_tls_config -> failwith "not supported: TLS"
  | `OpenSSL _server_tls_config -> failwith "not supported: OpenSSL"
  | `TLS_native _server_tls_config -> failwith "not supported: TLS_native"
  | `TCP (`Port port) -> Printf.sprintf "port %d" port
  | `TCP (`Socket (fd : Lwt_unix.file_descr)) ->
    Printf.sprintf "fd:%d" (Unix_representations.int_of_file_descr (Lwt_unix.unix_file_descr fd) : int)
  | `Unix_domain_socket (`File path) -> Printf.sprintf "%s" path
  | _ -> failwith "not supported: other kind of socket"

let log_request (req : Request.t) f =
  let t0 = Mtime_clock.elapsed () in
  let response = ref `NotExecuted in
  (* I guess this is a bit complicated way to do it *)
  Lwt.finalize
    (fun () ->
       Lwt.catch (fun () ->
           f () >>= fun ((response', _) as result) ->
           response := `Result response';
           return result
         )
         (function exn ->
            response := `Exn exn;
            Lwt.fail exn)
    )
    (fun () ->
       let t1 = Mtime_clock.elapsed () in
       let response_str =
         match !response with
         | `NotExecuted -> ""
         | `Result (result : Cohttp.Response.t) ->
           Printf.sprintf "HTTP %d" (Cohttp.Code.code_of_status result.status)
         | `Exn exn ->
           Printf.sprintf "%s" (Printexc.to_string exn)
       in
       Log.info (fun m ->
           m "%s %s handled in %.1f ms%s%s"
             (Cohttp.Code.string_of_method req.meth)
             ([%derive.show: string] (req |> Request.uri |> Uri.path_and_query))
             (Mtime.Span.((abs_diff t0 t1) |> to_ms))
             (if String.length response_str > 0 then " " else "")
             response_str
         );
       return ()
    )

let server prog_config =
  let environment = make_environment prog_config in
  let callback _conn req body =
    log_request req (fun () ->
        handle_request environment req body
        >>= fun (status, headers, body) ->
        Server.respond_string ~status ~body () >>= fun (response, body) ->
        let with_header header = Cohttp.Header.add header "X-Iw2pg-Version" Version.version in
        let response = match headers with
          | None -> { response with headers = with_header (Cohttp.Header.init ()) }
          | Some headers -> { response with headers = with_header headers }
        in
        return (response, body)
      )
  in
  (* (`TCP (`Port prog_config.listen_at)) *)
  Log.info (fun m -> m "Starting server at %s" (string_of_server prog_config.listen_at));
  Server.create ~mode:prog_config.listen_at (Server.make ~callback ())

let iw2pg prog_config = ignore (Lwt_main.run (server prog_config))

let main () =
  Logging.setup_logging ();
  let _ = Hashtbl.randomize () in
  let open Cmdliner in
  let open Cmdargs in
  let wrap_to_prog_config listen_at config_file log_level =
    Logs.set_level log_level;
    iw2pg { listen_at; config_file }
  in
  let version = Version.version in
  let log_level_env = Arg.env_var "IW2PG_LOG_LEVEL" in
  let iw2pg_t = Term.(const wrap_to_prog_config $ listen_at $ config_file $ Logs_cli.level ~env:log_level_env ()) in
  Term.exit @@ Term.eval (iw2pg_t, Term.info ~version "iw2pg")
