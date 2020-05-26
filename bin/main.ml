open Lwt
open Cohttp
open Cohttp_lwt_unix
module Lexer = Influxdb_write_to_postgresql.Lexer
module Db_writer = Influxdb_write_to_postgresql.Db_writer

let conninfo_env_name = "IWTP_CONNINFO"

let server =
  let conninfo =
    try Unix.getenv conninfo_env_name
    with Not_found ->
      Printf.eprintf "Environment variable %s not provided, exiting\n" conninfo_env_name;
      exit 1
  in
  let db = Db_writer.create { Db_writer.conninfo } in
  let callback _conn req body =
    let uri = req |> Request.uri |> Uri.to_string in
    let meth = req |> Request.meth |> Code.string_of_method in
    let headers = req |> Request.headers |> Header.to_string in
    body |> Cohttp_lwt.Body.to_string >|= (fun body ->
        let rec try_write retries =
          try
            let measurements = Lexer.lines (Sedlexing.Utf8.from_string body) in
            Db_writer.write db measurements;
            `Ok measurements
          with
          | Lexer.Error error ->
            `Error ("lexer error: " ^ Lexer.string_of_error error)
          | Db_writer.Error (Db_writer.PgError (Postgresql.Connection_failure message)) ->
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
        (Printf.sprintf "Uri: %s\nMethod: %s\nHeaders\nHeaders: %s\nBody: %s\nMeasurements: %s"
           uri meth headers body
           (match results with
            | `Ok results ->
              String.concat "\n" (List.map Lexer.string_of_measurement results)
            | `Error error -> error)))
    >>= (fun body -> Server.respond_string ~status:`OK ~body ())
  in
  Server.create ~mode:(`TCP (`Port 8080)) (Server.make ~callback ())

let () = ignore (Lwt_main.run server)
