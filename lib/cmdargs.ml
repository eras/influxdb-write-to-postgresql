open Cmdliner

let listen_at =
  let doc = "Listen to the given port or unix domain socket (unix domain socket must start with /)" in
  let env = Arg.env_var "IW2PG_PORT" ~doc in
  let converter : Conduit_lwt_unix.server Arg.converter =
    let parser : string -> (Conduit_lwt_unix.server, [`Msg of string]) result =
      fun arg ->
        let buf = Sedlexing.Utf8.from_string arg in
        match%sedlex buf with
        | Plus ('0'..'9') ->
            let port = int_of_string (Sedlexing.Utf8.lexeme buf) in
            if port >= 1 && port <= 65535 then
              Ok (`TCP (`Port port))
            else
              Error (`Msg "Listening port must be in 1..65535")
        | '/', Plus(any) ->
          Ok (`Unix_domain_socket (`File (Sedlexing.Utf8.lexeme buf)))
        | "fd:", Plus('0'..'9') ->
          let arg = Sedlexing.Utf8.lexeme buf in
          Ok (`Unix_domain_socket (`File (String.sub arg 3 (String.length arg - 3))))
        | _ ->
          Error (`Msg "Listen must start with a number of a /")
    in
    let printer : Conduit_lwt_unix.server Arg.printer =
      fun fmt -> function
        | `TLS _server_tls_config -> failwith "not supported: TLS"
        | `OpenSSL _server_tls_config -> failwith "not supported: OpenSSL"
        | `TLS_native _server_tls_config -> failwith "not supported: TLS_native"
        | `TCP (`Port port) -> Format.fprintf fmt "%d" port
        | `TCP (`Socket (fd : Lwt_unix.file_descr)) ->
          Format.fprintf fmt "fd:%d" (Unix_representations.int_of_file_descr (Lwt_unix.unix_file_descr fd) : int)
        | `Unix_domain_socket (`File path) -> Format.fprintf fmt "%s" path
        | _ -> failwith "not supported: other kind of socket"
    in
    Arg.conv (parser, printer)
  in
  Arg.(value & opt converter (`TCP (`Port 8086)) & info ["p"; "listen"] ~env ~docv:"PORT" ~doc)

let config_file =
  let doc = "Read mapping/database configuration from this yaml file; " ^
            "look at the provided config.example.yaml for more information." in
  let env = Arg.env_var "IW2PG_CONFIG" ~doc in
  Arg.(value & opt string "config.yaml" & info ["c"; "config"] ~env ~docv:"CONFIG" ~doc)
