open Cmdliner

let port =
  let doc = "Listen to the given port" in
  let env = Arg.env_var "IW2PG_PORT" ~doc in
  Arg.(value & opt int 8086 & info ["p"; "port"] ~env ~docv:"PORT" ~doc)

let config_file =
  let doc = "Read mapping/database configuration from this yaml file; " ^
            "look at the provided config.example.yaml for more information." in
  let env = Arg.env_var "IW2PG_CONFIG" ~doc in
  Arg.(value & opt string "config.yaml" & info ["c"; "config"] ~env ~docv:"CONFIG" ~doc)
