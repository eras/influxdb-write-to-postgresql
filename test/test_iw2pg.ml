open Influxdb_write_to_postgresql
open OUnit2
open Lwt

let take_db_info db_spec =
  match Lazy.force db_spec with
  | Db_writer.DbInfo info -> info
  | _ -> assert false

let make_config ?(time=Config.TimestampTZ { time_field = "time" }) db_spec ~influxdb_name ~authentication =
  let time_json = match time with
    | Config.TimestampTZ { time_field } -> `O [("seconds", `O [("time_field", `String time_field)])]
    | Config.TimestampTZ3 { time_field } -> `O [("milliseconds", `O [("time_field", `String time_field)])]
    | Config.TimestampTZ6 { time_field } -> `O [("microseconds", `O [("time_field", `String time_field)])]
    | Config.TimestampTZ9 { time_field } -> `O [("nanoseconds", `O [("time_field", `String time_field)])]
    | Config.TimestampTZPlusNanos { time_field; nano_field } -> `O [("tz+nanoseconds", `O [("time_field", `String time_field);
                                                                                           ("nano_field", `String nano_field)])]
  in
  let db_spec = take_db_info db_spec in
  `O ["users", `O ["testuser",
                   `O ["password", `O ["type", `String "plain";
                                       "password", `String "userpassword"]]];
      "databases", `O [influxdb_name,
                       `O ["db_host", `String db_spec.db_host;
                           "db_port", `Float (db_spec.db_port |> float_of_int);
                           "db_user", `String db_spec.db_user;
                           "db_password", `String db_spec.db_password;
                           "db_name", `String db_spec.db_name;
                           "time", time_json;
                           (* "create_table", `O ["regexp", `String {|/^.+$/|};
                            *                     "method", `A [`String "CreateTable"]]; *)
                           (* "time_column", `String "time"; *)
                           (* "tags_jsonb_column", `String "tags"; *)
                           (* "fields_jsonb_column", `String "fields"; *)
                           "allowed_users", (
                             match authentication with
                             | false -> `Null
                             | true -> `A [`String "testuser"]
                           )
                          ]]
     ]

let check_content db_spec query check =
  let db_spec = take_db_info db_spec in
  let db = new Postgresql.connection ~host:db_spec.db_host ~port:(string_of_int db_spec.db_port) ~dbname:db_spec.db_name ~user:db_spec.db_user ~password:db_spec.db_password () in
  let result = db#exec ~expect:[Postgresql.Tuples_ok] query in
  check (result#get_all_lst);
  db#finish

let flip f a b = f b a

let output_yaml_config channel yaml =
  match Yaml.to_string yaml with
  | Ok str -> output_string channel str
  | Error (`Msg message) -> failwith message

exception Timeout of string

let _ = Printexc.register_printer (function
    | Timeout msg -> Some ("Timeout: " ^ msg)
    | _ -> None
  )

let post ~body ~headers ~uri =
  let open Cohttp in
  let open Cohttp_lwt_unix in
  let post =
    Lwt.catch (fun () ->
        Client.post ~body:(Cohttp_lwt.Body.of_string body) ~headers uri >>= fun (resp, body) ->
        let code = resp |> Response.status |> Code.code_of_status in
        body |> Cohttp_lwt.Body.to_string >>= fun body ->
        return (Ok (code, body))
      ) (fun exn -> return (Error exn))
  in
  Lwt.pick [post;
            Lwt_unix.sleep 5.0 >>= fun () -> Lwt.fail (Timeout "post")]

let expect ~expect_code = function
  | Error exn ->
    Printf.ksprintf assert_failure "Failed to connect: %s" (Printexc.to_string exn)
  | Ok (code, body) ->
    assert_equal ~msg:("Body: " ^ body) ~printer:string_of_int expect_code code;
    Lwt.return ()

let string_of_signal =
  let signal_to_string_map =
    let open Sys in
    [sigabrt   , "SIGABRT";
     sigalrm   , "SIGALRM";
     sigfpe    , "SIGFPE";
     sighup    , "SIGHUP";
     sigill    , "SIGILL";
     sigint    , "SIGINT";
     sigkill   , "SIGKILL";
     sigpipe   , "SIGPIPE";
     sigquit   , "SIGQUIT";
     sigsegv   , "SIGSEGV";
     sigterm   , "SIGTERM";
     sigusr1   , "SIGUSR1";
     sigusr2   , "SIGUSR2";
     sigchld   , "SIGCHLD";
     sigcont   , "SIGCONT";
     sigstop   , "SIGSTOP";
     sigtstp   , "SIGTSTP";
     sigttin   , "SIGTTIN";
     sigttou   , "SIGTTOU";
     sigvtalrm , "SIGVTALRM";
     sigprof   , "SIGPROF";
     sigbus    , "SIGBUS";
     sigpoll   , "SIGPOLL";
     sigsys    , "SIGSYS";
     sigtrap   , "SIGTRAP";
     sigurg    , "SIGURG";
     sigxcpu   , "SIGXCPU";
     sigxfsz   , "SIGXFSZ"]
  in
  fun signal ->
    List.assoc_opt signal signal_to_string_map |> Option.value ~default:("(OCaml numbering) " ^ string_of_int signal)

let waitpid_printer (pid, status) =
  Printf.sprintf "Pid %d exited  %s" pid
    (match status with
     | Unix.WEXITED i -> "with normal exit status " ^ string_of_int i
     | Unix.WSIGNALED i -> "killed by signal " ^ string_of_signal i
     | Unix.WSTOPPED i -> "stopped by signal " ^ string_of_signal i)

let terminate pid =
  Unix.kill pid Sys.sigterm;
  assert_equal
    ~msg:"Process wait failed" ~printer:waitpid_printer
    (pid, Unix.WSIGNALED Sys.sigterm) (Unix.waitpid [] pid);
  return ()

let wait_head ~uri =
  let open Cohttp_lwt_unix in
  let counter = ref 0 in
  let rec retry retries_left =
    incr counter;
    Lwt.catch (fun () ->
        Lwt_unix.with_timeout 1.0 (fun () -> Client.head uri) >>= fun _resp ->
        return (Ok ())
      ) (fun exn -> return (Error exn)) >>= function
    | Ok () -> return ()
    | Error Lwt_unix.Timeout
    | Error (Unix.Unix_error(Unix.ECONNREFUSED, "connect", _)) when retries_left > 0 ->
      Lwt_unix.sleep 0.1 >>= fun () ->
      retry (retries_left - 1)
    | Error exn -> Lwt.fail exn
  in
  Lwt.pick [retry 50;
            Lwt_unix.sleep 5.0 >>= fun () -> Lwt.fail (Timeout ("wait_head " ^ string_of_int !counter))]

type log_target =
  | LT_Stdout
  | LT_Out_channel of out_channel

let with_iw2pg ?log_target prog_config f =
  let pid = Lwt_unix.fork () in
  let log_setup, log_parent_op, log_child_post_op =
    match log_target with
    | None ->
      ignore, ignore, ignore
    | Some LT_Stdout ->
      (fun () ->
         Logging.setup_logging ();
         Logging.set_level (Some Logs.Debug)),
      ignore, ignore
    | Some LT_Out_channel channel ->
      (fun () ->
         Logging.setup_out_channel_logging channel;
         Logging.set_level (Some Logs.Debug)),
      (fun () -> close_out channel),
      (fun () -> close_out channel)
  in
  if pid = 0 then begin
    (* DB is available due to the call with with_new_db *)
    log_setup ();
    Iw2pg.iw2pg_exn prog_config;
    log_child_post_op ();
    exit 0
  end else begin
    log_parent_op ();
    Lwt.finalize
      f
      (fun () ->
         terminate pid
      )
  end

type iw2pg_context = {
  db_spec: Db_writer.db_spec lazy_t;
  listen_at: Conduit_lwt_unix.server;
}

let setup ~make_config ctx f =
  Test_utils.with_new_db
    ~container_info:Test_utils.lwt_container_info
    ~db_spec_format:Test_utils.DbSpecInfo ctx None @@ fun { db_spec; _; } ->
  let (config_file, config_channel) = bracket_tmpfile ctx in
  let config = make_config db_spec in
  output_yaml_config config_channel config;
  close_out config_channel;
  let listen_socket = Unix.(socket PF_INET SOCK_STREAM 0) in
  Unix.(bind listen_socket (ADDR_INET (inet_addr_loopback, 0)));
  Unix.listen listen_socket 5;
  let listen_port =
    Unix.(
      match getsockname listen_socket with
      | ADDR_INET (_, port) -> port
      | _ -> assert false
    )
  in
  let listen_socket = Lwt_unix.of_unix_file_descr listen_socket in
  let listen_at = `TCP (`Socket listen_socket) in
  let prog_config =
    {
      Iw2pg.listen_at;
      config_file
    } in
  let listen_at = `TCP (`Port listen_port) in
  (* CCIO.(with_in config_file @@ fun file -> Printf.eprintf "%s\n%!" (read_all file)); *)
  let (log_file, log_channel) = bracket_tmpfile ctx in
  (* hack: open the log file now, so bracket_tmpfile can delete it even in subproces.. *)
  let log_file_in = open_in log_file in
  Lwt.finalize (fun () ->
      Lwt.catch
        (fun () ->
           with_iw2pg ~log_target:(LT_Out_channel log_channel) prog_config @@ fun () ->
           (* close this the log channel of this process *)
           close_out log_channel;
           (* close this the socket of this process *)
           Lwt_unix.close listen_socket >>= fun () ->
           f { db_spec; listen_at }
        )
        (fun exn ->
           logf ctx `Info "IW2PG log file contents begins:";
           CCIO.(read_lines_gen log_file_in
                 |> Test_utils.seq_of_gen
                 |> Seq.iter @@ fun line ->
                 logf ctx `Info "%s" line;
                );
           logf ctx `Info "IW2PG log file contents ends.";
           raise exn)
    ) (fun () ->
      close_in log_file_in;
      return ()
    )

let uri_at listen_at path =
  let port = match listen_at with
    | `TCP (`Port port) -> port
    | _ -> failwith "uri_at: Unsupported socket scheme"
  in
  Uri.of_string (Printf.sprintf "http://localhost:%d/%s" port path)

let send ?(path="write") _ctx ~body ~iw2pg_context:{ listen_at; _ } ~influxdb_name =
  let headers = Cohttp.Header.init () in
  let test_uri = uri_at listen_at "" in
  wait_head ~uri:test_uri >>= fun () ->
  let uri = uri_at listen_at (Printf.sprintf "%s?db=%s" path influxdb_name) in
  post ~uri ~headers ~body

let testSendOneRow ctx =
  let influxdb_name = "influxdb_test" in
  let make_config db_spec = make_config db_spec ~influxdb_name ~authentication:false in
  setup ctx ~make_config @@ fun ({ db_spec; _ } as iw2pg_context) ->
  let body = {|meas,tag1=tag_value_1 field1="field_value_1" 1591514002000000000|} in
  send ctx ~body ~iw2pg_context ~influxdb_name >>= expect ~expect_code:204 >>= fun () ->
  check_content db_spec
    ("SELECT EXTRACT(EPOCH FROM time), tags->>'tag1', fields->>'field1' FROM meas")
    (fun results ->
       let printer = [%derive.show: string list list] in
       assert_equal ~printer ~msg:"Database contents don't match"
         [["1591514002"; "tag_value_1"; "field_value_1"]] results
    );
  return ()

let testSendTwoRows ctx =
  let influxdb_name = "influxdb_test" in
  let make_config db_spec = make_config db_spec ~influxdb_name ~authentication:false in
  setup ctx ~make_config @@ fun ({ db_spec; _ } as iw2pg_context) ->
  let body = {|meas,tag1=tag_value_1 field1="field_value_1" 1591514002000000000
meas,tag1=tag_value_1 field1="field_value_1" 1591514003000000000|} in
  send ctx ~body ~iw2pg_context ~influxdb_name >>= expect ~expect_code:204 >>= fun () ->
  check_content db_spec
    ("SELECT EXTRACT(EPOCH FROM time), tags->>'tag1', fields->>'field1' FROM meas")
    (fun results ->
       let printer = [%derive.show: string list list] in
       let sort = List.sort compare in
       assert_equal ~printer ~msg:"Database contents don't match"
         (sort [["1591514002"; "tag_value_1"; "field_value_1"];
                ["1591514003"; "tag_value_1"; "field_value_1"]])
         (sort results)
    );
  return ()

let testFailingAuth ctx =
  let influxdb_name = "influxdb_test" in
  let make_config db_spec = make_config db_spec ~influxdb_name ~authentication:true in
  setup ctx ~make_config @@ fun ({ db_spec; _ } as iw2pg_context) ->
  let body = {|meas,tag1=tag_value_1 field1="field_value_1" 1591514002000000000|} in
  send ctx ~body ~iw2pg_context ~influxdb_name >>= expect ~expect_code:401 >>= fun () ->
  check_content db_spec
    ("SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name='meas'")
    (fun results ->
       let printer = [%derive.show: string list list] in
       assert_equal ~printer ~msg:"Expected no data in database" [["0"]] results
    );
  return ()

let testIncorrectPath ctx =
  let influxdb_name = "influxdb_test" in
  let make_config db_spec = make_config db_spec ~influxdb_name ~authentication:true in
  setup ctx ~make_config @@ fun ({ db_spec; _ } as iw2pg_context) ->
  let body = {|meas,tag1=tag_value_1 field1="field_value_1" 1591514002000000000|} in
  send ~path:"wrong" ctx ~body ~iw2pg_context ~influxdb_name >>= expect ~expect_code:404 >>= fun () ->
  check_content db_spec
    ("SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name='meas'")
    (fun results ->
       let printer = [%derive.show: string list list] in
       assert_equal ~printer ~msg:"Expected no data in database" [["0"]] results
    );
  return ()

let suite = "Iw2pg" >::: [
    "testSendOneRow" >:: OUnitLwt.lwt_wrapper testSendOneRow;
    "testSendTwoRows" >:: OUnitLwt.lwt_wrapper testSendTwoRows;
    "testFailingAuth" >:: OUnitLwt.lwt_wrapper testFailingAuth;
    "testIncorrectPath" >:: OUnitLwt.lwt_wrapper testIncorrectPath;
  ]
