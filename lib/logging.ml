let db_writer_src = Logs.Src.create "iw2pg.db_writer"
let iw2pg_src = Logs.Src.create "iw2pg.iw2pg"
let srcs = [db_writer_src; iw2pg_src]

let reporter ppf =
  let report (src: Logs.src) level ~over k msgf =
    let timestamp =
      let open Unix in
      let now = localtime (time ()) in
      Printf.sprintf "%04d-%02d-%02d %02d:%02d:%02d"
        (now.tm_year + 1900)
        (now.tm_mon + 1)
        now.tm_mday
        now.tm_hour
        now.tm_min
        now.tm_sec
    in
    let with_timestamp h k ppf fmt =
      let k _ = over (); k () in
      Format.kfprintf k ppf
        ("%s %s %a @[" ^^ fmt ^^ "@]@.")
        timestamp
        (Logs.Src.name src)
        Logs.pp_header (level, h)
    in
    msgf @@ fun ?header ?tags:_tags fmt -> with_timestamp header k ppf fmt
  in
  { Logs.report = report }

let setup_logging () =
  Logs.set_reporter (reporter (Format.std_formatter));
  Logs.set_level (Some Info)

let setup_buffer_logging callback =
  let formatter string offset len =
    callback (String.sub string offset len)
  in
  Logs.set_reporter (reporter (Format.make_formatter formatter ignore));
  Logs.set_level (Some Info)

let setup_out_channel_logging out_channel =
  Logs.set_reporter (reporter (Format.formatter_of_out_channel out_channel));
  Logs.set_level (Some Info)

let set_level : Logs.level option -> unit = fun level ->
  srcs |> List.iter @@ fun src ->
  Logs.Src.set_level src level
