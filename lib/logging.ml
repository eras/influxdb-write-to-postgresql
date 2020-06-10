let db_writer_src = Logs.Src.create "iw2pg.db_writer"
let iw2pg_src = Logs.Src.create "iw2pg.iw2pg"

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
