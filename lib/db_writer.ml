module Pg = Postgresql

type quote_mode = QuoteAlways

type t = {
  db: Pg.connection;
  quote_mode: quote_mode;
  quoted_time_field: string;
  subsecond_time_field: bool;
}

type error =
  | PgError of Pg.error

exception Error of error

module Internal =
struct
  let db_of_identifier str =
    let out = Buffer.create (String.length str) in
    let buf = Sedlexing.Utf8.from_string str in
    Buffer.add_char out '"';
    let rec loop () =
      match%sedlex buf with
      | '"' ->
        Buffer.add_string out "\\\"";
        loop ()
      | Star(Sub(any, '"')) ->
        let lexstr = Sedlexing.Utf8.lexeme buf in
        if String.length lexstr = 0 then (
          Buffer.add_char out '"';
          Buffer.contents out
        ) else (
          Buffer.add_string out str;
          loop ()
        )
      | _ -> assert false
    in
    loop ()

  let db_fields (meas : Lexer.measurement) =
    List.concat [List.map fst meas.tags |> List.map db_of_identifier;
                 List.map fst meas.fields |> List.map db_of_identifier]

  let db_raw_of_value =
    let open Lexer in
    function
    | String x -> x
    | Int x -> Int64.to_string x
    | FloatNum x -> Printf.sprintf "%f" x
    | Boolean true -> "true"
    | Boolean false -> "false"

  let db_raw_values (meas : Lexer.measurement) =
    List.concat [List.map (fun (_, value) -> db_raw_of_value (String value)) meas.tags;
                 List.map (fun (_, field) -> db_raw_of_value field) meas.fields]

  let map_first f els =
    match els with
    | x::els -> f x::els
    | els -> els

  let db_placeholders (meas : Lexer.measurement) =
    let enumerate els =
      List.rev (fst (List.fold_left (fun (xs, n) _ -> ((n::xs), succ n)) ([], 1) els))
    in
    List.concat [
      (match meas.time with
       | None -> []
       | Some _ -> ["time"]);
      List.map fst meas.tags;
      List.map fst meas.fields
    ]
    |> enumerate
    |> List.map (Printf.sprintf "$%d")
    |>
    match meas.time with
    | None -> (fun xs -> "CURRENT_TIMESTAMP"::xs)
    | Some _ -> map_first (fun x -> Printf.sprintf "to_timestamp(%s)" x)

  let insert_of_measurement t (meas : Lexer.measurement) =
    let query =
      "INSERT INTO " ^ db_of_identifier meas.measurement ^
      "(" ^ String.concat ", " (t.quoted_time_field::db_fields meas) ^ ")" ^
      " VALUES (" ^ String.concat "," (db_placeholders meas) ^ ")"
    in
    let params = db_raw_values meas in
    let params =
      match meas.time with
      | None -> params
      | Some x ->
        Printf.sprintf "%s" (
          (* TODO: what about negative values? Check that 'rem' works as expected *)
          if t.subsecond_time_field
          then Printf.sprintf "%Ld.%09Ld" (Int64.div x 1000000000L) (Int64.rem x 1000000000L)
          else Printf.sprintf "%Ld" (Int64.div x 1000000000L)
        )::params
    in
    (query, params |> Array.of_list)
end

open Internal

let create ~conninfo =
  try
    let db = new Pg.connection ~conninfo () in
    let quote_mode = QuoteAlways in
    let quoted_time_field = db_of_identifier "time" in
    let subsecond_time_field = false in
    { db; quote_mode; quoted_time_field; subsecond_time_field }
  with Pg.Error error ->
    raise (Error (PgError error))

let string_of_error error =
  match error with
  | PgError error -> Pg.string_of_error error

let write t (measurements: Lexer.measurement list) =
  try
    ignore (t.db#exec ~expect:[Pg.Command_ok] "BEGIN TRANSACTION");
    (* TODO: group requests by their parameters and use multi-value inserts *)
    List.iter (
      fun measurement -> 
        let (query, params) = insert_of_measurement t measurement in
        ignore (t.db#exec ~params ~expect:[Pg.Command_ok] query);
    ) measurements;
    ignore (t.db#exec ~expect:[Pg.Command_ok] "COMMIT");
  with Pg.Error error ->
    ignore (t.db#exec ~expect:[Pg.Command_ok] "ROLLBACK");
    raise (Error (PgError error))

let close t =
  t.db#finish
