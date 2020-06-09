type value =
  | String of string
  | Int of int64
  | FloatNum of float
  | Boolean of bool

let string_of_value = function
  | String x -> x
  | Int x -> Int64.to_string x
  | FloatNum x -> Printf.sprintf "%f" x
  | Boolean true -> "true"
  | Boolean false -> "false"

type measurement = {
  measurement: string;
  tags: (string * string) list;
  fields: (string * value) list;
  time: int64 option;
}

let make_measurement ~measurement ~tags ~fields ~time =
  assert (measurement <> "");
  assert (fields <> []);
  { measurement; tags; fields; time }

let string_of_tag (name, value) = Printf.sprintf "tag %s=%s" name value
let string_of_field (name, value) = Printf.sprintf "field %s=%s" name (string_of_value value)

let string_of_measurement meas =
  meas.measurement ^
  String.concat "" (List.map (fun (tag, value) -> "," ^ tag ^ "=" ^ value) meas.tags) ^
  " " ^
  String.concat "," (List.map (fun (field, value) -> field ^ "=" ^ string_of_value value) meas.fields) ^
  match meas.time with
  | None -> ""
  | Some t -> " " ^ Int64.to_string t

(* <measurement>[,<tag_key>=<tag_value>[,<tag_key>=<tag_value>]] <field_key>=<field_value>[,<field_key>=<field_value>] [<timestamp>] *)

let digit = [%sedlex.regexp? '0'..'9']
let first_letter = [%sedlex.regexp? alphabetic | digit | ("\\", (',' | '"'))]
let other_letter = [%sedlex.regexp? first_letter | '_']

let tag_value = [%sedlex.regexp? (Star (Sub(any, ('\\' | ' ' | '=' | ',')) | ('\\', ('\\' | ' ' | '=' | ','))))]
let identifier = [%sedlex.regexp? first_letter, Star other_letter]
let boolean = [%sedlex.regexp? "true" | "false"]
let string = [%sedlex.regexp? '"', (Star (Sub(any, ('\\' | '"')) | ('\\', ('\\' | '"')))), '"']
let integer = [%sedlex.regexp? Plus digit]
let integer_suffix = [%sedlex.regexp? integer, "i"]
let float = [%sedlex.regexp? Plus digit, Opt (".", Star digit)]
(* let value = [%sedlex.regexp? integer_suffix | string | float | boolean] *)

type error_info = Parse_error

type error = {
  info: error_info;
  message: string;
}

exception Error of error

let log_raise error =
  (* ( match error with
   *   | Error error ->
   *     Printf.fprintf stderr "raising %s\n%!" error.message
   *   | _ -> ()
   * ); *)
  raise error

let string_of_error error =
  error.message

let _ = Printexc.register_printer (function
    | Error error -> Some (string_of_error error)
    | _ -> None
  )

let identifier buf =
  match%sedlex buf with
  | identifier -> (Sedlexing.Utf8.lexeme buf)
  | _ -> log_raise (Error {info = Parse_error; message = "Expected identifier; received <" ^ Sedlexing.Utf8.lexeme buf ^ ">"})

(* let tag_value = [%sedlex.regexp? (Star (Sub(any, ('\\' | ' ' | '=' | ',')) | ('\\', ('\\' | ' ' | '=' | ','))))] *)
let unquote_tag_value buf =
  let out = Buffer.create 20 in
  let rec loop () =
    match%sedlex buf with
    | Star (Sub(any, ('\\' | ' ' | '=' | ','))) ->
      Buffer.add_string out (Sedlexing.Utf8.lexeme buf);
      quoted ()
    | _ -> log_raise (Error {info = Parse_error; message = "Expected tag value; received <" ^ Sedlexing.Utf8.lexeme buf ^ ">"})
  and quoted () =
     match%sedlex buf with
      | '\\', ('\\' | ' ' | '=' | ',') ->
        let str = Sedlexing.Utf8.lexeme buf in
        let letter = String.sub str 1 (String.length str - 1) in
        Buffer.add_string out letter;
        loop ()
      | _ ->
        Buffer.contents out
  in
  loop ()

(* let string = [%sedlex.regexp? '"', (Star (Sub(any, ('\\' | '"')) | ('\\', ('\\' | '"')))), '"'] *)
let unquote_field_value buf =
  let out = Buffer.create 20 in
  let rec loop () =
    match%sedlex buf with
    | Star (Sub(any, ('\\' | '"'))) ->
      Buffer.add_string out (Sedlexing.Utf8.lexeme buf);
      quoted ()
    | _ -> log_raise (Error {info = Parse_error; message = "Expected field value; received <" ^ Sedlexing.Utf8.lexeme buf ^ ">"})
  and quoted () =
     match%sedlex buf with
      | '\\', ('\\' | '"') ->
        let str = Sedlexing.Utf8.lexeme buf in
        let letter = String.sub str 1 (String.length str - 1) in
        Buffer.add_string out letter;
        loop ()
      | _ ->
        Buffer.contents out
  in
  loop ()

let tag_value buf =
  match%sedlex buf with
  | tag_value -> unquote_tag_value (Sedlexing.Utf8.from_string (Sedlexing.Utf8.lexeme buf))
  | _ -> log_raise (Error {info = Parse_error; message = "Expected tag value; received <" ^ Sedlexing.Utf8.lexeme buf ^ ">"})

let equals buf =
  match%sedlex buf with
  | '=' -> (Sedlexing.Utf8.lexeme buf)
  | _ -> log_raise (Error {info = Parse_error; message = "Expected equal sign; received <" ^ Sedlexing.Utf8.lexeme buf ^ ">"})

let string str =
  unquote_field_value (Sedlexing.Utf8.from_string (String.sub str 1 (String.length str - 2)))

let value buf =
  (* let value = [%sedleex.regexp? integer_suffix | string | float | boolean] *)
  match%sedlex buf with
  | integer_suffix ->
    let str = Sedlexing.Utf8.lexeme buf in
    Int (Int64.of_string (String.sub str 0 (String.length str - 1)))
  | string -> String (string (Sedlexing.Utf8.lexeme buf))
  | float -> FloatNum (Scanf.sscanf (Sedlexing.Utf8.lexeme buf) "%f" (fun x -> x))
  | boolean ->
    (match Sedlexing.Utf8.lexeme buf with
     | "true" -> Boolean true
     | "false" -> Boolean false
     | _ -> assert false)
  | _ -> log_raise (Error {info = Parse_error; message = "Expected value; received <" ^ Sedlexing.Utf8.lexeme buf ^ ">"})

let tags buf =
  let rec loop fields =
    match%sedlex buf with
    | "," ->
      let identifier = identifier buf in
      let _equals = equals buf in
      let tag_value = tag_value buf in
      let field = (identifier, tag_value) in
      loop (field::fields)
    | _ -> List.rev fields
  in
  loop []

let fields buf =
  match%sedlex buf with
  | ' ' ->
    let rec loop fields =
      let identifier = identifier buf in
      let _equals = equals buf in
      let value = value buf in
      let field = (identifier, value) in
      (* Printf.fprintf stderr "%s=%s\n%!" identifier (string_of_value value); *)
      let fields = field::fields in
      match%sedlex buf with
      | "," -> loop fields
      | _ -> List.rev fields
    in
    loop []
  | _ -> log_raise (Error {info = Parse_error; message = "Expected fields"})

let time buf =
  match%sedlex buf with
  | " ", integer ->
    let str = Sedlexing.Utf8.lexeme buf in
    Some (Int64.of_string (String.sub str 1 (String.length str - 1)))
  | " " ->
    log_raise (Error {info = Parse_error; message = "Expected time"})
  | _ -> None (* last field, so we may need to encounter newline as well *)

let rest buf =
  let measurement = Sedlexing.Utf8.lexeme buf in
  let tags = tags buf in
  (* Printf.printf "tags: %s\n%!" (String.concat "," (List.map string_of_tag tags)); *)
  let fields = fields buf in
  (* Printf.printf "fields: %s\n%!" (String.concat "," (List.map string_of_field fields)); *)
  let time = time buf in
  { measurement; tags; fields; time }

(* <measurement>[,<tag_key>=<tag_value>[,<tag_key>=<tag_value>]] <field_key>=<field_value>[,<field_key>=<field_value>] [<timestamp>] *)
let line buf =
  (* Printf.fprintf stderr "tidii\n%!"; *)
  (* let buf = Sedlexing.Utf8.from_string str in *)
  match%sedlex buf with
  | identifier ->
    rest buf
  | _ -> log_raise (Error {info = Parse_error; message = "Expected measurement name (table)"})

let lines buf =
  let rec loop aux =
    match%sedlex buf with
    | identifier ->
      let field = rest buf in
      let aux = field::aux in
      ( match%sedlex buf with
        | '\n' -> loop aux
        | eof -> List.rev aux
        | _ -> log_raise (Error {info = Parse_error; message = "A new entry expected; received <" ^ Sedlexing.Utf8.lexeme buf ^ ">"})
      )
    | eof -> List.rev aux
    | _ -> log_raise (Error {info = Parse_error; message = "End of data expected; received <" ^ Sedlexing.Utf8.lexeme buf ^ ">"})
  in
  loop []

