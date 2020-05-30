type error =
  | MalformedUTF8

exception Error of error

let is_unquoted_ascii x =
  let i = Uchar.to_int x in
  if i >= 1 && i <= 127 then
    let c = Uchar.to_char x in
    (c >= 'a' && c <= 'z')
    || (c >= '0' && c <= '9')
    || (c == '_')
  else
    false

let is_unescaped_ascii x =
  let i = Uchar.to_int x in
  if i >= 1 && i <= 127 then
    let c = Uchar.to_char x in
    (c >= 'a' && c <= 'z')
    || (c >= 'A' && c <= 'Z')
    || (c >= '0' && c <= '9')
    || (c == '_')
  else
    false

let db_of_identifier str =
  let out = Buffer.create (String.length str) in
  Buffer.add_string out "U&\"";
  let decoder = Uutf.decoder ~encoding:`UTF_8 (`String str) in
  let any_special = ref false in
  let rec loop () =
    match Uutf.decode decoder with
    | `Await -> assert false
    | `Uchar x when x == Uchar.of_char '\\' || x == Uchar.of_char '"' ->
      any_special := true;
      Buffer.add_char out '\\';
      Buffer.add_char out (Uchar.to_char x);
      loop ()
    | `Uchar x when is_unquoted_ascii x ->
      Buffer.add_char out (Uchar.to_char x);
      loop ()
    | `Uchar x when is_unescaped_ascii x ->
      any_special := true;
      Buffer.add_char out (Uchar.to_char x);
      loop ()
    | `Uchar x when Uchar.to_int x < (1 lsl 16) ->
      any_special := true;
      Printf.ksprintf (Buffer.add_string out) "\\%04x" (Uchar.to_int x);
      loop ()
    | `Uchar x when Uchar.to_int x < (1 lsl 24) ->
      any_special := true;
      Printf.ksprintf (Buffer.add_string out) "\\+%06x" (Uchar.to_int x);
      loop ()
    | `Uchar _ | `Malformed _ ->
      any_special := true;
      raise (Error MalformedUTF8)
    | `End when !any_special ->
      Buffer.add_char out '"';
      Buffer.contents out
    | `End ->
      str (* return original identifier as nothing special was done *)
  in
  loop ()

(* gives a string suitable for the VALUES expression of INSERT for the two insert cases: JSON and direct *)
let map_fst f = List.map (fun (k, v) -> (f k, v))
let map_snd f = List.map (fun (k, v) -> (k, f v))

(* Samy but work with any mapping (ie. any container or Seq) *)
let map_fst' map f = map (fun (k, v) -> (f k, v))
let map_snd' map f = map (fun (k, v) -> (k, f v))
