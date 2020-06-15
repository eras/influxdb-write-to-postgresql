let valuefy f_exn x =
  try `Value (f_exn x)
  with exn -> `Exn (exn, Printexc.get_raw_backtrace ())

let unvaluefy_exn = function
  | `Value x -> x
  | `Exn (e, raw_backtrace) ->
    Printexc.raise_with_backtrace e raw_backtrace

let time_exn f x =
  let t0 = Mtime_clock.elapsed () in
  let res = valuefy f x in
  let t1 = Mtime_clock.elapsed () in
  (Mtime.Span.(abs_diff t0 t1), unvaluefy_exn res)

let is_unquoted_ascii ~first x =
  let i = Uchar.to_int x in
  if i >= 1 && i <= 127 then
    let c = Uchar.to_char x in
    (c >= 'a' && c <= 'z')
    || (c == '_')
    || (not first &&
        (c >= '0' && c <= '9'))
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

(* gives a string suitable for the VALUES expression of INSERT for the two insert cases: JSON and direct *)
let map_fst f = List.map (fun (k, v) -> (f k, v))
let map_snd f = List.map (fun (k, v) -> (k, f v))

(* Map the first and the other elements with separate functions *)
let map_rest first_f rest_f xs =
  match xs with
  | [] -> []
  | x::xs -> first_f x::List.map rest_f xs

(* Samy but work with any mapping (ie. any container or Seq) *)
let map_fst' map f = map (fun (k, v) -> (f k, v))
let map_snd' map f = map (fun (k, v) -> (k, f v))

let rec yaml_of_yojson : _ -> Yaml.value = function
  | `Null -> `Null
  | `Bool x -> `Bool x
  | `Float x -> `Float x
  | `String x -> `String x
  | `Assoc kv -> `O (map_snd yaml_of_yojson kv)
  | `List xs -> `A (List.map yaml_of_yojson xs)
  | `Int x -> `Float (float_of_int x)
  | `Intlit x -> `Float (float_of_string x)
  | `Tuple _ -> failwith "Tuple->Yaml conversion not supported"
  | `Variant _ -> failwith "Variant->Yaml conversion not supported"

