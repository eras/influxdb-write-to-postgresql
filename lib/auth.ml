type config = {
  users : Config.users;
}

type t = {
  config : config
}

type context = {
  allowed_users : string list option;
}

type request = {
  user : string option;
  password : string option;
  token : string option;
}

let create config =
  { config }

let auth_ok (pw : Config.password) request =
  match pw.type_, request with
  | Plain, { user = _; password = Some password; token = None } ->
    pw.password = password
  | Plain, _ ->
    false

let permitted t ~(context : context) ~(request : request) =
  let user_info = Option.map (fun user -> List.assoc_opt user t.config.users) request.user in
  match context.allowed_users, request, user_info with
  | None, { user = None; password = None; token = None }, _ ->
    (* If no authentication is provided and not configured, permit *)
    true
  | None, _, _ ->
    (* If authentication is provided but not configured, reject *)
    true
  | Some allowed_users,
    ({ user = Some user; _ } as request),
    Some (Some user_info) ->
    (* If authentication is provided and configured, then does it
       match? *)
    List.mem user allowed_users && auth_ok user_info.password request
  | Some _, _, _ ->
    (* Otherwise, reject *)
    false
