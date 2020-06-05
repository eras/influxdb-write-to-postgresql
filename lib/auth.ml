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

type result =
  | AuthSuccess
  | AuthFailed

let create config =
  { config }

let auth_ok (_context : context) (pw : Config.password) request =
  match pw.type_, request with
  | Plain, { user = _; password = Some password; token = None; } ->
    if pw.password = password
    then AuthSuccess
    else AuthFailed
  | Plain, _ ->
    AuthFailed

let permitted t ~(context : context) ~(request : request) =
  let user_info = Option.map (fun user -> List.assoc_opt user t.config.users) request.user in
  match context.allowed_users, request, user_info with
  | None, { user = None; password = None; token = None; }, _ ->
    (* If no authentication is provided and not configured, permit *)
    AuthSuccess
  | None, _, _ ->
    (* If authentication is provided but not configured, reject *)
    AuthSuccess
  | Some allowed_users,
    ({ user = Some user; _ } as request),
    Some (Some user_info) when List.mem user allowed_users ->
    (* If authentication is provided and configured, then does it
       match? *)
    auth_ok context user_info.password request
  | Some _, _, _ ->
    (* Otherwise, reject *)
    AuthFailed
