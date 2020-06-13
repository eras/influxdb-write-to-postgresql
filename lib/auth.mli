type t

type config = {
  users : Config.users;
}

val create : config -> t

type context = {
  allowed_users : string list option;
}

type request = {
  user : string option;
  password : string option;
}

type result =
  | AuthSuccess
  | AuthFailed

(** For testing *)
val show_result : result -> string

(** For human-readable message *)
val string_of_result : result -> string

type error =
  | FailedToParseAuthorization
  | FailedToParseAuthorizationToken
  | FailedToParseAuthorizationBasic
  | CryptokitError of Cryptokit.error
  | Argon2Error of Argon2.ErrorCodes.t

(** For testing *)
val show_error :  error -> string

exception Error of error

(** [string_of_error] converts error to a human-readable message *)
val string_of_error : error -> string

(** [permitted_exn] determines if user/password combination is permitted_exn
    in the given context *)
val permitted_exn : t -> context:context -> request:request -> result

(** [permitted_header_exn] picks the required information from the header
    and hands over to [permitted_exn].
*)
val permitted_header_exn : t -> context:context -> header:Cohttp.Header.t -> result
