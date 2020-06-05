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
  token : string option;
}

(** [permitted] determines if user/password combination is permitted
    in the given context *)
val permitted : t -> context:context -> request:request -> bool
