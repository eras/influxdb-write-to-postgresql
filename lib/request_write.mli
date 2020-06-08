val handle :
  Requests.request_environment ->
  Cohttp_lwt_unix.Request.t ->
  Cohttp_lwt.Body.t ->
  ([> `Internal_server_error | `OK | `Unauthorized ] * Cohttp.Header.t option *
   string)
    Lwt.t
