val db_writer_src : Logs.src
val iw2pg_src : Logs.src

val set_level : Logs.level option -> unit
val setup_logging : unit -> unit
val stop_logging : unit -> unit

val setup_buffer_logging : (string -> unit) -> unit

val setup_out_channel_logging : out_channel -> unit
