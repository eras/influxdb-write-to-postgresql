FROM ocaml/opam2:debian-10 AS builder

RUN sudo apt-get -y install libpq-dev m4 pkg-config libgmp-dev libssl-dev zlib1g-dev libffi-dev libargon2-1
RUN opam update

RUN opam install -y dune lwt lwt_ppx postgresql cohttp-lwt-unix \
    sedlex postgresql ounit2 ounit2-lwt uutf yojson menhir containers \
    yaml decoders-yojson ppx_deriving_yojson cmdliner re cryptokit \
    unix-type-representations argon2 anycache git
RUN opam install -y logs mtime

COPY dune-project influxdb_write_to_postgresql.* /work/
COPY [".git", "/work/.git"] # for version information
WORKDIR /work
RUN sudo git reset --hard
# but copy extra files (if any) so development experience is nicer
COPY main /work/main/
COPY lib /work/lib/
COPY test /work/test/

RUN sudo chown -R opam /work

RUN eval $(opam env) && dune build --profile release && dune install --prefix=/tmp/iw2pg
RUN /tmp/iw2pg/bin/iw2pg --version

#FROM alpine:latest
#RUN apk --no-cache add libpq ca-certificates
FROM debian:buster-slim

RUN apt-get update && apt-get install -y libpq5 ca-certificates libgmpxx4ldbl libargon2-1
RUN rm -rf /var/cache/apt /var/lib/apt
WORKDIR /app
COPY --from=builder /tmp/iw2pg/bin/iw2pg /app/iw2pg

RUN ls -l /app/iw2pg

ENTRYPOINT ["/app/iw2pg"]
