FROM ocaml/opam2:debian-10 AS builder

RUN sudo apt-get -y install libpq-dev m4
RUN opam update
RUN opam install dune lwt lwt_ppx postgresql cohttp-lwt-unix sedlex postgresql
RUN opam install ounit2
RUN opam install uutf
RUN opam install yojson
RUN opam install -y menhir
RUN opam install containers
RUN sudo apt-get -y install pkg-config
RUN opam install -y yaml decoders-yojson
RUN opam install -y ppx_deriving_yojson
RUN opam install -y cmdliner
RUN opam install -y re
RUN sudo apt-get -y install libgmp-dev libssl-dev zlib1g-dev
RUN opam install -y cryptokit
RUN opam install -y unix-type-representations

COPY dune-project influxdb_write_to_postgresql.* /work/
COPY main /work/main/
COPY lib /work/lib/
COPY test /work/test/

WORKDIR /work

RUN sudo chown -R opam /work

RUN whoami
RUN ls -la /work
RUN eval $(opam env) && dune build --profile release

#FROM alpine:latest
#RUN apk --no-cache add libpq ca-certificates
FROM debian:buster-slim

RUN apt-get update && apt-get install -y libpq5 ca-certificates libgmpxx4ldbl
RUN rm -rf /var/cache/apt /var/lib/apt
WORKDIR /app
COPY --from=builder /work/_build/default/main/iw2pg.exe /app/iw2pg

RUN ls -l /app/iw2pg

ENTRYPOINT ["/app/iw2pg"]
