FROM ocaml/opam2:debian-10 AS builder

RUN sudo apt-get -y install libpq-dev m4
RUN opam update
RUN opam install dune lwt lwt_ppx postgresql cohttp-lwt-unix sedlex postgresql
RUN opam install ounit2
RUN opam install uutf

COPY dune-project influxdb_write_to_postgresql.* /work/
COPY bin /work/bin/
COPY lib /work/lib/
COPY test /work/test/

WORKDIR /work

RUN sudo chown -R opam /work

RUN whoami
RUN ls -la /work
RUN eval $(opam env) && dune build

#FROM alpine:latest
#RUN apk --no-cache add libpq ca-certificates
FROM debian:buster-slim

RUN apt-get update && apt-get install -y libpq5 ca-certificates
RUN rm -rf /var/cache/apt /var/lib/apt
WORKDIR /app
COPY --from=builder /work/_build/default/bin/main.exe /app/influxdb-write-to-postgresql

RUN ls -l /app/influxdb-write-to-postgresql

ENTRYPOINT ["/app/influxdb-write-to-postgresql"]
