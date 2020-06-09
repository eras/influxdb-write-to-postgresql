Copyright Erkki Seppälä <erkki.seppala@vincit.fi> 2020

License: MIT

# Influx-Write-To-Postgresql #

IW2PG is a service for receiving inbound [Influxdb line
protocol](https://v2.docs.influxdata.com/v2.0/reference/syntax/line-protocol/)
via simple [HTTP
POST](https://docs.influxdata.com/influxdb/v1.8/guides/write_data/)
and converting it to `INSERT` commands to a PostgreSQL database; and
by extension to a [TimeScaleDB](https://www.timescale.com/).

Basically it listens in some port—8086 by default—and upon receiving a
connection it reads all the data there is and sends the appriopriate
`INSERT .. ON CONFLICT .. DO UPDATE SET ..` on it within a
transaction, using `ALTER TABLE tablename ADD COLUMN ..` to add new
columns as needed (if not using JSONB fields) or even add whole new
tables with `CREATE TABLE` if so configured (so, by default).

## Why? ##

I felt the Influxdb query language was limited but its input facility
excellent. PostgreSQL/TimeScaleDB also allows storing relational data
in the same database which can be quite nice for making queries with
joins.

## Configuration ##

Currently the supported [YAML](https://yaml.org) configuration is
minimal (JSON also supported by the YAML library, not tested
though). Here is an [example](doc/config.minimal.yaml) (note that this
does not provide user authentication):

```
databases:
  somedb:
    db_host: dbserver1
    db_port: 5432
    db_name: test
    db_user: user42
    db_password: passw0rd
```

`somedb` is the label used in endpoint
http://localhost:8086/write?db=somedb and determines which db to send
the data to. Other parameters should be self-explanatory.

## Database setup ##

There are three ways to go about setting tables. Either you can

  * put all the tags and fields in JSONB fields (providing best dynamic behavior and is the default configuration)
  * or you can put all the tags and fields in separate fields of a table
  * or you can mix and match

The default is to also create new tables automatically. You can
control this with the `create_tables` configuration. If you create the
tables manually, then you must provide a `UNIQUE INDEX` covering
`time` column and all the `tags` columns (or that one jsonb
column). The index is used to automatically determine which fields are
used for the tags.

```
CREATE TABLE testtable (
    time timestamptz NOT NULL,
    tags JSONB NOT NULL,          -- all the tags
    fields JSONB NOT NULL,        -- all the fields
    PRIMARY KEY(time, tags)
);
```

Putting tags in a JSONB field (default setting) provides the best
Influxdb emulation, as IW2PG cannot create new tags fields due to the
problem of it requiring recreating the index whenever a new field is
added; however if it is fine to limit the set of fields beforehand,
then that may provide the nicest SQL query experience.

Similarly, when puttings tags in separate fields and creating tables
manually, you must provide a `UNIQUE INDEX` covering `time` and all
the `tags`.

```
CREATE TABLE testtable (
    time timestamptz NOT NULL,
    id STRING NOT NULL,           -- tag
    location STRING NOT NULL,     -- tag
    temperature DOUBLE PRECISION, -- field
    co2 DOUBLE PRECISION,         -- field
    PRIMARY KEY(time, id, location)    -- list time and all the tags
);
```

Even when putting fields as separate fields is IW2PG able to add new
fields by issuing `ALTER TABLE testtable ADD COLUMN ..`, which is fast
because the values are `NULL`able and thus doesn't require rewriting
the table columns in modern PostgreSQL.

### Supported PostgreSQL data types ###

| Data type |
|---|
| integer |
| double precision |
| numeric |
| text, varchar |
| boolean |
| jsonb |

Note that JSONB cannot be used as data per se as the Influxdb line
protocol doesn't support it.

## Building and running ##

You can find the complete compilation instructions from the
Dockerfile. Or you can just use

```
docker build -t iw2pg .
```

to build an image (takes about 8 min on my laptop) based on
[`debian:buster-slim`](https://hub.docker.com/_/debian) and start it
with

```
docker run --rm --name iw2pg -p 8086:8086 -v $PWD/config.json:/app/config.json iw2pg
```

for a first test after creating `config.json`. For real use you
probably want to use `-d --restart always` as well. The container
doesn't (currently) contain any written data so no volumes are
required for persisting data.

Supported command line parameters and their environment variable versions:

| Environment variable | Command-line switch             | Description                                           |
|----------------------|---------------------------------|-------------------------------------------------------|
| IW2PG\_PORT          | -p n --port n                   | Set the TCP port or Unix Domain Socket to listen on.  |
| IW2PG\_CONFIG        | -c file.yaml --config file.yaml | Set the configuration file.                           |

The container also supports the `--help` switch:

```
docker run --rm iw2pg --help
```

## Developing ##

Written in OCaml. Read the Dockerfile for exact dependencies (so the
list of required apt and opam packages); use `dune build` to build.

Run tests with `dune runtest`. It requires docker to run the image
[`timescale/timescaledb:latest-pg11`](https://hub.docker.com/r/timescale/timescaledb/).

## Running tests ##

```
docker pull timescale/timescaledb:latest-pg11
dune runtest
```

You of course need to have docker configured for your user and you
only need to do the pull the first time. It would also get
automatically pulled from inside the test, but due to the default
timeouts and that the operation may take some time, I suggest
downloading it beforehand.

<!-- Local Variables: -->
<!-- tab-width: 8 -->
<!-- End: -->
