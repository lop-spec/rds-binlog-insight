# Reproducible native binlog decoder candidate

This directory is an unpromoted decoder candidate. It is not wired into the
production image or service.

Source recovery baseline:

- repository branch: `feat/binlog-rows`
- first source commit: `abd2619a0d1471feaed56c42afd0d5a2db557cd9`
- recovered `parser-slim/main.go` SHA-256: `49070c794c1745fec183d678e7d1a43b243ab1e038790806bfa9f59ac30bcaa8`
- recovered `go.mod` SHA-256: `1c712a24ec54844116ad73ed7903c51fd66491ed067bd739633a341f55f73ff7`
- recovered `go.sum` SHA-256: `c39582031d8010dddf869d5eae6ea0f65ef09334d71d7bc2be5045751bc92acf`

The recovered source is a later slim derivative, not a byte-for-byte copy of
the legacy full decoder. The full-output fields omitted by that derivative are
restored here and are checked against frozen legacy output. The exact
`schema_version_id` formula was recovered from the legacy binary symbol
`main.schemaVersionID` and independently confirmed against the frozen ROW
fixture:

```
sha256(lower(database_name) + NUL + lower(table_name) + NUL + columns_json)
```

The event reader intentionally does not use `BinlogParser.ParseReader` or
`ParseSingleEvent`: it reads every 19-byte header and complete body with
`io.ReadFull`, verifies size and position, then calls `BinlogParser.Parse`.
Checksum verification remains enabled in go-mysql. FDE must be first, unknown
event types and duplicate FDE are rejected, and each rows event must reference
an observed TableMap ID. Contract runs additionally require GTID and (for ROW)
TableMap coverage.

Build and test with the digest-pinned builder without publishing an image:

```sh
docker build --file parser/Dockerfile \
  --output type=local,dest=parser/build parser
```

Only the existing CI may produce release images or packages. Passing the
synthetic parser contract proves decoder compatibility for that fixture; it is
not production deployment, throughput, recovery, or 30-day query acceptance.
