# Changelog

## v0.2.0 - 2026-06-20

### Features

- **http:** add per-token sliding-window rate limiting (25a377c)
- **http:** add request body size limit to prevent OOM DoS (d251bb5)
- stabilize setup state routing contract (29a6540)
- enrich setup cli readiness output (a3f3c91)
- serve setup ui assets from setup mode (e884b80)
- refine setup wizard operations ui (6ad8321)
- implement setup wizard form shell (ddd84ca)
- add setup draft endpoints (a2a82c2)
- add setup machine id derivation endpoint (c63c247)
- normalize setup network interfaces (ab93298)
- persist setup drafts (6d6028d)
- validate setup wizard inputs (427ebb2)
- add setup mode http server (d2e6fa3)

### Bug Fixes

- **http:** add X-Content-Type-Options header and sanitize internal errors (920c9dc)
- **http:** validate Content-Type header on JSON request bodies (fe59039)
- **http:** suppress internal raft state in HTTP error responses (927269c)
- make setup wizard mount and load cleanly (fce9701)

## v0.1.1 - 2026-06-20

### Bug Fixes

- rename package to replicore (aba8d4d)

