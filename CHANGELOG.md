# Changelog

## v0.5.1 - 2026-06-21

### Bug Fixes

- machine identity setting resilience (1a9e256)
- docker entrypoint and confs (29ce3d3)
- simplify auth token and docker compose (b7a34cf)
- really remove caddy from docker compose (8d6f8eb)
- simplify env and remove caddy from docker (8602d1d)

## v0.5.0 - 2026-06-21

### Features

- log values as base58, remove sensitive fields, use consistent naming (01364cb)
- add cluster role radio to setup UI (e6acb9d)
- accept base58 alongside hex for CLUSTER_SECRET and all secret fields (c9d2840)

### Bug Fixes

- switch Docker to Debian-slim and add volume permission fix (17ebdfa)
- use Caddy reverse-proxy CLI instead of Caddyfile mount (b46deea)

## v0.4.0 - 2026-06-21

### Features

- add Caddy reverse proxy and docker-compose.yml (12c42aa)
- add Dockerfile (multi-stage, node:lts-alpine) and docker-entrypoint (c0cf892)

## v0.3.0 - 2026-06-21

### Features

- **docs:** add OpenAPI 3.1 spec and Scalar API Reference UI (fbe6139)
- **setup:** full setup UI hardening (83cd69e)
- log rate limited HTTP requests safely (c99fb7f)
- add IP-based HTTP rate limiting (a3ab07c)
- validate HTTP admin input (4d3a5b1)
- validate HTTP CRUD input (ad1cca6)
- add HTTP validation helpers (7c48356)

### Bug Fixes

- **test:** increase durability timeout to 60s for degraded topology test (f7dbb7c)
- **test:** use quorum.reachableVoters instead of heartbeat aliveness for degraded topology test (676944e)
- disable heartbeat-based durability in ack-delay timeout test (414ec6e)
- **test:** wait for quorum before first write in degraded topology test (a0edaa2)
- fail fast when RPC extension is missing (70c9e7b)
- sanitize unexpected HTTP server errors (7061118)
- return 400 for malformed public JSON (d4955b9)
- stabilize excluded perturbation durability paths (d4fd7ea)

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

