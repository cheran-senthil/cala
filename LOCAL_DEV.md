# Local Dev Without Nix

The upstream flow uses Nix (`direnv allow` / `nix develop`, then `make reset-deps`).
This repo was also brought up **without Nix** — plain Docker + the existing
`cargo` toolchain — for machines that don't have Nix installed. Both are equivalent;
the Makefile just orchestrates the steps below through Nix.

## One-time setup

```bash
# Postgres 16 in Docker.
# NOTE: host port 5433 (not 5432) — 5432 may be taken by another service/tunnel.
# Creds match flake.nix: user / password / pg.
docker run -d --name cala-pg \
  -e POSTGRES_USER=user -e POSTGRES_PASSWORD=password -e POSTGRES_DB=pg \
  -p 5433:5432 postgres:16

export DATABASE_URL="postgres://user:password@127.0.0.1:5433/pg?sslmode=disable"
export PG_CON="$DATABASE_URL"   # tests read PG_CON; app/sqlx read DATABASE_URL

# Seed the schema — the equivalent of `nix run .#setup-db-dev`, which is literally
# `cd cala-ledger && sqlx migrate run`. This applies the 3 migrations AND records
# them in _sqlx_migrations (with an advisory lock), which the app's migrator relies
# on. Requires sqlx-cli:  cargo install sqlx-cli --no-default-features --features native-tls,postgres
cd cala-ledger && sqlx migrate run && cd ..
```

## Build & test

```bash
export DATABASE_URL="postgres://user:password@127.0.0.1:5433/pg?sslmode=disable"
export PG_CON="$DATABASE_URL"

SQLX_OFFLINE=true cargo build --locked -p cala-ledger      # offline (uses cala-ledger/.sqlx cache)
SQLX_OFFLINE=true cargo test  --locked -p cala-ledger      # full suite (parallel is fine once seeded)
```

## Verification status

Verified with `cargo test -p cala-ledger` against a Docker Postgres (the recipe
above): **105 passing, 0 failing** on a fresh database, across all 16 test binaries.
That includes the 8 streaming tests in `cala-ledger/tests/ec_rollup_stream.rs`, which
are end-to-end (not mocks): they boot the `job` runtime, `CalaLedger::init` with
`ec_rollup_streaming(true)`, post real transactions, and poll the
eventually-consistent sets to convergence — plus `ec_recalc_race.rs` (the concurrent
poster-vs-fold stress).

Not yet run through the upstream `nix develop` / `cargo nextest run` /
`make check-code` flow (this machine has no Nix; see intro). If you have Nix, that is
the one remaining check — the likely-only difference is crate-version pinning.

## Gotcha (why this matters)

The integration tests build the ledger with `.exec_migrations(false)` — they assume
the schema is **already applied**. Seed it with `sqlx migrate run` (above), NOT by
piping the `.sql` files through `psql`: raw `psql` creates the tables/types but not
the `_sqlx_migrations` bookkeeping, so the app's own migrator re-runs migration 1 and
fails with `type "debitorcredit" already exists` / `relation "jobs" already exists`.
Seeding via the sqlx migrator (which records state + takes an advisory lock) makes
parallel test runs safe.

## Reset the DB

```bash
PGPASSWORD=password docker exec cala-pg psql -U user -d postgres \
  -c "DROP DATABASE IF EXISTS pg WITH (FORCE);" -c 'CREATE DATABASE pg OWNER "user";'
cd cala-ledger && sqlx migrate run && cd ..
```
