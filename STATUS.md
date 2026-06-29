# Status

**As of: 2026-06-29 — pre-first-real-commit (everything untracked)**

Branch: `main` (remote: `origin/main`)
Commit in repo: `7d54084` — only `.gitignore` + `LICENSE` committed; all source is untracked.

## What Is Implemented

Rust port of `sz_rabbit_consumer-v4` (Python). Multi-file `src/` layout:

- `src/main.rs` — CLI entry point (clap, tokio runtime, graceful shutdown)
- `src/config.rs` — Configuration via clap with env fallbacks matching the Python sibling
- `src/record.rs` — Record parsing and dead-letter classification
- `src/consumer.rs` — AMQP consumer loop (lapin/tokio), std::thread engine worker pool
- `src/lib.rs` — Library crate root; exposes consumer, config, record modules

Cargo bin name: `sz_rabbit_consumer`

## Key Design Points

- AMQP I/O on lapin + tokio; Senzing `add_record` calls on a pool of `std::thread` workers
  (one engine handle per OS thread, per SDK contract)
- Dead-letter handling for `SzBadInputError`, `SzRetryTimeoutExceededError`, `SENZ0082`
- Fatal shutdown on other engine errors
- Periodic `get_stats()` and long-record monitoring / rejection
- Graceful shutdown on SIGINT/SIGTERM

## CI

- `.github/workflows/ci.yml`: lint (fmt+clippy), build (release), integration (Postgres), coverage (tarpaulin)
- `.github/workflows/security.yml`: daily cargo-audit + cargo-deny
- `.github/dependabot.yml`: weekly updates for cargo, github-actions, docker
- Integration tests run against real Senzing engine + Postgres in CI; they self-skip locally
  when `SENZING_ENGINE_CONFIGURATION_JSON` is unset (documented loud skip)

## Known Issues Requiring Action Before Merge

1. **GitHub Actions SHA-pinning**: all `uses:` lines in `.github/workflows/` are tag-only
   (e.g. `actions/checkout@v4`, `dtolnay/rust-toolchain@stable`) — must be SHA-pinned
   with `# vX.Y.Z` comment per project supply-chain policy. See NEXT_STEPS.md.
2. **Dependabot cooldown**: `dependabot.yml` lacks `cooldown.default-days >= 21` entry.
3. **Cargo.lock untracked**: present on disk, not gitignored — will be committed in the
   first real commit.

## Checks Run This Session

- `cargo fmt -- --check` ✅ rc=0
- `cargo clippy --all-targets --all-features -- -D warnings` ✅ rc=0
- `cargo test` ✅ rc=0 (13 unit + 4 integration; integration ran against local Senzing engine)
- `cargo deny check` ✅ rc=0 (warnings only: duplicate transitive deps, unused allow entries)
- `cargo audit` ✅ rc=0 (0 vulnerabilities)
