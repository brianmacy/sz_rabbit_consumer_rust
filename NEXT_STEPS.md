# Next Steps

## Immediate (before first real commit + push)

1. **SHA-pin all GitHub Actions** — every `uses:` in `.github/workflows/ci.yml` and
   `.github/workflows/security.yml` must be replaced with a full 40-char SHA and a
   `# vX.Y.Z` tag comment. Tag-only pins are a Criterion 1 violation. Actions to fix:

   ci.yml:
   - `actions/checkout@v4` (4 occurrences)
   - `dtolnay/rust-toolchain@stable` (3 occurrences)
   - `actions/cache@v4` (4 occurrences)
   - `codecov/codecov-action@v4` (1 occurrence)
   - `docker/setup-buildx-action@v3` (1 occurrence)

   security.yml:
   - `actions/checkout@v4` (2 occurrences)
   - `dtolnay/rust-toolchain@stable` (1 occurrence)

2. **Add Dependabot cooldown** — add `cooldown: { default-days: 21 }` (or per-ecosystem
   overrides >= 21) to `.github/dependabot.yml`.

3. **Stage and commit all source files** — the initial commit only contains `.gitignore`
   and `LICENSE`. Stage `src/`, `Cargo.toml`, `Cargo.lock`, `tests/`, `Dockerfile`,
   `.dockerignore`, `.github/`, `deny.toml`, `CHANGELOG.md`, `README.md`, `STATUS.md`,
   `NEXT_STEPS.md` and push to `origin/main`.

## Near-term

4. **Open PR** — once committed, open a pull request to trigger CI (fmt, clippy, build,
   unit tests). Integration tests require the Postgres service, so they run only in the
   `integration` and `coverage` CI jobs.

5. **MSSQL Dockerfile re-enable** — the Docker matrix currently only builds the `postgres`
   variant because the Microsoft apt repo key is SHA1-signed (expired Feb 2026). Track the
   upstream fix and re-add `mssql` / `both` matrix entries when the key is re-signed.

6. **prettier formatting** — `npx prettier --check "**/*.md"` flags `CHANGELOG.md` and
   `README.md` (informational; no prettier config in the repo, so not a hard gate).

## Ongoing

7. Add regression tests for any new bugs discovered during integration testing.
8. Monitor Dependabot PRs; review and merge within the 21-day cooldown window.
