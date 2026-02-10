# Repository Guidelines

## Project Overview
- Underway is a Rust 2021 library for durable background workflows on Postgres.
- Core concepts: tasks, queues, workers, schedulers, and workflows (workflows wrap tasks).
- Public APIs are documented and linted; clippy warnings are treated as errors.

## Project Structure
- src/ contains library modules: workflow, queue, worker, scheduler, task.
- examples/ contains standalone crates with their own Cargo.toml files.
- migrations/ holds SQL migrations executed by `underway::run_migrations`.
- .sqlx/ stores SQLX query metadata for offline builds.
- docker-compose.yaml provides a local Postgres service.

## Build, Lint, Test, Docs
- `cargo build` builds the library crate.
- `cargo build --all-features` matches CI feature coverage.
- `cargo test` runs unit, doc, and integration tests (DB required for sqlx::test).
- `cargo test --lib <test_name>` runs a single unit test by name.
- `cargo test --lib task_execution_success -- --nocapture` (single test example).
- `cargo test --lib sanity_check_run_migrations -- --nocapture` (sqlx::test example).
- `cargo test --doc --all-features` runs doc tests only (CI uses this).
- `cargo clippy --all --all-targets --all-features -- -Dwarnings` (CI).
- `cargo +nightly fmt --all` formats code (nightly rustfmt required).
- `cargo +nightly fmt --all -- --check` checks formatting.
- `RUSTDOCFLAGS="-D rustdoc::broken-intra-doc-links" cargo doc --all-features --no-deps`.
- `cargo run --manifest-path examples/basic/Cargo.toml` runs a sample crate.
- `cargo run --manifest-path examples/scheduled/Cargo.toml` runs another example.

### CI parity checks
- CI installs the nightly toolchain for rustfmt/clippy.
- CI sets `SQLX_OFFLINE=true` for clippy and doc builds.
- CI runs `cargo test --workspace --all-features --lib` for library tests.
- Doc tests are run separately with `cargo test --all-features --doc`.

### Single-test recipes
Use these to filter by name or module path.
```bash
cargo test --lib task::tests::retry_policy_defaults
cargo test --lib task_execution_failure -- --nocapture
cargo test --doc Workflow
```

### Example crates
- `examples/basic` basic workflow usage.
- `examples/scheduled` scheduled workflows.
- `examples/step` step-by-step usage.
- `examples/multitask` multi-task workflows.
- `examples/graceful_shutdown` shutdown flow.
- `examples/tracing` tracing instrumentation.
- `examples/rag` sample integration (see its README).

### Database + SQLX setup
- `docker-compose up -d postgres` starts local Postgres.
- Set `DATABASE_URL=postgres://postgres:postgres@localhost:5432/underway`.
- `cargo install sqlx-cli --no-default-features --features native-tls,postgres`.
- `cargo sqlx database setup` creates the DB and runs migrations.
- `SQLX_OFFLINE=true` allows clippy/docs with cached .sqlx data.
- Update SQLX metadata after query changes: `cargo sqlx prepare`.
- If offline metadata is stale, rerun `cargo sqlx prepare` with DATABASE_URL set.

### Database test quickstart
```bash
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/underway
docker-compose up -d postgres
cargo sqlx database setup
cargo test --lib
```

## Coding Style

### Formatting and Imports
- Follow rustfmt.toml: 4-space indentation, wrap comments, format doc strings.
- rustfmt uses `format_code_in_doc_comments = true` and `format_strings = true`.
- Import granularity is `Crate`; group imports as Std / External / Crate.
- Order imports as std first, external crates second, `crate` last.
- Prefer grouped imports like `use std::{...};` and `use crate::{...};`.
- Avoid wildcard imports except for obvious preludes.

### Naming and API shape
- Use snake_case for functions/vars/modules and CamelCase for types/traits.
- Use SCREAMING_SNAKE_CASE for consts and static values.
- Keep public re-exports in `src/lib.rs` and document them.
- Favor builder patterns (Workflow::builder, Queue::builder) and typestate modules.
- Keep internal helpers `pub(crate)` unless needed in the public API.

### Types and Trait Bounds
- Use Result aliases: `type Result<T = ()> = std::result::Result<T, Error>`.
- Task inputs must be `Serialize + DeserializeOwned + Send + 'static`.
- Task outputs must be `Serialize + Send + 'static`.
- Workflow state requires `Clone + Send + Sync + 'static`.
- Use newtypes (TaskId/WorkflowId) around Uuid and implement Display/Deref.
- Use `Arc` for shared queues/tasks and clone it instead of borrowing.

### Error Handling
- Define module Error enums with `thiserror::Error`.
- Prefer `#[error(transparent)]` for wrapper variants.
- Use `?` to propagate errors and keep signatures clean with Result aliases.
- For task execution, return `task::Error::Retryable` or `task::Error::Fatal`.
- Use `ToTaskResult` to convert external errors into retryable/fatal errors.
- Consider `#[non_exhaustive]` for new public error enums (task::Error uses it).
- Avoid panics in library code; assertions are fine in tests.

### Async, Concurrency, Tracing
- Use tokio for async runtime; prefer `JoinSet` for task groups.
- Use `CancellationToken` for cooperative shutdown.
- Instrument async entry points with `#[instrument]` and meaningful fields.
- Record IDs with `tracing::Span::current().record` for observability.
- Use `jiff::Span` and `ToSpan` helpers for durations (e.g., `1.hour()`).

### SQLX and Database Usage
- Prefer `sqlx::query!`, `query_as!`, and `query_scalar!` macros.
- Use raw string literals (`r#"..."#`) for SQL blocks.
- Cast custom types in queries (TaskId, RetryPolicy, enums) as shown in src.
- Keep `.sqlx/` metadata updated via `cargo sqlx prepare`.
- Tests using `#[sqlx::test]` require a live database.

### Migrations and schema
- Add migrations in `migrations/` and keep them reversible when possible.
- Library users rely on `underway::run_migrations`; update docs if schema changes.
- After editing SQL or migrations, refresh SQLX metadata (`cargo sqlx prepare`).

### Documentation
- Crate enables `#![warn(missing_docs)]`; public items must have `///` docs.
- Use module-level `//!` docs for overviews and examples.
- Mark non-runnable examples with `no_run` or `compile_fail`.
- Keep examples in docs aligned with current APIs and feature flags.

## Testing Guidelines
- Tests live in `mod tests` blocks within source files.
- Use `#[tokio::test]` for async tests and `#[sqlx::test]` for DB tests.
- Keep test names descriptive and snake_case.
- Prefer small, focused tests near the code they exercise.

## Configuration Tips
- Set `DATABASE_URL` for examples/tests that hit Postgres.
- Use `docker-compose.yaml` for local Postgres during development.
- Set `SQLX_OFFLINE=true` for linting/docs without a database.
- The examples also read `DATABASE_URL` from the environment.

## Commit and PR Guidelines
- Commit messages are short, imperative, and lowercase (e.g., `clean up tests a bit`).
- Keep PRs focused; mention migrations or behavior changes explicitly.
- If public APIs or behavior changes, update `CHANGELOG.md` and add tests.

## Cursor/Copilot Rules
- No `.cursor/rules`, `.cursorrules`, or `.github/copilot-instructions.md` found.
