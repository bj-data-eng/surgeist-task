# surgeist-task

Task scheduling and work-plane contracts for Surgeist.

This crate owns the task subsystem boundary for Surgeist: typed task
definitions, execution policy, cancellation truth, progress reporting,
resource-class admission, and scheduler-facing contracts.

Keep app authoring APIs focused on Surgeist tasks and task context. Executor
details such as Tokio, thread pools, process workers, or connector runtimes
belong behind this crate's task subsystem contracts.

## API Artifact

The committed API coordination artifact lives at `api/public-api.txt`, but the
generator is owned by the root `surgeist` repo.

Refresh this crate's artifact from the root repo with:

```sh
cargo run --manifest-path api/generator/Cargo.toml -- --crate surgeist-task
```

API refresh tooling is command-only and must not run as part of normal
`cargo test`.
