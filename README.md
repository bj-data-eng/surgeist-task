# surgeist-task

Task scheduling and work-plane contracts for Surgeist.

This crate owns the task subsystem boundary for Surgeist. It defines typed task
identity, scope, policy, lifecycle, cancellation, progress, queueing,
coordination, executor contracts, test fakes, and the Tokio-backed executor used
behind those contracts.

Root `surgeist` owns app reducers, effects, UI/window integration, retained tree
bridges, native wake bridges, app resource/service descriptors, and mapping task
events into app inputs. Keep root-specific concepts out of this crate, and keep
duplicate task semantics out of root.

## Task Lifecycle

Tasks are represented by semantic ids, keys, scopes, policies, attempts, and a
`TaskRecord`. A task may move through these non-terminal states:

- `Queued`: accepted for execution but not yet running.
- `Running`: currently executing an active attempt.
- `Waiting`: active but waiting on external work.
- `Blocked`: active but unable to make progress until a dependency changes.
- `Cancelling`: cancellation has been requested and the active attempt is still
  settling.

Terminal states are:

- `Completed`: the active attempt returned successfully before cancellation
  took effect.
- `Failed`: the active attempt ended with a task failure.
- `Cancelled`: the active attempt cooperatively observed cancellation and ended
  as cancelled.
- `FinishedAfterCancel`: cancellation was requested, but the work completed
  successfully before it could stop.
- `FailedToCancel`: cancellation was requested, but the work failed before it
  could stop.

Attempt ids are part of lifecycle truth. Records reject stale attempts, do not
accept non-advancing attempt ids, and keep terminal states terminal.

## Queue And Coalescing

`TaskEventQueue` is bounded by `QueuePolicy`. Non-progress events are retained in
FIFO order until drained. Progress events use explicit `CoalescingKey` values and
coalesce by task id, attempt id, and key, so rapid progress updates replace older
updates in the same slot instead of flooding the queue.

`DrainBudget` controls how many events a drain may return. `QueueReport` and
`DrainReport` expose pending, dropped, and coalesced event counts so app/runtime
integration can surface backpressure without depending on queue internals.

## Cancellation Truth

`CancellationToken` records that cancellation was requested and preserves the
first `CancelReason`. Task code observes cancellation through `TaskContext` and
the shared cancellation view. Executors own terminal lifecycle emission after a
runnable returns or panics.

Cancellation is cooperative for normal async work and truthful for blocking
work. A cancellation request does not claim that non-abortable blocking work has
stopped. Blocking work remains active until its runnable returns and the executor
emits exactly one terminal lifecycle event: cancelled, finished-after-cancel, or
failed-to-cancel depending on the observed outcome.

## Runtime Foundation

Tokio is the task runtime foundation for the concrete executor in this crate.
`TokioTaskExecutor` uses Tokio for async scheduling, timers, join handling, and
blocking-pool execution, but Tokio handles and channels stay behind Surgeist task
contracts. App and task authors should depend on `TaskExecutor`,
`TaskSpawnRequest`, `TaskContext`, `TaskEventSink`, and task event types rather
than Tokio-specific runtime details.

## API Artifacts

The committed API coordination artifact lives at `api/public-api.txt`. The root
Surgeist repository owns source-derived API artifact refresh. Do not copy or
provision `api/generator` in this crate, and do not hand-edit generated API
output.

When root integration needs to refresh this crate's artifact, run the
root-owned generator workflow from the root repo after it updates the
`surgeist-task` pointer and facade wiring. The current root command shape is:

```sh
cargo run --manifest-path api/generator/Cargo.toml -- --crate surgeist-task
```

## Baseline Checks

Run these before handing off crate-local task subsystem work:

```sh
cargo test -p surgeist-task
cargo clippy -p surgeist-task --all-targets -- -D warnings
cargo fmt --check
```
