# Surgeist Root Task Integration Handoff

This plan is for the root `surgeist` coordinator to copy into the root repo
after the `surgeist-task` consolidation work is committed and fetchable.

## Preconditions

- Confirm the `surgeist-task` commit selected for root integration is committed,
  pushed, and fetchable from the configured submodule remote before updating the
  root submodule pointer.
- Keep `surgeist-task` as the owner of task semantics. Root integration should
  delete or replace duplicate definitions instead of adding conversion layers
  that preserve parallel task models.
- Root owns API artifact refresh. Do not copy `api/generator` into
  `surgeist-task`; run the root-owned source-derived API generator or artifact
  workflow after the task pointer and facade wiring are updated.

## Integration Tasks

1. Replace root `src/app/task.rs` semantics with imports from `surgeist_task`.
2. Replace root `src/app/executor.rs` duplicate executor contracts with imports
   or thin app-specific adapters over `surgeist_task`.
3. Delete or replace root `src/app/runtime_tokio.rs`. Tokio-backed task execution
   now belongs in `surgeist-task`.
4. Replace root `src/app/coord.rs` task-owned semantics with `surgeist_task`
   types, including:
   - `CoalescingKey`;
   - `TaskEvent`;
   - `TaskEventKind::Progress`;
   - `TaskProgressEvent`;
   - `TaskProgressValue`;
   - task progress coalescing;
   - task subscription/observer counts;
   - unobserved task policy decisions.
5. Keep only root-specific coordination in `src/app/coord.rs`: app subscription
   targets, surface/resource/service mapping, and narrow `AppScope` to
   `TaskScope` adapters where root app state requires app-specific scope data.
6. Keep root `src/app/runtime.rs`, `src/app/proxy.rs`, `src/app/effect.rs`, and
   `src/app/provenance.rs` as app integration code, but make them consume
   `surgeist_task` task events, queue reports, handles, and executor requests.
7. Update `src/app/mod.rs` module declarations and reexports to remove the root
   feature-gated Tokio executor and avoid duplicate task type definitions.
8. Remove root's direct `tokio` dependency if root no longer owns any
   Tokio-specific implementation after consuming `surgeist-task`.
9. Remove root `app-runtime-tokio` feature if no other root-owned code uses it.
10. Update root app tests so they assert root integration behavior, not task
    crate internals.
11. Run the root-owned source-derived API generator or API artifact workflow for
    `surgeist-task` and the facade after the task pointer is updated. Do not copy
    `api/generator` into the task crate.

## Duplicate Definition Check

After root integration edits, run:

```sh
rg -n "struct CoalescingKey|struct ProgressEvent|enum TaskStatus|struct TaskRecord|enum TaskPriority|struct TaskPolicy|enum UnobservedPolicy|struct CancellationToken|enum CancelReason|struct CancelReason|enum BlockingPolicy|enum ExecutorEvent|struct TaskId|struct TaskAttemptId|struct TaskName|struct TaskKey|struct CorrelationId|struct TaskProvenance|enum TaskEvent|struct TaskEvent|enum TaskEventKind|struct TaskProgressEvent|enum TaskProgressValue|enum TaskJobEvent|enum TaskLifecycleEvent|trait RuntimeExecutor|struct SpawnRequest|struct TokioExecutor|struct TaskEventQueue|struct QueuePolicy|struct TaskCoordination|struct TaskRegistration|struct TaskHandle|struct ExecutorTaskHandle|app-runtime-tokio" src/app Cargo.toml
```

Expected result: no root-owned duplicate definitions remain. Any matches should
point to imports from `surgeist_task` or root-only adapter names with different
semantics.

## Root Verification

Run these before committing the root pointer update:

```sh
cargo test -p surgeist-task
cargo test -p surgeist app::tests
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
git diff --submodule=log
```

## Handoff Notes

- `surgeist-task` owns task ids, attempts, keys, names, scopes, policy,
  lifecycle, cancellation, task events, progress snapshots, event queues,
  observation/coordination contracts, executor contracts, fake executor, and
  Tokio-backed task systems.
- Root app owns reducers, effects, UI/window/root/resource/service descriptors,
  native wake bridge, retained bridge, app-specific provenance, and mapping task
  events into app inputs.
- API artifact refresh is intentionally pending for root. Root should refresh
  artifacts only after it pins the fetchable `surgeist-task` commit and updates
  the facade.
