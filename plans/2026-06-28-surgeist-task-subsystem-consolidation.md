# Surgeist Task Subsystem Consolidation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move Surgeist task identity, lifecycle, progress, cancellation, queue, executor, and scheduler-facing contracts into `surgeist-task` so root `surgeist` and the future DSL consume one shared task subsystem instead of reinventing task behavior.

**Architecture:** `surgeist-task` owns the work-plane model. Tasks are separate from the UI plane: they never mutate UI/app state directly, but they emit typed events that root app/runtime code can drain into reducers, status bindings, progress bars, cancel controls, diagnostics, and future DSL-generated subscriptions. Tokio is the crate's primary task runtime foundation because Surgeist should not reinvent task scheduling, async IO, cancellation coordination, timers, or blocking-pool management; Tokio types still stay out of the public authoring API.

**Tech Stack:** Rust 2024, `surgeist-task`, `tokio`, crate-local unit tests, fake executor tests, root integration follow-up, `guidance/surgeist-rust-modeling-guide.md`.

---

## Review Gate

Each numbered task below is a scoped implementation unit. Before starting a
numbered task, assign one worker to that task or a tightly coupled subset of
that task. Before committing the task or moving to the next numbered task,
assign a separate reviewer to inspect that task's changes against this plan,
the crate boundary, and the modeling guide. Reconcile reviewer findings before
the task commit. After all numbered tasks are complete, run the final holistic
review checklist.

After this plan is implemented in `surgeist-task`, request a clean reviewer before root integration begins. The reviewer must check:

- `surgeist-task` owns task semantics without depending on root `surgeist`, window, retained, render, layout, or app runtime types.
- Root app integration can consume `surgeist_task::*` without keeping duplicate task models.
- Tokio is used internally by the task systems, but Tokio runtime handles, join handles, and channels are not exposed through Surgeist authoring APIs.
- Cancellation, stale attempts, progress coalescing, bounded queues, and non-abortable blocking work are test-covered.
- Public APIs use semantic types, private fields, explicit constructors, and narrow conversion boundaries per `guidance/surgeist-rust-modeling-guide.md`.

## Boundary Rules

- Task crate owns: task ids, attempts, keys, names, scopes, policy, lifecycle, cancellation, task events, progress snapshots, event queues, observation/coordination contracts, executor contracts, fake executor, and Tokio-backed task systems.
- Root app owns: app reducers, app effects, UI/window/root/resource/service descriptors, native wake bridge, retained bridge, and mapping task events into app inputs.
- Root integration must delete or replace duplicate root task/executor/coordination types after `surgeist-task` is green; do not keep parallel definitions with the same semantics.
- `surgeist-task` must not depend on root `surgeist`, `surgeist-window`, or host UI crates.

## File Structure

Create these focused modules:

- `src/id.rs`: task ids, attempt ids, names, keys, correlation ids, observer ids, resource class ids, and coalescing keys.
- `src/scope.rs`: UI-agnostic task scopes as path segments.
- `src/provenance.rs`: task event provenance and attempt/correlation metadata.
- `src/policy.rs`: task priority, unobserved policy, retry policy, and blocking policy.
- `src/cancel.rs`: cancellation token and cancellation reason.
- `src/lifecycle.rs`: task status, terminal status, attempts, task record, task snapshot.
- `src/event.rs`: task events, progress events, completion/failure/cancel events, typed event envelope.
- `src/queue.rs`: bounded task event queue, coalesced progress slots, drain budget, drain report.
- `src/coordination.rs`: task subscriptions, observers, observer policy, observation state.
- `src/executor.rs`: backend-neutral task executor trait, spawn request, task context, fake executor.
- `src/runtime_tokio.rs`: Tokio-backed executor/runtime implementation behind Surgeist task contracts.
- `src/testing.rs`: crate-local fakes and test helpers.
- `src/lib.rs`: public module declarations and intentional reexports.
- `Cargo.toml`: Tokio dependency for task runtime systems.
- `README.md`: task subsystem boundary and baseline commands.

## API Shape Contract

This plan should produce an ergonomic first Rust API, not only low-level
building blocks. Do not postpone API usability until a final polish pass. The
initial implementation should already make common task authoring hard to misuse
while keeping the future template/Smarty DSL, ETL graph DSL, macro DSL, and app
authoring syntax out of this crate.

The first API shape is not final forever. After initial consolidation and root
integration, run additional focused cycles against real call sites. This section
defines the implementation target for this plan so workers do not invent a
different task API in each layer.

Registration should let callers describe the task's identity, scope, and policy
without exposing scheduler internals:

```rust
let registration = TaskRegistration::<ImportInput>::try_new("import.orders")?
    .key(|input| {
        TaskKey::try_new(format!("import:{}", input.path))
            .map_err(TaskRegistrationError::invalid_key)
    })
    .scope(|input| {
        TaskScope::try_workspace(&input.workspace)
            .map_err(TaskRegistrationError::invalid_scope)
    })
    .with_policy(TaskPolicy::continue_when_unobserved().dedupe_by_key());
```

Spawning should support an explicit low-level constructor for integration code
and a builder for ordinary Rust call sites. Both forms must require an explicit
event sink; production APIs must not silently install a recording/test sink.
The builder's normal `sink(...)` method should accept a concrete type
implementing `TaskEventSink<Output>` so examples do not need to name
`Arc<dyn ...>` internals. Provide `sink_arc(...)` for root/integration code that
already owns a shared erased sink.

```rust
let rows = CoalescingKey::try_new("rows")?;

let request = TaskSpawnRequest::builder(task_id, attempt_id)
    .key(key)
    .scope(scope)
    .cancellation(token)
    .sink(sink)
    .input(input)
    .blocking_fn(move |input, ctx| {
        ctx.progress_units(rows.clone(), 10, Some(100))?;
        ctx.output(output_from(input))?;
        Ok(())
    })
    .build()?;
```

The lower-level constructor should remain available for root integration and
tests that need exact control:

```rust
let request = TaskSpawnRequest::new(
    task_id,
    attempt_id,
    key,
    scope,
    cancellation,
    sink,
    input,
    runnable,
);
```

`TaskContext` must include convenience helpers for the common event path:

- `progress_units(key, completed, total)`;
- `progress_percent(key, percent)`;
- `progress_message(key, message)`;
- `output(output)`;
- `is_cancelled()`.

Those context helpers should wrap typed `TaskJobEvent` constructors such as
`TaskJobEvent::progress_units(...)`, `TaskJobEvent::progress_percent(...)`,
`TaskJobEvent::progress_message(...)`, and `TaskJobEvent::output(...)`. The event
model remains explicit and typed; the helpers only remove repetitive call-site
plumbing.

Executors, not job code, own terminal lifecycle emission. A job returns `Ok(())`
for success, `Err(TaskFailure)` for failure or cooperative cancellation, and
observes cancellation through the shared token. The executor emits exactly one terminal lifecycle event for the
attempt after the runnable returns or panics. `TaskContext` must not expose
`completed()` or `failed(...)` helpers, and it must not expose a generic
`emit(TaskEvent<Output>)` method that accepts lifecycle events. Job code may only
emit non-terminal job events through `TaskContext` helpers or
`emit_job_event(TaskJobEvent<Output>)`.

`TaskRunnable` should include closure helpers such as `blocking_fn(...)` and
`async_fn(...)` where object safety allows it, plus explicit boxed-job
constructors `blocking_job(...)` and `async_job(...)` for test doubles and
advanced integrations. `RecordingTaskEventSink` and `TaskContext::for_test(...)`
belong in crate test support, not production-facing examples or normal public
API. Examples should implement the public `TaskEventSink` trait directly with a
small local sink.

Common examples must not require callers to name Tokio types, raw
`Arc<dyn ...>` internals, supervisor structures, root app concepts, windows, or
retained tree handles. Tokio is the runtime foundation for the concrete executor
in this crate, but it remains hidden behind Surgeist task contracts.

## Task 1: Semantic IDs, Scopes, And Provenance

**Files:**
- Create: `src/id.rs`
- Create: `src/scope.rs`
- Create: `src/provenance.rs`
- Modify: `src/lib.rs`
- Test: `src/id.rs`, `src/scope.rs`, `src/provenance.rs`

- [ ] **Step 1: Write ID, scope, and provenance tests**

Add module-local tests covering:

```rust
#[test]
fn ids_preserve_domain_meaning() {
    assert_eq!(TaskId::from_u64(7).as_u64(), 7);
    assert_eq!(TaskAttemptId::from_u64(3).as_u64(), 3);
    assert_eq!(TaskName::try_new("import").unwrap().as_str(), "import");
    assert_eq!(TaskKey::try_new("import:orders").unwrap().as_str(), "import:orders");
    assert_eq!(CorrelationId::from_u64(99).as_u64(), 99);
}

#[test]
fn string_ids_reject_empty_or_whitespace_values() {
    assert_eq!(TaskName::try_new("").unwrap_err().kind(), TaskIdErrorKind::Empty);
    assert_eq!(TaskKey::try_new("   ").unwrap_err().kind(), TaskIdErrorKind::Empty);
    assert_eq!(
        CoalescingKey::try_new("rows\ncount").unwrap_err().kind(),
        TaskIdErrorKind::InvalidCharacter
    );
}

#[test]
fn scope_is_ui_agnostic_path_data() {
    let scope = TaskScope::app()
        .then(TaskScopeSegment::try_new("workspace", "alpha").unwrap())
        .then(TaskScopeSegment::try_new("resource", "orders").unwrap());

    assert!(!scope.is_app());
    assert_eq!(scope.last_value("workspace"), Some("alpha"));
    assert_eq!(scope.last_value("resource"), Some("orders"));
}

#[test]
fn scope_segments_reject_invalid_or_reserved_namespaces() {
    assert_eq!(
        TaskScopeSegment::try_new("", "alpha").unwrap_err().kind(),
        TaskScopeErrorKind::EmptyNamespace
    );
    assert_eq!(
        TaskScopeSegment::try_new("ui", "button").unwrap_err().kind(),
        TaskScopeErrorKind::ReservedNamespace
    );
}

#[test]
fn provenance_names_task_attempt_and_correlation() {
    let provenance = TaskProvenance::new(TaskId::from_u64(1), TaskAttemptId::from_u64(2))
        .with_correlation(CorrelationId::from_u64(3))
        .with_sequence(4);

    assert_eq!(provenance.task_id(), TaskId::from_u64(1));
    assert_eq!(provenance.attempt_id(), TaskAttemptId::from_u64(2));
    assert_eq!(provenance.correlation_id(), Some(CorrelationId::from_u64(3)));
    assert_eq!(provenance.sequence(), Some(4));
}
```

- [ ] **Step 2: Run failing tests**

Run:

```sh
cargo test -p surgeist-task
```

Expected: fail with missing modules and types.

- [ ] **Step 3: Implement semantic IDs**

In `src/id.rs`, define private-field newtypes:

```rust
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TaskId(u64);
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TaskAttemptId(u64);
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CorrelationId(u64);

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TaskName(String);
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TaskKey(String);
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ObserverId(String);
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ResourceClassId(String);
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CoalescingKey(String);
```

Each numeric id must expose `from_u64` and `as_u64`. Each string id must expose `try_new` and `as_str`. Add:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskIdErrorKind {
    Empty,
    InvalidCharacter,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskIdError {
    kind: TaskIdErrorKind,
    value: String,
}
```

Validation rules:

- trim leading/trailing whitespace before storing;
- reject empty strings after trimming;
- reject ASCII control characters;
- keep `new` out of the public API for string ids unless it is `pub(crate)` and used only by tests that already pass validated literals.

- [ ] **Step 4: Implement task scope**

In `src/scope.rs`, define:

```rust
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TaskScopeSegment {
    namespace: String,
    value: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TaskScope {
    segments: Vec<TaskScopeSegment>,
}
```

Expose `TaskScopeSegment::try_new`, `namespace`, and `value`. Expose `TaskScope::app`, `TaskScope::try_custom`, `TaskScope::try_workspace`, `TaskScope::try_resource`, `then`, `try_then`, `segments`, `is_app`, and `last_value(namespace)`. `app()` is infallible because it uses crate-owned literals. Constructors that accept caller or input data must be fallible.

Define:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskScopeErrorKind {
    EmptyNamespace,
    EmptyValue,
    InvalidCharacter,
    ReservedNamespace,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskScopeError {
    kind: TaskScopeErrorKind,
    namespace: String,
    value: String,
}
```

Validation rules:

- trim namespace and value before storing;
- reject empty namespace or value;
- reject ASCII control characters;
- reject reserved namespaces `ui`, `window`, `render`, and `retained`; root app integration can map app/window concepts into task scopes through explicit adapters, but task crate scopes remain UI-agnostic.

- [ ] **Step 5: Implement provenance**

In `src/provenance.rs`, define `TaskProvenance` with private fields:

```rust
pub struct TaskProvenance {
    task_id: TaskId,
    attempt_id: TaskAttemptId,
    correlation_id: Option<CorrelationId>,
    parent_correlation_id: Option<CorrelationId>,
    sequence: Option<u64>,
}
```

Expose `new`, `with_correlation`, `with_parent`, `with_sequence`, and accessors. `correlation_id()` must return `Option<CorrelationId>`; do not invent a sentinel correlation id when a task event has no correlation.

- [ ] **Step 6: Wire public reexports**

In `src/lib.rs`, declare modules and reexport only the public front door:

```rust
mod id;
mod provenance;
mod scope;

pub use id::{
    CoalescingKey, CorrelationId, ObserverId, ResourceClassId, TaskAttemptId, TaskId,
    TaskIdError, TaskIdErrorKind, TaskKey, TaskName,
};
pub use provenance::TaskProvenance;
pub use scope::{TaskScope, TaskScopeError, TaskScopeErrorKind, TaskScopeSegment};
```

- [ ] **Step 7: Verify and commit**

Run:

```sh
cargo test -p surgeist-task
cargo fmt --check
cargo clippy -p surgeist-task --all-targets -- -D warnings
```

Expected: all pass.

Commit:

```sh
git add src/lib.rs src/id.rs src/scope.rs src/provenance.rs
git commit -m "Add task identity scope and provenance"
```

## Task 2: Policy, Cancellation, Lifecycle, And Snapshots

**Files:**
- Create: `src/policy.rs`
- Create: `src/cancel.rs`
- Create: `src/lifecycle.rs`
- Modify: `src/lib.rs`
- Test: `src/policy.rs`, `src/cancel.rs`, `src/lifecycle.rs`

- [ ] **Step 1: Write lifecycle tests**

Add:

```rust
#[test]
fn task_record_rejects_stale_attempts() {
    let mut record = TaskRecord::queued(
        TaskId::from_u64(1),
        TaskKey::try_new("search:rust").unwrap(),
        TaskScope::app(),
        TaskPolicy::cancel_when_unobserved(),
    );

    record.start_attempt(TaskAttemptId::from_u64(1)).unwrap();
    record.mark_running(TaskAttemptId::from_u64(1)).unwrap();
    record.start_attempt(TaskAttemptId::from_u64(2)).unwrap();

    assert!(record.accepts_attempt(TaskAttemptId::from_u64(2)));
    assert!(!record.accepts_attempt(TaskAttemptId::from_u64(1)));

    record.complete(TaskAttemptId::from_u64(2)).unwrap();
    let error = record.mark_running(TaskAttemptId::from_u64(2)).unwrap_err();
    assert_eq!(error.kind(), TaskLifecycleErrorKind::TerminalState);

    let restart_error = record.start_attempt(TaskAttemptId::from_u64(3)).unwrap_err();
    assert_eq!(restart_error.kind(), TaskLifecycleErrorKind::TerminalState);
}

#[test]
fn cancellation_is_honest_until_terminal_event_arrives() {
    let mut record = TaskRecord::queued(
        TaskId::from_u64(2),
        TaskKey::try_new("media:import").unwrap(),
        TaskScope::app(),
        TaskPolicy::continue_when_unobserved(),
    );

    record.start_attempt(TaskAttemptId::from_u64(1)).unwrap();
    record.mark_running(TaskAttemptId::from_u64(1)).unwrap();
    let token = record.request_cancel(CancelReason::user_requested());

    assert!(token.is_cancelled());
    assert_eq!(record.status(), TaskStatus::Cancelling);

    record.finish_after_cancel(TaskAttemptId::from_u64(1)).unwrap();
    assert_eq!(record.status(), TaskStatus::FinishedAfterCancel);
    assert!(record.snapshot().is_terminal());
}
```

- [ ] **Step 2: Implement policy**

In `src/policy.rs`, define:

```rust
pub enum UnobservedPolicy { Continue, LowerPriority, Pause, Cancel }
pub enum TaskPriority { Low, Normal, High }
pub enum BlockingPolicy { Abortable, Blocking, NonAbortableReportCancelling }

pub struct RetryPolicy {
    max_attempts: u32,
}

pub struct TaskPolicy {
    dedupe_by_key: bool,
    unobserved: UnobservedPolicy,
    priority: TaskPriority,
    retry: RetryPolicy,
    blocking: BlockingPolicy,
}
```

Expose `TaskPolicy::continue_when_unobserved`, `TaskPolicy::cancel_when_unobserved`, `dedupe_by_key` as the builder-style mutator, `dedupes_by_key` as the boolean accessor, `with_priority`, `with_retry_attempts`, `with_blocking_policy`, and accessors.

- [ ] **Step 3: Implement cancellation**

In `src/cancel.rs`, define cloneable `CancellationToken` around `Arc<CancellationState>` plus:

```rust
struct CancellationState {
    cancelled: AtomicBool,
    reason: Mutex<Option<CancelReason>>,
}

pub struct CancellationView {
    state: Arc<CancellationState>,
}

pub struct CancelReason {
    kind: CancelReasonKind,
    message: String,
}

pub enum CancelReasonKind {
    UserRequested,
    Timeout,
    Policy,
    Shutdown,
    DependencyFailed,
}
```

Expose `CancellationToken::new`, `cancel`, `is_cancelled`, `view`, `CancelReason::user_requested`, `CancelReason::timeout`, `CancelReason::policy`, `CancelReason::shutdown`, `CancelReason::dependency_failed`, `kind`, and `message`. Expose `CancellationView::is_cancelled` and `reason`, but do not expose `cancel` on the view. `message()` is display/detail text only; code must branch on `CancelReasonKind`, not parse the message.

- [ ] **Step 4: Implement lifecycle record**

In `src/lifecycle.rs`, define `TaskRecord` with private fields:

```rust
pub enum TaskStatus { Queued, Running, Waiting, Blocked, Completed, Failed, Cancelling, Cancelled, FinishedAfterCancel, FailedToCancel }
pub enum TaskTerminalStatus { Completed, Failed, Cancelled, FinishedAfterCancel, FailedToCancel }

pub struct TaskRecord {
    id: TaskId,
    key: TaskKey,
    scope: TaskScope,
    policy: TaskPolicy,
    status: TaskStatus,
    attempt_id: Option<TaskAttemptId>,
    cancellation: CancellationToken,
    cancel_reason: Option<CancelReason>,
    observer_count: usize,
}
```

Expose `queued`, `start_attempt`, `mark_running`, `mark_waiting`, `mark_blocked`, `complete`, `fail`, `request_cancel`, `confirm_cancelled`, `finish_after_cancel`, `fail_to_cancel`, accessors, `accepts_attempt`, and `snapshot`.

Observer ownership rule: `TaskRecord::observer_count` is a derived snapshot field
only. Task 2 must not expose observer membership mutation or arbitrary observer
count setters. Task 4 introduces the crate-local coordination snapshot type and
sync path once `TaskCoordination` exists.

`start_attempt` must return `Result<TaskAttemptId, TaskLifecycleError>`. It must reject:

- transitions out of terminal states;
- non-advancing attempts where the new attempt id is less than or equal to the current attempt id.

All lifecycle methods after `start_attempt` must take the producing `TaskAttemptId` and return `Result<(), TaskLifecycleError>`. They must reject:

- stale attempts that do not match the current attempt;
- transitions out of terminal states;
- terminal events for queued attempts that were never started;
- `finish_after_cancel` unless the current status is `Cancelling`.

Define:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskLifecycleErrorKind {
    StaleAttempt,
    TerminalState,
    NotRunning,
    NotCancelling,
    NonAdvancingAttempt,
    InvalidTransition,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskLifecycleError {
    kind: TaskLifecycleErrorKind,
    task_id: TaskId,
    attempt_id: TaskAttemptId,
    status: TaskStatus,
}
```

Define `TaskSnapshot` with task id, key, scope, status, attempt id, observer count, and terminal helper `is_terminal`.

- [ ] **Step 5: Wire public reexports and verify**

Run:

```sh
cargo test -p surgeist-task
cargo fmt --check
cargo clippy -p surgeist-task --all-targets -- -D warnings
```

Commit:

```sh
git add src/lib.rs src/policy.rs src/cancel.rs src/lifecycle.rs
git commit -m "Add task policy cancellation and lifecycle"
```

## Task 3: Task Events, Progress, And Queue Backpressure

**Files:**
- Create: `src/event.rs`
- Create: `src/queue.rs`
- Modify: `src/lib.rs`
- Test: `src/event.rs`, `src/queue.rs`

- [ ] **Step 1: Write event and queue tests**

Add:

```rust
#[test]
fn progress_events_coalesce_by_task_attempt_and_key() {
    let mut queue = TaskEventQueue::<String>::new(QueuePolicy::bounded(8));
    let task = TaskId::from_u64(1);
    let attempt = TaskAttemptId::from_u64(1);

    queue.push(TaskEvent::from_job_event(
        task,
        attempt,
        TaskJobEvent::progress_units(CoalescingKey::try_new("bytes").unwrap(), 10, Some(100)),
    )).unwrap();
    queue.push(TaskEvent::from_job_event(
        task,
        attempt,
        TaskJobEvent::progress_units(CoalescingKey::try_new("bytes").unwrap(), 20, Some(100)),
    )).unwrap();

    let drained = queue.drain(DrainBudget::new().max_events(8));
    assert_eq!(drained.events().len(), 1);
    assert_eq!(drained.coalesced_events(), 1);
    assert_eq!(drained.events()[0].progress_value(), Some(&TaskProgressValue::units(20, Some(100))));
}

#[test]
fn bounded_queue_reports_overflow_without_panicking() {
    let mut queue = TaskEventQueue::<()>::new(QueuePolicy::bounded(1));
    let task = TaskId::from_u64(1);
    let attempt = TaskAttemptId::from_u64(1);

    queue
        .push(TaskEvent::from_job_event(
            task,
            attempt,
            TaskJobEvent::diagnostic(TaskDiagnosticLevel::Info, "first"),
        ))
        .unwrap();
    let error = queue
        .push(TaskEvent::from_job_event(
            task,
            attempt,
            TaskJobEvent::diagnostic(TaskDiagnosticLevel::Info, "second"),
        ))
        .unwrap_err();

    assert_eq!(error.code(), QueueErrorCode::Overflow);
    assert_eq!(queue.report().dropped_events(), 1);
}

#[test]
fn unique_progress_slots_count_against_queue_capacity() {
    let mut queue = TaskEventQueue::<()>::new(QueuePolicy::bounded(2));
    let task = TaskId::from_u64(1);
    let attempt = TaskAttemptId::from_u64(1);

    queue.push(TaskEvent::from_job_event(
        task,
        attempt,
        TaskJobEvent::progress_message(CoalescingKey::try_new("file:a").unwrap(), "a"),
    )).unwrap();
    queue.push(TaskEvent::from_job_event(
        task,
        attempt,
        TaskJobEvent::progress_message(CoalescingKey::try_new("file:b").unwrap(), "b"),
    )).unwrap();

    let error = queue.push(TaskEvent::from_job_event(
        task,
        attempt,
        TaskJobEvent::progress_message(CoalescingKey::try_new("file:c").unwrap(), "c"),
    )).unwrap_err();

    assert_eq!(error.code(), QueueErrorCode::Overflow);
}
```

- [ ] **Step 2: Implement task events**

In `src/event.rs`, define separate semantic event payloads:

```rust
pub enum TaskLifecycleEvent {
    Started,
    Completed,
    Failed,
    Cancelled,
    FailedToCancel,
    FinishedAfterCancel,
}

pub struct TaskProgressEvent {
    key: CoalescingKey,
    value: TaskProgressValue,
}

pub enum TaskProgressValue {
    Units { completed: u64, total: Option<u64> },
    Percent(PercentComplete),
    Message(String),
}

pub struct PercentComplete {
    basis_points: u16,
}

pub struct TaskOutputEvent<Output> {
    output: Output,
}

pub enum TaskDiagnosticLevel {
    Info,
    Warning,
    Error,
}

pub struct TaskDiagnosticEvent {
    level: TaskDiagnosticLevel,
    message: String,
}

pub enum TaskEventKind<Output = ()> {
    Lifecycle(TaskLifecycleEvent),
    Progress(TaskProgressEvent),
    Output(TaskOutputEvent<Output>),
    Diagnostic(TaskDiagnosticEvent),
}

pub enum TaskJobEvent<Output = ()> {
    Progress(TaskProgressEvent),
    Output(TaskOutputEvent<Output>),
    Diagnostic(TaskDiagnosticEvent),
}

pub struct TaskEvent<Output = ()> {
    provenance: TaskProvenance,
    kind: TaskEventKind<Output>,
}
```

Expose `TaskEvent` constructors: `started`, `from_job_event`, `completed`, `failed`, `cancelled`, `failed_to_cancel`, `finished_after_cancel`, plus accessors and `progress_value`. Expose `TaskJobEvent` constructors: `progress`, `progress_units`, `progress_percent`, `progress_message`, `output`, and `diagnostic`. Expose `TaskProgressValue::units(completed, total)`, `TaskProgressValue::percent(percent)`, and `TaskProgressValue::message(message)`. `PercentComplete::try_from_basis_points` must reject values greater than 10_000. App-specific result data belongs in typed `TaskOutputEvent<Output>`, not in progress payload strings.

Completion output rule: `TaskLifecycleEvent::Completed` is the terminal lifecycle transition. `TaskOutputEvent<Output>` carries reducer/app-facing output and may be emitted before `Completed` or in the same executor turn immediately before `Completed`. Root integration must apply output events through app input lanes and lifecycle events through task status lanes; do not treat output as direct state mutation.

Terminal lifecycle emission rule: executors emit terminal lifecycle events after
the runnable returns or panics. Jobs may emit progress, output, and diagnostic
events through `TaskContext` as `TaskJobEvent` values, but they must not emit
terminal lifecycle events. On `Ok(())` without observed cancellation, the
executor emits `TaskEvent::completed(...)`. On `Ok(())` after cancellation was
requested for non-abortable work, the executor emits
`TaskEvent::finished_after_cancel(...)`. On `Err(TaskFailureKind::Cancelled)`,
the executor emits `TaskEvent::cancelled(...)`. On other `Err(TaskFailure)`
values or panic, the executor emits a `TaskDiagnosticEvent` with
`TaskDiagnosticLevel::Error` carrying the failure message, then emits
`TaskEvent::failed(...)`. Failed cancellation requests emit
`TaskEvent::failed_to_cancel(...)`.

- [ ] **Step 3: Implement queue policy and drain report**

In `src/queue.rs`, define:

```rust
pub struct QueuePolicy { capacity: usize }
pub struct DrainBudget { max_events: usize }
pub struct TaskEventQueue<Output = ()> {
    policy: QueuePolicy,
    fifo: VecDeque<TaskEvent<Output>>,
    progress: HashMap<ProgressSlot, TaskEvent<Output>>,
    progress_order: VecDeque<ProgressSlot>,
    dropped_events: usize,
    coalesced_events: usize,
}
pub struct QueueReport { capacity: usize, pending_events: usize, dropped_events: usize, coalesced_events: usize }
pub struct DrainReport<Output = ()> { events: Vec<TaskEvent<Output>>, queue: QueueReport, coalesced_events: usize }
pub enum QueueErrorCode { Overflow }
pub struct QueueError { code: QueueErrorCode, message: String }
```

Use FIFO ordering for non-progress events. Coalesce progress by `(task_id, attempt_id, coalescing_key)` and keep only the latest value for a slot.

Capacity rule: `QueuePolicy::capacity` is the total number of pending event slots across FIFO events and unique progress slots. Updating an existing progress slot does not consume another slot. Adding a new progress key consumes one slot and must return `QueueErrorCode::Overflow` when the total slot count is already at capacity.

- [ ] **Step 4: Verify and commit**

Run:

```sh
cargo test -p surgeist-task
cargo fmt --check
cargo clippy -p surgeist-task --all-targets -- -D warnings
```

Commit:

```sh
git add src/lib.rs src/event.rs src/queue.rs
git commit -m "Add task events and bounded queues"
```

## Task 4: Task Registration, Observation, And Coordination

**Files:**
- Create: `src/coordination.rs`
- Modify: `src/lifecycle.rs`
- Modify: `src/lib.rs`
- Test: `src/coordination.rs`

- [ ] **Step 1: Write registration and observer tests**

Add:

```rust
#[derive(Clone, Debug, Eq, PartialEq)]
struct ImportInput {
    path: String,
}

#[test]
fn task_registration_derives_scope_key_and_policy() {
    let registration = TaskRegistration::<ImportInput>::try_new("import").unwrap()
        .scope(|input| {
            let workspace =
                TaskScope::try_workspace("alpha").map_err(TaskRegistrationError::invalid_scope)?;
            let file = TaskScopeSegment::try_new("file", &input.path)
                .map_err(TaskRegistrationError::invalid_scope)?;
            workspace
                .try_then(file)
                .map_err(TaskRegistrationError::invalid_scope)
        })
        .key(|input| {
            TaskKey::try_new(format!("import:{}", input.path))
                .map_err(TaskRegistrationError::invalid_key)
        })
        .with_policy(TaskPolicy::continue_when_unobserved().dedupe_by_key());

    let input = ImportInput { path: "orders.xlsx".into() };
    assert_eq!(registration.name().as_str(), "import");
    assert_eq!(registration.key_for(&input).unwrap().as_str(), "import:orders.xlsx");
    assert_eq!(registration.scope_for(&input).unwrap().last_value("file"), Some("orders.xlsx"));
    assert!(registration.policy().dedupes_by_key());
}

#[test]
fn observation_policy_does_not_cancel_until_last_observer_detaches() {
    let mut coordination = TaskCoordination::default();
    let key = TaskKey::try_new("compile:main").unwrap();
    let left = ObserverId::try_new("surface:left").unwrap();
    let right = ObserverId::try_new("surface:right").unwrap();
    let policy = TaskPolicy::cancel_when_unobserved();

    coordination.observe(key.clone(), left.clone());
    coordination.observe(key.clone(), right.clone());
    assert_eq!(coordination.observer_count(&key), 2);

    let first = coordination.unobserve(&key, &left, &policy);
    assert_eq!(first, ObservationChange::StillObserved { remaining: 1 });
    assert_eq!(coordination.observer_count(&key), 1);
    assert!(coordination.is_observed(&key));

    let second = coordination.unobserve(&key, &right, &policy);
    assert_eq!(second, ObservationChange::CancelRequested);
    assert_eq!(coordination.observer_count(&key), 0);
    assert!(!coordination.is_observed(&key));

    let missing = coordination.unobserve(&key, &right, &policy);
    assert_eq!(missing, ObservationChange::ObserverNotAttached);
}
```

- [ ] **Step 2: Implement registration and coordination**

In `src/coordination.rs`, define:

```rust
pub struct TaskRegistration<Input> {
    name: TaskName,
    scope: Arc<dyn Fn(&Input) -> Result<TaskScope, TaskRegistrationError> + Send + Sync>,
    key: Arc<dyn Fn(&Input) -> Result<TaskKey, TaskRegistrationError> + Send + Sync>,
    policy: TaskPolicy,
}

pub struct TaskRegistrationError {
    kind: TaskRegistrationErrorKind,
    message: String,
}

pub enum TaskRegistrationErrorKind {
    InvalidName,
    InvalidScope,
    InvalidKey,
}

pub struct TaskCoordination {
    observers: HashMap<TaskKey, HashSet<ObserverId>>,
}

pub struct ObserverCountSnapshot {
    key: TaskKey,
    count: usize,
}

pub enum ObservationChange {
    ObserverNotAttached,
    ObserverDetached,
    StillObserved { remaining: usize },
    CancelRequested,
    PriorityLowered,
    Paused,
    ContinuedUnobserved,
}
```

Expose `TaskRegistration::try_new`, `scope`, `key`, `with_policy`, `name`, `scope_for`, `key_for`, and `policy`. `scope(...)` and `key(...)` must accept closures returning `Result<_, TaskRegistrationError>`, while `scope_for(...)` and `key_for(...)` must return those results to the caller. Do not implement blanket `From<TaskIdError>` or `From<TaskScopeError>` conversions for `TaskRegistrationError`; those conversions cannot know whether the failing phase was registration naming, key derivation, or scope derivation. Provide explicit constructors such as `invalid_name(TaskIdError)`, `invalid_key(TaskIdError)`, and `invalid_scope(TaskScopeError)` so closures preserve semantic phase when mapping lower-level validation errors. Expose `TaskCoordination::observe`, `unobserve`, `observer_count`, `snapshot_count`, and `is_observed`. `ObserverCountSnapshot` accessors must expose `key` and `count`, but its constructor must be crate-local so only `TaskCoordination` can produce one. Add `TaskRecord::sync_observer_count(ObserverCountSnapshot)` as a `pub(crate)` method in Task 4, not Task 2, so lifecycle snapshots can consume coordination-owned observer counts without accepting arbitrary integers.

`unobserve` must accept `&TaskPolicy` and return `ObservationChange`. It must:

- return `ObserverNotAttached` when the observer was not attached before this call;
- return `StillObserved { remaining }` when at least one observer remains;
- return `CancelRequested` for `UnobservedPolicy::Cancel`;
- return `PriorityLowered` for `UnobservedPolicy::LowerPriority`;
- return `Paused` for `UnobservedPolicy::Pause`;
- return `ContinuedUnobserved` for `UnobservedPolicy::Continue`;
- return `ObserverDetached` only when an observer was removed and no policy action is needed.

Policy decisions must occur only when the call actually removes the last attached observer. Idempotent or unmatched detach calls must not trigger cancellation, pausing, priority lowering, or continuation policy.

These are task coordination decisions only. Root app integration consumes `ObservationChange` and emits app effects or task executor calls; `surgeist-task` must not cancel UI/app work directly.

- [ ] **Step 3: Verify and commit**

Run:

```sh
cargo test -p surgeist-task
cargo fmt --check
cargo clippy -p surgeist-task --all-targets -- -D warnings
```

Commit:

```sh
git add src/lib.rs src/coordination.rs src/lifecycle.rs
git commit -m "Add task registration and observation"
```

## Task 5: Backend-Neutral Executor Contract And Fake Executor

**Files:**
- Create: `src/executor.rs`
- Create: `src/testing.rs`
- Create: `examples/basic_task.rs`
- Modify: `src/lib.rs`
- Test: `src/executor.rs`, `src/testing.rs`, `examples/basic_task.rs`

**Worker slicing:** Do not assign all of Task 5 to one implementation worker.
Use separate scoped worker/reviewer cycles for:

1. executor trait, event sink trait, runnable traits, handles, errors, failures,
   and drain report;
2. spawn request constructors/builder, concrete input handling, task context,
   context helpers, read-only cancellation view, and runnable closure helpers;
3. fake executor semantics and crate test helpers;
4. `examples/basic_task.rs` with a local example sink.

Each slice must come back with its focused tests and reviewer result before the
coordinator assigns the next slice.

- [ ] **Step 1: Worker 5A - Executor and event sink core**

Scope: implement the backend-neutral executor core in `src/executor.rs`:

- `TaskExecutor`;
- `TaskEventSink`;
- `BlockingTaskJob`;
- `AsyncTaskJob`;
- `TaskRunnable` enum shell;
- `TaskHandle`;
- `ExecutorTaskHandle`;
- `ExecutorError`;
- `TaskFailure`;
- `TaskEventSinkError`;
- `ExecutorDrainReport`.

Also wire the module and intentional reexports in `src/lib.rs`.

Write focused tests for handle constructors/accessors, error/failure accessors,
drain report lifecycle counters, and event sink error modeling. Do not implement
fake executor behavior, builder ergonomics, context helpers, or the example in
this slice.

Request a separate reviewer for Worker 5A before continuing.

- [ ] **Step 2: Worker 5B - Spawn request, context, and ergonomic helpers**

Scope: implement the request/context authoring surface in `src/executor.rs`:

- `TaskSpawnRequest::new`;
- `TaskSpawnRequest::<(), Output>::new_unit`;
- `TaskSpawnRequest::builder`;
- `TaskSpawnRequestBuilder::{key, scope, cancellation, sink, input, unit_input,
  runnable, blocking_fn, async_fn, policy, build}`;
- `TaskSpawnRequest::with_policy`;
- `TaskRunnable::{blocking_fn, async_fn, blocking_job, async_job}`;
- `TaskContext::{task_id, attempt_id, cancellation_view, is_cancelled,
  emit_job_event, progress_units, progress_percent, progress_message, output,
  diagnostic}`;
- `TaskContext::for_test` in test support only.

Write focused tests proving missing input is impossible through successful
`build()`, `unit_input()` supplies `()`, production construction requires an
explicit sink, context helpers emit only non-terminal `TaskJobEvent` data, sink
errors map to `TaskFailureKind::SinkFailed`, and `TaskContext` exposes no mutable
cancellation authority.

Request a separate reviewer for Worker 5B before continuing.

- [ ] **Step 3: Worker 5C - Fake executor semantics and crate test helpers**

Scope: implement `src/testing.rs` crate test helpers and fake executor behavior:

- `RecordingTaskEventSink` for crate tests only;
- recorded task jobs used by tests;
- `FakeExecutor<Input, Output>` implementing `TaskExecutor<Input, Output>`;
- spawn/cancel recording;
- synchronous runnable execution;
- executor-owned terminal lifecycle emission;
- lifecycle-specific `drain_finished` counters and clearing behavior.

Add the fake executor tests below in this slice.

Add:

```rust
#[test]
fn fake_executor_records_spawn_and_cancel_requests() {
    let mut executor = FakeExecutor::<String, String>::default();
    let sink = RecordingTaskEventSink::new();
    let token = CancellationToken::new();
    let request = TaskSpawnRequest::new(
        TaskId::from_u64(1),
        TaskAttemptId::from_u64(1),
        TaskKey::try_new("import:orders").unwrap(),
        TaskScope::app(),
        token.clone(),
        sink.handle(),
        "orders.xlsx".to_owned(),
        TaskRunnable::blocking_job(RecordedTaskJob::new("loaded")),
    )
    .with_policy(TaskPolicy::continue_when_unobserved());

    let handle = executor.spawn_task(request).unwrap();
    assert_eq!(handle.task_id(), TaskId::from_u64(1));
    assert_eq!(executor.spawned().len(), 1);

    executor.cancel(TaskHandle::new(TaskId::from_u64(1), TaskAttemptId::from_u64(1))).unwrap();
    assert_eq!(executor.cancelled().len(), 1);
    assert!(token.is_cancelled());
    assert_eq!(sink.events()[0].progress_value(), Some(&TaskProgressValue::message("loaded")));
}

#[test]
fn non_abortable_blocking_request_keeps_cancelling_status_available() {
    let sink = RecordingTaskEventSink::new();
    let token = CancellationToken::new();
    let request = TaskSpawnRequest::<(), ()>::new_unit(
        TaskId::from_u64(2),
        TaskAttemptId::from_u64(1),
        TaskKey::try_new("polars:300-files").unwrap(),
        TaskScope::app(),
        token.clone(),
        sink.handle(),
        TaskRunnable::blocking_job(RecordedTaskJob::new("done")),
    )
    .with_policy(TaskPolicy::continue_when_unobserved().with_blocking_policy(BlockingPolicy::NonAbortableReportCancelling));

    assert_eq!(request.policy().blocking_policy(), BlockingPolicy::NonAbortableReportCancelling);
}

#[test]
fn fake_executor_emits_finished_after_cancel_for_non_abortable_completed_work() {
    let mut executor = FakeExecutor::<(), ()>::default();
    let sink = RecordingTaskEventSink::new();
    let token = CancellationToken::new();
    let request = TaskSpawnRequest::new_unit(
        TaskId::from_u64(3),
        TaskAttemptId::from_u64(1),
        TaskKey::try_new("nonabortable:finish").unwrap(),
        TaskScope::app(),
        token.clone(),
        sink.handle(),
        TaskRunnable::blocking_job(RecordedTaskJob::new("done")),
    )
    .with_policy(TaskPolicy::continue_when_unobserved().with_blocking_policy(BlockingPolicy::NonAbortableReportCancelling));

    token.cancel(CancelReason::user_requested());
    executor.spawn_blocking_task(request).unwrap();
    let report = executor.drain_finished();
    let cleared = executor.drain_finished();

    assert_eq!(report.finished_after_cancel_tasks(), 1);
    assert_eq!(cleared.finished_after_cancel_tasks(), 0);
    assert_eq!(sink.terminal_lifecycle_events(), vec![TaskLifecycleEvent::FinishedAfterCancel]);
}

#[test]
fn fake_executor_emits_cancelled_for_cooperatively_cancelled_work() {
    let mut executor = FakeExecutor::<(), ()>::default();
    let sink = RecordingTaskEventSink::new();
    let token = CancellationToken::new();
    let request = TaskSpawnRequest::new_unit(
        TaskId::from_u64(4),
        TaskAttemptId::from_u64(1),
        TaskKey::try_new("cancelled:cooperative").unwrap(),
        TaskScope::app(),
        token.clone(),
        sink.handle(),
        TaskRunnable::blocking_job(CancelledTaskJob::new()),
    );

    token.cancel(CancelReason::user_requested());
    executor.spawn_task(request).unwrap();
    let report = executor.drain_finished();
    let cleared = executor.drain_finished();

    assert_eq!(report.cancelled_tasks(), 1);
    assert_eq!(cleared.cancelled_tasks(), 0);
    assert!(sink.diagnostics().is_empty());
    assert_eq!(sink.terminal_lifecycle_events(), vec![TaskLifecycleEvent::Cancelled]);
}

#[test]
fn fake_executor_drain_reports_completed_and_clears_count() {
    let mut executor = FakeExecutor::<(), ()>::default();
    let sink = RecordingTaskEventSink::new();
    let request = TaskSpawnRequest::new_unit(
        TaskId::from_u64(5),
        TaskAttemptId::from_u64(1),
        TaskKey::try_new("complete:job").unwrap(),
        TaskScope::app(),
        CancellationToken::new(),
        sink.handle(),
        TaskRunnable::blocking_job(RecordedTaskJob::new("done")),
    );

    executor.spawn_task(request).unwrap();
    let report = executor.drain_finished();
    let cleared = executor.drain_finished();

    assert_eq!(report.completed_tasks(), 1);
    assert_eq!(cleared.completed_tasks(), 0);
    assert_eq!(sink.terminal_lifecycle_events(), vec![TaskLifecycleEvent::Completed]);
}

#[test]
fn fake_executor_drain_reports_failed_and_clears_count() {
    let mut executor = FakeExecutor::<(), ()>::default();
    let sink = RecordingTaskEventSink::new();
    let request = TaskSpawnRequest::new_unit(
        TaskId::from_u64(6),
        TaskAttemptId::from_u64(1),
        TaskKey::try_new("failed:job").unwrap(),
        TaskScope::app(),
        CancellationToken::new(),
        sink.handle(),
        TaskRunnable::blocking_job(FailingTaskJob::new("failed")),
    );

    executor.spawn_task(request).unwrap();
    let report = executor.drain_finished();
    let cleared = executor.drain_finished();

    assert_eq!(report.failed_tasks(), 1);
    assert_eq!(cleared.failed_tasks(), 0);
    assert_eq!(sink.terminal_lifecycle_events(), vec![TaskLifecycleEvent::Failed]);
    assert_eq!(sink.diagnostics().len(), 1);
}

#[test]
fn fake_executor_cancel_after_terminal_completion_does_not_emit_failed_to_cancel() {
    let mut executor = FakeExecutor::<(), ()>::default();
    let sink = RecordingTaskEventSink::new();
    let request = TaskSpawnRequest::new_unit(
        TaskId::from_u64(7),
        TaskAttemptId::from_u64(1),
        TaskKey::try_new("cancel:after-terminal").unwrap(),
        TaskScope::app(),
        CancellationToken::new(),
        sink.handle(),
        TaskRunnable::blocking_job(RecordedTaskJob::new("done")),
    );

    executor.spawn_task(request).unwrap();
    executor.cancel(TaskHandle::new(TaskId::from_u64(7), TaskAttemptId::from_u64(1))).unwrap_err();
    let report = executor.drain_finished();
    let cleared = executor.drain_finished();

    assert_eq!(report.completed_tasks(), 1);
    assert_eq!(report.failed_to_cancel_tasks(), 0);
    assert_eq!(cleared.failed_to_cancel_tasks(), 0);
    assert_eq!(sink.terminal_lifecycle_events(), vec![TaskLifecycleEvent::Completed]);
}
```

- [ ] **Step 4: Contract details shared by Workers 5A-5C**

In `src/executor.rs`, define:

```rust
pub trait TaskExecutor<Input, Output> {
    fn spawn_task(&mut self, request: TaskSpawnRequest<Input, Output>) -> Result<ExecutorTaskHandle, ExecutorError>;
    fn spawn_blocking_task(&mut self, request: TaskSpawnRequest<Input, Output>) -> Result<ExecutorTaskHandle, ExecutorError>;
    fn cancel(&mut self, handle: TaskHandle) -> Result<(), ExecutorError>;
    fn drain_finished(&mut self) -> ExecutorDrainReport;
    fn name(&self) -> &'static str;
}

pub trait TaskEventSink<Output>: Send + Sync + 'static {
    fn emit(&self, event: TaskEvent<Output>) -> Result<(), TaskEventSinkError>;
}

pub trait BlockingTaskJob<Input, Output>: Send + 'static {
    fn run(self: Box<Self>, input: Input, context: TaskContext<Output>) -> Result<(), TaskFailure>;
}

pub trait AsyncTaskJob<Input, Output>: Send + 'static {
    fn run(
        self: Box<Self>,
        input: Input,
        context: TaskContext<Output>,
    ) -> Pin<Box<dyn Future<Output = Result<(), TaskFailure>> + Send + 'static>>;
}

pub enum TaskRunnable<Input, Output> {
    Blocking(Box<dyn BlockingTaskJob<Input, Output>>),
    Async(Box<dyn AsyncTaskJob<Input, Output>>),
}

pub struct TaskSpawnRequest<Input = (), Output = ()> {
    task_id: TaskId,
    attempt_id: TaskAttemptId,
    key: TaskKey,
    scope: TaskScope,
    policy: TaskPolicy,
    cancellation: CancellationToken,
    input: Input,
    runnable: TaskRunnable<Input, Output>,
    event_sink: Arc<dyn TaskEventSink<Output>>,
}
pub struct TaskContext<Output = ()> {
    task_id: TaskId,
    attempt_id: TaskAttemptId,
    cancellation: CancellationView,
    event_sink: Arc<dyn TaskEventSink<Output>>,
}
pub struct TaskHandle { task_id: TaskId, attempt_id: TaskAttemptId }
pub struct ExecutorTaskHandle { task_id: TaskId, attempt_id: TaskAttemptId }
pub struct ExecutorError { kind: ExecutorErrorKind, task_id: Option<TaskId>, attempt_id: Option<TaskAttemptId>, message: String }
pub enum ExecutorErrorKind { InvalidRequest, SpawnFailed, CancellationFailed, JoinFailed, SinkFailed }
pub struct TaskFailure { kind: TaskFailureKind, message: String }
pub enum TaskFailureKind { Cancelled, Panicked, Failed, InvalidInput, SinkFailed }
pub struct TaskEventSinkError { kind: TaskEventSinkErrorKind, message: String }
pub enum TaskEventSinkErrorKind { Closed, Overflow, SerializationFailed }
pub struct ExecutorDrainReport {
    completed_tasks: usize,
    failed_tasks: usize,
    cancelled_tasks: usize,
    finished_after_cancel_tasks: usize,
    failed_to_cancel_tasks: usize,
}
```

`TaskSpawnRequest::new` must take task id, attempt id, key, scope, the `CancellationToken` owned by the task record, an explicit `Arc<dyn TaskEventSink<Output>>`, concrete `input: Input`, and `TaskRunnable<Input, Output>`. It must not install a default sink, and `TaskSpawnRequest` must not store missing input as `Option<Input>`. For no-input jobs, expose `TaskSpawnRequest::<(), Output>::new_unit(...)` or a builder `unit_input()` convenience that supplies `()`. Test helpers may expose `TaskSpawnRequest::new_for_test(...)` or `RecordingTaskEventSink` to reduce test boilerplate, but production constructors and examples must make event routing explicit through the public `TaskEventSink` trait.

Expose `TaskHandle::new(task_id, attempt_id)`, `task_id`, and `attempt_id`.
Expose `ExecutorTaskHandle::new(task_id, attempt_id)`, `task_id`, and
`attempt_id`. Keep fields private.

`TaskContext` must expose `task_id`, `attempt_id`, `cancellation_view`, `emit_job_event(TaskJobEvent<Output>)`, and `is_cancelled`. `CancellationView` is a read-only cloneable view that exposes `is_cancelled` and `reason` but not `cancel`. `emit_job_event` must wrap the job event with the task provenance and forward it to `TaskEventSink` as a full `TaskEvent`. The cancellation view inside `TaskContext` must read from the same token carried by `TaskSpawnRequest`, so cancellation requested from the task record is visible to the running job without giving job code authority to flip the token. It must not expose UI, app state, retained tree, window handles, lifecycle event emission, or mutable cancellation authority.

`TaskContext::emit_job_event` and the convenience helpers must return `Result<(), TaskFailure>`. Sink failures must be converted into `TaskFailure { kind: TaskFailureKind::SinkFailed, ... }`, preserving enough message context for diagnostics. Job closures returning `Result<(), TaskFailure>` must be able to use `?` with context helper calls without seeing `TaskEventSinkError` directly.

Expose `TaskFailure::failed(message)`, `TaskFailure::cancelled(message)`,
`TaskFailure::invalid_input(message)`, `TaskFailure::sink_failed(message)`,
`kind`, and `message`. These failures are returned by jobs. Executors must map
`TaskFailureKind::Cancelled` directly to `TaskEvent::cancelled` without an error
diagnostic. Executors map other failure kinds to an error diagnostic followed by
the appropriate failed terminal lifecycle event.

Implement the ergonomic helpers from the API Shape Contract during this task,
not in a later cleanup pass:

- `TaskSpawnRequest::builder(task_id, attempt_id)` with required `key`,
  `scope`, `cancellation`, `sink`, `input`, and `runnable` validation in
  `build()`;
- `TaskSpawnRequestBuilder::key(TaskKey)`;
- `TaskSpawnRequestBuilder::scope(TaskScope)`;
- `TaskSpawnRequestBuilder::cancellation(CancellationToken)`;
- `TaskSpawnRequestBuilder::sink<S>(S) where S: TaskEventSink<Output> + 'static`;
- `TaskSpawnRequestBuilder::sink_arc(Arc<dyn TaskEventSink<Output>>)`;
- `TaskSpawnRequestBuilder::input(Input)`;
- `TaskSpawnRequestBuilder::unit_input()` for `Input = ()`;
- `TaskSpawnRequestBuilder::runnable(TaskRunnable<Input, Output>)`;
- `TaskSpawnRequestBuilder::blocking_fn(...)`;
- `TaskSpawnRequestBuilder::async_fn(...)` where object safety allows it without
  exposing Tokio types;
- `TaskSpawnRequestBuilder::policy(TaskPolicy)`;
- `TaskSpawnRequestBuilder::build()`;
- `TaskSpawnRequest::with_policy(TaskPolicy)`;
- `TaskSpawnRequest::<(), Output>::new_unit(...)`;
- `TaskRunnable::blocking_fn(...)`;
- `TaskRunnable::async_fn(...)` where object safety allows it without exposing
  Tokio types;
- `TaskRunnable::blocking_job(...)`;
- `TaskRunnable::async_job(...)`;
- `TaskContext::progress_units(...)`;
- `TaskContext::progress_percent(...)`;
- `TaskContext::progress_message(...)`;
- `TaskContext::output(...)`;
- `TaskContext::diagnostic(...)`;
- `TaskContext::emit_job_event(TaskJobEvent<Output>)`;
- `TaskContext::for_test(...)` in test support;
- `RecordingTaskEventSink` in `src/testing.rs` for crate tests only.

Add tests proving the context helpers emit non-terminal typed `TaskEvent` values,
the fake executor emits exactly one `Completed` event after an `Ok(())` job, the
fake executor emits an error diagnostic plus exactly one `Failed` event after an
ordinary non-cancelled `Err(TaskFailure)` job, the fake executor emits exactly
one `Cancelled` event after `TaskFailureKind::Cancelled`, and production spawn construction still requires an
explicit event sink. Because `TaskContext` has no terminal lifecycle helpers,
job code cannot duplicate executor-owned terminal events through the ergonomic
API.

The fake executor must execute the runnable synchronously during `spawn_task`/`spawn_blocking_task`, record emitted events through the provided sink, emit the executor-owned terminal lifecycle event after the runnable returns or panics, and return an `ExecutorTaskHandle`. `drain_finished` must return terminal outcome counts accumulated by the fake executor and clear them. `ExecutorDrainReport` counts task lifecycle outcomes, not backend join outcomes, so `Cancelled`, `FinishedAfterCancel`, and `FailedToCancel` have separate counters. Because the fake executor is synchronous, cancelling a handle after the fake job has already reached a terminal state must return a cancellation error without emitting a second terminal lifecycle event. This keeps fake and real paths aligned enough that tests prove jobs can emit task events.

Request a separate reviewer for Worker 5C before continuing.

- [ ] **Step 5: Worker 5D - Compileable basic task example**

Create `examples/basic_task.rs` showing the intended Rust-facing shape from the
API Shape Contract. The example should compile without requiring callers to name
Tokio types, raw `Arc<dyn ...>` internals, supervisor structures, root app
concepts, windows, or retained tree handles.

The example must not import `RecordingTaskEventSink` or any `src/testing.rs`
helper. Define a tiny local sink in the example that implements the public
`TaskEventSink` trait and stores emitted events in local example state. This
keeps the example production-facing: it demonstrates the sink contract users
must implement without promoting test scaffolding into normal API. The example
should pass that local sink through `TaskSpawnRequestBuilder::sink(example_sink)`
rather than naming `Arc<dyn TaskEventSink<_>>`.

Run:

```sh
cargo check -p surgeist-task --example basic_task
```

Request a separate reviewer for Worker 5D before final Task 5 verification.

- [ ] **Step 6: Verify and commit Task 5**

Run:

```sh
cargo test -p surgeist-task
cargo check -p surgeist-task --example basic_task
cargo fmt --check
cargo clippy -p surgeist-task --all-targets -- -D warnings
```

Commit:

```sh
git add src/lib.rs src/executor.rs src/testing.rs examples/basic_task.rs
git commit -m "Add task executor contract"
```

## Task 6: Tokio-Backed Task Runtime

**Files:**
- Create: `src/runtime_tokio.rs`
- Modify: `Cargo.toml`
- Modify: `src/lib.rs`
- Test: `src/runtime_tokio.rs`

- [ ] **Step 1: Add Tokio runtime contract tests**

Add:

```rust
#[test]
fn tokio_executor_is_hidden_behind_task_executor_contract() {
    let executor = TokioTaskExecutor::new().unwrap();
    assert_eq!(executor.name(), "tokio");
}

fn drain_until_finished(
    executor: &mut TokioTaskExecutor,
    mut is_finished: impl FnMut(&ExecutorDrainReport) -> bool,
) -> ExecutorDrainReport {
    for _ in 0..50 {
        let report = executor.drain_finished();
        if is_finished(&report) {
            return report;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    executor.drain_finished()
}

#[test]
fn tokio_executor_runs_async_job_and_routes_events() {
    let mut executor = TokioTaskExecutor::new().unwrap();
    let sink = RecordingTaskEventSink::new();
    let request = TaskSpawnRequest::new_unit(
        TaskId::from_u64(1),
        TaskAttemptId::from_u64(1),
        TaskKey::try_new("async:job").unwrap(),
        TaskScope::app(),
        CancellationToken::new(),
        sink.handle(),
        TaskRunnable::async_job(RecordedAsyncTaskJob::new(TaskProgressValue::message("async-done"))),
    );

    executor.spawn_task(request).unwrap();
    let report = drain_until_finished(&mut executor, |report| report.completed_tasks() >= 1);

    assert_eq!(report.completed_tasks(), 1);
    assert_eq!(sink.events()[0].progress_value(), Some(&TaskProgressValue::message("async-done")));
}

#[test]
fn tokio_executor_runs_blocking_job_on_blocking_pool() {
    let mut executor = TokioTaskExecutor::new().unwrap();
    let sink = RecordingTaskEventSink::new();
    let request = TaskSpawnRequest::new_unit(
        TaskId::from_u64(2),
        TaskAttemptId::from_u64(1),
        TaskKey::try_new("blocking:job").unwrap(),
        TaskScope::app(),
        CancellationToken::new(),
        sink.handle(),
        TaskRunnable::blocking_job(RecordedTaskJob::new("blocking-done")),
    );

    executor.spawn_blocking_task(request).unwrap();
    let report = drain_until_finished(&mut executor, |report| report.completed_tasks() >= 1);

    assert_eq!(report.completed_tasks(), 1);
    assert_eq!(sink.events()[0].progress_value(), Some(&TaskProgressValue::message("blocking-done")));
}

#[test]
fn tokio_executor_cancel_flips_the_running_jobs_token() {
    let mut executor = TokioTaskExecutor::new().unwrap();
    let token = CancellationToken::new();
    let sink = RecordingTaskEventSink::new();
    let handle = TaskHandle::new(TaskId::from_u64(3), TaskAttemptId::from_u64(1));
    let request = TaskSpawnRequest::new_unit(
        handle.task_id(),
        handle.attempt_id(),
        TaskKey::try_new("cancel:job").unwrap(),
        TaskScope::app(),
        token.clone(),
        sink.handle(),
        TaskRunnable::async_job(WaitingTaskJob::new()),
    );

    executor.spawn_task(request).unwrap();
    executor.cancel(handle).unwrap();

    assert!(token.is_cancelled());
}

#[test]
fn tokio_executor_emits_finished_after_cancel_for_non_abortable_completed_work() {
    let mut executor = TokioTaskExecutor::new().unwrap();
    let token = CancellationToken::new();
    let sink = RecordingTaskEventSink::new();
    let request = TaskSpawnRequest::new_unit(
        TaskId::from_u64(6),
        TaskAttemptId::from_u64(1),
        TaskKey::try_new("tokio:nonabortable-finish").unwrap(),
        TaskScope::app(),
        token.clone(),
        sink.handle(),
        TaskRunnable::blocking_job(RecordedTaskJob::new("done")),
    )
    .with_policy(TaskPolicy::continue_when_unobserved().with_blocking_policy(BlockingPolicy::NonAbortableReportCancelling));

    token.cancel(CancelReason::user_requested());
    executor.spawn_blocking_task(request).unwrap();
    let report = drain_until_finished(&mut executor, |report| report.finished_after_cancel_tasks() >= 1);

    assert_eq!(report.finished_after_cancel_tasks(), 1);
    assert_eq!(sink.terminal_lifecycle_events(), vec![TaskLifecycleEvent::FinishedAfterCancel]);
}

#[test]
fn tokio_executor_emits_cancelled_for_cooperatively_cancelled_work() {
    let mut executor = TokioTaskExecutor::new().unwrap();
    let token = CancellationToken::new();
    let sink = RecordingTaskEventSink::new();
    let request = TaskSpawnRequest::new_unit(
        TaskId::from_u64(7),
        TaskAttemptId::from_u64(1),
        TaskKey::try_new("tokio:cancelled").unwrap(),
        TaskScope::app(),
        token.clone(),
        sink.handle(),
        TaskRunnable::async_job(CancelledAsyncTaskJob::new()),
    );

    token.cancel(CancelReason::user_requested());
    executor.spawn_task(request).unwrap();
    let report = drain_until_finished(&mut executor, |report| report.cancelled_tasks() >= 1);

    assert_eq!(report.cancelled_tasks(), 1);
    assert_eq!(sink.terminal_lifecycle_events(), vec![TaskLifecycleEvent::Cancelled]);
}

#[test]
fn tokio_drain_finished_does_not_wait_for_unfinished_jobs() {
    let mut executor = TokioTaskExecutor::new().unwrap();
    let sink = RecordingTaskEventSink::new();
    let request = TaskSpawnRequest::new_unit(
        TaskId::from_u64(5),
        TaskAttemptId::from_u64(1),
        TaskKey::try_new("slow:job").unwrap(),
        TaskScope::app(),
        CancellationToken::new(),
        sink.handle(),
        TaskRunnable::async_job(WaitingTaskJob::new()),
    );

    executor.spawn_task(request).unwrap();
    let report = executor.drain_finished();

    assert_eq!(report.completed_tasks(), 0);
    assert_eq!(report.failed_tasks(), 0);
    assert_eq!(report.cancelled_tasks(), 0);
    assert_eq!(report.finished_after_cancel_tasks(), 0);
    assert_eq!(report.failed_to_cancel_tasks(), 0);
    assert!(sink.events().is_empty());
}

#[test]
fn tokio_executor_reports_panics_as_failed_events() {
    let mut executor = TokioTaskExecutor::new().unwrap();
    let sink = RecordingTaskEventSink::new();
    let request = TaskSpawnRequest::new_unit(
        TaskId::from_u64(4),
        TaskAttemptId::from_u64(1),
        TaskKey::try_new("panic:job").unwrap(),
        TaskScope::app(),
        CancellationToken::new(),
        sink.handle(),
        TaskRunnable::async_job(PanickingTaskJob::new()),
    );

    executor.spawn_task(request).unwrap();
    let report = drain_until_finished(&mut executor, |report| report.failed_tasks() >= 1);

    assert_eq!(report.failed_tasks(), 1);
    assert!(sink.events().iter().any(|event| event.is_failed()));
}
```

- [ ] **Step 2: Add Tokio dependency**

In `Cargo.toml`, add:

```toml
[dependencies]
tokio = { version = "=1.48.0", features = ["rt-multi-thread", "sync", "time"] }
```

- [ ] **Step 3: Implement Tokio-backed task runtime**

In `src/runtime_tokio.rs`, define:

```rust
pub struct TokioTaskExecutor {
    runtime: tokio::runtime::Runtime,
    supervisor: TokioTaskSupervisor,
}
```

Expose `new`, `name`, and implement `TaskExecutor<Input, Output>` for sendable inputs and outputs. This is not an optional implementation detail; it is the primary task runtime Surgeist provides so app authors do not have to assemble Tokio scheduling, task handles, blocking pools, and cancellation by hand. The implementation must:

- create an executor-owned Tokio runtime;
- run `TaskRunnable::Async` with `runtime.spawn`;
- run `TaskRunnable::Blocking` with `runtime.spawn_blocking`;
- return `ExecutorTaskHandle` values with the task id and attempt id from the request;
- route all job-emitted progress, output, and diagnostic data through
  `TaskContext::emit_job_event` and `TaskEventSink`;
- store cancellation tokens by `TaskHandle` so `cancel` can request cooperative cancellation;
- emit exactly one terminal lifecycle event for each completed attempt;
- map `TaskFailureKind::Cancelled` to `TaskEvent::cancelled`;
- map non-abortable jobs that return `Ok(())` after cancellation was requested
  to `TaskEvent::finished_after_cancel`;
- report other `TaskFailure` values and job panics as an error diagnostic
  followed by `TaskEvent::failed`.

Define an internal `TokioTaskSupervisor` that owns hidden join handles and a completion inbox. Spawned Tokio jobs must send a completion record into that inbox when they finish, fail, or panic. `TokioTaskExecutor::drain_finished` must implement the backend-neutral `TaskExecutor::drain_finished` method by draining only completion records that are already available. It must not call `block_on`, sleep, poll loops, or otherwise wait for unfinished jobs. Tests that need a finished job must wait outside `drain_finished` with bounded polling or a test-only completion signal. `drain_finished` must emit the correct terminal lifecycle event, remove finished tasks from the supervisor, and return lifecycle-specific `ExecutorDrainReport` counts. Root app integration can call this through a task executor trait object from its runtime turn without depending on Tokio types and without risking a blocked UI/app turn.

Do not expose `tokio::runtime::Runtime`, `tokio::task::JoinHandle`, or Tokio channels in the public API.

- [ ] **Step 4: Verify**

Run:

```sh
cargo test -p surgeist-task
cargo fmt --check
cargo clippy -p surgeist-task --all-targets -- -D warnings
```

Commit:

```sh
git add Cargo.toml src/lib.rs src/runtime_tokio.rs
git commit -m "Add Tokio task runtime"
```

## Task 7: Stress Prototypes For UI-Separate Task Behavior

**Files:**
- Modify: `src/testing.rs`
- Test: `src/testing.rs`

- [ ] **Step 1: Add deterministic prototype tests**

Add tests for:

```rust
#[test]
fn prototype_latest_wins_rejects_stale_completion() {
    let mut harness = TaskPrototypeHarness::latest_wins();

    harness.start_attempt(TaskAttemptId::from_u64(1));
    harness.start_attempt(TaskAttemptId::from_u64(2));
    harness.complete_attempt(TaskAttemptId::from_u64(1), "old".to_owned());
    harness.complete_attempt(TaskAttemptId::from_u64(2), "new".to_owned());
    harness.drain(DrainBudget::new().max_events(8));

    assert_eq!(harness.latest_output(), Some("new"));
    assert_eq!(harness.stale_events(), 1);
    assert_eq!(harness.snapshot().status(), TaskStatus::Completed);
}

#[test]
fn prototype_progress_flood_coalesces_and_drains_by_budget() {
    let mut harness = TaskPrototypeHarness::progress_flood();

    for index in 0..10_000 {
        harness.enqueue_progress(CoalescingKey::try_new("rows").unwrap(), index.to_string()).unwrap();
    }

    let drained = harness.drain(DrainBudget::new().max_events(128));

    assert_eq!(drained.events().len(), 1);
    assert_eq!(drained.events()[0].progress_value(), Some(&TaskProgressValue::message("9999")));
    assert_eq!(drained.coalesced_events(), 9_999);
}

#[test]
fn prototype_non_abortable_import_reports_cancelling_until_finished_after_cancel() {
    let mut harness = TaskPrototypeHarness::non_abortable_import();

    harness.start_attempt(TaskAttemptId::from_u64(1));
    harness.cancel(CancelReason::user_requested());

    assert_eq!(harness.snapshot().status(), TaskStatus::Cancelling);

    harness.finish_after_cancel();

    assert_eq!(harness.snapshot().status(), TaskStatus::FinishedAfterCancel);
    assert!(harness.snapshot().is_terminal());
}

#[test]
fn prototype_two_observers_share_one_task_until_last_observer_detaches() {
    let mut harness = TaskPrototypeHarness::shared_observers();
    let key = TaskKey::try_new("compile:main").unwrap();

    harness.observe(key.clone(), ObserverId::try_new("left").unwrap());
    harness.observe(key.clone(), ObserverId::try_new("right").unwrap());
    assert_eq!(harness.observer_count(&key), 2);

    harness.unobserve(&key, &ObserverId::try_new("left").unwrap(), &TaskPolicy::cancel_when_unobserved());
    assert_eq!(harness.observer_count(&key), 1);
    assert!(harness.is_observed(&key));

    harness.unobserve(&key, &ObserverId::try_new("right").unwrap(), &TaskPolicy::cancel_when_unobserved());
    assert_eq!(harness.observer_count(&key), 0);
    assert!(!harness.is_observed(&key));
}
```

- [ ] **Step 2: Implement prototype harness**

In `src/testing.rs`, add `TaskPrototypeHarness` with private fields:

```rust
pub struct TaskPrototypeHarness {
    record: TaskRecord,
    coordination: TaskCoordination,
    queue: TaskEventQueue<String>,
    stale_events: usize,
}
```

Expose focused methods only for the tests: `latest_wins`, `progress_flood`, `non_abortable_import`, `shared_observers`, `start_attempt`, `complete_attempt`, `cancel`, `finish_after_cancel`, `observe`, `unobserve`, `observer_count`, `is_observed`, `enqueue_progress`, `drain`, `snapshot`, `latest_output`, and `stale_events`.

- [ ] **Step 3: Verify and commit**

Run:

```sh
cargo test -p surgeist-task
cargo fmt --check
cargo clippy -p surgeist-task --all-targets -- -D warnings
```

Commit:

```sh
git add src/testing.rs
git commit -m "Add task subsystem stress prototypes"
```

## Task 8: README And Root Integration Handoff

**Files:**
- Modify: `README.md`
- Create: `plans/2026-06-28-surgeist-root-task-integration-handoff.md`

- [ ] **Step 1: Update README**

Document:

- task crate boundary;
- task lifecycle states;
- queue/coalescing behavior;
- cancellation truth for abortable and non-abortable work;
- Tokio as the task runtime foundation hidden behind Surgeist task contracts;
- baseline commands:

```sh
cargo test -p surgeist-task
cargo clippy -p surgeist-task --all-targets -- -D warnings
cargo fmt --check
```

- [ ] **Step 2: Document root-owned API artifact handoff**

Do not provision or copy `api/generator` into `surgeist-task`. The public API
generator is root-owned tooling. This task crate should leave API artifact
refresh to the root integration handoff, where root can run the source-derived
API workflow across the facade and linked crates from one place.

If `api/public-api.txt` remains stale after task implementation, do not hand-edit
it. Name that root-owned API artifact refresh is pending in the handoff plan.

- [ ] **Step 3: Write root integration handoff plan**

Create `plans/2026-06-28-surgeist-root-task-integration-handoff.md` in this task repo for the root coordinator to copy into root. It must instruct root workers to:

- replace root `src/app/task.rs` semantics with imports from `surgeist_task`;
- replace root `src/app/executor.rs` duplicate executor contracts with imports or thin app-specific adapters over `surgeist_task`;
- delete or replace root `src/app/runtime_tokio.rs`; Tokio-backed task execution now belongs in `surgeist-task`;
- replace root `src/app/coord.rs` task-owned semantics with `surgeist_task` types:
  - `CoalescingKey`;
  - `TaskEvent`;
  - `TaskEventKind::Progress`;
  - `TaskProgressEvent`;
  - `TaskProgressValue`;
  - task progress coalescing;
  - task subscription/observer counts;
  - unobserved task policy decisions.
- keep only root-specific coordination in `src/app/coord.rs`: app subscription targets, surface/resource/service mapping, and narrow `AppScope` to `TaskScope` adapters where root app state requires app-specific scope data;
- keep root `src/app/runtime.rs`, `src/app/proxy.rs`, `src/app/effect.rs`, and `src/app/provenance.rs` as app integration code, but make them consume `surgeist_task` task events, queue reports, handles, and executor requests;
- update `src/app/mod.rs` module declarations and reexports to remove the root feature-gated Tokio executor and avoid duplicate task type definitions;
- remove root's direct `tokio` dependency if root no longer owns any Tokio-specific implementation after consuming `surgeist-task`;
- remove root `app-runtime-tokio` feature if no other root-owned code uses it;
- update root app tests so they assert root integration behavior, not task crate internals;
- run the root-owned source-derived API generator or API artifact workflow for
  `surgeist-task` and the facade after the task pointer is updated; do not copy
  `api/generator` into the task crate;
- add a root diff check that no duplicate root definitions remain:

```sh
rg -n "struct CoalescingKey|struct ProgressEvent|enum TaskStatus|struct TaskRecord|enum TaskPriority|struct TaskPolicy|enum UnobservedPolicy|struct CancellationToken|enum CancelReason|struct CancelReason|enum BlockingPolicy|enum ExecutorEvent|struct TaskId|struct TaskAttemptId|struct TaskName|struct TaskKey|struct CorrelationId|struct TaskProvenance|enum TaskEvent|struct TaskEvent|enum TaskEventKind|struct TaskProgressEvent|enum TaskProgressValue|enum TaskJobEvent|enum TaskLifecycleEvent|trait RuntimeExecutor|struct SpawnRequest|struct TokioExecutor|struct TaskEventQueue|struct QueuePolicy|struct TaskCoordination|struct TaskRegistration|struct TaskHandle|struct ExecutorTaskHandle|app-runtime-tokio" src/app Cargo.toml
```

Expected: no root-owned duplicate definitions; references should point to imports or root-only adapter names with different semantics.

- run root checks before pointer update:

```sh
cargo test -p surgeist-task
cargo test -p surgeist app::tests
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
git diff --submodule=log
```

- confirm the `surgeist-task` commit pinned by root is committed, pushed, and
  fetchable from the configured submodule remote before asking root to commit
  the pointer update.

- [ ] **Step 4: Verify task crate and commit**

Run:

```sh
cargo test -p surgeist-task
cargo check -p surgeist-task --example basic_task
cargo clippy -p surgeist-task --all-targets -- -D warnings
cargo fmt --check
git diff --stat
git diff -- README.md plans/2026-06-28-surgeist-root-task-integration-handoff.md
```

Commit:

```sh
git add README.md plans/2026-06-28-surgeist-root-task-integration-handoff.md
git commit -m "Document task subsystem handoff"
```

## Final Review Checklist

Before marking implementation complete:

- Confirm every numbered task had a scoped worker result, separate reviewer
  result, reconciled findings, focused checks, and logical task commit before
  the next numbered task began.
- Run all task crate checks from Task 8.
- Request a clean reviewer against this plan and `guidance/surgeist-rust-modeling-guide.md`.
- Reviewer must verify there is no root/app/window dependency in `surgeist-task`.
- Reviewer must verify the implemented Rust API satisfies the API Shape Contract
  enough for initial consolidation without requiring app/root workers to build
  repetitive low-level event plumbing.
- Reviewer must verify `TaskEvent` carries enough task id and attempt id data for root stale-event filtering.
- Reviewer must verify cancellation does not pretend non-abortable blocking work has stopped before its terminal event arrives.
- Reviewer must verify progress coalescing is explicit and tested.
- Reviewer must verify root integration handoff deletes duplicate task behavior rather than adding conversion layers.
