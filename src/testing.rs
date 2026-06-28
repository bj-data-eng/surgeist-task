//! Test and example support for exercising task executor contracts.
//!
//! These helpers are intentionally explicit. Production task spawning still
//! requires callers to provide a real `TaskEventSink`.

use std::{
    collections::HashMap,
    future::Future,
    marker::PhantomData,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    thread,
};

use crate::{
    BlockingPolicy, BlockingTaskJob, CancelReason, CancellationToken, CoalescingKey, DrainBudget,
    DrainReport, ExecutorDrainReport, ExecutorError, ExecutorTaskHandle, ObservationChange,
    ObserverId, QueueError, QueuePolicy, TaskAttemptId, TaskContext, TaskCoordination,
    TaskDiagnosticEvent, TaskDiagnosticLevel, TaskEvent, TaskEventKind, TaskEventQueue,
    TaskEventSink, TaskEventSinkError, TaskExecutor, TaskFailure, TaskFailureKind, TaskHandle,
    TaskId, TaskJobEvent, TaskKey, TaskLifecycleEvent, TaskPolicy, TaskRecord, TaskRunnable,
    TaskScope, TaskSnapshot, TaskSpawnRequest,
};

#[derive(Debug)]
struct RecordingTaskEventSinkState<Output> {
    events: Mutex<Vec<TaskEvent<Output>>>,
}

#[derive(Debug)]
pub struct RecordingTaskEventSink<Output = ()> {
    state: Arc<RecordingTaskEventSinkState<Output>>,
}

impl<Output> RecordingTaskEventSink<Output> {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RecordingTaskEventSinkState {
                events: Mutex::new(Vec::new()),
            }),
        }
    }

    pub fn handle(&self) -> Arc<dyn TaskEventSink<Output>>
    where
        Output: Send + 'static,
    {
        Arc::new(RecordingTaskEventSinkHandle {
            state: Arc::clone(&self.state),
        })
    }
}

impl<Output> RecordingTaskEventSink<Output>
where
    Output: Clone,
{
    pub fn events(&self) -> Vec<TaskEvent<Output>> {
        self.state
            .events
            .lock()
            .expect("recording task event sink mutex poisoned")
            .clone()
    }

    pub fn terminal_lifecycle_events(&self) -> Vec<TaskLifecycleEvent> {
        self.events()
            .into_iter()
            .filter_map(|event| match event.kind() {
                TaskEventKind::Lifecycle(
                    lifecycle @ (TaskLifecycleEvent::Completed
                    | TaskLifecycleEvent::Failed
                    | TaskLifecycleEvent::Cancelled
                    | TaskLifecycleEvent::FailedToCancel
                    | TaskLifecycleEvent::FinishedAfterCancel),
                ) => Some(*lifecycle),
                _ => None,
            })
            .collect()
    }

    pub fn diagnostics(&self) -> Vec<TaskDiagnosticEvent> {
        self.events()
            .into_iter()
            .filter_map(|event| match event.kind() {
                TaskEventKind::Diagnostic(diagnostic) => Some(diagnostic.clone()),
                _ => None,
            })
            .collect()
    }
}

impl<Output> Default for RecordingTaskEventSink<Output> {
    fn default() -> Self {
        Self::new()
    }
}

struct RecordingTaskEventSinkHandle<Output> {
    state: Arc<RecordingTaskEventSinkState<Output>>,
}

impl<Output> TaskEventSink<Output> for RecordingTaskEventSinkHandle<Output>
where
    Output: Send + 'static,
{
    fn emit(&self, event: TaskEvent<Output>) -> Result<(), TaskEventSinkError> {
        self.state
            .events
            .lock()
            .expect("recording task event sink mutex poisoned")
            .push(event);
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordedTaskJob {
    message: String,
}

impl RecordedTaskJob {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl<Input, Output> BlockingTaskJob<Input, Output> for RecordedTaskJob
where
    Input: Send + 'static,
    Output: Send + 'static,
{
    fn run(
        self: Box<Self>,
        _input: Input,
        context: TaskContext<Output>,
    ) -> Result<(), TaskFailure> {
        context.progress_message(
            crate::CoalescingKey::try_new("recorded").expect("static coalescing key is valid"),
            self.message,
        )
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CancelledTaskJob;

impl CancelledTaskJob {
    pub const fn new() -> Self {
        Self
    }
}

impl<Input, Output> BlockingTaskJob<Input, Output> for CancelledTaskJob
where
    Input: Send + 'static,
    Output: Send + 'static,
{
    fn run(
        self: Box<Self>,
        _input: Input,
        _context: TaskContext<Output>,
    ) -> Result<(), TaskFailure> {
        Err(TaskFailure::cancelled("task cancelled cooperatively"))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FailingTaskJob {
    message: String,
}

impl FailingTaskJob {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl<Input, Output> BlockingTaskJob<Input, Output> for FailingTaskJob
where
    Input: Send + 'static,
    Output: Send + 'static,
{
    fn run(
        self: Box<Self>,
        _input: Input,
        _context: TaskContext<Output>,
    ) -> Result<(), TaskFailure> {
        Err(TaskFailure::failed(self.message))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FakeSpawnRecord {
    task_id: TaskId,
    attempt_id: TaskAttemptId,
    key: TaskKey,
    scope: TaskScope,
    policy: TaskPolicy,
}

impl FakeSpawnRecord {
    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub const fn attempt_id(&self) -> TaskAttemptId {
        self.attempt_id
    }

    pub fn key(&self) -> &TaskKey {
        &self.key
    }

    pub fn scope(&self) -> &TaskScope {
        &self.scope
    }

    pub fn policy(&self) -> &TaskPolicy {
        &self.policy
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FakeCancelRecord {
    task_id: TaskId,
    attempt_id: TaskAttemptId,
}

impl FakeCancelRecord {
    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub const fn attempt_id(&self) -> TaskAttemptId {
        self.attempt_id
    }
}

#[derive(Debug)]
pub struct FakeExecutor<Input, Output> {
    spawned: Vec<FakeSpawnRecord>,
    cancelled: Vec<FakeCancelRecord>,
    handles: HashMap<TaskHandle, FakeHandleState>,
    finished: ExecutorDrainReport,
    _input: PhantomData<fn(Input)>,
    _output: PhantomData<fn() -> Output>,
}

impl<Input, Output> FakeExecutor<Input, Output> {
    pub fn spawned(&self) -> &[FakeSpawnRecord] {
        &self.spawned
    }

    pub fn cancelled(&self) -> &[FakeCancelRecord] {
        &self.cancelled
    }

    fn spawn(
        &mut self,
        request: TaskSpawnRequest<Input, Output>,
    ) -> Result<ExecutorTaskHandle, ExecutorError>
    where
        Input: Send + 'static,
        Output: Send + 'static,
    {
        let (task_id, attempt_id, key, scope, policy, cancellation, input, runnable, event_sink) =
            request.into_parts();

        self.spawned.push(FakeSpawnRecord {
            task_id,
            attempt_id,
            key,
            scope,
            policy: policy.clone(),
        });

        let handle = TaskHandle::new(task_id, attempt_id);
        self.handles.insert(
            handle,
            FakeHandleState {
                cancellation: cancellation.clone(),
                terminal: false,
            },
        );

        let context = TaskContext::new(
            task_id,
            attempt_id,
            cancellation.view(),
            Arc::clone(&event_sink),
        );
        let result = run_runnable(runnable, input, context);
        let terminal = map_terminal_outcome(&policy, &cancellation, result);
        emit_terminal(
            task_id,
            attempt_id,
            terminal,
            &event_sink,
            &mut self.finished,
        )?;

        if let Some(state) = self.handles.get_mut(&handle) {
            state.terminal = true;
        }

        Ok(ExecutorTaskHandle::new(task_id, attempt_id))
    }
}

impl<Input, Output> Default for FakeExecutor<Input, Output> {
    fn default() -> Self {
        Self {
            spawned: Vec::new(),
            cancelled: Vec::new(),
            handles: HashMap::new(),
            finished: ExecutorDrainReport::new(),
            _input: PhantomData,
            _output: PhantomData,
        }
    }
}

impl<Input, Output> TaskExecutor<Input, Output> for FakeExecutor<Input, Output>
where
    Input: Send + 'static,
    Output: Send + 'static,
{
    fn spawn_task(
        &mut self,
        request: TaskSpawnRequest<Input, Output>,
    ) -> Result<ExecutorTaskHandle, ExecutorError> {
        self.spawn(request)
    }

    fn spawn_blocking_task(
        &mut self,
        request: TaskSpawnRequest<Input, Output>,
    ) -> Result<ExecutorTaskHandle, ExecutorError> {
        self.spawn(request)
    }

    fn cancel(&mut self, handle: TaskHandle) -> Result<(), ExecutorError> {
        let state = self.handles.get(&handle).ok_or_else(|| {
            ExecutorError::cancellation_failed("fake executor task handle was not found")
                .with_task_attempt(handle.task_id(), handle.attempt_id())
        })?;

        self.cancelled.push(FakeCancelRecord {
            task_id: handle.task_id(),
            attempt_id: handle.attempt_id(),
        });
        state.cancellation.cancel(CancelReason::user_requested());

        if state.terminal {
            return Err(ExecutorError::cancellation_failed(
                "fake executor task already reached a terminal state",
            )
            .with_task_attempt(handle.task_id(), handle.attempt_id()));
        }

        Ok(())
    }

    fn drain_finished(&mut self) -> ExecutorDrainReport {
        let report = self.finished;
        self.finished = ExecutorDrainReport::new();
        report
    }

    fn name(&self) -> &'static str {
        "fake"
    }
}

#[derive(Debug)]
struct FakeHandleState {
    cancellation: CancellationToken,
    terminal: bool,
}

#[derive(Debug)]
enum RunnableOutcome {
    Returned(Result<(), TaskFailure>),
    Panicked(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TerminalOutcome {
    Completed,
    Failed(String),
    Cancelled,
    FailedToCancel(String),
    FinishedAfterCancel,
}

fn run_runnable<Input, Output>(
    runnable: TaskRunnable<Input, Output>,
    input: Input,
    context: TaskContext<Output>,
) -> RunnableOutcome
where
    Input: Send + 'static,
    Output: Send + 'static,
{
    match runnable {
        TaskRunnable::Blocking(job) => {
            match catch_unwind(AssertUnwindSafe(|| job.run(input, context))) {
                Ok(result) => RunnableOutcome::Returned(result),
                Err(payload) => RunnableOutcome::Panicked(panic_message(payload)),
            }
        }
        TaskRunnable::Async(job) => {
            match catch_unwind(AssertUnwindSafe(|| job.run(input, context))) {
                Ok(future) => block_on_catching_unwind(future),
                Err(payload) => RunnableOutcome::Panicked(panic_message(payload)),
            }
        }
    }
}

fn block_on_catching_unwind(
    mut future: Pin<Box<dyn Future<Output = Result<(), TaskFailure>> + Send + 'static>>,
) -> RunnableOutcome {
    let mut context = Context::from_waker(Waker::noop());

    loop {
        match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(&mut context))) {
            Ok(Poll::Ready(result)) => return RunnableOutcome::Returned(result),
            Ok(Poll::Pending) => thread::yield_now(),
            Err(payload) => return RunnableOutcome::Panicked(panic_message(payload)),
        }
    }
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "task panicked".to_owned()
    }
}

fn map_terminal_outcome(
    policy: &TaskPolicy,
    cancellation: &CancellationToken,
    outcome: RunnableOutcome,
) -> TerminalOutcome {
    match outcome {
        RunnableOutcome::Returned(Ok(()))
            if cancellation.is_cancelled()
                && policy.blocking_policy() == BlockingPolicy::NonAbortableReportCancelling =>
        {
            TerminalOutcome::FinishedAfterCancel
        }
        RunnableOutcome::Returned(Ok(())) => TerminalOutcome::Completed,
        RunnableOutcome::Returned(Err(failure)) if failure.kind() == TaskFailureKind::Cancelled => {
            TerminalOutcome::Cancelled
        }
        RunnableOutcome::Returned(Err(failure)) if cancellation.is_cancelled() => {
            TerminalOutcome::FailedToCancel(failure.message().to_owned())
        }
        RunnableOutcome::Returned(Err(failure)) => {
            TerminalOutcome::Failed(failure.message().to_owned())
        }
        RunnableOutcome::Panicked(message) if cancellation.is_cancelled() => {
            TerminalOutcome::FailedToCancel(message)
        }
        RunnableOutcome::Panicked(message) => TerminalOutcome::Failed(message),
    }
}

fn emit_terminal<Output>(
    task_id: TaskId,
    attempt_id: TaskAttemptId,
    outcome: TerminalOutcome,
    event_sink: &Arc<dyn TaskEventSink<Output>>,
    finished: &mut ExecutorDrainReport,
) -> Result<(), ExecutorError>
where
    Output: Send + 'static,
{
    if let TerminalOutcome::Failed(message) | TerminalOutcome::FailedToCancel(message) = &outcome {
        event_sink
            .emit(TaskEvent::from_job_event(
                task_id,
                attempt_id,
                TaskJobEvent::diagnostic(TaskDiagnosticLevel::Error, message.clone()),
            ))
            .map_err(|error| {
                ExecutorError::sink_failed(format!(
                    "failed to emit fake executor diagnostic: {}",
                    error.message()
                ))
                .with_task_attempt(task_id, attempt_id)
            })?;
    }

    let event = match outcome {
        TerminalOutcome::Completed => {
            finished.increment_completed_tasks();
            TaskEvent::completed(task_id, attempt_id)
        }
        TerminalOutcome::Failed(_) => {
            finished.increment_failed_tasks();
            TaskEvent::failed(task_id, attempt_id)
        }
        TerminalOutcome::Cancelled => {
            finished.increment_cancelled_tasks();
            TaskEvent::cancelled(task_id, attempt_id)
        }
        TerminalOutcome::FailedToCancel(_) => {
            finished.increment_failed_to_cancel_tasks();
            TaskEvent::failed_to_cancel(task_id, attempt_id)
        }
        TerminalOutcome::FinishedAfterCancel => {
            finished.increment_finished_after_cancel_tasks();
            TaskEvent::finished_after_cancel(task_id, attempt_id)
        }
    };

    event_sink.emit(event).map_err(|error| {
        ExecutorError::sink_failed(format!(
            "failed to emit fake executor terminal lifecycle event: {}",
            error.message()
        ))
        .with_task_attempt(task_id, attempt_id)
    })
}

pub struct TaskPrototypeHarness {
    record: TaskRecord,
    coordination: TaskCoordination,
    queue: TaskEventQueue<String>,
    stale_events: usize,
}

impl TaskPrototypeHarness {
    pub fn latest_wins() -> Self {
        Self::new(
            TaskKey::try_new("latest-wins").expect("static task key is valid"),
            TaskPolicy::continue_when_unobserved(),
        )
    }

    pub fn progress_flood() -> Self {
        let mut harness = Self::new(
            TaskKey::try_new("progress-flood").expect("static task key is valid"),
            TaskPolicy::continue_when_unobserved(),
        );
        harness.start_attempt(TaskAttemptId::from_u64(1));
        harness
    }

    pub fn non_abortable_import() -> Self {
        Self::new(
            TaskKey::try_new("import:non-abortable").expect("static task key is valid"),
            TaskPolicy::continue_when_unobserved()
                .with_blocking_policy(BlockingPolicy::NonAbortableReportCancelling),
        )
    }

    pub fn shared_observers() -> Self {
        Self::new(
            TaskKey::try_new("compile:main").expect("static task key is valid"),
            TaskPolicy::cancel_when_unobserved(),
        )
    }

    pub fn start_attempt(&mut self, attempt_id: TaskAttemptId) {
        self.record
            .start_attempt(attempt_id)
            .expect("prototype attempt should start");
        self.record
            .mark_running(attempt_id)
            .expect("prototype attempt should run");
    }

    pub fn complete_attempt(&mut self, attempt_id: TaskAttemptId, output: String) {
        if !self.record.accepts_attempt(attempt_id) {
            self.stale_events += 1;
            return;
        }

        let output_key = TaskKey::try_new(format!("latest-output:{output}"))
            .expect("prototype output key should be valid");
        let mut record = TaskRecord::queued(
            self.record.id(),
            output_key,
            self.record.scope().clone(),
            self.record.policy().clone(),
        );
        record
            .start_attempt(attempt_id)
            .expect("prototype completion attempt should start");
        record
            .mark_running(attempt_id)
            .expect("prototype completion attempt should run");
        record
            .complete(attempt_id)
            .expect("prototype completion attempt should complete");
        self.record = record;

        self.queue
            .push(TaskEvent::from_job_event(
                self.record.id(),
                attempt_id,
                TaskJobEvent::output(output),
            ))
            .expect("prototype queue has capacity for output");
        self.queue
            .push(TaskEvent::completed(self.record.id(), attempt_id))
            .expect("prototype queue has capacity for completion");
    }

    pub fn cancel(&mut self, reason: CancelReason) {
        self.record.request_cancel(reason);
    }

    pub fn finish_after_cancel(&mut self) {
        let attempt_id = self
            .record
            .attempt_id()
            .expect("prototype attempt should be present");
        self.record
            .finish_after_cancel(attempt_id)
            .expect("prototype attempt should finish after cancel");
    }

    pub fn observe(&mut self, key: TaskKey, observer: ObserverId) {
        self.coordination.observe(key.clone(), observer);
        self.record
            .sync_observer_count(self.coordination.snapshot_count(&key));
    }

    pub fn unobserve(
        &mut self,
        key: &TaskKey,
        observer: &ObserverId,
        policy: &TaskPolicy,
    ) -> ObservationChange {
        let change = self.coordination.unobserve(key, observer, policy);
        self.record
            .sync_observer_count(self.coordination.snapshot_count(key));
        change
    }

    pub fn observer_count(&self, key: &TaskKey) -> usize {
        self.coordination.observer_count(key)
    }

    pub fn is_observed(&self, key: &TaskKey) -> bool {
        self.coordination.is_observed(key)
    }

    pub fn enqueue_progress(
        &mut self,
        key: CoalescingKey,
        message: String,
    ) -> Result<(), QueueError> {
        let attempt_id = self
            .record
            .attempt_id()
            .expect("prototype attempt should be present");
        self.queue.push(TaskEvent::from_job_event(
            self.record.id(),
            attempt_id,
            TaskJobEvent::progress_message(key, message),
        ))
    }

    pub fn drain(&mut self, budget: DrainBudget) -> DrainReport<String> {
        self.queue.drain(budget)
    }

    pub fn snapshot(&self) -> TaskSnapshot {
        self.record.snapshot()
    }

    pub fn latest_output(&self) -> Option<&str> {
        self.record.key().as_str().strip_prefix("latest-output:")
    }

    pub const fn stale_events(&self) -> usize {
        self.stale_events
    }

    fn new(key: TaskKey, policy: TaskPolicy) -> Self {
        Self {
            record: TaskRecord::queued(TaskId::from_u64(1), key, TaskScope::app(), policy),
            coordination: TaskCoordination::default(),
            queue: TaskEventQueue::new(QueuePolicy::bounded(256)),
            stale_events: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        CancelReason, CoalescingKey, DrainBudget, ObserverId, TaskAttemptId, TaskKey, TaskPolicy,
        TaskProgressValue, TaskStatus,
    };

    use super::TaskPrototypeHarness;

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
            harness
                .enqueue_progress(CoalescingKey::try_new("rows").unwrap(), index.to_string())
                .unwrap();
        }

        let drained = harness.drain(DrainBudget::new().max_events(128));

        assert_eq!(drained.events().len(), 1);
        assert_eq!(
            drained.events()[0].progress_value(),
            Some(&TaskProgressValue::message("9999"))
        );
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

        harness.unobserve(
            &key,
            &ObserverId::try_new("left").unwrap(),
            &TaskPolicy::cancel_when_unobserved(),
        );
        assert_eq!(harness.observer_count(&key), 1);
        assert!(harness.is_observed(&key));

        harness.unobserve(
            &key,
            &ObserverId::try_new("right").unwrap(),
            &TaskPolicy::cancel_when_unobserved(),
        );
        assert_eq!(harness.observer_count(&key), 0);
        assert!(!harness.is_observed(&key));
    }
}
