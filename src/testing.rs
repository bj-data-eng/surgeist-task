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
    BlockingPolicy, BlockingTaskJob, CancelReason, CancellationToken, ExecutorDrainReport,
    ExecutorError, ExecutorTaskHandle, TaskAttemptId, TaskContext, TaskDiagnosticEvent,
    TaskDiagnosticLevel, TaskEvent, TaskEventKind, TaskEventSink, TaskEventSinkError, TaskExecutor,
    TaskFailure, TaskFailureKind, TaskHandle, TaskId, TaskJobEvent, TaskKey, TaskLifecycleEvent,
    TaskPolicy, TaskRunnable, TaskScope, TaskSpawnRequest,
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
        RunnableOutcome::Returned(Err(failure)) => {
            TerminalOutcome::Failed(failure.message().to_owned())
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
    if let TerminalOutcome::Failed(message) = &outcome {
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
