use std::{future::Future, pin::Pin, sync::Arc};

use crate::{
    CancellationToken, CancellationView, CoalescingKey, PercentComplete, TaskAttemptId,
    TaskDiagnosticLevel, TaskEvent, TaskId, TaskJobEvent, TaskKey, TaskPolicy, TaskScope,
};

pub trait TaskExecutor<Input, Output> {
    fn spawn_task(
        &mut self,
        request: TaskSpawnRequest<Input, Output>,
    ) -> Result<ExecutorTaskHandle, ExecutorError>;

    fn spawn_blocking_task(
        &mut self,
        request: TaskSpawnRequest<Input, Output>,
    ) -> Result<ExecutorTaskHandle, ExecutorError>;

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

impl<Input, Output> TaskRunnable<Input, Output> {
    pub fn blocking_fn<F>(function: F) -> Self
    where
        F: FnOnce(Input, TaskContext<Output>) -> Result<(), TaskFailure> + Send + 'static,
    {
        Self::blocking_job(BlockingFnTaskJob { function })
    }

    pub fn async_fn<F, Fut>(function: F) -> Self
    where
        F: FnOnce(Input, TaskContext<Output>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), TaskFailure>> + Send + 'static,
    {
        Self::async_job(AsyncFnTaskJob { function })
    }

    pub fn blocking_job<Job>(job: Job) -> Self
    where
        Job: BlockingTaskJob<Input, Output>,
    {
        Self::Blocking(Box::new(job))
    }

    pub fn async_job<Job>(job: Job) -> Self
    where
        Job: AsyncTaskJob<Input, Output>,
    {
        Self::Async(Box::new(job))
    }
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

impl<Input, Output> TaskSpawnRequest<Input, Output> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        task_id: TaskId,
        attempt_id: TaskAttemptId,
        key: TaskKey,
        scope: TaskScope,
        cancellation: CancellationToken,
        event_sink: Arc<dyn TaskEventSink<Output>>,
        input: Input,
        runnable: TaskRunnable<Input, Output>,
    ) -> Self {
        Self {
            task_id,
            attempt_id,
            key,
            scope,
            policy: TaskPolicy::continue_when_unobserved(),
            cancellation,
            input,
            runnable,
            event_sink,
        }
    }

    pub fn builder(
        task_id: TaskId,
        attempt_id: TaskAttemptId,
    ) -> TaskSpawnRequestBuilder<Input, Output> {
        TaskSpawnRequestBuilder::new(task_id, attempt_id)
    }

    pub fn with_policy(mut self, policy: TaskPolicy) -> Self {
        self.policy = policy;
        self
    }

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

    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub fn cancellation_view(&self) -> CancellationView {
        self.cancellation.view()
    }

    pub fn input(&self) -> &Input {
        &self.input
    }

    pub fn runnable(&self) -> &TaskRunnable<Input, Output> {
        &self.runnable
    }

    pub fn event_sink(&self) -> Arc<dyn TaskEventSink<Output>> {
        Arc::clone(&self.event_sink)
    }

    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        TaskId,
        TaskAttemptId,
        TaskKey,
        TaskScope,
        TaskPolicy,
        CancellationToken,
        Input,
        TaskRunnable<Input, Output>,
        Arc<dyn TaskEventSink<Output>>,
    ) {
        (
            self.task_id,
            self.attempt_id,
            self.key,
            self.scope,
            self.policy,
            self.cancellation,
            self.input,
            self.runnable,
            self.event_sink,
        )
    }
}

impl<Output> TaskSpawnRequest<(), Output> {
    pub fn new_unit(
        task_id: TaskId,
        attempt_id: TaskAttemptId,
        key: TaskKey,
        scope: TaskScope,
        cancellation: CancellationToken,
        event_sink: Arc<dyn TaskEventSink<Output>>,
        runnable: TaskRunnable<(), Output>,
    ) -> Self {
        Self::new(
            task_id,
            attempt_id,
            key,
            scope,
            cancellation,
            event_sink,
            (),
            runnable,
        )
    }
}

pub struct TaskSpawnRequestBuilder<Input = (), Output = ()> {
    task_id: TaskId,
    attempt_id: TaskAttemptId,
    key: Option<TaskKey>,
    scope: Option<TaskScope>,
    policy: TaskPolicy,
    cancellation: Option<CancellationToken>,
    input: Option<Input>,
    runnable: Option<TaskRunnable<Input, Output>>,
    event_sink: Option<Arc<dyn TaskEventSink<Output>>>,
}

impl<Input, Output> TaskSpawnRequestBuilder<Input, Output> {
    fn new(task_id: TaskId, attempt_id: TaskAttemptId) -> Self {
        Self {
            task_id,
            attempt_id,
            key: None,
            scope: None,
            policy: TaskPolicy::continue_when_unobserved(),
            cancellation: None,
            input: None,
            runnable: None,
            event_sink: None,
        }
    }

    pub fn key(mut self, key: TaskKey) -> Self {
        self.key = Some(key);
        self
    }

    pub fn scope(mut self, scope: TaskScope) -> Self {
        self.scope = Some(scope);
        self
    }

    pub fn cancellation(mut self, cancellation: CancellationToken) -> Self {
        self.cancellation = Some(cancellation);
        self
    }

    pub fn sink<S>(mut self, sink: S) -> Self
    where
        S: TaskEventSink<Output>,
    {
        self.event_sink = Some(Arc::new(sink));
        self
    }

    pub fn sink_arc(mut self, sink: Arc<dyn TaskEventSink<Output>>) -> Self {
        self.event_sink = Some(sink);
        self
    }

    pub fn input(mut self, input: Input) -> Self {
        self.input = Some(input);
        self
    }

    pub fn runnable(mut self, runnable: TaskRunnable<Input, Output>) -> Self {
        self.runnable = Some(runnable);
        self
    }

    pub fn blocking_fn<F>(self, function: F) -> Self
    where
        F: FnOnce(Input, TaskContext<Output>) -> Result<(), TaskFailure> + Send + 'static,
    {
        self.runnable(TaskRunnable::blocking_fn(function))
    }

    pub fn async_fn<F, Fut>(self, function: F) -> Self
    where
        F: FnOnce(Input, TaskContext<Output>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), TaskFailure>> + Send + 'static,
    {
        self.runnable(TaskRunnable::async_fn(function))
    }

    pub fn policy(mut self, policy: TaskPolicy) -> Self {
        self.policy = policy;
        self
    }

    pub fn build(self) -> Result<TaskSpawnRequest<Input, Output>, ExecutorError> {
        let key = self
            .key
            .ok_or_else(|| ExecutorError::invalid_request("missing task spawn request key"))?;
        let scope = self
            .scope
            .ok_or_else(|| ExecutorError::invalid_request("missing task spawn request scope"))?;
        let cancellation = self.cancellation.ok_or_else(|| {
            ExecutorError::invalid_request("missing task spawn request cancellation token")
        })?;
        let event_sink = self
            .event_sink
            .ok_or_else(|| ExecutorError::invalid_request("missing task spawn request sink"))?;
        let input = self
            .input
            .ok_or_else(|| ExecutorError::invalid_request("missing task spawn request input"))?;
        let runnable = self
            .runnable
            .ok_or_else(|| ExecutorError::invalid_request("missing task spawn request runnable"))?;

        Ok(TaskSpawnRequest {
            task_id: self.task_id,
            attempt_id: self.attempt_id,
            key,
            scope,
            policy: self.policy,
            cancellation,
            input,
            runnable,
            event_sink,
        })
    }
}

impl<Output> TaskSpawnRequestBuilder<(), Output> {
    pub fn unit_input(mut self) -> Self {
        self.input = Some(());
        self
    }
}

pub struct TaskContext<Output = ()> {
    task_id: TaskId,
    attempt_id: TaskAttemptId,
    cancellation: CancellationView,
    event_sink: Arc<dyn TaskEventSink<Output>>,
}

impl<Output> TaskContext<Output>
where
    Output: 'static,
{
    pub fn new(
        task_id: TaskId,
        attempt_id: TaskAttemptId,
        cancellation: CancellationView,
        event_sink: Arc<dyn TaskEventSink<Output>>,
    ) -> Self {
        Self {
            task_id,
            attempt_id,
            cancellation,
            event_sink,
        }
    }

    #[cfg(test)]
    pub fn for_test(
        task_id: TaskId,
        attempt_id: TaskAttemptId,
        cancellation: CancellationToken,
        event_sink: Arc<dyn TaskEventSink<Output>>,
    ) -> Self {
        Self::new(task_id, attempt_id, cancellation.view(), event_sink)
    }

    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub const fn attempt_id(&self) -> TaskAttemptId {
        self.attempt_id
    }

    pub fn cancellation_view(&self) -> CancellationView {
        self.cancellation.clone()
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    pub fn emit_job_event(&self, event: TaskJobEvent<Output>) -> Result<(), TaskFailure> {
        self.event_sink
            .emit(TaskEvent::from_job_event(
                self.task_id,
                self.attempt_id,
                event,
            ))
            .map_err(|error| {
                TaskFailure::sink_failed(format!("task event sink failed: {}", error.message()))
            })
    }

    pub fn progress_units(
        &self,
        key: CoalescingKey,
        completed: u64,
        total: Option<u64>,
    ) -> Result<(), TaskFailure> {
        self.emit_job_event(TaskJobEvent::progress_units(key, completed, total))
    }

    pub fn progress_percent(
        &self,
        key: CoalescingKey,
        percent: PercentComplete,
    ) -> Result<(), TaskFailure> {
        self.emit_job_event(TaskJobEvent::progress_percent(key, percent))
    }

    pub fn progress_message(
        &self,
        key: CoalescingKey,
        message: impl Into<String>,
    ) -> Result<(), TaskFailure> {
        self.emit_job_event(TaskJobEvent::progress_message(key, message))
    }

    pub fn output(&self, output: Output) -> Result<(), TaskFailure> {
        self.emit_job_event(TaskJobEvent::output(output))
    }

    pub fn diagnostic(
        &self,
        level: TaskDiagnosticLevel,
        message: impl Into<String>,
    ) -> Result<(), TaskFailure> {
        self.emit_job_event(TaskJobEvent::diagnostic(level, message))
    }
}

struct BlockingFnTaskJob<F> {
    function: F,
}

impl<Input, Output, F> BlockingTaskJob<Input, Output> for BlockingFnTaskJob<F>
where
    F: FnOnce(Input, TaskContext<Output>) -> Result<(), TaskFailure> + Send + 'static,
{
    fn run(self: Box<Self>, input: Input, context: TaskContext<Output>) -> Result<(), TaskFailure> {
        let Self { function } = *self;
        function(input, context)
    }
}

struct AsyncFnTaskJob<F> {
    function: F,
}

impl<Input, Output, F, Fut> AsyncTaskJob<Input, Output> for AsyncFnTaskJob<F>
where
    F: FnOnce(Input, TaskContext<Output>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), TaskFailure>> + Send + 'static,
{
    fn run(
        self: Box<Self>,
        input: Input,
        context: TaskContext<Output>,
    ) -> Pin<Box<dyn Future<Output = Result<(), TaskFailure>> + Send + 'static>> {
        let Self { function } = *self;
        Box::pin(function(input, context))
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TaskHandle {
    task_id: TaskId,
    attempt_id: TaskAttemptId,
}

impl TaskHandle {
    pub const fn new(task_id: TaskId, attempt_id: TaskAttemptId) -> Self {
        Self {
            task_id,
            attempt_id,
        }
    }

    pub const fn task_id(self) -> TaskId {
        self.task_id
    }

    pub const fn attempt_id(self) -> TaskAttemptId {
        self.attempt_id
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ExecutorTaskHandle {
    task_id: TaskId,
    attempt_id: TaskAttemptId,
}

impl ExecutorTaskHandle {
    pub const fn new(task_id: TaskId, attempt_id: TaskAttemptId) -> Self {
        Self {
            task_id,
            attempt_id,
        }
    }

    pub const fn task_id(self) -> TaskId {
        self.task_id
    }

    pub const fn attempt_id(self) -> TaskAttemptId {
        self.attempt_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutorError {
    kind: ExecutorErrorKind,
    task_id: Option<TaskId>,
    attempt_id: Option<TaskAttemptId>,
    message: String,
}

impl ExecutorError {
    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self::new(ExecutorErrorKind::InvalidRequest, message)
    }

    pub fn spawn_failed(message: impl Into<String>) -> Self {
        Self::new(ExecutorErrorKind::SpawnFailed, message)
    }

    pub fn cancellation_failed(message: impl Into<String>) -> Self {
        Self::new(ExecutorErrorKind::CancellationFailed, message)
    }

    pub fn join_failed(message: impl Into<String>) -> Self {
        Self::new(ExecutorErrorKind::JoinFailed, message)
    }

    pub fn sink_failed(message: impl Into<String>) -> Self {
        Self::new(ExecutorErrorKind::SinkFailed, message)
    }

    pub fn with_task_id(mut self, task_id: TaskId) -> Self {
        self.task_id = Some(task_id);
        self
    }

    pub fn with_attempt_id(mut self, attempt_id: TaskAttemptId) -> Self {
        self.attempt_id = Some(attempt_id);
        self
    }

    pub fn with_task_attempt(mut self, task_id: TaskId, attempt_id: TaskAttemptId) -> Self {
        self.task_id = Some(task_id);
        self.attempt_id = Some(attempt_id);
        self
    }

    pub const fn kind(&self) -> ExecutorErrorKind {
        self.kind
    }

    pub const fn task_id(&self) -> Option<TaskId> {
        self.task_id
    }

    pub const fn attempt_id(&self) -> Option<TaskAttemptId> {
        self.attempt_id
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    fn new(kind: ExecutorErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            task_id: None,
            attempt_id: None,
            message: message.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ExecutorErrorKind {
    InvalidRequest,
    SpawnFailed,
    CancellationFailed,
    JoinFailed,
    SinkFailed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskFailure {
    kind: TaskFailureKind,
    message: String,
}

impl TaskFailure {
    pub fn failed(message: impl Into<String>) -> Self {
        Self::new(TaskFailureKind::Failed, message)
    }

    pub fn cancelled(message: impl Into<String>) -> Self {
        Self::new(TaskFailureKind::Cancelled, message)
    }

    pub fn panicked(message: impl Into<String>) -> Self {
        Self::new(TaskFailureKind::Panicked, message)
    }

    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::new(TaskFailureKind::InvalidInput, message)
    }

    pub fn sink_failed(message: impl Into<String>) -> Self {
        Self::new(TaskFailureKind::SinkFailed, message)
    }

    pub const fn kind(&self) -> TaskFailureKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    fn new(kind: TaskFailureKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TaskFailureKind {
    Cancelled,
    Panicked,
    Failed,
    InvalidInput,
    SinkFailed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskEventSinkError {
    kind: TaskEventSinkErrorKind,
    message: String,
}

impl TaskEventSinkError {
    pub fn closed(message: impl Into<String>) -> Self {
        Self::new(TaskEventSinkErrorKind::Closed, message)
    }

    pub fn overflow(message: impl Into<String>) -> Self {
        Self::new(TaskEventSinkErrorKind::Overflow, message)
    }

    pub fn serialization_failed(message: impl Into<String>) -> Self {
        Self::new(TaskEventSinkErrorKind::SerializationFailed, message)
    }

    pub const fn kind(&self) -> TaskEventSinkErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    fn new(kind: TaskEventSinkErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TaskEventSinkErrorKind {
    Closed,
    Overflow,
    SerializationFailed,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ExecutorDrainReport {
    completed_tasks: usize,
    failed_tasks: usize,
    cancelled_tasks: usize,
    finished_after_cancel_tasks: usize,
    failed_to_cancel_tasks: usize,
}

impl ExecutorDrainReport {
    pub const fn new() -> Self {
        Self {
            completed_tasks: 0,
            failed_tasks: 0,
            cancelled_tasks: 0,
            finished_after_cancel_tasks: 0,
            failed_to_cancel_tasks: 0,
        }
    }

    pub const fn from_counts(
        completed_tasks: usize,
        failed_tasks: usize,
        cancelled_tasks: usize,
        finished_after_cancel_tasks: usize,
        failed_to_cancel_tasks: usize,
    ) -> Self {
        Self {
            completed_tasks,
            failed_tasks,
            cancelled_tasks,
            finished_after_cancel_tasks,
            failed_to_cancel_tasks,
        }
    }

    pub const fn completed_tasks(self) -> usize {
        self.completed_tasks
    }

    pub const fn failed_tasks(self) -> usize {
        self.failed_tasks
    }

    pub const fn cancelled_tasks(self) -> usize {
        self.cancelled_tasks
    }

    pub const fn finished_after_cancel_tasks(self) -> usize {
        self.finished_after_cancel_tasks
    }

    pub const fn failed_to_cancel_tasks(self) -> usize {
        self.failed_to_cancel_tasks
    }

    pub const fn with_completed_tasks(mut self, completed_tasks: usize) -> Self {
        self.completed_tasks = completed_tasks;
        self
    }

    pub const fn with_failed_tasks(mut self, failed_tasks: usize) -> Self {
        self.failed_tasks = failed_tasks;
        self
    }

    pub const fn with_cancelled_tasks(mut self, cancelled_tasks: usize) -> Self {
        self.cancelled_tasks = cancelled_tasks;
        self
    }

    pub const fn with_finished_after_cancel_tasks(
        mut self,
        finished_after_cancel_tasks: usize,
    ) -> Self {
        self.finished_after_cancel_tasks = finished_after_cancel_tasks;
        self
    }

    pub const fn with_failed_to_cancel_tasks(mut self, failed_to_cancel_tasks: usize) -> Self {
        self.failed_to_cancel_tasks = failed_to_cancel_tasks;
        self
    }

    pub fn increment_completed_tasks(&mut self) {
        self.completed_tasks += 1;
    }

    pub fn increment_failed_tasks(&mut self) {
        self.failed_tasks += 1;
    }

    pub fn increment_cancelled_tasks(&mut self) {
        self.cancelled_tasks += 1;
    }

    pub fn increment_finished_after_cancel_tasks(&mut self) {
        self.finished_after_cancel_tasks += 1;
    }

    pub fn increment_failed_to_cancel_tasks(&mut self) {
        self.failed_to_cancel_tasks += 1;
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        pin::Pin,
        sync::{Arc, Mutex},
    };

    use crate::{
        BlockingPolicy, CancelReason, CancellationToken, PercentComplete, TaskAttemptId,
        TaskDiagnosticLevel, TaskEvent, TaskEventKind, TaskId, TaskKey, TaskLifecycleEvent,
        TaskPolicy, TaskPriority, TaskProgressValue, TaskScope,
    };

    use crate::testing::{
        CancelledTaskJob, FailingTaskJob, FakeExecutor, RecordedTaskJob, RecordingTaskEventSink,
    };

    use super::{
        AsyncTaskJob, ExecutorDrainReport, ExecutorError, ExecutorErrorKind, ExecutorTaskHandle,
        TaskContext, TaskEventSink, TaskEventSinkError, TaskEventSinkErrorKind, TaskExecutor,
        TaskFailure, TaskFailureKind, TaskHandle, TaskRunnable, TaskSpawnRequest,
    };

    #[test]
    fn task_handles_preserve_task_and_attempt_ids() {
        let task_id = TaskId::from_u64(7);
        let attempt_id = TaskAttemptId::from_u64(3);

        let task_handle = TaskHandle::new(task_id, attempt_id);
        let executor_handle = ExecutorTaskHandle::new(task_id, attempt_id);

        assert_eq!(task_handle.task_id(), task_id);
        assert_eq!(task_handle.attempt_id(), attempt_id);
        assert_eq!(executor_handle.task_id(), task_id);
        assert_eq!(executor_handle.attempt_id(), attempt_id);
    }

    #[test]
    fn executor_errors_preserve_kind_optional_ids_and_message() {
        let error = ExecutorError::spawn_failed("runtime refused task")
            .with_task_attempt(TaskId::from_u64(11), TaskAttemptId::from_u64(2));

        assert_eq!(error.kind(), ExecutorErrorKind::SpawnFailed);
        assert_eq!(error.task_id(), Some(TaskId::from_u64(11)));
        assert_eq!(error.attempt_id(), Some(TaskAttemptId::from_u64(2)));
        assert_eq!(error.message(), "runtime refused task");

        let unscoped = ExecutorError::invalid_request("missing runnable");

        assert_eq!(unscoped.kind(), ExecutorErrorKind::InvalidRequest);
        assert_eq!(unscoped.task_id(), None);
        assert_eq!(unscoped.attempt_id(), None);
        assert_eq!(unscoped.message(), "missing runnable");
    }

    #[test]
    fn task_failures_preserve_kind_and_message() {
        let cases = [
            (
                TaskFailure::failed("ordinary failure"),
                TaskFailureKind::Failed,
                "ordinary failure",
            ),
            (
                TaskFailure::cancelled("stopped cooperatively"),
                TaskFailureKind::Cancelled,
                "stopped cooperatively",
            ),
            (
                TaskFailure::invalid_input("bad input"),
                TaskFailureKind::InvalidInput,
                "bad input",
            ),
            (
                TaskFailure::sink_failed("sink closed"),
                TaskFailureKind::SinkFailed,
                "sink closed",
            ),
            (
                TaskFailure::panicked("panic payload"),
                TaskFailureKind::Panicked,
                "panic payload",
            ),
        ];

        for (failure, kind, message) in cases {
            assert_eq!(failure.kind(), kind);
            assert_eq!(failure.message(), message);
        }
    }

    #[test]
    fn event_sink_errors_preserve_kind_and_message() {
        let cases = [
            (
                TaskEventSinkError::closed("receiver dropped"),
                TaskEventSinkErrorKind::Closed,
                "receiver dropped",
            ),
            (
                TaskEventSinkError::overflow("queue full"),
                TaskEventSinkErrorKind::Overflow,
                "queue full",
            ),
            (
                TaskEventSinkError::serialization_failed("payload failed"),
                TaskEventSinkErrorKind::SerializationFailed,
                "payload failed",
            ),
        ];

        for (error, kind, message) in cases {
            assert_eq!(error.kind(), kind);
            assert_eq!(error.message(), message);
        }
    }

    #[test]
    fn executor_drain_report_counters_start_at_zero_and_represent_counts() {
        let empty = ExecutorDrainReport::new();

        assert_eq!(empty.completed_tasks(), 0);
        assert_eq!(empty.failed_tasks(), 0);
        assert_eq!(empty.cancelled_tasks(), 0);
        assert_eq!(empty.finished_after_cancel_tasks(), 0);
        assert_eq!(empty.failed_to_cancel_tasks(), 0);

        let report = ExecutorDrainReport::new()
            .with_completed_tasks(2)
            .with_failed_tasks(3)
            .with_cancelled_tasks(5)
            .with_finished_after_cancel_tasks(7)
            .with_failed_to_cancel_tasks(11);

        assert_eq!(report.completed_tasks(), 2);
        assert_eq!(report.failed_tasks(), 3);
        assert_eq!(report.cancelled_tasks(), 5);
        assert_eq!(report.finished_after_cancel_tasks(), 7);
        assert_eq!(report.failed_to_cancel_tasks(), 11);
    }

    #[test]
    fn spawn_request_builder_requires_explicit_production_fields() {
        let error = TaskSpawnRequest::<String, String>::builder(task_id(), attempt_id())
            .build()
            .err()
            .unwrap();

        assert_eq!(error.kind(), ExecutorErrorKind::InvalidRequest);
        assert!(error.message().contains("key"));

        let error = TaskSpawnRequest::<String, String>::builder(task_id(), attempt_id())
            .key(task_key())
            .scope(task_scope())
            .cancellation(CancellationToken::new())
            .input("input".to_owned())
            .blocking_fn(|_, _| Ok(()))
            .build()
            .err()
            .unwrap();

        assert_eq!(error.kind(), ExecutorErrorKind::InvalidRequest);
        assert!(error.message().contains("sink"));

        let request = TaskSpawnRequest::builder(task_id(), attempt_id())
            .key(task_key())
            .scope(task_scope())
            .cancellation(CancellationToken::new())
            .sink(RecordingSink::<String>::default())
            .input("input".to_owned())
            .blocking_fn(|input, context| {
                assert_eq!(input, "input");
                context.output("output".to_owned())
            })
            .build()
            .unwrap();

        assert_eq!(request.task_id(), task_id());
        assert_eq!(request.attempt_id(), attempt_id());
        assert_eq!(request.key(), &task_key());
        assert_eq!(request.scope(), &task_scope());
    }

    #[test]
    fn unit_input_supplies_no_input_value() {
        let request = TaskSpawnRequest::<(), String>::builder(task_id(), attempt_id())
            .key(task_key())
            .scope(task_scope())
            .cancellation(CancellationToken::new())
            .sink(RecordingSink::<String>::default())
            .unit_input()
            .blocking_fn(|(), _| Ok(()))
            .build()
            .unwrap();

        assert_eq!(request.input(), &());
    }

    #[test]
    fn spawn_request_constructors_preserve_identity_policy_cancellation_sink_and_runnable() {
        let cancellation = CancellationToken::new();
        let sink = Arc::new(RecordingSink::<String>::default());
        let policy = TaskPolicy::continue_when_unobserved().with_priority(TaskPriority::High);

        let request = TaskSpawnRequest::new(
            task_id(),
            attempt_id(),
            task_key(),
            task_scope(),
            cancellation.clone(),
            sink.clone(),
            "payload".to_owned(),
            TaskRunnable::blocking_fn(|_, _| Ok(())),
        )
        .with_policy(policy.clone());

        assert_eq!(request.task_id(), task_id());
        assert_eq!(request.attempt_id(), attempt_id());
        assert_eq!(request.key(), &task_key());
        assert_eq!(request.scope(), &task_scope());
        assert_eq!(request.policy(), &policy);
        assert!(!request.cancellation_view().is_cancelled());
        cancellation.cancel(CancelReason::user_requested());
        assert!(request.cancellation_view().is_cancelled());
        assert_eq!(Arc::strong_count(&sink), 2);
        assert!(matches!(request.runnable(), TaskRunnable::Blocking(_)));

        let unit = TaskSpawnRequest::<(), String>::new_unit(
            task_id(),
            attempt_id(),
            task_key(),
            task_scope(),
            CancellationToken::new(),
            Arc::new(RecordingSink::<String>::default()),
            TaskRunnable::async_fn(|(), _| async { Ok(()) }),
        );

        assert_eq!(unit.input(), &());
        assert!(matches!(unit.runnable(), TaskRunnable::Async(_)));
    }

    #[test]
    fn task_context_helpers_emit_non_terminal_typed_events() {
        let sink = Arc::new(RecordingSink::<String>::default());
        let context = TaskContext::for_test(
            task_id(),
            attempt_id(),
            CancellationToken::new(),
            sink.clone(),
        );

        context
            .progress_units(progress_key(), 10, Some(100))
            .unwrap();
        context
            .progress_percent(
                progress_key(),
                PercentComplete::try_from_basis_points(2_500).unwrap(),
            )
            .unwrap();
        context.progress_message(progress_key(), "halfway").unwrap();
        context.output("loaded".to_owned()).unwrap();
        context
            .diagnostic(TaskDiagnosticLevel::Warning, "careful")
            .unwrap();

        let events = sink.events();
        assert_eq!(events.len(), 5);
        assert!(events.iter().all(|event| {
            event.provenance().task_id() == task_id()
                && event.provenance().attempt_id() == attempt_id()
                && !matches!(event.kind(), TaskEventKind::Lifecycle(_))
        }));
        assert!(matches!(
            events[0].kind(),
            TaskEventKind::Progress(progress)
                if matches!(progress.value(), TaskProgressValue::Units {
                    completed: 10,
                    total: Some(100)
                })
        ));
        assert!(matches!(
            events[3].kind(),
            TaskEventKind::Output(output) if output.output() == "loaded"
        ));
        assert!(matches!(
            events[4].kind(),
            TaskEventKind::Diagnostic(diagnostic)
                if diagnostic.level() == TaskDiagnosticLevel::Warning
                    && diagnostic.message() == "careful"
        ));
    }

    #[test]
    fn task_context_maps_sink_errors_to_task_failures() {
        let context = TaskContext::<String>::for_test(
            task_id(),
            attempt_id(),
            CancellationToken::new(),
            Arc::new(FailingSink),
        );

        let failure = context.output("lost".to_owned()).unwrap_err();

        assert_eq!(failure.kind(), TaskFailureKind::SinkFailed);
        assert!(failure.message().contains("sink closed"));
    }

    #[test]
    fn task_context_exposes_read_only_cancellation_view() {
        let cancellation = CancellationToken::new();
        let context = TaskContext::<String>::for_test(
            task_id(),
            attempt_id(),
            cancellation.clone(),
            Arc::new(RecordingSink::default()),
        );

        assert_eq!(context.task_id(), task_id());
        assert_eq!(context.attempt_id(), attempt_id());
        assert!(!context.is_cancelled());
        assert!(!context.cancellation_view().is_cancelled());

        cancellation.cancel(CancelReason::user_requested());

        assert!(context.is_cancelled());
        assert_eq!(
            context.cancellation_view().reason().unwrap().kind(),
            crate::CancelReasonKind::UserRequested
        );
    }

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
        assert_eq!(executor.spawned()[0].task_id(), TaskId::from_u64(1));
        assert_eq!(
            executor.spawned()[0].attempt_id(),
            TaskAttemptId::from_u64(1)
        );
        assert_eq!(
            executor.spawned()[0].key(),
            &TaskKey::try_new("import:orders").unwrap()
        );
        assert_eq!(executor.spawned()[0].scope(), &TaskScope::app());
        assert_eq!(
            executor.spawned()[0].policy(),
            &TaskPolicy::continue_when_unobserved()
        );

        executor
            .cancel(TaskHandle::new(
                TaskId::from_u64(1),
                TaskAttemptId::from_u64(1),
            ))
            .unwrap_err();
        assert_eq!(executor.cancelled().len(), 1);
        assert_eq!(executor.cancelled()[0].task_id(), TaskId::from_u64(1));
        assert_eq!(
            executor.cancelled()[0].attempt_id(),
            TaskAttemptId::from_u64(1)
        );
        assert!(token.is_cancelled());
        assert_eq!(
            sink.events()[0].progress_value(),
            Some(&TaskProgressValue::message("loaded"))
        );
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
        .with_policy(
            TaskPolicy::continue_when_unobserved()
                .with_blocking_policy(BlockingPolicy::NonAbortableReportCancelling),
        );

        assert_eq!(
            request.policy().blocking_policy(),
            BlockingPolicy::NonAbortableReportCancelling
        );
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
        .with_policy(
            TaskPolicy::continue_when_unobserved()
                .with_blocking_policy(BlockingPolicy::NonAbortableReportCancelling),
        );

        token.cancel(CancelReason::user_requested());
        executor.spawn_blocking_task(request).unwrap();
        let report = executor.drain_finished();
        let cleared = executor.drain_finished();

        assert_eq!(report.finished_after_cancel_tasks(), 1);
        assert_eq!(cleared.finished_after_cancel_tasks(), 0);
        assert_eq!(
            sink.terminal_lifecycle_events(),
            vec![TaskLifecycleEvent::FinishedAfterCancel]
        );
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
        assert_eq!(
            sink.terminal_lifecycle_events(),
            vec![TaskLifecycleEvent::Cancelled]
        );
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
        assert_eq!(
            sink.terminal_lifecycle_events(),
            vec![TaskLifecycleEvent::Completed]
        );
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
        assert_eq!(
            sink.terminal_lifecycle_events(),
            vec![TaskLifecycleEvent::Failed]
        );
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
        executor
            .cancel(TaskHandle::new(
                TaskId::from_u64(7),
                TaskAttemptId::from_u64(1),
            ))
            .unwrap_err();
        let report = executor.drain_finished();
        let cleared = executor.drain_finished();

        assert_eq!(report.completed_tasks(), 1);
        assert_eq!(report.failed_to_cancel_tasks(), 0);
        assert_eq!(cleared.failed_to_cancel_tasks(), 0);
        assert_eq!(
            sink.terminal_lifecycle_events(),
            vec![TaskLifecycleEvent::Completed]
        );
    }

    #[test]
    fn fake_executor_maps_async_job_construction_panic_to_failed_lifecycle() {
        let mut executor = FakeExecutor::<(), ()>::default();
        let sink = RecordingTaskEventSink::new();
        let request = TaskSpawnRequest::new_unit(
            TaskId::from_u64(8),
            TaskAttemptId::from_u64(1),
            TaskKey::try_new("panic:async-construction").unwrap(),
            TaskScope::app(),
            CancellationToken::new(),
            sink.handle(),
            TaskRunnable::async_job(PanickingAsyncConstructionJob),
        );

        executor.spawn_task(request).unwrap();
        let report = executor.drain_finished();
        let cleared = executor.drain_finished();

        assert_eq!(report.failed_tasks(), 1);
        assert_eq!(cleared.failed_tasks(), 0);
        assert_eq!(
            sink.terminal_lifecycle_events(),
            vec![TaskLifecycleEvent::Failed]
        );
        assert_eq!(sink.diagnostics().len(), 1);
        assert_eq!(sink.diagnostics()[0].message(), "async construction panic");
    }

    struct PanickingAsyncConstructionJob;

    impl AsyncTaskJob<(), ()> for PanickingAsyncConstructionJob {
        fn run(
            self: Box<Self>,
            _input: (),
            _context: TaskContext<()>,
        ) -> Pin<Box<dyn Future<Output = Result<(), TaskFailure>> + Send + 'static>> {
            panic!("async construction panic")
        }
    }

    fn task_id() -> TaskId {
        TaskId::from_u64(42)
    }

    fn attempt_id() -> TaskAttemptId {
        TaskAttemptId::from_u64(3)
    }

    fn task_key() -> TaskKey {
        TaskKey::try_new("import:orders").unwrap()
    }

    fn task_scope() -> TaskScope {
        TaskScope::try_workspace("alpha").unwrap()
    }

    fn progress_key() -> crate::CoalescingKey {
        crate::CoalescingKey::try_new("rows").unwrap()
    }

    #[derive(Default)]
    struct RecordingSink<Output> {
        events: Mutex<Vec<TaskEvent<Output>>>,
    }

    impl<Output> RecordingSink<Output>
    where
        Output: Clone + Send + 'static,
    {
        fn events(&self) -> Vec<TaskEvent<Output>> {
            self.events.lock().expect("events mutex poisoned").clone()
        }
    }

    impl<Output> TaskEventSink<Output> for RecordingSink<Output>
    where
        Output: Send + 'static,
    {
        fn emit(&self, event: TaskEvent<Output>) -> Result<(), TaskEventSinkError> {
            self.events
                .lock()
                .expect("events mutex poisoned")
                .push(event);
            Ok(())
        }
    }

    struct FailingSink;

    impl TaskEventSink<String> for FailingSink {
        fn emit(&self, _event: TaskEvent<String>) -> Result<(), TaskEventSinkError> {
            Err(TaskEventSinkError::closed("sink closed"))
        }
    }
}
