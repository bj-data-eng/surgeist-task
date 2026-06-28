use std::{
    collections::HashMap,
    sync::{Arc, mpsc},
};

use tokio::{runtime::Runtime, task::JoinHandle};

use crate::{
    BlockingPolicy, CancelReason, CancellationToken, ExecutorDrainReport, ExecutorError,
    ExecutorTaskHandle, TaskContext, TaskDiagnosticLevel, TaskEvent, TaskEventSink, TaskExecutor,
    TaskFailure, TaskFailureKind, TaskHandle, TaskId, TaskJobEvent, TaskPolicy, TaskRunnable,
    TaskSpawnRequest,
};

pub struct TokioTaskExecutor {
    runtime: Runtime,
    supervisor: TokioTaskSupervisor,
}

impl TokioTaskExecutor {
    pub fn new() -> Result<Self, ExecutorError> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_time()
            .build()
            .map_err(|error| ExecutorError::spawn_failed(error.to_string()))?;

        Ok(Self {
            runtime,
            supervisor: TokioTaskSupervisor::new(),
        })
    }

    pub fn name(&self) -> &'static str {
        "tokio"
    }

    fn spawn<Input, Output>(
        &mut self,
        request: TaskSpawnRequest<Input, Output>,
        mode: SpawnMode,
    ) -> Result<ExecutorTaskHandle, ExecutorError>
    where
        Input: Send + 'static,
        Output: Send + 'static,
    {
        let (task_id, attempt_id, _key, _scope, policy, cancellation, input, runnable, event_sink) =
            request.into_parts();
        let handle = TaskHandle::new(task_id, attempt_id);
        let cancellation_for_state = cancellation.clone();

        let context = TaskContext::new(
            task_id,
            attempt_id,
            cancellation.view(),
            Arc::clone(&event_sink),
        );
        let completion_sender = self.supervisor.completion_sender();
        let task = match (mode, runnable) {
            (SpawnMode::Async, TaskRunnable::Async(job)) => {
                let job_handle = self
                    .runtime
                    .spawn(async move { job.run(input, context).await });
                self.runtime.spawn(async move {
                    let joined = job_handle.await;
                    let outcome = RunnableOutcome::from_join_result(joined);
                    let record = CompletionRecord::new(
                        task_id,
                        attempt_id,
                        policy,
                        cancellation.is_cancelled(),
                        outcome,
                    );
                    let _ = completion_sender.send(record.into_emitter(event_sink));
                })
            }
            (SpawnMode::Blocking, TaskRunnable::Blocking(job)) => {
                let job_handle = self.runtime.spawn_blocking(move || job.run(input, context));
                self.runtime.spawn(async move {
                    let joined = job_handle.await;
                    let outcome = RunnableOutcome::from_join_result(joined);
                    let record = CompletionRecord::new(
                        task_id,
                        attempt_id,
                        policy,
                        cancellation.is_cancelled(),
                        outcome,
                    );
                    let _ = completion_sender.send(record.into_emitter(event_sink));
                })
            }
            (SpawnMode::Async, TaskRunnable::Blocking(_)) => {
                return Err(ExecutorError::invalid_request(
                    "spawn_task requires an async task runnable",
                )
                .with_task_attempt(task_id, attempt_id));
            }
            (SpawnMode::Blocking, TaskRunnable::Async(_)) => {
                return Err(ExecutorError::invalid_request(
                    "spawn_blocking_task requires a blocking task runnable",
                )
                .with_task_attempt(task_id, attempt_id));
            }
        };

        self.supervisor.insert(
            handle,
            TokioTaskState {
                cancellation: cancellation_for_state,
                _join_handle: task,
            },
        );

        Ok(ExecutorTaskHandle::new(task_id, attempt_id))
    }
}

impl<Input, Output> TaskExecutor<Input, Output> for TokioTaskExecutor
where
    Input: Send + 'static,
    Output: Send + 'static,
{
    fn spawn_task(
        &mut self,
        request: TaskSpawnRequest<Input, Output>,
    ) -> Result<ExecutorTaskHandle, ExecutorError> {
        self.spawn(request, SpawnMode::Async)
    }

    fn spawn_blocking_task(
        &mut self,
        request: TaskSpawnRequest<Input, Output>,
    ) -> Result<ExecutorTaskHandle, ExecutorError> {
        self.spawn(request, SpawnMode::Blocking)
    }

    fn cancel(&mut self, handle: TaskHandle) -> Result<(), ExecutorError> {
        let state = self.supervisor.active.get(&handle).ok_or_else(|| {
            ExecutorError::cancellation_failed("tokio executor task handle was not found")
                .with_task_attempt(handle.task_id(), handle.attempt_id())
        })?;

        state.cancellation.cancel(CancelReason::user_requested());
        Ok(())
    }

    fn drain_finished(&mut self) -> ExecutorDrainReport {
        self.supervisor.drain_finished()
    }

    fn name(&self) -> &'static str {
        self.name()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SpawnMode {
    Async,
    Blocking,
}

struct TokioTaskSupervisor {
    active: HashMap<TaskHandle, TokioTaskState>,
    completed_sender: mpsc::Sender<CompletionEmitter>,
    completed_receiver: mpsc::Receiver<CompletionEmitter>,
}

impl TokioTaskSupervisor {
    fn new() -> Self {
        let (completed_sender, completed_receiver) = mpsc::channel();
        Self {
            active: HashMap::new(),
            completed_sender,
            completed_receiver,
        }
    }

    fn completion_sender(&self) -> mpsc::Sender<CompletionEmitter> {
        self.completed_sender.clone()
    }

    fn insert(&mut self, handle: TaskHandle, state: TokioTaskState) {
        self.active.insert(handle, state);
    }

    fn drain_finished(&mut self) -> ExecutorDrainReport {
        let mut report = ExecutorDrainReport::new();

        while let Ok(completion) = self.completed_receiver.try_recv() {
            self.active.remove(&completion.handle);
            completion.emit(&mut report);
        }

        report
    }
}

struct TokioTaskState {
    cancellation: CancellationToken,
    _join_handle: JoinHandle<()>,
}

struct CompletionEmitter {
    handle: TaskHandle,
    emit: Box<dyn FnOnce(&mut ExecutorDrainReport) + Send + 'static>,
}

impl CompletionEmitter {
    fn emit(self, report: &mut ExecutorDrainReport) {
        (self.emit)(report);
    }
}

#[derive(Debug)]
struct CompletionRecord {
    task_id: TaskId,
    attempt_id: crate::TaskAttemptId,
    policy: TaskPolicy,
    cancellation_requested: bool,
    outcome: RunnableOutcome,
}

impl CompletionRecord {
    fn new(
        task_id: TaskId,
        attempt_id: crate::TaskAttemptId,
        policy: TaskPolicy,
        cancellation_requested: bool,
        outcome: RunnableOutcome,
    ) -> Self {
        Self {
            task_id,
            attempt_id,
            policy,
            cancellation_requested,
            outcome,
        }
    }

    fn into_emitter<Output>(self, event_sink: Arc<dyn TaskEventSink<Output>>) -> CompletionEmitter
    where
        Output: Send + 'static,
    {
        let handle = TaskHandle::new(self.task_id, self.attempt_id);
        CompletionEmitter {
            handle,
            emit: Box::new(move |report| {
                let outcome =
                    map_terminal_outcome(&self.policy, self.cancellation_requested, self.outcome);
                emit_terminal(self.task_id, self.attempt_id, outcome, &event_sink, report);
            }),
        }
    }
}

#[derive(Debug)]
enum RunnableOutcome {
    Returned(Result<(), TaskFailure>),
    Panicked(String),
    JoinFailed(String),
}

impl RunnableOutcome {
    fn from_join_result(joined: Result<Result<(), TaskFailure>, tokio::task::JoinError>) -> Self {
        match joined {
            Ok(result) => Self::Returned(result),
            Err(error) if error.is_panic() => Self::Panicked(join_error_message(error)),
            Err(error) => Self::JoinFailed(error.to_string()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TerminalOutcome {
    Completed,
    Failed(String),
    Cancelled,
    FinishedAfterCancel,
}

fn map_terminal_outcome(
    policy: &TaskPolicy,
    cancellation_requested: bool,
    outcome: RunnableOutcome,
) -> TerminalOutcome {
    match outcome {
        RunnableOutcome::Returned(Ok(()))
            if cancellation_requested
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
        RunnableOutcome::Panicked(message) | RunnableOutcome::JoinFailed(message) => {
            TerminalOutcome::Failed(message)
        }
    }
}

fn emit_terminal<Output>(
    task_id: TaskId,
    attempt_id: crate::TaskAttemptId,
    outcome: TerminalOutcome,
    event_sink: &Arc<dyn TaskEventSink<Output>>,
    report: &mut ExecutorDrainReport,
) where
    Output: Send + 'static,
{
    if let TerminalOutcome::Failed(message) = &outcome {
        let _ = event_sink.emit(TaskEvent::from_job_event(
            task_id,
            attempt_id,
            TaskJobEvent::diagnostic(TaskDiagnosticLevel::Error, message.clone()),
        ));
    }

    let event = match outcome {
        TerminalOutcome::Completed => {
            report.increment_completed_tasks();
            TaskEvent::completed(task_id, attempt_id)
        }
        TerminalOutcome::Failed(_) => {
            report.increment_failed_tasks();
            TaskEvent::failed(task_id, attempt_id)
        }
        TerminalOutcome::Cancelled => {
            report.increment_cancelled_tasks();
            TaskEvent::cancelled(task_id, attempt_id)
        }
        TerminalOutcome::FinishedAfterCancel => {
            report.increment_finished_after_cancel_tasks();
            TaskEvent::finished_after_cancel(task_id, attempt_id)
        }
    };

    let _ = event_sink.emit(event);
}

fn join_error_message(error: tokio::task::JoinError) -> String {
    match error.try_into_panic() {
        Ok(payload) => panic_message(payload),
        Err(error) => error.to_string(),
    }
}

fn panic_message(payload: Box<dyn std::any::Any + Send + 'static>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "task panicked".to_owned()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        pin::Pin,
        task::{Context, Poll},
        thread,
        time::Duration,
    };

    use crate::{
        AsyncTaskJob, BlockingPolicy, CancelReason, CancellationToken, CoalescingKey,
        ExecutorDrainReport, TaskAttemptId, TaskContext, TaskEventKind, TaskExecutor, TaskFailure,
        TaskId, TaskKey, TaskLifecycleEvent, TaskPolicy, TaskProgressValue, TaskRunnable,
        TaskScope, TaskSpawnRequest,
        testing::{RecordedTaskJob, RecordingTaskEventSink},
    };

    use super::TokioTaskExecutor;

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
            let report = <TokioTaskExecutor as TaskExecutor<(), ()>>::drain_finished(executor);
            if is_finished(&report) {
                return report;
            }
            thread::sleep(Duration::from_millis(10));
        }

        <TokioTaskExecutor as TaskExecutor<(), ()>>::drain_finished(executor)
    }

    #[test]
    fn tokio_executor_runs_async_job_and_routes_events() {
        let mut executor = TokioTaskExecutor::new().unwrap();
        let sink: RecordingTaskEventSink<()> = RecordingTaskEventSink::new();
        let request = TaskSpawnRequest::new_unit(
            TaskId::from_u64(1),
            TaskAttemptId::from_u64(1),
            TaskKey::try_new("async:job").unwrap(),
            TaskScope::app(),
            CancellationToken::new(),
            sink.handle(),
            TaskRunnable::async_job(RecordedAsyncTaskJob::new(TaskProgressValue::message(
                "async-done",
            ))),
        );

        executor.spawn_task(request).unwrap();
        let report = drain_until_finished(&mut executor, |report| report.completed_tasks() >= 1);

        assert_eq!(report.completed_tasks(), 1);
        assert_eq!(
            sink.events()[0].progress_value(),
            Some(&TaskProgressValue::message("async-done"))
        );
    }

    #[test]
    fn tokio_executor_runs_blocking_job_on_blocking_pool() {
        let mut executor = TokioTaskExecutor::new().unwrap();
        let sink: RecordingTaskEventSink<()> = RecordingTaskEventSink::new();
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
        assert_eq!(
            sink.events()[0].progress_value(),
            Some(&TaskProgressValue::message("blocking-done"))
        );
    }

    #[test]
    fn tokio_executor_cancel_flips_the_running_jobs_token() {
        let mut executor = TokioTaskExecutor::new().unwrap();
        let token = CancellationToken::new();
        let sink: RecordingTaskEventSink<()> = RecordingTaskEventSink::new();
        let handle = crate::TaskHandle::new(TaskId::from_u64(3), TaskAttemptId::from_u64(1));
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
        <TokioTaskExecutor as TaskExecutor<(), ()>>::cancel(&mut executor, handle).unwrap();

        assert!(token.is_cancelled());
    }

    #[test]
    fn tokio_executor_emits_finished_after_cancel_for_non_abortable_completed_work() {
        let mut executor = TokioTaskExecutor::new().unwrap();
        let token = CancellationToken::new();
        let sink: RecordingTaskEventSink<()> = RecordingTaskEventSink::new();
        let request = TaskSpawnRequest::new_unit(
            TaskId::from_u64(6),
            TaskAttemptId::from_u64(1),
            TaskKey::try_new("tokio:nonabortable-finish").unwrap(),
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
        let report = drain_until_finished(&mut executor, |report| {
            report.finished_after_cancel_tasks() >= 1
        });

        assert_eq!(report.finished_after_cancel_tasks(), 1);
        assert_eq!(
            sink.terminal_lifecycle_events(),
            vec![TaskLifecycleEvent::FinishedAfterCancel]
        );
    }

    #[test]
    fn tokio_executor_emits_cancelled_for_cooperatively_cancelled_work() {
        let mut executor = TokioTaskExecutor::new().unwrap();
        let token = CancellationToken::new();
        let sink: RecordingTaskEventSink<()> = RecordingTaskEventSink::new();
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
        assert_eq!(
            sink.terminal_lifecycle_events(),
            vec![TaskLifecycleEvent::Cancelled]
        );
    }

    #[test]
    fn tokio_drain_finished_does_not_wait_for_unfinished_jobs() {
        let mut executor = TokioTaskExecutor::new().unwrap();
        let sink: RecordingTaskEventSink<()> = RecordingTaskEventSink::new();
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
        let report = <TokioTaskExecutor as TaskExecutor<(), ()>>::drain_finished(&mut executor);

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
        let sink: RecordingTaskEventSink<()> = RecordingTaskEventSink::new();
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
        assert!(sink.events().iter().any(|event| matches!(
            event.kind(),
            TaskEventKind::Lifecycle(TaskLifecycleEvent::Failed)
        )));
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct RecordedAsyncTaskJob {
        value: TaskProgressValue,
    }

    impl RecordedAsyncTaskJob {
        fn new(value: TaskProgressValue) -> Self {
            Self { value }
        }
    }

    impl<Input, Output> AsyncTaskJob<Input, Output> for RecordedAsyncTaskJob
    where
        Input: Send + 'static,
        Output: Send + 'static,
    {
        fn run(
            self: Box<Self>,
            _input: Input,
            context: TaskContext<Output>,
        ) -> Pin<Box<dyn Future<Output = Result<(), TaskFailure>> + Send + 'static>> {
            Box::pin(async move {
                context.emit_job_event(crate::TaskJobEvent::progress(
                    crate::TaskProgressEvent::new(
                        CoalescingKey::try_new("recorded").unwrap(),
                        self.value,
                    ),
                ))
            })
        }
    }

    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    struct CancelledAsyncTaskJob;

    impl CancelledAsyncTaskJob {
        const fn new() -> Self {
            Self
        }
    }

    impl<Input, Output> AsyncTaskJob<Input, Output> for CancelledAsyncTaskJob
    where
        Input: Send + 'static,
        Output: Send + 'static,
    {
        fn run(
            self: Box<Self>,
            _input: Input,
            _context: TaskContext<Output>,
        ) -> Pin<Box<dyn Future<Output = Result<(), TaskFailure>> + Send + 'static>> {
            Box::pin(async { Err(TaskFailure::cancelled("task cancelled cooperatively")) })
        }
    }

    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    struct WaitingTaskJob;

    impl WaitingTaskJob {
        const fn new() -> Self {
            Self
        }
    }

    impl<Input, Output> AsyncTaskJob<Input, Output> for WaitingTaskJob
    where
        Input: Send + 'static,
        Output: Send + 'static,
    {
        fn run(
            self: Box<Self>,
            _input: Input,
            _context: TaskContext<Output>,
        ) -> Pin<Box<dyn Future<Output = Result<(), TaskFailure>> + Send + 'static>> {
            Box::pin(WaitingFuture)
        }
    }

    struct WaitingFuture;

    impl Future for WaitingFuture {
        type Output = Result<(), TaskFailure>;

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    struct PanickingTaskJob;

    impl PanickingTaskJob {
        const fn new() -> Self {
            Self
        }
    }

    impl<Input, Output> AsyncTaskJob<Input, Output> for PanickingTaskJob
    where
        Input: Send + 'static,
        Output: Send + 'static,
    {
        fn run(
            self: Box<Self>,
            _input: Input,
            _context: TaskContext<Output>,
        ) -> Pin<Box<dyn Future<Output = Result<(), TaskFailure>> + Send + 'static>> {
            Box::pin(async { panic!("task panic") })
        }
    }
}
