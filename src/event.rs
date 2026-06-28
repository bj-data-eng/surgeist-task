use crate::{CoalescingKey, TaskAttemptId, TaskId, TaskProvenance};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TaskLifecycleEvent {
    Started,
    Completed,
    Failed,
    Cancelled,
    FailedToCancel,
    FinishedAfterCancel,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskProgressEvent {
    key: CoalescingKey,
    value: TaskProgressValue,
}

impl TaskProgressEvent {
    pub fn new(key: CoalescingKey, value: TaskProgressValue) -> Self {
        Self { key, value }
    }

    pub fn key(&self) -> &CoalescingKey {
        &self.key
    }

    pub fn value(&self) -> &TaskProgressValue {
        &self.value
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskProgressValue {
    Units { completed: u64, total: Option<u64> },
    Percent(PercentComplete),
    Message(String),
}

impl TaskProgressValue {
    pub const fn units(completed: u64, total: Option<u64>) -> Self {
        Self::Units { completed, total }
    }

    pub const fn percent(percent: PercentComplete) -> Self {
        Self::Percent(percent)
    }

    pub fn message(message: impl Into<String>) -> Self {
        Self::Message(message.into())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PercentComplete {
    basis_points: u16,
}

impl PercentComplete {
    pub fn try_from_basis_points(basis_points: u16) -> Result<Self, PercentCompleteError> {
        if basis_points > 10_000 {
            return Err(PercentCompleteError {
                basis_points,
                kind: PercentCompleteErrorKind::GreaterThanOneHundredPercent,
            });
        }

        Ok(Self { basis_points })
    }

    pub const fn basis_points(self) -> u16 {
        self.basis_points
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PercentCompleteErrorKind {
    GreaterThanOneHundredPercent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PercentCompleteError {
    basis_points: u16,
    kind: PercentCompleteErrorKind,
}

impl PercentCompleteError {
    pub const fn basis_points(&self) -> u16 {
        self.basis_points
    }

    pub const fn kind(&self) -> PercentCompleteErrorKind {
        self.kind
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskOutputEvent<Output> {
    output: Output,
}

impl<Output> TaskOutputEvent<Output> {
    pub fn new(output: Output) -> Self {
        Self { output }
    }

    pub fn output(&self) -> &Output {
        &self.output
    }

    pub fn into_output(self) -> Output {
        self.output
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TaskDiagnosticLevel {
    Info,
    Warning,
    Error,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskDiagnosticEvent {
    level: TaskDiagnosticLevel,
    message: String,
}

impl TaskDiagnosticEvent {
    pub fn new(level: TaskDiagnosticLevel, message: impl Into<String>) -> Self {
        Self {
            level,
            message: message.into(),
        }
    }

    pub const fn level(&self) -> TaskDiagnosticLevel {
        self.level
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskEventKind<Output = ()> {
    Lifecycle(TaskLifecycleEvent),
    Progress(TaskProgressEvent),
    Output(TaskOutputEvent<Output>),
    Diagnostic(TaskDiagnosticEvent),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskJobEvent<Output = ()> {
    Progress(TaskProgressEvent),
    Output(TaskOutputEvent<Output>),
    Diagnostic(TaskDiagnosticEvent),
}

impl<Output> TaskJobEvent<Output> {
    pub fn progress(event: TaskProgressEvent) -> Self {
        Self::Progress(event)
    }

    pub fn progress_units(key: CoalescingKey, completed: u64, total: Option<u64>) -> Self {
        Self::progress(TaskProgressEvent::new(
            key,
            TaskProgressValue::units(completed, total),
        ))
    }

    pub fn progress_percent(key: CoalescingKey, percent: PercentComplete) -> Self {
        Self::progress(TaskProgressEvent::new(
            key,
            TaskProgressValue::percent(percent),
        ))
    }

    pub fn progress_message(key: CoalescingKey, message: impl Into<String>) -> Self {
        Self::progress(TaskProgressEvent::new(
            key,
            TaskProgressValue::message(message),
        ))
    }

    pub fn output(output: Output) -> Self {
        Self::Output(TaskOutputEvent::new(output))
    }

    pub fn diagnostic(level: TaskDiagnosticLevel, message: impl Into<String>) -> Self {
        Self::Diagnostic(TaskDiagnosticEvent::new(level, message))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskEvent<Output = ()> {
    provenance: TaskProvenance,
    kind: TaskEventKind<Output>,
}

impl<Output> TaskEvent<Output> {
    pub fn started(task_id: TaskId, attempt_id: TaskAttemptId) -> Self {
        Self::lifecycle(task_id, attempt_id, TaskLifecycleEvent::Started)
    }

    pub fn from_job_event(
        task_id: TaskId,
        attempt_id: TaskAttemptId,
        event: TaskJobEvent<Output>,
    ) -> Self {
        let kind = match event {
            TaskJobEvent::Progress(event) => TaskEventKind::Progress(event),
            TaskJobEvent::Output(event) => TaskEventKind::Output(event),
            TaskJobEvent::Diagnostic(event) => TaskEventKind::Diagnostic(event),
        };

        Self::new(TaskProvenance::new(task_id, attempt_id), kind)
    }

    pub fn completed(task_id: TaskId, attempt_id: TaskAttemptId) -> Self {
        Self::lifecycle(task_id, attempt_id, TaskLifecycleEvent::Completed)
    }

    pub fn failed(task_id: TaskId, attempt_id: TaskAttemptId) -> Self {
        Self::lifecycle(task_id, attempt_id, TaskLifecycleEvent::Failed)
    }

    pub fn cancelled(task_id: TaskId, attempt_id: TaskAttemptId) -> Self {
        Self::lifecycle(task_id, attempt_id, TaskLifecycleEvent::Cancelled)
    }

    pub fn failed_to_cancel(task_id: TaskId, attempt_id: TaskAttemptId) -> Self {
        Self::lifecycle(task_id, attempt_id, TaskLifecycleEvent::FailedToCancel)
    }

    pub fn finished_after_cancel(task_id: TaskId, attempt_id: TaskAttemptId) -> Self {
        Self::lifecycle(task_id, attempt_id, TaskLifecycleEvent::FinishedAfterCancel)
    }

    pub fn new(provenance: TaskProvenance, kind: TaskEventKind<Output>) -> Self {
        Self { provenance, kind }
    }

    pub fn provenance(&self) -> &TaskProvenance {
        &self.provenance
    }

    pub fn kind(&self) -> &TaskEventKind<Output> {
        &self.kind
    }

    pub fn progress_event(&self) -> Option<&TaskProgressEvent> {
        match &self.kind {
            TaskEventKind::Progress(event) => Some(event),
            _ => None,
        }
    }

    pub fn progress_value(&self) -> Option<&TaskProgressValue> {
        self.progress_event().map(TaskProgressEvent::value)
    }

    fn lifecycle(
        task_id: TaskId,
        attempt_id: TaskAttemptId,
        lifecycle: TaskLifecycleEvent,
    ) -> Self {
        Self::new(
            TaskProvenance::new(task_id, attempt_id),
            TaskEventKind::Lifecycle(lifecycle),
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::{PercentComplete, TaskProgressValue};

    #[test]
    fn percent_complete_rejects_values_above_one_hundred_percent() {
        assert!(PercentComplete::try_from_basis_points(10_000).is_ok());
        assert!(PercentComplete::try_from_basis_points(10_001).is_err());
    }

    #[test]
    fn progress_values_keep_semantic_payloads() {
        let percent = PercentComplete::try_from_basis_points(2_500).unwrap();

        assert_eq!(
            TaskProgressValue::units(20, Some(100)),
            TaskProgressValue::Units {
                completed: 20,
                total: Some(100)
            }
        );
        assert_eq!(
            TaskProgressValue::percent(percent),
            TaskProgressValue::Percent(percent)
        );
        assert_eq!(
            TaskProgressValue::message("indexing"),
            TaskProgressValue::Message("indexing".to_owned())
        );
    }
}
