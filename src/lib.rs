//! Task scheduling and work-plane contracts for Surgeist.
//!
//! This crate owns the task subsystem boundary for Surgeist. Keep app authors
//! oriented around Surgeist tasks, task context, cancellation, progress,
//! resource classes, and scheduler policy. Concrete executors such as Tokio,
//! thread pools, or process workers belong behind these contracts.

#![forbid(unsafe_code)]

/// Crate identity string used by smoke tests and API artifacts.
pub const CRATE_NAME: &str = "surgeist-task";

mod cancel;
mod event;
mod id;
mod lifecycle;
mod policy;
mod provenance;
mod queue;
mod scope;

pub use cancel::{CancelReason, CancelReasonKind, CancellationToken, CancellationView};
pub use event::{
    PercentComplete, PercentCompleteError, PercentCompleteErrorKind, TaskDiagnosticEvent,
    TaskDiagnosticLevel, TaskEvent, TaskEventKind, TaskJobEvent, TaskLifecycleEvent,
    TaskOutputEvent, TaskProgressEvent, TaskProgressValue,
};
pub use id::{
    CoalescingKey, CorrelationId, ObserverId, ResourceClassId, TaskAttemptId, TaskId, TaskIdError,
    TaskIdErrorKind, TaskKey, TaskName,
};
pub use lifecycle::{
    TaskLifecycleError, TaskLifecycleErrorKind, TaskRecord, TaskSnapshot, TaskStatus,
    TaskTerminalStatus,
};
pub use policy::{BlockingPolicy, RetryPolicy, TaskPolicy, TaskPriority, UnobservedPolicy};
pub use provenance::TaskProvenance;
pub use queue::{
    DrainBudget, DrainReport, QueueError, QueueErrorCode, QueuePolicy, QueueReport, TaskEventQueue,
};
pub use scope::{TaskScope, TaskScopeError, TaskScopeErrorKind, TaskScopeSegment};

#[cfg(test)]
mod tests {
    use super::CRATE_NAME;

    #[test]
    fn exposes_crate_identity() {
        assert_eq!(CRATE_NAME, "surgeist-task");
    }
}
