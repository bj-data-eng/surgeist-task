//! Task scheduling and work-plane contracts for Surgeist.
//!
//! This crate owns the task subsystem boundary for Surgeist. Keep app authors
//! oriented around Surgeist tasks, task context, cancellation, progress,
//! resource classes, and scheduler policy. Concrete executors such as Tokio,
//! thread pools, or process workers belong behind these contracts.

#![forbid(unsafe_code)]

/// Crate identity string used by smoke tests and API artifacts.
pub const CRATE_NAME: &str = "surgeist-task";

mod id;
mod provenance;
mod scope;

pub use id::{
    CoalescingKey, CorrelationId, ObserverId, ResourceClassId, TaskAttemptId, TaskId, TaskIdError,
    TaskIdErrorKind, TaskKey, TaskName,
};
pub use provenance::TaskProvenance;
pub use scope::{TaskScope, TaskScopeError, TaskScopeErrorKind, TaskScopeSegment};

#[cfg(test)]
mod tests {
    use super::CRATE_NAME;

    #[test]
    fn exposes_crate_identity() {
        assert_eq!(CRATE_NAME, "surgeist-task");
    }
}
