use crate::{
    CancelReason, CancellationToken, ObserverCountSnapshot, TaskAttemptId, TaskId, TaskKey,
    TaskPolicy, TaskScope,
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TaskStatus {
    Queued,
    Running,
    Waiting,
    Blocked,
    Completed,
    Failed,
    Cancelling,
    Cancelled,
    FinishedAfterCancel,
    FailedToCancel,
}

impl TaskStatus {
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed
                | Self::Failed
                | Self::Cancelled
                | Self::FinishedAfterCancel
                | Self::FailedToCancel
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TaskTerminalStatus {
    Completed,
    Failed,
    Cancelled,
    FinishedAfterCancel,
    FailedToCancel,
}

#[derive(Clone, Debug)]
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

impl TaskRecord {
    pub fn queued(id: TaskId, key: TaskKey, scope: TaskScope, policy: TaskPolicy) -> Self {
        Self {
            id,
            key,
            scope,
            policy,
            status: TaskStatus::Queued,
            attempt_id: None,
            cancellation: CancellationToken::new(),
            cancel_reason: None,
            observer_count: 0,
        }
    }

    pub fn start_attempt(
        &mut self,
        attempt_id: TaskAttemptId,
    ) -> Result<TaskAttemptId, TaskLifecycleError> {
        if self.status.is_terminal() {
            return Err(self.error(TaskLifecycleErrorKind::TerminalState, attempt_id));
        }

        if let Some(current_attempt_id) = self.attempt_id
            && attempt_id <= current_attempt_id
        {
            return Err(self.error(TaskLifecycleErrorKind::NonAdvancingAttempt, attempt_id));
        }

        self.attempt_id = Some(attempt_id);
        self.status = TaskStatus::Queued;
        Ok(attempt_id)
    }

    pub fn mark_running(&mut self, attempt_id: TaskAttemptId) -> Result<(), TaskLifecycleError> {
        self.ensure_active_attempt(attempt_id)?;
        self.status = TaskStatus::Running;
        Ok(())
    }

    pub fn mark_waiting(&mut self, attempt_id: TaskAttemptId) -> Result<(), TaskLifecycleError> {
        self.ensure_active_attempt(attempt_id)?;
        self.status = TaskStatus::Waiting;
        Ok(())
    }

    pub fn mark_blocked(&mut self, attempt_id: TaskAttemptId) -> Result<(), TaskLifecycleError> {
        self.ensure_active_attempt(attempt_id)?;
        self.status = TaskStatus::Blocked;
        Ok(())
    }

    pub fn complete(&mut self, attempt_id: TaskAttemptId) -> Result<(), TaskLifecycleError> {
        self.ensure_terminal_event_attempt(attempt_id)?;
        self.status = TaskStatus::Completed;
        Ok(())
    }

    pub fn fail(&mut self, attempt_id: TaskAttemptId) -> Result<(), TaskLifecycleError> {
        self.ensure_terminal_event_attempt(attempt_id)?;
        self.status = TaskStatus::Failed;
        Ok(())
    }

    pub fn request_cancel(&mut self, reason: CancelReason) -> CancellationToken {
        if self.status.is_terminal() {
            return self.cancellation.clone();
        }

        self.cancellation.cancel(reason.clone());
        self.cancel_reason = Some(reason);
        self.status = TaskStatus::Cancelling;
        self.cancellation.clone()
    }

    pub fn confirm_cancelled(
        &mut self,
        attempt_id: TaskAttemptId,
    ) -> Result<(), TaskLifecycleError> {
        self.ensure_terminal_event_attempt(attempt_id)?;
        self.status = TaskStatus::Cancelled;
        Ok(())
    }

    pub fn finish_after_cancel(
        &mut self,
        attempt_id: TaskAttemptId,
    ) -> Result<(), TaskLifecycleError> {
        self.ensure_active_attempt(attempt_id)?;
        if self.status != TaskStatus::Cancelling {
            return Err(self.error(TaskLifecycleErrorKind::NotCancelling, attempt_id));
        }

        self.status = TaskStatus::FinishedAfterCancel;
        Ok(())
    }

    pub fn fail_to_cancel(&mut self, attempt_id: TaskAttemptId) -> Result<(), TaskLifecycleError> {
        self.ensure_terminal_event_attempt(attempt_id)?;
        self.status = TaskStatus::FailedToCancel;
        Ok(())
    }

    pub fn id(&self) -> TaskId {
        self.id
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

    pub fn status(&self) -> TaskStatus {
        self.status
    }

    pub fn attempt_id(&self) -> Option<TaskAttemptId> {
        self.attempt_id
    }

    pub fn cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub fn cancel_reason(&self) -> Option<&CancelReason> {
        self.cancel_reason.as_ref()
    }

    pub fn observer_count(&self) -> usize {
        self.observer_count
    }

    pub(crate) fn sync_observer_count(&mut self, snapshot: ObserverCountSnapshot) {
        if snapshot.key() == &self.key {
            self.observer_count = snapshot.count();
        }
    }

    pub fn accepts_attempt(&self, attempt_id: TaskAttemptId) -> bool {
        !self.status.is_terminal() && self.attempt_id == Some(attempt_id)
    }

    pub fn snapshot(&self) -> TaskSnapshot {
        TaskSnapshot {
            id: self.id,
            key: self.key.clone(),
            scope: self.scope.clone(),
            status: self.status,
            attempt_id: self.attempt_id,
            observer_count: self.observer_count,
        }
    }

    fn ensure_active_attempt(&self, attempt_id: TaskAttemptId) -> Result<(), TaskLifecycleError> {
        if self.status.is_terminal() {
            return Err(self.error(TaskLifecycleErrorKind::TerminalState, attempt_id));
        }

        match self.attempt_id {
            Some(current_attempt_id) if current_attempt_id == attempt_id => Ok(()),
            Some(_) => Err(self.error(TaskLifecycleErrorKind::StaleAttempt, attempt_id)),
            None => Err(self.error(TaskLifecycleErrorKind::NotRunning, attempt_id)),
        }
    }

    fn ensure_terminal_event_attempt(
        &self,
        attempt_id: TaskAttemptId,
    ) -> Result<(), TaskLifecycleError> {
        self.ensure_active_attempt(attempt_id)?;

        if self.status == TaskStatus::Queued {
            return Err(self.error(TaskLifecycleErrorKind::NotRunning, attempt_id));
        }

        Ok(())
    }

    fn error(&self, kind: TaskLifecycleErrorKind, attempt_id: TaskAttemptId) -> TaskLifecycleError {
        TaskLifecycleError {
            kind,
            task_id: self.id,
            attempt_id,
            status: self.status,
        }
    }
}

const _: fn(&mut TaskRecord, ObserverCountSnapshot) = TaskRecord::sync_observer_count;

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

impl TaskLifecycleError {
    pub fn kind(&self) -> TaskLifecycleErrorKind {
        self.kind
    }

    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub fn attempt_id(&self) -> TaskAttemptId {
        self.attempt_id
    }

    pub fn status(&self) -> TaskStatus {
        self.status
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskSnapshot {
    id: TaskId,
    key: TaskKey,
    scope: TaskScope,
    status: TaskStatus,
    attempt_id: Option<TaskAttemptId>,
    observer_count: usize,
}

impl TaskSnapshot {
    pub fn id(&self) -> TaskId {
        self.id
    }

    pub fn key(&self) -> &TaskKey {
        &self.key
    }

    pub fn scope(&self) -> &TaskScope {
        &self.scope
    }

    pub fn status(&self) -> TaskStatus {
        self.status
    }

    pub fn attempt_id(&self) -> Option<TaskAttemptId> {
        self.attempt_id
    }

    pub fn observer_count(&self) -> usize {
        self.observer_count
    }

    pub fn is_terminal(&self) -> bool {
        self.status.is_terminal()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        CancelReason, TaskAttemptId, TaskId, TaskKey, TaskLifecycleErrorKind, TaskPolicy,
        TaskRecord, TaskScope, TaskStatus,
    };

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

        record.mark_running(TaskAttemptId::from_u64(2)).unwrap();
        record.complete(TaskAttemptId::from_u64(2)).unwrap();
        let error = record.mark_running(TaskAttemptId::from_u64(2)).unwrap_err();
        assert_eq!(error.kind(), TaskLifecycleErrorKind::TerminalState);

        let restart_error = record
            .start_attempt(TaskAttemptId::from_u64(3))
            .unwrap_err();
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

        record
            .finish_after_cancel(TaskAttemptId::from_u64(1))
            .unwrap();
        assert_eq!(record.status(), TaskStatus::FinishedAfterCancel);
        assert!(record.snapshot().is_terminal());
    }

    #[test]
    fn terminal_events_reject_queued_attempts() {
        let mut record = TaskRecord::queued(
            TaskId::from_u64(3),
            TaskKey::try_new("index:docs").unwrap(),
            TaskScope::app(),
            TaskPolicy::continue_when_unobserved(),
        );

        let attempt_id = record.start_attempt(TaskAttemptId::from_u64(1)).unwrap();

        let complete_error = record.complete(attempt_id).unwrap_err();
        assert_eq!(complete_error.kind(), TaskLifecycleErrorKind::NotRunning);

        let fail_error = record.fail(attempt_id).unwrap_err();
        assert_eq!(fail_error.kind(), TaskLifecycleErrorKind::NotRunning);

        let cancel_error = record.confirm_cancelled(attempt_id).unwrap_err();
        assert_eq!(cancel_error.kind(), TaskLifecycleErrorKind::NotRunning);

        let fail_to_cancel_error = record.fail_to_cancel(attempt_id).unwrap_err();
        assert_eq!(
            fail_to_cancel_error.kind(),
            TaskLifecycleErrorKind::NotRunning
        );

        assert_eq!(record.status(), TaskStatus::Queued);
    }

    #[test]
    fn terminal_cancel_request_does_not_mutate_completed_record() {
        let mut record = TaskRecord::queued(
            TaskId::from_u64(4),
            TaskKey::try_new("sync:calendar").unwrap(),
            TaskScope::app(),
            TaskPolicy::continue_when_unobserved(),
        );

        let attempt_id = record.start_attempt(TaskAttemptId::from_u64(1)).unwrap();
        record.mark_running(attempt_id).unwrap();
        record.complete(attempt_id).unwrap();
        let snapshot = record.snapshot();

        let token = record.request_cancel(CancelReason::user_requested());

        assert!(!token.is_cancelled());
        assert_eq!(record.status(), TaskStatus::Completed);
        assert_eq!(record.cancel_reason(), None);
        assert_eq!(record.snapshot(), snapshot);
    }

    #[test]
    fn sync_observer_count_only_updates_matching_task_key() {
        let key = TaskKey::try_new("index:docs").unwrap();
        let mut record = TaskRecord::queued(
            TaskId::from_u64(5),
            key.clone(),
            TaskScope::app(),
            TaskPolicy::continue_when_unobserved(),
        );

        record.sync_observer_count(crate::ObserverCountSnapshot::new(key, 3));
        assert_eq!(record.observer_count(), 3);

        let other_key = TaskKey::try_new("index:other").unwrap();
        record.sync_observer_count(crate::ObserverCountSnapshot::new(other_key, 9));
        assert_eq!(record.observer_count(), 3);
    }
}
