use crate::{CorrelationId, TaskAttemptId, TaskId};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskProvenance {
    task_id: TaskId,
    attempt_id: TaskAttemptId,
    correlation_id: Option<CorrelationId>,
    parent_correlation_id: Option<CorrelationId>,
    sequence: Option<u64>,
}

impl TaskProvenance {
    pub fn new(task_id: TaskId, attempt_id: TaskAttemptId) -> Self {
        Self {
            task_id,
            attempt_id,
            correlation_id: None,
            parent_correlation_id: None,
            sequence: None,
        }
    }

    pub fn with_correlation(mut self, correlation_id: CorrelationId) -> Self {
        self.correlation_id = Some(correlation_id);
        self
    }

    pub fn with_parent(mut self, parent_correlation_id: CorrelationId) -> Self {
        self.parent_correlation_id = Some(parent_correlation_id);
        self
    }

    pub fn with_sequence(mut self, sequence: u64) -> Self {
        self.sequence = Some(sequence);
        self
    }

    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub fn attempt_id(&self) -> TaskAttemptId {
        self.attempt_id
    }

    pub fn correlation_id(&self) -> Option<CorrelationId> {
        self.correlation_id
    }

    pub fn parent_correlation_id(&self) -> Option<CorrelationId> {
        self.parent_correlation_id
    }

    pub fn sequence(&self) -> Option<u64> {
        self.sequence
    }
}

#[cfg(test)]
mod tests {
    use super::TaskProvenance;
    use crate::{CorrelationId, TaskAttemptId, TaskId};

    #[test]
    fn provenance_names_task_attempt_and_correlation() {
        let provenance = TaskProvenance::new(TaskId::from_u64(1), TaskAttemptId::from_u64(2))
            .with_correlation(CorrelationId::from_u64(3))
            .with_sequence(4);

        assert_eq!(provenance.task_id(), TaskId::from_u64(1));
        assert_eq!(provenance.attempt_id(), TaskAttemptId::from_u64(2));
        assert_eq!(
            provenance.correlation_id(),
            Some(CorrelationId::from_u64(3))
        );
        assert_eq!(provenance.sequence(), Some(4));
    }
}
