use std::collections::{HashMap, VecDeque};

use crate::{CoalescingKey, TaskAttemptId, TaskEvent, TaskId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueuePolicy {
    capacity: usize,
}

impl QueuePolicy {
    pub const fn bounded(capacity: usize) -> Self {
        Self { capacity }
    }

    pub const fn capacity(self) -> usize {
        self.capacity
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DrainBudget {
    max_events: usize,
}

impl DrainBudget {
    pub const fn new() -> Self {
        Self {
            max_events: usize::MAX,
        }
    }

    pub const fn max_events(mut self, max_events: usize) -> Self {
        self.max_events = max_events;
        self
    }

    pub const fn event_limit(self) -> usize {
        self.max_events
    }
}

impl Default for DrainBudget {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct TaskEventQueue<Output = ()> {
    policy: QueuePolicy,
    fifo: VecDeque<TaskEvent<Output>>,
    progress: HashMap<ProgressSlot, TaskEvent<Output>>,
    progress_order: VecDeque<ProgressSlot>,
    dropped_events: usize,
    coalesced_events: usize,
}

impl<Output> TaskEventQueue<Output> {
    pub fn new(policy: QueuePolicy) -> Self {
        Self {
            policy,
            fifo: VecDeque::new(),
            progress: HashMap::new(),
            progress_order: VecDeque::new(),
            dropped_events: 0,
            coalesced_events: 0,
        }
    }

    pub fn push(&mut self, event: TaskEvent<Output>) -> Result<(), QueueError> {
        if let Some(slot) = ProgressSlot::from_event(&event) {
            return self.push_progress(slot, event);
        }

        if self.pending_events() >= self.policy.capacity {
            self.dropped_events += 1;
            return Err(QueueError::overflow(self.policy.capacity));
        }

        self.fifo.push_back(event);
        Ok(())
    }

    pub fn drain(&mut self, budget: DrainBudget) -> DrainReport<Output> {
        let mut events = Vec::new();
        let mut remaining = budget.max_events;

        while remaining > 0 {
            let Some(event) = self.fifo.pop_front() else {
                break;
            };

            events.push(event);
            remaining -= 1;
        }

        while remaining > 0 {
            let Some(slot) = self.progress_order.pop_front() else {
                break;
            };

            let Some(event) = self.progress.remove(&slot) else {
                continue;
            };

            events.push(event);
            remaining -= 1;
        }

        DrainReport {
            events,
            queue: self.report(),
            coalesced_events: self.coalesced_events,
        }
    }

    pub fn report(&self) -> QueueReport {
        QueueReport {
            capacity: self.policy.capacity,
            pending_events: self.pending_events(),
            dropped_events: self.dropped_events,
            coalesced_events: self.coalesced_events,
        }
    }

    fn push_progress(
        &mut self,
        slot: ProgressSlot,
        event: TaskEvent<Output>,
    ) -> Result<(), QueueError> {
        if let std::collections::hash_map::Entry::Occupied(mut entry) =
            self.progress.entry(slot.clone())
        {
            entry.insert(event);
            self.coalesced_events += 1;
            return Ok(());
        }

        if self.pending_events() >= self.policy.capacity {
            self.dropped_events += 1;
            return Err(QueueError::overflow(self.policy.capacity));
        }

        self.progress.insert(slot.clone(), event);
        self.progress_order.push_back(slot);
        Ok(())
    }

    fn pending_events(&self) -> usize {
        self.fifo.len() + self.progress.len()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueueReport {
    capacity: usize,
    pending_events: usize,
    dropped_events: usize,
    coalesced_events: usize,
}

impl QueueReport {
    pub const fn capacity(self) -> usize {
        self.capacity
    }

    pub const fn pending_events(self) -> usize {
        self.pending_events
    }

    pub const fn dropped_events(self) -> usize {
        self.dropped_events
    }

    pub const fn coalesced_events(self) -> usize {
        self.coalesced_events
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DrainReport<Output = ()> {
    events: Vec<TaskEvent<Output>>,
    queue: QueueReport,
    coalesced_events: usize,
}

impl<Output> DrainReport<Output> {
    pub fn events(&self) -> &[TaskEvent<Output>] {
        &self.events
    }

    pub fn into_events(self) -> Vec<TaskEvent<Output>> {
        self.events
    }

    pub const fn queue(&self) -> QueueReport {
        self.queue
    }

    pub const fn coalesced_events(&self) -> usize {
        self.coalesced_events
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum QueueErrorCode {
    Overflow,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueueError {
    code: QueueErrorCode,
    message: String,
}

impl QueueError {
    pub fn code(&self) -> QueueErrorCode {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    fn overflow(capacity: usize) -> Self {
        Self {
            code: QueueErrorCode::Overflow,
            message: format!("task event queue capacity {capacity} is full"),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ProgressSlot {
    task_id: TaskId,
    attempt_id: TaskAttemptId,
    key: CoalescingKey,
}

impl ProgressSlot {
    fn from_event<Output>(event: &TaskEvent<Output>) -> Option<Self> {
        let progress = event.progress_event()?;

        Some(Self {
            task_id: event.provenance().task_id(),
            attempt_id: event.provenance().attempt_id(),
            key: progress.key().clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        CoalescingKey, DrainBudget, QueueErrorCode, QueuePolicy, TaskAttemptId,
        TaskDiagnosticLevel, TaskEvent, TaskEventQueue, TaskId, TaskJobEvent, TaskProgressValue,
    };

    #[test]
    fn progress_events_coalesce_by_task_attempt_and_key() {
        let mut queue = TaskEventQueue::<String>::new(QueuePolicy::bounded(8));
        let task = TaskId::from_u64(1);
        let attempt = TaskAttemptId::from_u64(1);

        queue
            .push(TaskEvent::from_job_event(
                task,
                attempt,
                TaskJobEvent::progress_units(
                    CoalescingKey::try_new("bytes").unwrap(),
                    10,
                    Some(100),
                ),
            ))
            .unwrap();
        queue
            .push(TaskEvent::from_job_event(
                task,
                attempt,
                TaskJobEvent::progress_units(
                    CoalescingKey::try_new("bytes").unwrap(),
                    20,
                    Some(100),
                ),
            ))
            .unwrap();

        let drained = queue.drain(DrainBudget::new().max_events(8));
        assert_eq!(drained.events().len(), 1);
        assert_eq!(drained.coalesced_events(), 1);
        assert_eq!(
            drained.events()[0].progress_value(),
            Some(&TaskProgressValue::units(20, Some(100)))
        );
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

        queue
            .push(TaskEvent::from_job_event(
                task,
                attempt,
                TaskJobEvent::progress_message(CoalescingKey::try_new("file:a").unwrap(), "a"),
            ))
            .unwrap();
        queue
            .push(TaskEvent::from_job_event(
                task,
                attempt,
                TaskJobEvent::progress_message(CoalescingKey::try_new("file:b").unwrap(), "b"),
            ))
            .unwrap();

        let error = queue
            .push(TaskEvent::from_job_event(
                task,
                attempt,
                TaskJobEvent::progress_message(CoalescingKey::try_new("file:c").unwrap(), "c"),
            ))
            .unwrap_err();

        assert_eq!(error.code(), QueueErrorCode::Overflow);
    }
}
