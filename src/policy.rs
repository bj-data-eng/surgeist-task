#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum UnobservedPolicy {
    Continue,
    LowerPriority,
    Pause,
    Cancel,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TaskPriority {
    Low,
    Normal,
    High,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum BlockingPolicy {
    Abortable,
    Blocking,
    NonAbortableReportCancelling,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RetryPolicy {
    max_attempts: u32,
}

impl RetryPolicy {
    pub const fn new(max_attempts: u32) -> Self {
        Self { max_attempts }
    }

    pub const fn max_attempts(self) -> u32 {
        self.max_attempts
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TaskPolicy {
    dedupe_by_key: bool,
    unobserved: UnobservedPolicy,
    priority: TaskPriority,
    retry: RetryPolicy,
    blocking: BlockingPolicy,
}

impl TaskPolicy {
    pub const fn continue_when_unobserved() -> Self {
        Self {
            dedupe_by_key: false,
            unobserved: UnobservedPolicy::Continue,
            priority: TaskPriority::Normal,
            retry: RetryPolicy::new(1),
            blocking: BlockingPolicy::Abortable,
        }
    }

    pub const fn cancel_when_unobserved() -> Self {
        Self {
            dedupe_by_key: false,
            unobserved: UnobservedPolicy::Cancel,
            priority: TaskPriority::Normal,
            retry: RetryPolicy::new(1),
            blocking: BlockingPolicy::Abortable,
        }
    }

    pub fn dedupe_by_key(mut self) -> Self {
        self.dedupe_by_key = true;
        self
    }

    pub const fn dedupes_by_key(&self) -> bool {
        self.dedupe_by_key
    }

    pub fn with_priority(mut self, priority: TaskPriority) -> Self {
        self.priority = priority;
        self
    }

    pub fn with_retry_attempts(mut self, max_attempts: u32) -> Self {
        self.retry = RetryPolicy::new(max_attempts);
        self
    }

    pub fn with_blocking_policy(mut self, blocking: BlockingPolicy) -> Self {
        self.blocking = blocking;
        self
    }

    pub const fn unobserved_policy(&self) -> UnobservedPolicy {
        self.unobserved
    }

    pub const fn priority(&self) -> TaskPriority {
        self.priority
    }

    pub const fn retry_policy(&self) -> RetryPolicy {
        self.retry
    }

    pub const fn blocking_policy(&self) -> BlockingPolicy {
        self.blocking
    }
}
