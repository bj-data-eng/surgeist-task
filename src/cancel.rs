use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

#[derive(Debug)]
struct CancellationState {
    cancelled: AtomicBool,
    reason: Mutex<Option<CancelReason>>,
}

#[derive(Clone, Debug)]
pub struct CancellationToken {
    state: Arc<CancellationState>,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self {
            state: Arc::new(CancellationState {
                cancelled: AtomicBool::new(false),
                reason: Mutex::new(None),
            }),
        }
    }

    pub fn cancel(&self, reason: CancelReason) {
        let was_cancelled = self.state.cancelled.swap(true, Ordering::SeqCst);
        if !was_cancelled {
            *self
                .state
                .reason
                .lock()
                .expect("cancellation mutex poisoned") = Some(reason);
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::SeqCst)
    }

    pub fn view(&self) -> CancellationView {
        CancellationView {
            state: Arc::clone(&self.state),
        }
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
pub struct CancellationView {
    state: Arc<CancellationState>,
}

impl CancellationView {
    pub fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::SeqCst)
    }

    pub fn reason(&self) -> Option<CancelReason> {
        self.state
            .reason
            .lock()
            .expect("cancellation mutex poisoned")
            .clone()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CancelReason {
    kind: CancelReasonKind,
    message: String,
}

impl CancelReason {
    pub fn user_requested() -> Self {
        Self::new(
            CancelReasonKind::UserRequested,
            "user requested cancellation",
        )
    }

    pub fn timeout() -> Self {
        Self::new(CancelReasonKind::Timeout, "task cancellation timeout")
    }

    pub fn policy() -> Self {
        Self::new(CancelReasonKind::Policy, "task cancelled by policy")
    }

    pub fn shutdown() -> Self {
        Self::new(CancelReasonKind::Shutdown, "task cancelled during shutdown")
    }

    pub fn dependency_failed() -> Self {
        Self::new(
            CancelReasonKind::DependencyFailed,
            "task dependency failed before completion",
        )
    }

    pub fn kind(&self) -> CancelReasonKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    fn new(kind: CancelReasonKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CancelReasonKind {
    UserRequested,
    Timeout,
    Policy,
    Shutdown,
    DependencyFailed,
}
