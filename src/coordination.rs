use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::{
    ObserverId, TaskIdError, TaskKey, TaskName, TaskPolicy, TaskScope, TaskScopeError,
    UnobservedPolicy,
};

type ScopeDeriver<Input> = dyn Fn(&Input) -> Result<TaskScope, TaskRegistrationError> + Send + Sync;
type KeyDeriver<Input> = dyn Fn(&Input) -> Result<TaskKey, TaskRegistrationError> + Send + Sync;

pub struct TaskRegistration<Input> {
    name: TaskName,
    scope: Arc<ScopeDeriver<Input>>,
    key: Arc<KeyDeriver<Input>>,
    policy: TaskPolicy,
}

impl<Input> TaskRegistration<Input> {
    pub fn try_new(name: impl Into<String>) -> Result<Self, TaskRegistrationError> {
        let name = TaskName::try_new(name).map_err(TaskRegistrationError::invalid_name)?;

        Ok(Self {
            name,
            scope: Arc::new(|_| Ok(TaskScope::app())),
            key: Arc::new(|_| {
                Err(TaskRegistrationError::new(
                    TaskRegistrationErrorKind::InvalidKey,
                    "task key derivation has not been configured",
                ))
            }),
            policy: TaskPolicy::continue_when_unobserved(),
        })
    }

    pub fn scope(
        mut self,
        scope: impl Fn(&Input) -> Result<TaskScope, TaskRegistrationError> + Send + Sync + 'static,
    ) -> Self {
        self.scope = Arc::new(scope);
        self
    }

    pub fn key(
        mut self,
        key: impl Fn(&Input) -> Result<TaskKey, TaskRegistrationError> + Send + Sync + 'static,
    ) -> Self {
        self.key = Arc::new(key);
        self
    }

    pub fn with_policy(mut self, policy: TaskPolicy) -> Self {
        self.policy = policy;
        self
    }

    pub fn name(&self) -> &TaskName {
        &self.name
    }

    pub fn scope_for(&self, input: &Input) -> Result<TaskScope, TaskRegistrationError> {
        (self.scope)(input)
    }

    pub fn key_for(&self, input: &Input) -> Result<TaskKey, TaskRegistrationError> {
        (self.key)(input)
    }

    pub fn policy(&self) -> &TaskPolicy {
        &self.policy
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskRegistrationError {
    kind: TaskRegistrationErrorKind,
    message: String,
}

impl TaskRegistrationError {
    pub fn invalid_name(error: TaskIdError) -> Self {
        Self::new(
            TaskRegistrationErrorKind::InvalidName,
            format!("invalid task name {:?}: {:?}", error.value(), error.kind()),
        )
    }

    pub fn invalid_key(error: TaskIdError) -> Self {
        Self::new(
            TaskRegistrationErrorKind::InvalidKey,
            format!("invalid task key {:?}: {:?}", error.value(), error.kind()),
        )
    }

    pub fn invalid_scope(error: TaskScopeError) -> Self {
        Self::new(
            TaskRegistrationErrorKind::InvalidScope,
            format!(
                "invalid task scope segment {:?}:{:?}: {:?}",
                error.namespace(),
                error.value(),
                error.kind()
            ),
        )
    }

    pub fn kind(&self) -> TaskRegistrationErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    fn new(kind: TaskRegistrationErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TaskRegistrationErrorKind {
    InvalidName,
    InvalidScope,
    InvalidKey,
}

#[derive(Clone, Debug, Default)]
pub struct TaskCoordination {
    observers: HashMap<TaskKey, HashSet<ObserverId>>,
}

impl TaskCoordination {
    pub fn observe(&mut self, key: TaskKey, observer: ObserverId) {
        self.observers.entry(key).or_default().insert(observer);
    }

    pub fn unobserve(
        &mut self,
        key: &TaskKey,
        observer: &ObserverId,
        policy: &TaskPolicy,
    ) -> ObservationChange {
        let Some(observers) = self.observers.get_mut(key) else {
            return ObservationChange::ObserverNotAttached;
        };

        if !observers.remove(observer) {
            return ObservationChange::ObserverNotAttached;
        }

        let remaining = observers.len();
        if remaining > 0 {
            return ObservationChange::StillObserved { remaining };
        }

        self.observers.remove(key);

        match policy.unobserved_policy() {
            UnobservedPolicy::Cancel => ObservationChange::CancelRequested,
            UnobservedPolicy::LowerPriority => ObservationChange::PriorityLowered,
            UnobservedPolicy::Pause => ObservationChange::Paused,
            UnobservedPolicy::Continue => ObservationChange::ContinuedUnobserved,
        }
    }

    pub fn observer_count(&self, key: &TaskKey) -> usize {
        self.observers.get(key).map(HashSet::len).unwrap_or(0)
    }

    pub fn snapshot_count(&self, key: &TaskKey) -> ObserverCountSnapshot {
        ObserverCountSnapshot::new(key.clone(), self.observer_count(key))
    }

    pub fn is_observed(&self, key: &TaskKey) -> bool {
        self.observer_count(key) > 0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObserverCountSnapshot {
    key: TaskKey,
    count: usize,
}

impl ObserverCountSnapshot {
    pub(crate) fn new(key: TaskKey, count: usize) -> Self {
        Self { key, count }
    }

    pub fn key(&self) -> &TaskKey {
        &self.key
    }

    pub fn count(&self) -> usize {
        self.count
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ObservationChange {
    ObserverNotAttached,
    ObserverDetached,
    StillObserved { remaining: usize },
    CancelRequested,
    PriorityLowered,
    Paused,
    ContinuedUnobserved,
}

#[cfg(test)]
mod tests {
    use crate::{
        ObservationChange, ObserverId, TaskCoordination, TaskKey, TaskPolicy, TaskRegistration,
        TaskRegistrationError, TaskScope, TaskScopeSegment,
    };

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct ImportInput {
        path: String,
    }

    #[test]
    fn task_registration_derives_scope_key_and_policy() {
        let registration = TaskRegistration::<ImportInput>::try_new("import")
            .unwrap()
            .scope(|input| {
                let workspace = TaskScope::try_workspace("alpha")
                    .map_err(TaskRegistrationError::invalid_scope)?;
                let file = TaskScopeSegment::try_new("file", &input.path)
                    .map_err(TaskRegistrationError::invalid_scope)?;
                workspace
                    .try_then(file)
                    .map_err(TaskRegistrationError::invalid_scope)
            })
            .key(|input| {
                TaskKey::try_new(format!("import:{}", input.path))
                    .map_err(TaskRegistrationError::invalid_key)
            })
            .with_policy(TaskPolicy::continue_when_unobserved().dedupe_by_key());

        let input = ImportInput {
            path: "orders.xlsx".into(),
        };
        assert_eq!(registration.name().as_str(), "import");
        assert_eq!(
            registration.key_for(&input).unwrap().as_str(),
            "import:orders.xlsx"
        );
        assert_eq!(
            registration.scope_for(&input).unwrap().last_value("file"),
            Some("orders.xlsx")
        );
        assert!(registration.policy().dedupes_by_key());
    }

    #[test]
    fn observation_policy_does_not_cancel_until_last_observer_detaches() {
        let mut coordination = TaskCoordination::default();
        let key = TaskKey::try_new("compile:main").unwrap();
        let left = ObserverId::try_new("surface:left").unwrap();
        let right = ObserverId::try_new("surface:right").unwrap();
        let policy = TaskPolicy::cancel_when_unobserved();

        coordination.observe(key.clone(), left.clone());
        coordination.observe(key.clone(), right.clone());
        assert_eq!(coordination.observer_count(&key), 2);

        let first = coordination.unobserve(&key, &left, &policy);
        assert_eq!(first, ObservationChange::StillObserved { remaining: 1 });
        assert_eq!(coordination.observer_count(&key), 1);
        assert!(coordination.is_observed(&key));

        let second = coordination.unobserve(&key, &right, &policy);
        assert_eq!(second, ObservationChange::CancelRequested);
        assert_eq!(coordination.observer_count(&key), 0);
        assert!(!coordination.is_observed(&key));

        let missing = coordination.unobserve(&key, &right, &policy);
        assert_eq!(missing, ObservationChange::ObserverNotAttached);
    }
}
