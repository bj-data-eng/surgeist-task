const RESERVED_NAMESPACES: &[&str] = &["ui", "window", "render", "retained"];

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TaskScopeSegment {
    namespace: String,
    value: String,
}

impl TaskScopeSegment {
    pub fn try_new(
        namespace: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, TaskScopeError> {
        let namespace = namespace.into();
        let value = value.into();
        let trimmed_namespace = namespace.trim();
        let trimmed_value = value.trim();

        if trimmed_namespace.is_empty() {
            return Err(TaskScopeError {
                kind: TaskScopeErrorKind::EmptyNamespace,
                namespace,
                value,
            });
        }

        if trimmed_value.is_empty() {
            return Err(TaskScopeError {
                kind: TaskScopeErrorKind::EmptyValue,
                namespace,
                value,
            });
        }

        if trimmed_namespace
            .chars()
            .chain(trimmed_value.chars())
            .any(|character| character.is_ascii_control())
        {
            return Err(TaskScopeError {
                kind: TaskScopeErrorKind::InvalidCharacter,
                namespace,
                value,
            });
        }

        if RESERVED_NAMESPACES.contains(&trimmed_namespace) {
            return Err(TaskScopeError {
                kind: TaskScopeErrorKind::ReservedNamespace,
                namespace,
                value,
            });
        }

        Ok(Self {
            namespace: trimmed_namespace.to_owned(),
            value: trimmed_value.to_owned(),
        })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct TaskScope {
    segments: Vec<TaskScopeSegment>,
}

impl TaskScope {
    pub fn app() -> Self {
        Self {
            segments: Vec::new(),
        }
    }

    pub fn try_custom(
        namespace: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, TaskScopeError> {
        Ok(Self::app().then(TaskScopeSegment::try_new(namespace, value)?))
    }

    pub fn try_workspace(value: impl Into<String>) -> Result<Self, TaskScopeError> {
        Self::try_custom("workspace", value)
    }

    pub fn try_resource(value: impl Into<String>) -> Result<Self, TaskScopeError> {
        Self::try_custom("resource", value)
    }

    pub fn then(mut self, segment: TaskScopeSegment) -> Self {
        self.segments.push(segment);
        self
    }

    pub fn try_then(self, segment: TaskScopeSegment) -> Result<Self, TaskScopeError> {
        Ok(self.then(segment))
    }

    pub fn segments(&self) -> &[TaskScopeSegment] {
        &self.segments
    }

    pub fn is_app(&self) -> bool {
        self.segments.is_empty()
    }

    pub fn last_value(&self, namespace: &str) -> Option<&str> {
        let namespace = namespace.trim();
        self.segments
            .iter()
            .rev()
            .find(|segment| segment.namespace() == namespace)
            .map(TaskScopeSegment::value)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TaskScopeErrorKind {
    EmptyNamespace,
    EmptyValue,
    InvalidCharacter,
    ReservedNamespace,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskScopeError {
    kind: TaskScopeErrorKind,
    namespace: String,
    value: String,
}

impl TaskScopeError {
    pub fn kind(&self) -> TaskScopeErrorKind {
        self.kind
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

#[cfg(test)]
mod tests {
    use super::{TaskScope, TaskScopeErrorKind, TaskScopeSegment};

    #[test]
    fn scope_is_ui_agnostic_path_data() {
        let scope = TaskScope::app()
            .then(TaskScopeSegment::try_new("workspace", "alpha").unwrap())
            .then(TaskScopeSegment::try_new("resource", "orders").unwrap());

        assert!(!scope.is_app());
        assert_eq!(scope.last_value("workspace"), Some("alpha"));
        assert_eq!(scope.last_value("resource"), Some("orders"));
    }

    #[test]
    fn try_then_accepts_prevalidated_scope_segment() {
        let workspace = TaskScope::try_workspace("alpha").unwrap();
        let file = TaskScopeSegment::try_new("resource", "orders.csv").unwrap();

        let scope = workspace.try_then(file).unwrap();

        assert_eq!(scope.last_value("workspace"), Some("alpha"));
        assert_eq!(scope.last_value("resource"), Some("orders.csv"));
    }

    #[test]
    fn scope_segments_reject_invalid_or_reserved_namespaces() {
        assert_eq!(
            TaskScopeSegment::try_new("", "alpha").unwrap_err().kind(),
            TaskScopeErrorKind::EmptyNamespace
        );
        assert_eq!(
            TaskScopeSegment::try_new("ui", "button")
                .unwrap_err()
                .kind(),
            TaskScopeErrorKind::ReservedNamespace
        );
    }
}
