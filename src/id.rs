#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TaskId(u64);

impl TaskId {
    pub const fn from_u64(value: u64) -> Self {
        Self(value)
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TaskAttemptId(u64);

impl TaskAttemptId {
    pub const fn from_u64(value: u64) -> Self {
        Self(value)
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CorrelationId(u64);

impl CorrelationId {
    pub const fn from_u64(value: u64) -> Self {
        Self(value)
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TaskName(String);

impl TaskName {
    pub fn try_new(value: impl Into<String>) -> Result<Self, TaskIdError> {
        validate_string_id(value).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TaskKey(String);

impl TaskKey {
    pub fn try_new(value: impl Into<String>) -> Result<Self, TaskIdError> {
        validate_string_id(value).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ObserverId(String);

impl ObserverId {
    pub fn try_new(value: impl Into<String>) -> Result<Self, TaskIdError> {
        validate_string_id(value).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ResourceClassId(String);

impl ResourceClassId {
    pub fn try_new(value: impl Into<String>) -> Result<Self, TaskIdError> {
        validate_string_id(value).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CoalescingKey(String);

impl CoalescingKey {
    pub fn try_new(value: impl Into<String>) -> Result<Self, TaskIdError> {
        validate_string_id(value).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TaskIdErrorKind {
    Empty,
    InvalidCharacter,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskIdError {
    kind: TaskIdErrorKind,
    value: String,
}

impl TaskIdError {
    pub fn kind(&self) -> TaskIdErrorKind {
        self.kind
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

fn validate_string_id(value: impl Into<String>) -> Result<String, TaskIdError> {
    let value = value.into();
    let trimmed = value.trim();

    if trimmed.is_empty() {
        return Err(TaskIdError {
            kind: TaskIdErrorKind::Empty,
            value,
        });
    }

    if trimmed
        .chars()
        .any(|character| character.is_ascii_control())
    {
        return Err(TaskIdError {
            kind: TaskIdErrorKind::InvalidCharacter,
            value,
        });
    }

    Ok(trimmed.to_owned())
}

#[cfg(test)]
mod tests {
    use super::{
        CoalescingKey, CorrelationId, TaskAttemptId, TaskId, TaskIdErrorKind, TaskKey, TaskName,
    };

    #[test]
    fn ids_preserve_domain_meaning() {
        assert_eq!(TaskId::from_u64(7).as_u64(), 7);
        assert_eq!(TaskAttemptId::from_u64(3).as_u64(), 3);
        assert_eq!(TaskName::try_new("import").unwrap().as_str(), "import");
        assert_eq!(
            TaskKey::try_new("import:orders").unwrap().as_str(),
            "import:orders"
        );
        assert_eq!(CorrelationId::from_u64(99).as_u64(), 99);
    }

    #[test]
    fn string_ids_reject_empty_or_whitespace_values() {
        assert_eq!(
            TaskName::try_new("").unwrap_err().kind(),
            TaskIdErrorKind::Empty
        );
        assert_eq!(
            TaskKey::try_new("   ").unwrap_err().kind(),
            TaskIdErrorKind::Empty
        );
        assert_eq!(
            CoalescingKey::try_new("rows\ncount").unwrap_err().kind(),
            TaskIdErrorKind::InvalidCharacter
        );
    }
}
