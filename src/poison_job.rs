use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildEvent {
    pub build_id: String,
    pub operation: String,
    pub attempts: u32,
    pub diagnostic: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLetterDiagnostic {
    pub build_id: String,
    pub operation: String,
    pub attempts: u32,
    pub diagnostic: String,
    pub action: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobDecision {
    Retry,
    DeadLetter(DeadLetterDiagnostic),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventError {
    MissingField(&'static str),
    InvalidAttempts,
}

impl fmt::Display for EventError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingField(name) => write!(f, "missing event field: {name}"),
            Self::InvalidAttempts => write!(f, "attempts must be an unsigned integer"),
        }
    }
}

impl std::error::Error for EventError {}

impl BuildEvent {
    pub fn from_payload(payload: &str) -> Result<Self, EventError> {
        Ok(Self {
            build_id: string_field(payload, "build_id")
                .ok_or(EventError::MissingField("build_id"))?,
            operation: string_field(payload, "operation")
                .ok_or(EventError::MissingField("operation"))?,
            attempts: number_field(payload, "attempts").ok_or(EventError::InvalidAttempts)?,
            diagnostic: string_field(payload, "diagnostic")
                .ok_or(EventError::MissingField("diagnostic"))?,
        })
    }

    pub fn decide(self, max_attempts: u32) -> JobDecision {
        if self.attempts < max_attempts {
            JobDecision::Retry
        } else {
            JobDecision::DeadLetter(DeadLetterDiagnostic {
                build_id: self.build_id,
                operation: self.operation,
                attempts: self.attempts,
                diagnostic: self.diagnostic,
                action: "inspect release logs, correct the input, then replay",
            })
        }
    }
}

impl DeadLetterDiagnostic {
    pub fn to_json(&self) -> String {
        format!(
            "{{\"build_id\":\"{}\",\"operation\":\"{}\",\"attempts\":{},\"diagnostic\":\"{}\",\"action\":\"{}\"}}",
            escape(&self.build_id), escape(&self.operation), self.attempts,
            escape(&self.diagnostic), escape(self.action)
        )
    }
}

fn quoted_value<'a>(json: &'a str, key: &str) -> Option<&'a str> {
    let marker = format!("\"{key}\"");
    let tail = json.get(json.find(&marker)? + marker.len()..)?;
    let tail = tail.get(tail.find(':')? + 1..)?.trim_start();
    tail.strip_prefix('"')
}

fn string_field(json: &str, key: &str) -> Option<String> {
    let tail = quoted_value(json, key)?;
    let mut out = String::new();
    let mut escaped = false;
    for ch in tail.chars() {
        if escaped {
            out.push(match ch {
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                other => other,
            });
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == '"' {
            return Some(out);
        } else {
            out.push(ch);
        }
    }
    None
}

fn number_field(json: &str, key: &str) -> Option<u32> {
    let marker = format!("\"{key}\"");
    let tail = json.get(json.find(&marker)? + marker.len()..)?;
    let tail = tail.get(tail.find(':')? + 1..)?.trim_start();
    let digits: String = tail.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
}

pub(crate) fn escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exhausted_release_becomes_actionable_dead_letter() {
        let event = BuildEvent::from_payload(
            r#"{"build_id":"build-1842","operation":"publish-cli","attempts":3,"diagnostic":"artifact checksum mismatch"}"#,
        ).unwrap();

        let JobDecision::DeadLetter(diagnostic) = event.decide(3) else {
            panic!("expected dead-letter decision");
        };

        assert_eq!(diagnostic.build_id, "build-1842");
        assert_eq!(diagnostic.operation, "publish-cli");
        assert!(diagnostic.action.contains("replay"));
    }

    #[test]
    fn fresh_failure_stays_available_for_retry() {
        let event = BuildEvent {
            build_id: "b7".into(),
            operation: "package".into(),
            attempts: 1,
            diagnostic: "compiler exit 1".into(),
        };
        assert_eq!(event.decide(3), JobDecision::Retry);
    }
}
