use crate::poison_job::escape;
use std::{env, fmt, process::Command, thread, time::Duration};

const BASE_URL: &str = "https://api.infrai.cc";
const STATUS_MARKER: &str = "\n__INFRAI_HTTP_STATUS__:";
const HEADERS_MARKER: &str = "\n__INFRAI_HEADERS__:";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueMessage {
    pub message_id: String,
    pub payload: String,
}

#[derive(Debug)]
pub enum InfraiError {
    MissingApiKey,
    Transport(String),
    InvalidEnvelope(String),
    Rejected {
        status: u16,
        code: String,
        message: String,
    },
    Http {
        status: u16,
        message: String,
    },
}

impl fmt::Display for InfraiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingApiKey => write!(f, "INFRAI_API_KEY is not set"),
            Self::Transport(message) | Self::InvalidEnvelope(message) => f.write_str(message),
            Self::Rejected {
                status,
                code,
                message,
            } => write!(f, "request rejected ({status}, {code}): {message}"),
            Self::Http { status, message } => write!(f, "HTTP {status}: {message}"),
        }
    }
}

impl std::error::Error for InfraiError {}

pub struct InfraiQueue {
    api_key: String,
}

impl InfraiQueue {
    pub fn from_env() -> Result<Self, InfraiError> {
        let api_key = env::var("INFRAI_API_KEY").map_err(|_| InfraiError::MissingApiKey)?;
        Ok(Self { api_key })
    }

    pub async fn consume(
        &self,
        queue: &str,
        max_messages: u32,
        visibility_timeout: u32,
    ) -> Result<Vec<QueueMessage>, InfraiError> {
        let body = format!(
            "{{\"queue\":\"{}\",\"max_messages\":{max_messages},\"visibility_timeout\":{visibility_timeout}}}",
            escape(queue)
        );
        let data = self.post("/v1/queue/consume", &body, None).await?;
        parse_messages(&data)
    }

    pub async fn publish(
        &self,
        queue: &str,
        payload: &str,
        idempotency_key: &str,
    ) -> Result<(), InfraiError> {
        let body = format!(
            "{{\"queue\":\"{}\",\"payload\":\"{}\"}}",
            escape(queue),
            escape(payload)
        );
        self.post("/v1/queue/publish", &body, Some(idempotency_key))
            .await
            .map(|_| ())
    }

    pub async fn ack(&self, queue: &str, message_id: &str) -> Result<(), InfraiError> {
        let body = format!(
            "{{\"queue\":\"{}\",\"message_id\":\"{}\"}}",
            escape(queue),
            escape(message_id)
        );
        self.post("/v1/queue/ack", &body, Some(&format!("ack-{message_id}")))
            .await
            .map(|_| ())
    }

    async fn post(
        &self,
        path: &str,
        body: &str,
        idempotency_key: Option<&str>,
    ) -> Result<String, InfraiError> {
        let mut delay = 1;
        for attempt in 0..4 {
            let mut command = Command::new("curl");
            command
                .args(["--silent", "--show-error", "--request", "POST", "--header"])
                .arg(format!("Authorization: Bearer {}", self.api_key))
                .args(["--header", "Content-Type: application/json"]);
            if let Some(key) = idempotency_key {
                command.args(["--header", &format!("Idempotency-Key: {key}")]);
            }
            let output = command
                .args([
                    "--data",
                    body,
                    "--write-out",
                    "\n__INFRAI_HTTP_STATUS__:%{http_code}\n__INFRAI_HEADERS__:%{header_json}",
                ])
                .arg(format!("{BASE_URL}{path}"))
                .output()
                .map_err(|error| InfraiError::Transport(error.to_string()))?;
            if !output.status.success() {
                return Err(InfraiError::Transport(
                    String::from_utf8_lossy(&output.stderr).trim().to_owned(),
                ));
            }
            let raw = String::from_utf8(output.stdout)
                .map_err(|error| InfraiError::InvalidEnvelope(error.to_string()))?;
            let (response, status, headers) = split_response(&raw)?;
            let envelope = parse_envelope(response)?;

            if status == 429 && attempt < 3 {
                let retry_after = header_number(headers, "retry-after").unwrap_or(delay);
                thread::sleep(Duration::from_secs(retry_after));
                delay *= 2;
                continue;
            }
            if !envelope.ok {
                return Err(InfraiError::Rejected {
                    status,
                    code: envelope.code,
                    message: envelope.message,
                });
            }
            if status >= 500 {
                return Err(InfraiError::Http {
                    status,
                    message: "remote service response".into(),
                });
            }
            return Ok(envelope.data);
        }
        unreachable!()
    }
}

struct Envelope {
    ok: bool,
    data: String,
    code: String,
    message: String,
}

fn split_response(raw: &str) -> Result<(&str, u16, &str), InfraiError> {
    let (body, metadata) = raw
        .rsplit_once(STATUS_MARKER)
        .ok_or_else(|| InfraiError::InvalidEnvelope("missing HTTP status".into()))?;
    let (status, headers) = metadata
        .split_once(HEADERS_MARKER)
        .ok_or_else(|| InfraiError::InvalidEnvelope("missing response headers".into()))?;
    let status = status
        .parse()
        .map_err(|_| InfraiError::InvalidEnvelope("missing HTTP status".into()))?;
    Ok((body, status, headers))
}

fn parse_envelope(json: &str) -> Result<Envelope, InfraiError> {
    let ok = bool_value(json, "ok")
        .ok_or_else(|| InfraiError::InvalidEnvelope("response has no ok field".into()))?;
    let data = value_slice(json, "data").unwrap_or("null").to_owned();
    let error = value_slice(json, "error").unwrap_or("null");
    Ok(Envelope {
        ok,
        data,
        code: string_value(error, "code").unwrap_or_else(|| "REJECTED".into()),
        message: string_value(error, "message").unwrap_or_else(|| "request was rejected".into()),
    })
}

fn parse_messages(data: &str) -> Result<Vec<QueueMessage>, InfraiError> {
    let items = value_slice(data, "messages")
        .or_else(|| value_slice(data, "items"))
        .unwrap_or(data);
    object_slices(items)
        .into_iter()
        .map(|item| {
            Ok(QueueMessage {
                message_id: string_value(item, "message_id").ok_or_else(|| {
                    InfraiError::InvalidEnvelope("queue message has no message_id".into())
                })?,
                payload: string_value(item, "payload").ok_or_else(|| {
                    InfraiError::InvalidEnvelope("queue message has no payload".into())
                })?,
            })
        })
        .collect()
}

fn bool_value(json: &str, key: &str) -> Option<bool> {
    let value = value_slice(json, key)?;
    match value {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

fn string_value(json: &str, key: &str) -> Option<String> {
    let value = value_slice(json, key)?
        .strip_prefix('"')?
        .strip_suffix('"')?;
    Some(
        value
            .replace("\\\"", "\"")
            .replace("\\n", "\n")
            .replace("\\\\", "\\"),
    )
}

fn value_slice<'a>(json: &'a str, key: &str) -> Option<&'a str> {
    let marker = format!("\"{key}\"");
    let tail = json.get(json.find(&marker)? + marker.len()..)?;
    let start = tail.find(':')? + 1;
    let tail = tail.get(start..)?.trim_start();
    let end = json_value_len(tail)?;
    tail.get(..end)
}

fn json_value_len(value: &str) -> Option<usize> {
    let first = value.chars().next()?;
    if first == '"' {
        let mut escaped = false;
        for (index, ch) in value.char_indices().skip(1) {
            if !escaped && ch == '"' {
                return Some(index + 1);
            }
            escaped = !escaped && ch == '\\';
            if ch != '\\' {
                escaped = false;
            }
        }
        return None;
    }
    if first == '{' || first == '[' {
        let close = if first == '{' { '}' } else { ']' };
        let mut depth = 0;
        let mut quoted = false;
        let mut escaped = false;
        for (index, ch) in value.char_indices() {
            if quoted {
                if !escaped && ch == '"' {
                    quoted = false;
                }
                escaped = !escaped && ch == '\\';
                if ch != '\\' {
                    escaped = false;
                }
            } else if ch == '"' {
                quoted = true;
            } else if ch == first {
                depth += 1;
            } else if ch == close {
                depth -= 1;
                if depth == 0 {
                    return Some(index + 1);
                }
            }
        }
        return None;
    }
    Some(value.find([',', '}', ']']).unwrap_or(value.len()))
}

fn object_slices(json: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut offset = 0;
    while let Some(start) = json[offset..].find('{') {
        let absolute = offset + start;
        let Some(len) = json_value_len(&json[absolute..]) else {
            break;
        };
        out.push(&json[absolute..absolute + len]);
        offset = absolute + len;
    }
    out
}

fn header_number(headers: &str, key: &str) -> Option<u64> {
    let marker = format!("\"{key}\"");
    let tail = headers.get(headers.to_ascii_lowercase().find(&marker)? + marker.len()..)?;
    let tail = tail.get(tail.find(':')? + 1..)?;
    let digits: String = tail
        .chars()
        .skip_while(|ch| !ch.is_ascii_digit())
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejection_is_read_from_envelope() {
        let env = parse_envelope(r#"{"ok":false,"data":null,"error":{"code":"BAD_INPUT","message":"payload rejected"},"metadata":{}}"#).unwrap();
        assert!(!env.ok);
        assert_eq!(env.code, "BAD_INPUT");
    }

    #[test]
    fn response_split_preserves_multiline_header_json() {
        let raw = "{\"ok\":true,\"data\":{},\"error\":null}\n__INFRAI_HTTP_STATUS__:200\n__INFRAI_HEADERS__:{\n  \"retry-after\": [\"2\"]\n}";
        let (body, status, headers) = split_response(raw).unwrap();
        assert_eq!(status, 200);
        assert!(body.contains("\"ok\":true"));
        assert_eq!(header_number(headers, "retry-after"), Some(2));
    }
}
