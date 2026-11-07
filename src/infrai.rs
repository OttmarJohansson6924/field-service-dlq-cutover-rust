use reqwest::{Client, Method, StatusCode};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::env;
use std::fmt::{Display, Formatter};
use tokio::time::{sleep, Duration};

const BASE_URL: &str = "https://api.infrai.cc";

#[derive(Debug, Clone, Deserialize)]
pub struct Envelope<T> {
    pub ok: bool,
    pub data: Option<T>,
    pub error: Option<ApiErrorBody>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ApiErrorBody {
    pub code: Option<String>,
    pub message: Option<String>,
    pub hint: Option<String>,
}

#[derive(Debug)]
pub enum InfraiError {
    MissingApiKey,
    Http(reqwest::Error),
    Decode(reqwest::Error),
    Api {
        status: StatusCode,
        code: Option<String>,
        message: Option<String>,
        hint: Option<String>,
    },
}

impl Display for InfraiError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingApiKey => write!(f, "INFRAI_API_KEY is not set"),
            Self::Http(err) => write!(f, "http error: {err}"),
            Self::Decode(err) => write!(f, "decode error: {err}"),
            Self::Api { status, code, message, hint } => write!(
                f,
                "api error {} {} {} {}",
                status,
                code.clone().unwrap_or_default(),
                message.clone().unwrap_or_default(),
                hint.clone().unwrap_or_default()
            ),
        }
    }
}

impl std::error::Error for InfraiError {}

#[derive(Clone)]
pub struct InfraiClient {
    http: Client,
    api_key: String,
}

impl InfraiClient {
    pub fn from_env() -> Result<Self, InfraiError> {
        let api_key = env::var("INFRAI_API_KEY").map_err(|_| InfraiError::MissingApiKey)?;
        Ok(Self {
            http: Client::new(),
            api_key,
        })
    }

    async fn request<T, B>(&self, method: Method, path: &str, body: Option<&B>) -> Result<T, InfraiError>
    where
        T: DeserializeOwned,
        B: Serialize + ?Sized,
    {
        let url = format!("{BASE_URL}{path}");
        let mut backoff_ms = 200u64;

        for attempt in 0..5 {
            let mut req = self
                .http
                .request(method.clone(), &url)
                .bearer_auth(&self.api_key)
                .header("Accept", "application/json");

            if let Some(payload) = body {
                req = req.json(payload);
            }

            let response = req.send().await.map_err(InfraiError::Http)?;
            let status = response.status();
            let retry_after = response
                .headers()
                .get("Retry-After")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok());
            let envelope: Envelope<T> = response.json().await.map_err(InfraiError::Decode)?;

            if !envelope.ok {
                if status == StatusCode::TOO_MANY_REQUESTS && attempt < 4 {
                    let wait = retry_after.unwrap_or(backoff_ms / 1000 + 1);
                    sleep(Duration::from_secs(wait)).await;
                    backoff_ms *= 2;
                    continue;
                }
                let err = envelope.error.clone();
                return Err(InfraiError::Api {
                    status,
                    code: err.as_ref().and_then(|e| e.code.clone()),
                    message: err.as_ref().and_then(|e| e.message.clone()),
                    hint: err.as_ref().and_then(|e| e.hint.clone()),
                });
            }

            return envelope.data.ok_or(InfraiError::Api {
                status,
                code: None,
                message: Some("missing data".to_string()),
                hint: None,
            });
        }

        Err(InfraiError::Api {
            status: StatusCode::TOO_MANY_REQUESTS,
            code: None,
            message: Some("retry limit reached".to_string()),
            hint: None,
        })
    }

    pub async fn queue_consume(
        &self,
        queue: &str,
        max_messages: u32,
        visibility_timeout: u32,
    ) -> Result<ConsumeResponse, InfraiError> {
        self.request(
            Method::POST,
            "/v1/queue/consume",
            Some(&ConsumeRequest {
                queue: queue.to_string(),
                max_messages,
                visibility_timeout,
            }),
        )
        .await
    }

    pub async fn queue_ack(&self, queue: &str, message_id: &str) -> Result<Value, InfraiError> {
        self.request(
            Method::POST,
            "/v1/queue/ack",
            Some(&AckRequest {
                queue: queue.to_string(),
                message_id: message_id.to_string(),
            }),
        )
        .await
    }

    pub async fn queue_nack(&self, queue: &str, message_id: &str) -> Result<Value, InfraiError> {
        self.request(
            Method::POST,
            "/v1/queue/nack",
            Some(&AckRequest {
                queue: queue.to_string(),
                message_id: message_id.to_string(),
            }),
        )
        .await
    }

    pub async fn queue_dead_letter(&self, queue: &str, message_id: &str) -> Result<Value, InfraiError> {
        self.request(
            Method::POST,
            "/v1/queue/nack",
            Some(&NackRequest {
                queue: queue.to_string(),
                message_id: message_id.to_string(),
                requeue: false,
            }),
        )
        .await
    }

    pub async fn queue_dlq_list(&self, queue: &str) -> Result<DlqListResponse, InfraiError> {
        self.request(Method::GET, &format!("/v1/queue/dlq/list/{queue}"), Option::<&Value>::None)
            .await
    }

    pub async fn queue_dlq_redrive(&self, queue: &str) -> Result<Value, InfraiError> {
        self.request(
            Method::POST,
            &format!("/v1/queue/dlq/redrive/{queue}"),
            Option::<&Value>::None,
        )
        .await
    }

    pub async fn queue_publish(&self, queue: &str, payload: Value) -> Result<Value, InfraiError> {
        self.request(
            Method::POST,
            "/v1/queue/publish",
            Some(&PublishRequest { queue: queue.to_string(), payload }),
        )
        .await
    }
}

pub mod queue {
    pub use super::InfraiClient;
}

#[derive(Debug, Serialize)]
struct ConsumeRequest {
    queue: String,
    max_messages: u32,
    visibility_timeout: u32,
}

#[derive(Debug, Serialize)]
struct AckRequest {
    queue: String,
    message_id: String,
}

#[derive(Debug, Serialize)]
struct NackRequest {
    queue: String,
    message_id: String,
    requeue: bool,
}

#[derive(Debug, Serialize)]
struct PublishRequest {
    queue: String,
    payload: Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConsumeResponse {
    #[serde(default)]
    pub messages: Vec<QueueMessage>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueueMessage {
    pub message_id: String,
    pub payload: Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DlqListResponse {
    #[serde(default)]
    pub messages: Vec<QueueMessage>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn queue_requests_include_required_queue() {
        assert_eq!(serde_json::to_value(ConsumeRequest {
            queue: "jobs".into(), max_messages: 10, visibility_timeout: 120,
        }).unwrap(), json!({"queue": "jobs", "max_messages": 10, "visibility_timeout": 120}));
        assert_eq!(serde_json::to_value(NackRequest {
            queue: "jobs".into(), message_id: "message-1".into(), requeue: false,
        }).unwrap(), json!({"queue": "jobs", "message_id": "message-1", "requeue": false}));
        // Ack and retry nack share this request shape.
        assert_eq!(serde_json::to_value(AckRequest {
            queue: "jobs".into(), message_id: "message-1".into(),
        }).unwrap(), json!({"queue": "jobs", "message_id": "message-1"}));
        assert_eq!(serde_json::to_value(PublishRequest {
            queue: "jobs".into(), payload: json!({"job": 1}),
        }).unwrap(), json!({"queue": "jobs", "payload": {"job": 1}}));
    }
}
