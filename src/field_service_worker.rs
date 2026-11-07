use crate::infrai::{InfraiClient, InfraiError, QueueMessage};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WorkOrderPhoto {
    pub label: String,
    pub captured: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DispatchStatus {
    pub state: String,
    pub technician_id: String,
    pub attempts: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WorkOrderJob {
    pub work_order_id: String,
    pub site_id: String,
    pub dispatch_status: DispatchStatus,
    pub required_photos: Vec<WorkOrderPhoto>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobDecision {
    Complete,
    RetryMissingPhotos { next_attempt: u32 },
    DeadLetterFollowUp,
}

#[derive(Debug)]
pub enum WorkerError {
    Infra(InfraiError),
    InvalidPayload(serde_json::Error),
}

impl Display for WorkerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Infra(err) => write!(f, "{err}"),
            Self::InvalidPayload(err) => write!(f, "invalid payload: {err}"),
        }
    }
}

impl std::error::Error for WorkerError {}

impl From<InfraiError> for WorkerError {
    fn from(value: InfraiError) -> Self {
        Self::Infra(value)
    }
}

pub fn decide_job(job: &WorkOrderJob) -> JobDecision {
    let missing_photos = job.required_photos.iter().any(|photo| !photo.captured);
    if !missing_photos {
        return JobDecision::Complete;
    }

    if job.dispatch_status.state == "follow_up_required" || job.dispatch_status.attempts >= 3 {
        JobDecision::DeadLetterFollowUp
    } else {
        JobDecision::RetryMissingPhotos {
            next_attempt: job.dispatch_status.attempts + 1,
        }
    }
}

pub async fn handle_message(client: &InfraiClient, message: &QueueMessage) -> Result<JobDecision, WorkerError> {
    let job: WorkOrderJob = serde_json::from_value(message.payload.clone()).map_err(WorkerError::InvalidPayload)?;
    let decision = decide_job(&job);

    match &decision {
        JobDecision::Complete => {
            client.queue_ack("field-service-jobs", &message.message_id).await?;
        }
        JobDecision::RetryMissingPhotos { next_attempt } => {
            let mut payload = message.payload.clone();
            payload["dispatch_status"]["attempts"] = json!(next_attempt);
            client.queue_publish("field-service-jobs", payload).await?;
            client.queue_ack("field-service-jobs", &message.message_id).await?;
        }
        JobDecision::DeadLetterFollowUp => {
            client.queue_dead_letter("field-service-jobs", &message.message_id).await?;
            client
                .queue_publish("field-service-follow-ups", json!({
                    "kind": "technician_follow_up",
                    "work_order_id": job.work_order_id,
                    "site_id": job.site_id,
                    "technician_id": job.dispatch_status.technician_id,
                    "reason": "missing_required_photo"
                }))
                .await?;
        }
    }

    Ok(decision)
}

pub async fn run_once(client: &InfraiClient) -> Result<Vec<JobDecision>, WorkerError> {
    let batch = client.queue_consume("field-service-jobs", 10, 120).await?;
    let mut decisions = Vec::with_capacity(batch.messages.len());

    for message in &batch.messages {
        decisions.push(handle_message(client, message).await?);
    }

    Ok(decisions)
}

pub async fn list_dead_letters(client: &InfraiClient, queue: &str) -> Result<Vec<QueueMessage>, WorkerError> {
    let dlq = client.queue_dlq_list(queue).await?;
    Ok(dlq.messages)
}

pub async fn redrive_dead_letters(client: &InfraiClient, queue: &str) -> Result<(), WorkerError> {
    client.queue_dlq_redrive(queue).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sends_three_strike_missing_photo_to_follow_up() {
        let job = WorkOrderJob {
            work_order_id: "WO-1042".to_string(),
            site_id: "SITE-9".to_string(),
            dispatch_status: DispatchStatus {
                state: "en_route".to_string(),
                technician_id: "TECH-18".to_string(),
                attempts: 3,
            },
            required_photos: vec![
                WorkOrderPhoto {
                    label: "meter".to_string(),
                    captured: true,
                },
                WorkOrderPhoto {
                    label: "panel".to_string(),
                    captured: false,
                },
            ],
        };

        let decision = decide_job(&job);
        assert_eq!(decision, JobDecision::DeadLetterFollowUp);
    }
}
