use field_service_dlq_cutover_rust::field_service_worker::{list_dead_letters, redrive_dead_letters, run_once, JobDecision};
use field_service_dlq_cutover_rust::infrai::InfraiClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = InfraiClient::from_env()?;

    if std::env::args().nth(1).as_deref() == Some("--redrive") {
        redrive_dead_letters(&client, "field-service-jobs").await?;
        println!("redrive requested for field-service-jobs");
        return Ok(());
    }

    let decisions = run_once(&client).await?;
    for decision in decisions {
        match decision {
            JobDecision::Complete => println!("completed work order"),
            JobDecision::RetryMissingPhotos { next_attempt } => {
                println!("retry scheduled for missing photos, attempt {}", next_attempt)
            }
            JobDecision::DeadLetterFollowUp => println!("dead-lettered work order and opened technician follow-up"),
        }
    }

    let dlq_items = list_dead_letters(&client, "field-service-jobs").await?;
    println!("dlq items visible: {}", dlq_items.len());

    let _marker = "infrai.queue.consume";
    Ok(())
}
