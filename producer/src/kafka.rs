use std::time::Duration;

use common::config::{Config, KeyMode};
use common::event::Event;
use common::store::Store;
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use tokio::task::JoinSet;
use tracing::{error, info};

// таймаут досылки при остановке должен быть меньше stop_grace_period в docker
pub const FLUSH_TIMEOUT: Duration = Duration::from_secs(20);

// собирает идемпотентный продюсер с acks=all
pub fn build_producer(cfg: &Config) -> Result<FutureProducer, KafkaError> {
    ClientConfig::new()
        .set("bootstrap.servers", &cfg.kafka_brokers)
        .set("client.id", &cfg.pod_id)
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("message.timeout.ms", "30000")
        .create()
}

fn build_record<'a>(cfg: &'a Config, key: &'a str, payload: &'a str) -> FutureRecord<'a, str, str> {
    let record: FutureRecord<'a, str, str> = FutureRecord::to(&cfg.topic).payload(payload);
    match cfg.key_mode {
        KeyMode::UserId => record.key(key),
        KeyMode::None => record,
    }
}

// досылает недоставленные события outbox
pub async fn resend_unsent(
    producer: &FutureProducer,
    store: &Store,
    cfg: &Config,
) -> Result<(), Box<dyn std::error::Error>> {
    let events = store.unsent().await?;
    if events.is_empty() {
        return Ok(());
    }
    info!(count = events.len(), "resending outbox");
    for e in events {
        let key = e.user_id.to_string();
        let payload = serde_json::to_string(&e)?;
        let record = build_record(cfg, &key, &payload);
        match producer.send(record, Timeout::After(FLUSH_TIMEOUT)).await {
            Ok(Ok(_)) => store.mark_sent(e.event_id).await?,
            Ok(Err((_, err))) => {
                error!(event_id = %e.event_id, error = %err, "outbox resend rejected by broker")
            }
            Err(err) => {
                error!(event_id = %e.event_id, error = %err, "outbox resend channel failed")
            }
        }
    }
    Ok(())
}

// поставновка события в очередь librdkafka 
// событие записывается в store
pub fn queue_event(
    producer: &FutureProducer,
    store: &Store,
    cfg: &Config,
    event: &Event,
    pending: &mut JoinSet<()>,
) {
    let key = event.user_id.to_string();
    let payload = match serde_json::to_string(event) {
        Ok(p) => p,
        Err(err) => {
            error!(event_id = %event.event_id, error = %err, "serialize failed");
            return;
        }
    };
    let record = build_record(cfg, &key, &payload);
    match producer.send_result(record) {
        Ok(delivery) => {
            let store = store.clone();
            let event_id = event.event_id;
            pending.spawn(async move {
                match delivery.await {
                    Ok(Ok(_)) => {
                        if let Err(err) = store.mark_sent(event_id).await {
                            error!(event_id = %event_id, error = %err, "mark_sent failed");
                        }
                    }
                    Ok(Err((_, err))) => {
                        error!(event_id = %event_id, error = %err, "broker rejected")
                    }
                    Err(err) => {
                        error!(event_id = %event_id, error = %err, "delivery channel failed")
                    }
                }
            });
        }
        Err((err, _)) => {
            error!(event_id = %event.event_id, error = %err, "queue rejected, event stays in outbox")
        }
    }
}
