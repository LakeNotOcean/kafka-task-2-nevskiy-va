mod kafka;
mod processor;

use common::config::Config;
use common::event::Event;
use common::sink::Sink;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::FutureProducer;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // инициализация логирования
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = Config::from_env()?;
    let sink = Sink::open(&cfg.db_path).await?;
    let offsets = kafka::ProcessedOffsets::default();
    let consumer = kafka::build_consumer(&cfg, offsets.clone())?;

    // для битых сообщений
    let dlt = kafka::build_dlt_producer(&cfg)?;
    consumer.subscribe(&[&cfg.topic])?;

    info!(pod = %cfg.pod_id, group = %cfg.group_id, topic = %cfg.topic, "consumer starting");
    run(&consumer, &dlt, &sink, &offsets, &cfg).await
}

async fn run(
    consumer: &StreamConsumer<kafka::GroupContext>,
    dlt: &FutureProducer,
    sink: &Sink,
    offsets: &kafka::ProcessedOffsets,
    cfg: &Config,
) -> Result<(), Box<dyn std::error::Error>> {
    let shutdown = common::shutdown::shutdown_signal();
    tokio::pin!(shutdown);
    let mut processed: u64 = 0;
    let mut duplicates: u64 = 0;
    let mut to_dlt: u64 = 0;

    loop {
        tokio::select! {
            received = consumer.recv() => {
                let msg = received?;
                let topic = msg.topic().to_owned();
                let partition = msg.partition();
                let offset = msg.offset();
                let src = kafka::SourcePos {
                    topic: &topic,
                    partition,
                    offset,
                };
                let parsed: Result<Event, String> = match msg.payload() {
                    Some(payload) => serde_json::from_slice(payload)
                        .map_err(|err| format!("deserialize failed: {err}")),
                    None => Err("message without payload".to_owned()),
                };

                match parsed {
                    Ok(event) => match processor::process(&event, cfg).await {
                        Ok(()) => {
                            let inserted = sink.record(&event, &cfg.pod_id, &topic, partition, offset).await?;
                            if inserted {
                                processed += 1;
                            } else {
                                duplicates += 1;
                                warn!(event_id = %event.event_id, "duplicate suppressed");
                            }
                        }
                        Err(reason) => {
                            kafka::send_to_dlt(dlt, cfg, &event, src, &reason).await?;
                            to_dlt += 1;
                            warn!(event_id = %event.event_id, reason = %reason, "poison moved to DLT");
                        }
                    },
                    Err(reason) => {
                        // мервтое сообщение в dlt
                        kafka::send_raw_to_dlt(dlt, cfg, msg.key(), msg.payload(), src, &reason).await?;
                        to_dlt += 1;
                        warn!(reason = %reason, partition, offset, "invalid message moved to DLT");
                    }
                }

                // искусственная задержка коммита
                if cfg.commit_delay_ms > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(cfg.commit_delay_ms)).await;
                }
                // обработали
                consumer.commit_message(&msg, CommitMode::Sync)?;
                offsets.track(&topic, partition, offset + 1);

                let total = processed + duplicates + to_dlt;
                if total > 0 && total.is_multiple_of(100) {
                    info!(processed, duplicates, to_dlt, "progress");
                }
            }
            _ = &mut shutdown => {
                info!("shutdown signal received");
                break;
            }
        }
    }

    info!(processed, duplicates, to_dlt, "consumer stopped");
    Ok(())
}
