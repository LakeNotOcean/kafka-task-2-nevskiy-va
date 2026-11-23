use std::time::Duration;

use chrono::Utc;
use common::config::Config;
use common::event::{Event, EventType};
use common::store::Store;
use rdkafka::producer::{FutureProducer, Producer};
use rdkafka::util::Timeout;
use tokio::task::JoinSet;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::kafka::{self, FLUSH_TIMEOUT};
use crate::shutdown::shutdown_signal;

// генерация по тикам
// остановка по SIGTERM/Ctrl-C
pub async fn generate(
    producer: &FutureProducer,
    store: &Store,
    cfg: &Config,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut counter = store.event_count().await?;
    let mut pending: JoinSet<()> = JoinSet::new();
    let mut tick = tokio::time::interval(Duration::from_secs_f64(1.0 / cfg.rate_per_sec as f64));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = tick.tick() => {
                let event = make_event(store, cfg, counter).await?;
                counter += 1;
                store.insert_event(&event).await?;
                kafka::queue_event(producer, store, cfg, &event, &mut pending);
                while let Some(joined) = pending.try_join_next() {
                    if let Err(err) = joined {
                        error!(error = %err, "delivery task failed");
                    }
                }
                if counter.is_multiple_of(100) {
                    info!(produced = counter, "progress");
                }
            }
            _ = &mut shutdown => {
                info!("shutdown signal received, flushing");
                break;
            }
        }
    }

    if let Err(err) = producer.flush(Timeout::After(FLUSH_TIMEOUT)) {
        warn!(error = %err, "flush failed");
    }
    while let Some(joined) = pending.join_next().await {
        if let Err(err) = joined {
            error!(error = %err, "delivery task failed");
        }
    }
    let unsent = store.unsent().await?;
    if unsent.is_empty() {
        info!("all events delivered");
    } else {
        warn!(count = unsent.len(), "undelivered events remain in outbox");
    }
    Ok(())
}

// генераиця события
// user_id выбирается по кругу внутри шарда на основе счетчика
// event_type по счетчику
async fn make_event(
    store: &Store,
    cfg: &Config,
    counter: u64,
) -> Result<Event, Box<dyn std::error::Error>> {
    let user_id = cfg.user_shard_index + (counter % cfg.users_per_producer) * cfg.user_shard_count;
    let seq = store.next_seq(user_id).await?;
    let event_type = match counter % 3 {
        0 => EventType::PageView,
        1 => EventType::AddToCart,
        _ => EventType::Purchase,
    };
    let poison = (counter + 1).is_multiple_of(cfg.poison_every_n);
    Ok(Event {
        event_id: Uuid::new_v4(),
        user_id,
        event_type,
        seq,
        produced_at: Utc::now(),
        payload: serde_json::json!({ "counter": counter, "poison": poison }),
    })
}
