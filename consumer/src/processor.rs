use std::time::Duration;

use common::config::Config;
use common::event::Event;
use tracing::error;

const MAX_ATTEMPTS: u32 = 3;

// Эмуляция задержки с помощью PROCESSING_DELAY_MS
pub async fn process(event: &Event, cfg: &Config) -> Result<(), String> {
    let delay = Duration::from_millis(cfg.processing_delay_ms);
    if !event.is_poison() {
        tokio::time::sleep(delay).await;
        return Ok(());
    }
    for attempt in 1..=MAX_ATTEMPTS {
        tokio::time::sleep(delay).await;
        error!(event_id = %event.event_id, attempt, "poison event processing failed");
    }
    Err(format!("poison event failed after {MAX_ATTEMPTS} attempts"))
}
