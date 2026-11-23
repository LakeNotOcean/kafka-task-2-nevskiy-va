mod generator;
mod kafka;
mod shutdown;

use common::config::Config;
use common::store::Store;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let cfg = Config::from_env()?;
    let store = Store::open(&cfg.db_path).await?;
    let producer = kafka::build_producer(&cfg)?;

    info!(pod = %cfg.pod_id, brokers = %cfg.kafka_brokers, topic = %cfg.topic, "producer starting");
    kafka::resend_unsent(&producer, &store, &cfg).await?;
    generator::generate(&producer, &store, &cfg).await
}
