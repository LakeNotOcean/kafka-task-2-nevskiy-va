use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::config::Config;
use common::event::Event;
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{
    BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer,
};
use rdkafka::error::KafkaError;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::{Offset, TopicPartitionList};
use tracing::{error, info};

// Трекинг обработанных оффсетов (next offset на партицию) для коммита на revoke.
#[derive(Clone, Default)]
pub struct ProcessedOffsets {
    inner: Arc<Mutex<HashMap<(String, i32), i64>>>,
}

// обрабока оффсета: изменение указателя в кафке
impl ProcessedOffsets {
    // запись оффсета в память консьюмера
    pub fn track(&self, topic: &str, partition: i32, next_offset: i64) {
        self.inner
            .lock()
            .expect("processed offsets mutex poisoned")
            .insert((topic.to_owned(), partition), next_offset);
    }

    // список оффсетов
    fn commit_list(&self, revoked: &TopicPartitionList) -> TopicPartitionList {
        let mut map = self.inner.lock().expect("processed offsets mutex poisoned");
        let mut tpl = TopicPartitionList::new();
        for elem in revoked.elements() {
            // remove, не get: после возврата партиции нельзя коммитить устаревший оффсет
            if let Some(next) = map.remove(&(elem.topic().to_owned(), elem.partition())) {
                if let Err(err) =
                    tpl.add_partition_offset(elem.topic(), elem.partition(), Offset::Offset(next))
                {
                    error!(error = %err, "failed to build revoke commit list");
                }
            }
        }
        tpl
    }
}

pub struct GroupContext {
    pub offsets: ProcessedOffsets,
}

impl ClientContext for GroupContext {}

impl ConsumerContext for GroupContext {
    // коммит перед ребалансом
    fn pre_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if let Rebalance::Revoke(revoked) = rebalance {
            let tpl = self.offsets.commit_list(revoked);
            if tpl.count() > 0 {
                match consumer.commit(&tpl, CommitMode::Sync) {
                    Ok(()) => {
                        info!(
                            partitions = tpl.count(),
                            "committed processed offsets before revoke"
                        )
                    }
                    Err(err) => error!(error = %err, "revoke commit failed"),
                }
            }
        }
    }

    fn post_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        match rebalance {
            Rebalance::Assign(tpl) => info!(partitions = ?tpl, "partitions assigned"),
            Rebalance::Revoke(tpl) => info!(partitions = ?tpl, "partitions revoked"),
            Rebalance::Error(err) => error!(error = %err, "rebalance error"),
        }
    }
}

pub fn build_consumer(
    cfg: &Config,
    offsets: ProcessedOffsets,
) -> Result<StreamConsumer<GroupContext>, KafkaError> {
    ClientConfig::new()
        .set("bootstrap.servers", &cfg.kafka_brokers)
        .set("group.id", &cfg.group_id)
        .set("client.id", &cfg.pod_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create_with_context(GroupContext { offsets })
}

pub fn build_dlt_producer(cfg: &Config) -> Result<FutureProducer, KafkaError> {
    ClientConfig::new()
        .set("bootstrap.servers", &cfg.kafka_brokers)
        .set("client.id", format!("{}-dlt", cfg.pod_id))
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("message.timeout.ms", "30000")
        .create()
}

#[derive(Clone, Copy)]
pub struct SourcePos<'a> {
    pub topic: &'a str,
    pub partition: i32,
    pub offset: i64,
}

pub async fn send_to_dlt(
    producer: &FutureProducer,
    cfg: &Config,
    event: &Event,
    src: SourcePos<'_>,
    reason: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let key = event.user_id.to_string();
    let payload = serde_json::to_string(event)?;
    send_dlt_bytes(producer, cfg, &key, payload.as_bytes(), src, reason).await
}

pub async fn send_raw_to_dlt(
    producer: &FutureProducer,
    cfg: &Config,
    key: Option<&[u8]>,
    payload: Option<&[u8]>,
    src: SourcePos<'_>,
    reason: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let key = key
        .map(|k| String::from_utf8_lossy(k).into_owned())
        .unwrap_or_default();
    send_dlt_bytes(producer, cfg, &key, payload.unwrap_or(&[]), src, reason).await
}

async fn send_dlt_bytes(
    producer: &FutureProducer,
    cfg: &Config,
    key: &str,
    payload: &[u8],
    src: SourcePos<'_>,
    reason: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let partition_str = src.partition.to_string();
    let offset_str = src.offset.to_string();
    let headers = OwnedHeaders::new()
        .insert(Header {
            key: "error",
            value: Some(reason),
        })
        .insert(Header {
            key: "source_topic",
            value: Some(src.topic),
        })
        .insert(Header {
            key: "source_partition",
            value: Some(partition_str.as_str()),
        })
        .insert(Header {
            key: "source_offset",
            value: Some(offset_str.as_str()),
        })
        .insert(Header {
            key: "consumer_pod",
            value: Some(cfg.pod_id.as_str()),
        });
    let record = FutureRecord::to(&cfg.dlt_topic)
        .key(key)
        .payload(payload)
        .headers(headers);
    // таймаут соответствует message.timeout.ms, чтобы не получить доставку после ошибки
    match producer
        .send(record, Timeout::After(Duration::from_secs(30)))
        .await
    {
        Ok(_) => Ok(()),
        Err((err, _)) => Err(err.into()),
    }
}
