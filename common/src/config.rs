use std::env;
use std::fmt;

// режим ключа кафки
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyMode {
    UserId,
    None,
}

// предварительная конфигурация потребителя
#[derive(Debug, Clone)]
pub struct Config {
    pub pod_id: String,
    pub kafka_brokers: String,
    pub topic: String,
    pub dlt_topic: String,
    pub partitions: u32,
    pub partition_index: u32,
    pub rate_per_sec: u32,
    pub poison_every_n: u64,
    pub user_shard_index: u64,
    pub user_shard_count: u64,
    pub users_per_producer: u64,
    pub key_mode: KeyMode,
    pub db_path: String,
}

// ошибки конфигурации
#[derive(Debug)]
pub struct ConfigError(pub String);

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConfigError {}

fn var(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_owned())
}

fn parse<T>(key: &str, default: &str) -> Result<T, ConfigError>
where
    T: std::str::FromStr,
    T::Err: fmt::Display,
{
    let raw = var(key, default);
    raw.parse()
        .map_err(|e| ConfigError(format!("{key}={raw}: {e}")))
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        let pod_id = var("POD_ID", "producer-1");
        let key_mode = match var("KEY_MODE", "user_id").as_str() {
            "user_id" => KeyMode::UserId,
            "none" => KeyMode::None,
            other => {
                return Err(ConfigError(format!(
                    "KEY_MODE={other}: ожидалось user_id или none"
                )))
            }
        };
        let cfg = Self {
            db_path: var("DB_PATH", &format!("data/{pod_id}.db")),
            pod_id,
            kafka_brokers: var("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092"),
            topic: var("TOPIC", "events"),
            dlt_topic: var("DLT_TOPIC", "events.dlt"),
            partitions: parse("PARTITIONS", "3")?,
            partition_index: parse("PARTITION_INDEX", "0")?,
            rate_per_sec: parse("RATE_PER_SEC", "10")?,
            poison_every_n: parse("POISON_EVERY_N", "50")?,
            user_shard_index: parse("USER_SHARD_INDEX", "0")?,
            user_shard_count: parse("USER_SHARD_COUNT", "3")?,
            users_per_producer: parse("USERS_PER_PRODUCER", "100")?,
            key_mode,
        };
        if cfg.rate_per_sec == 0 {
            return Err(ConfigError("RATE_PER_SEC должен быть > 0".into()));
        }
        if cfg.poison_every_n == 0 {
            return Err(ConfigError("POISON_EVERY_N должен быть > 0".into()));
        }
        if cfg.user_shard_count == 0 || cfg.user_shard_index >= cfg.user_shard_count {
            return Err(ConfigError(
                "USER_SHARD_INDEX должен быть в [0, USER_SHARD_COUNT)".into(),
            ));
        }
        if cfg.users_per_producer == 0 {
            return Err(ConfigError("USERS_PER_PRODUCER должен быть > 0".into()));
        }
        Ok(cfg)
    }
}
