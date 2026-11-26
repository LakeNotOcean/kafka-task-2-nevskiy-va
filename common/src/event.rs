use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    PageView,
    AddToCart,
    Purchase,
}

impl EventType {
    // Строковое представление для хранения в SQLite.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PageView => "page_view",
            Self::AddToCart => "add_to_cart",
            Self::Purchase => "purchase",
        }
    }
}

impl std::str::FromStr for EventType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "page_view" => Ok(Self::PageView),
            "add_to_cart" => Ok(Self::AddToCart),
            "purchase" => Ok(Self::Purchase),
            other => Err(format!("unknown event type: {other}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub event_id: Uuid,
    pub user_id: u64,
    #[serde(rename = "type")]
    pub event_type: EventType,
    pub seq: u64,
    pub produced_at: DateTime<Utc>,
    pub payload: serde_json::Value,
}

impl Event {
    pub fn is_poison(&self) -> bool {
        self.payload
            .get("poison")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    }
}
