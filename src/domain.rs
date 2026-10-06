//! A tiny domain model shared by the demos, so that payloads are realistic JSON
//! documents rather than bare strings.

use anyhow::Result;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Regions used to build hierarchical subjects such as `showcase.orders.eu.created`.
pub const REGIONS: [&str; 3] = ["eu", "us", "apac"];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Order {
    pub id: u64,
    pub customer: String,
    pub region: String,
    pub amount_cents: u64,
}

impl Order {
    /// Builds a deterministic sample order; ids cycle through customers and regions.
    pub fn sample(id: u64) -> Self {
        const CUSTOMERS: [&str; 4] = ["acme", "globex", "initech", "umbrella"];
        let idx = id.saturating_sub(1) as usize;
        Self {
            id,
            customer: CUSTOMERS[idx % CUSTOMERS.len()].to_owned(),
            region: REGIONS[idx % REGIONS.len()].to_owned(),
            amount_cents: 1_999 + 750 * id,
        }
    }

    /// Subject for this order under the given prefix, e.g. `showcase.orders.eu.created`.
    pub fn subject(&self, prefix: &str) -> String {
        format!("{prefix}.{}.created", self.region)
    }

    pub fn to_bytes(&self) -> Bytes {
        to_json_bytes(self)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }

    /// One-line human readable summary.
    pub fn describe(&self) -> String {
        format!(
            "order #{} {}/{} {}.{:02} EUR",
            self.id,
            self.customer,
            self.region,
            self.amount_cents / 100,
            self.amount_cents % 100
        )
    }
}

/// Serializes any value as a JSON payload. NATS payloads are opaque bytes; JSON is just a convention.
pub fn to_json_bytes<T: Serialize>(value: &T) -> Bytes {
    Bytes::from(serde_json::to_vec(value).expect("value is always serializable"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_round_trips_through_json() {
        let order = Order::sample(7);
        let decoded = Order::from_bytes(&order.to_bytes()).unwrap();
        assert_eq!(order, decoded);
    }

    #[test]
    fn sample_orders_cycle_through_regions() {
        let regions: Vec<String> = (1..=4).map(|id| Order::sample(id).region).collect();
        assert_eq!(regions, ["eu", "us", "apac", "eu"]);
        assert_eq!(
            Order::sample(1).subject("showcase.orders"),
            "showcase.orders.eu.created"
        );
    }
}
