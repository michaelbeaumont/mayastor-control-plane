use std::{collections::BTreeMap, str::FromStr};

use crate::nats::JetStreamable;
use anyhow::{anyhow, Error};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Result type for the events
#[derive(Debug, Clone)]
#[allow(missing_docs)]
pub enum Event {
    VolumeCreated,
    VolumeDeleted,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventCategory {
    Volume,
    Nexus,
}

impl FromStr for EventCategory {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "volume" => Ok(EventCategory::Volume),
            "nexus" => Ok(EventCategory::Nexus),
            _ => Err(anyhow!(
                "The string {:?} does not describe a valid category.",
                s
            )),
        }
    }
}

impl ToString for EventCategory {
    fn to_string(&self) -> String {
        match self {
            EventCategory::Volume => "volume".to_string(),
            EventCategory::Nexus => "nexus".to_string(),
        }
    }
}

// impl fmt::Display for EventCategory {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         match self {
//             EventCategory::Volume => write!(f, "volume"),
//             EventCategory::Nexus => write!(f, "nexus"),
//         }
//     }
// }

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventAction {
    Created,
    Deleted,
}

// impl Display for EventAction {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(f, "{:?}", self)
//     }
// }

impl FromStr for EventAction {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "created" => Ok(EventAction::Created),
            "deleted" => Ok(EventAction::Deleted),
            _ => Err(anyhow!(
                "The string {:?} does not describe a valid category.",
                s
            )),
        }
    }
}

impl ToString for EventAction {
    fn to_string(&self) -> String {
        match self {
            EventAction::Created => "created".to_string(),
            EventAction::Deleted => "deleted".to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct EventMessage {
    id: String,
    pub category: String,
    pub action: String,
    pub target: String,
}

impl JetStreamable for EventMessage {
    fn subject(&self) -> String {
        format!("stats.events.{}", self.category)
    }
}

impl EventMessage {
    pub fn from_event(event: BTreeMap<String, String>) -> Option<EventMessage> {
        match event.get("event").unwrap().as_str() {
            "VolumeCreated" => Some(EventMessage {
                id: Self::new_random(),
                category: EventCategory::Volume.to_string(),
                action: EventAction::Created.to_string(),
                target: event.get("target").unwrap().to_string(),
            }),
            "VolumeDeleted" => Some(EventMessage {
                id: Self::new_random(),
                category: EventCategory::Volume.to_string(),
                action: EventAction::Deleted.to_string(),
                target: event.get("target").unwrap().to_string(),
            }),
            "NexusCreated" => Some(EventMessage {
                id: Self::new_random(),
                category: EventCategory::Nexus.to_string(),
                action: EventAction::Created.to_string(),
                target: event.get("target").unwrap().to_string(),
            }),
            "NexusDeleted" => Some(EventMessage {
                id: Self::new_random(),
                category: EventCategory::Nexus.to_string(),
                action: EventAction::Deleted.to_string(),
                target: event.get("target").unwrap().to_string(),
            }),
            _ => {
                println!("Unexpected event message");
                None
            }
        }
    }

    pub fn new_random() -> String {
        let id = Uuid::new_v4();
        id.to_string()
    }
}
