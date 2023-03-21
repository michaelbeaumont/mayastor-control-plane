//use crate::message::Event;
use crate::{message::EventMessage, nats::TypedNats};
use anyhow::{anyhow, Result};
use std::{collections::BTreeMap, fmt::Debug};
use tokio::sync::mpsc::{Receiver, Sender};

//use tracing::Id;
use tracing::field::{Field, Visit};
//use tracing::span::Attributes;
use tracing_subscriber::{layer::Context, registry::LookupSpan, Layer};

async fn publish_events(
    nc: &TypedNats,
    mut recv: tokio::sync::mpsc::Receiver<BTreeMap<String, String>>,
) {
    while let Some(msg1) = recv.recv().await {
        if let Some(msg) = EventMessage::from_event(msg1) {
            if let Err(err) = nc.publish_jetstream(&msg).await {
                println!("{err:?}");
            }
        }
    }
}

pub struct EventHandle {
    recv: Option<Receiver<BTreeMap<String, String>>>,
    pub layer: EventManager,
}

impl EventHandle {
    pub fn init() -> Result<Self> {
        let (send, recv) = tokio::sync::mpsc::channel::<BTreeMap<String, String>>(128);

        let layer = EventManager::new(send);

        Ok(EventHandle {
            recv: Some(recv),
            layer,
        })
    }

    pub fn attach_nats(&mut self, nats: TypedNats) -> Result<()> {
        let recv = self.recv.take().ok_or_else(|| {
            anyhow!("connect_nats on TracingHandle should not be called more than once.")
        })?;
        tokio::spawn(async move {
            publish_events(&nats, recv).await;
            tracing::error!("do_logs terminated.");
        });

        Ok(())
    }
}

pub struct EventManager {
    sender: Sender<BTreeMap<String, String>>,
}

impl EventManager {
    fn new(sender: Sender<BTreeMap<String, String>>) -> EventManager {
        EventManager { sender }
    }
}

impl<S> Layer<S> for EventManager
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        let mut visitor = if let Some(span) = ctx.lookup_current() {
            let extensions = span.extensions();
            if let Some(visitor) = extensions.get::<JsonVisitor>() {
                visitor.clone()
            } else {
                JsonVisitor::default()
            }
        } else {
            JsonVisitor::default()
        };

        event.record(&mut visitor);

        let output = visitor.0;

        // make sure logging in this function call does not trigger an infinite loop
        // self.nc.publish_drone_log_message(&msg).unwrap();
        if self.sender.try_send(output).is_err() {
            println!("Warning: sender buffer is full.");
        }
    }
}

#[derive(Clone, Default)]
struct JsonVisitor(BTreeMap<String, String>);

impl Visit for JsonVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_string(), value.to_string());
    }
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.0
            .insert(field.name().to_string(), format!("{value:?}"));
    }
}
