//! Bounded ephemeral event bus used by the daemon.
#![cfg_attr(rustfmt, rustfmt_skip)]
use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc::{self, Receiver, SyncSender, TrySendError},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

use serde_json::{json, Value};

use crate::{
    debug::{self, DebugTopic},
    helpers::unix_time_millis,
    operation::{Audience, Event},
};

pub const DEFAULT_EVENT_CAPACITY: usize = 1024;
pub const DEFAULT_SUBSCRIBER_CAPACITY: usize = 256;
const DEFAULT_RECENT_EVENT_CAPACITY: usize = 4096;

#[derive(Debug)]
struct RecentEventIds {
    capacity: usize,
    order: VecDeque<String>,
    ids: HashSet<String>,
}

impl RecentEventIds {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            order: VecDeque::new(),
            ids: HashSet::new(),
        }
    }

    fn contains(&self, event_id: &str) -> bool { self.ids.contains(event_id) }

    fn insert(&mut self, event_id: String) {
        if !self.ids.insert(event_id.clone()) {
            return;
        }
        self.order.push_back(event_id);
        while self.order.len() > self.capacity {
            if let Some(expired) = self.order.pop_front() {
                self.ids.remove(&expired);
            }
        }
    }
}

#[derive(Debug)]
struct Subscriber {
    types: Vec<String>,
    sender: SyncSender<Event>,
}

/// Snapshot of the ephemeral event engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventEngineSnapshot {
    pub published: u64,
    pub delivered: u64,
    pub dropped: u64,
    pub subscribers: usize,
}

/// One connection-local event subscription.
#[derive(Debug)]
pub struct EventSubscription {
    id: u64,
    receiver: Receiver<Event>,
    subscribers: Arc<Mutex<BTreeMap<u64, Subscriber>>>,
}

impl EventSubscription {
    #[must_use]
    pub const fn id(&self) -> u64 { self.id }

    pub fn recv(&self) -> Result<Event, mpsc::RecvError> { self.receiver.recv() }

    pub fn try_recv(&self) -> Result<Event, mpsc::TryRecvError> { self.receiver.try_recv() }
}

impl Drop for EventSubscription {
    fn drop(&mut self) {
        self.subscribers
            .lock()
            .expect("event subscribers lock poisoned")
            .remove(&self.id);
    }
}

/// Running heartbeat producer. Dropping the handle stops its thread.
#[derive(Debug)]
pub struct HeartbeatHandle {
    stop: Option<mpsc::Sender<()>>,
    worker: Option<thread::JoinHandle<()>>,
}

impl HeartbeatHandle {
    fn signal_stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }

    pub fn stop(mut self) {
        self.signal_stop();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl Drop for HeartbeatHandle {
    fn drop(&mut self) {
        self.signal_stop();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

/// Thread-safe bounded event engine.
#[derive(Debug, Clone)]
pub struct EventEngine {
    input: SyncSender<Event>,
    subscribers: Arc<Mutex<BTreeMap<u64, Subscriber>>>,
    next_subscription: Arc<AtomicU64>,
    next_event: Arc<AtomicU64>,
    published: Arc<AtomicU64>,
    delivered: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
    recent_event_ids: Arc<Mutex<RecentEventIds>>,
}

impl Default for EventEngine {
    fn default() -> Self {
        Self::new(DEFAULT_EVENT_CAPACITY)
    }
}

impl EventEngine {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let (input, receiver) = mpsc::sync_channel::<Event>(capacity.max(1));
        let subscribers = Arc::new(Mutex::new(BTreeMap::<u64, Subscriber>::new()));
        let delivered = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicU64::new(0));
        let worker_subscribers = Arc::clone(&subscribers);
        let worker_delivered = Arc::clone(&delivered);
        let worker_dropped = Arc::clone(&dropped);

        thread::Builder::new()
            .name("og-event-worker".to_owned())
            .spawn(move || {
                while let Ok(event) = receiver.recv() {
                    let mut disconnected = Vec::new();
                    let mut registry = worker_subscribers
                        .lock()
                        .expect("event subscribers lock poisoned");
                    for (&id, subscriber) in registry.iter() {
                        if !matches_types(&subscriber.types, &event.event_type) {
                            continue;
                        }
                        match subscriber.sender.try_send(event.clone()) {
                            Ok(()) => {
                                worker_delivered.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(TrySendError::Full(_)) => {
                                worker_dropped.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(TrySendError::Disconnected(_)) => disconnected.push(id),
                        }
                    }
                    for id in disconnected {
                        registry.remove(&id);
                    }
                }
            })
            .expect("event worker thread must start");

        Self {
            input,
            subscribers,
            next_subscription: Arc::new(AtomicU64::new(1)),
            next_event: Arc::new(AtomicU64::new(1)),
            published: Arc::new(AtomicU64::new(0)),
            delivered,
            dropped,
            recent_event_ids: Arc::new(Mutex::new(RecentEventIds::new(DEFAULT_RECENT_EVENT_CAPACITY))),
        }
    }

    pub fn publish(&self, event: Event) -> bool {
        let event_type = event.event_type.clone();
        let event_id = event.id.clone();
        let mut recent = self.recent_event_ids
            .lock()
            .expect("recent event ids lock poisoned");
        if recent.contains(&event_id) {
            debug::log(
                DebugTopic::Events,
                None,
                format!("deduplicated type={event_type} event_id={event_id}"),
            );
            return true;
        }
        match self.input.try_send(event) {
            Ok(()) => {
                recent.insert(event_id.clone());
                drop(recent);
                self.published.fetch_add(1, Ordering::Relaxed);
                debug::log(
                    DebugTopic::Events,
                    None,
                    format!("queued type={event_type} event_id={event_id}"),
                );
                true
            }
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                debug::log(
                    DebugTopic::Events,
                    None,
                    format!(
                        "dropped type={event_type} event_id={event_id} reason=queue_unavailable"
                    ),
                );
                false
            }
        }
    }

    pub fn publish_to(&self, audience: Audience, event_type: &str, payload: Value) -> bool {
        let sequence = self.next_event.fetch_add(1, Ordering::Relaxed);
        self.publish(Event::new(
            format!("event-{sequence}"),
            event_type,
            audience,
            unix_time_millis(),
            payload,
        ))
    }

    pub fn publish_global(&self, event_type: &str, payload: Value) -> bool {
        self.publish_to(Audience::Global, event_type, payload)
    }

    #[must_use]
    pub fn subscribe(&self, types: Vec<String>) -> EventSubscription {
        let id = self.next_subscription.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = mpsc::sync_channel(DEFAULT_SUBSCRIBER_CAPACITY);
        self.subscribers
            .lock()
            .expect("event subscribers lock poisoned")
            .insert(
                id,
                Subscriber {
                    types: normalize_types(types),
                    sender,
                },
            );
        EventSubscription {
            id,
            receiver,
            subscribers: Arc::clone(&self.subscribers),
        }
    }

    #[must_use]
    pub fn start_heartbeat(&self, interval: Duration) -> HeartbeatHandle {
        let engine = self.clone();
        let (stop, worker_stop) = mpsc::channel::<()>();
        let started = Instant::now();
        let worker = thread::Builder::new()
            .name("og-event-heartbeat".to_owned())
            .spawn(move || {
                let mut sequence = 0u64;
                loop {
                    match worker_stop.recv_timeout(interval) {
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                        Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    }
                    sequence = sequence.saturating_add(1);
                    let snapshot = engine.snapshot();
                    let payload = json!({
                        "sequence": sequence as f64,
                        "uptimeMs": started.elapsed().as_millis() as f64,
                        "eventEngine": {
                            "published": snapshot.published as f64,
                            "delivered": snapshot.delivered as f64,
                            "dropped": snapshot.dropped as f64,
                            "subscribers": snapshot.subscribers as f64,
                        }
                    });
                    let published = engine.publish_global("core.heartbeat", payload);
                    let snapshot = engine.snapshot();
                    debug::log(
                        DebugTopic::Events,
                        None,
                        format!(
                            "heartbeat sequence={sequence} published={published} subscribers={} delivered={} dropped={}",
                            snapshot.subscribers, snapshot.delivered, snapshot.dropped
                        ),
                    );
                }
            })
            .expect("heartbeat thread must start");
        HeartbeatHandle {
            stop: Some(stop),
            worker: Some(worker),
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> EventEngineSnapshot {
        EventEngineSnapshot {
            published: self.published.load(Ordering::Relaxed),
            delivered: self.delivered.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            subscribers: self
                .subscribers
                .lock()
                .expect("event subscribers lock poisoned")
                .len(),
        }
    }
}

fn normalize_types(types: Vec<String>) -> Vec<String> {
    let mut types: Vec<_> = types
        .into_iter()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .collect();
    if types.is_empty() {
        types.push("*".to_owned());
    }
    types.sort();
    types.dedup();
    types
}

fn matches_types(filters: &[String], event_type: &str) -> bool {
    filters.iter().any(|filter| {
        filter == "*"
            || filter == event_type
            || filter
                .strip_suffix('*')
                .is_some_and(|prefix| event_type.starts_with(prefix))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn exact_and_prefix_subscriptions_receive_matching_events() { let engine = EventEngine::new(8); let subscription = engine.subscribe(vec!["query.*".to_owned()]); assert!(engine.publish_global("query.started", json!({ "id": 1 }))); let event = subscription .receiver .recv_timeout(Duration::from_secs(1)) .unwrap(); assert_eq!(event.event_type, "query.started"); }
    #[test] fn unmatched_events_are_not_delivered() { let engine = EventEngine::new(8); let subscription = engine.subscribe(vec!["sharing.*".to_owned()]); assert!(engine.publish_global("query.started", json!({}))); assert!(subscription .receiver .recv_timeout(Duration::from_millis(50)) .is_err()); }
    #[test] fn targeted_event_preserves_its_logical_audience() { let engine = EventEngine::new(8); let subscription = engine.subscribe(vec!["sharing.*".to_owned()]); assert!(engine.publish_to( Audience::identities(["identity-a".to_owned(), "identity-b".to_owned()]), "sharing.created", json!({"sharingId": "sharing-a"}), )); let event = subscription .receiver .recv_timeout(Duration::from_secs(1)) .unwrap(); assert_eq!( event.audience, Audience::Identities { identity_ids: vec!["identity-a".to_owned(), "identity-b".to_owned()], } ); }
    #[test] fn dropping_subscription_unregisters_it() { let engine = EventEngine::new(8); let subscription = engine.subscribe(vec!["*".to_owned()]); assert_eq!(engine.snapshot().subscribers, 1); drop(subscription); assert_eq!(engine.snapshot().subscribers, 0); }
    #[test] fn duplicate_event_ids_are_delivered_once() { let engine = EventEngine::new(8); let subscription = engine.subscribe(vec!["app.data.changed".to_owned()]); let event = Event::new("event-a", "app.data.changed", Audience::Global, unix_time_millis(), json!({"collection": "items"})); assert!(engine.publish(event.clone())); assert!(engine.publish(event)); let first = subscription.receiver.recv_timeout(Duration::from_secs(1)).unwrap(); assert_eq!(first.id, "event-a"); assert!(subscription.receiver.recv_timeout(Duration::from_millis(50)).is_err()); }
    #[test] fn heartbeat_contains_js_safe_runtime_metrics_and_stops_cleanly() { let engine = EventEngine::new(8); let subscription = engine.subscribe(vec!["core.heartbeat".to_owned()]); let heartbeat = engine.start_heartbeat(Duration::from_millis(10)); let event = subscription .receiver .recv_timeout(Duration::from_secs(1)) .unwrap(); assert_eq!(event.event_type, "core.heartbeat"); assert!(event.payload["sequence"].is_f64()); assert!(event.payload["uptimeMs"].is_f64()); assert!(event.payload["eventEngine"]["published"].is_f64()); heartbeat.stop(); }
}
