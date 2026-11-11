//! Paces the websocket messages forwarded to the frontend over a channel.
//!
//! A channel send doesn't wait for the frontend: a message above a small size waits
//! in the app process until the webview fetches it, with no bound on how many do.
//! A webview that is busy or throttled, as a minimized window can be, falls behind
//! a stream that never slows down, and the backlog keeps growing. So one message is
//! in flight at a time: the next one goes out once the frontend acknowledges the
//! previous one, and what arrives meanwhile is merged into it.

use std::{
    collections::VecDeque,
    sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError, Weak},
    time::{Duration, Instant},
};

use clashmap::ClashMap;
use tauri::ipc::InvokeResponseBody;

use crate::{Error, models::WsConnectionId};

/// Events kept while a batch is in flight, the oldest ones are dropped beyond it.
const MAX_PENDING_EVENTS: usize = 1000;
/// A message not acknowledged for this long ends the stream: the frontend is gone
/// or stuck, and is told to reconnect once it catches up.
const STALL_TIMEOUT: Duration = Duration::from_secs(60);

static FLOWS: LazyLock<ClashMap<WsConnectionId, Weak<ChannelFlow>>> = LazyLock::new(ClashMap::new);

type Sender = Box<dyn Fn(InvokeResponseBody) -> bool + Send + Sync>;

#[derive(Clone, Copy)]
pub(crate) enum FlowMode {
    /// Snapshots, of which only the latest one matters.
    Latest,
    /// Events, which are sent in batches, as a JSON array.
    Batch,
}

#[derive(Default)]
struct FlowState {
    /// When the message in flight was sent, `None` while nothing is in flight.
    in_flight_since: Option<Instant>,
    latest: Option<InvokeResponseBody>,
    events: VecDeque<String>,
    closed: bool,
}

pub(crate) struct ChannelFlow {
    mode: FlowMode,
    send: Sender,
    state: Mutex<FlowState>,
}

impl ChannelFlow {
    pub(crate) fn new(mode: FlowMode, send: impl Fn(InvokeResponseBody) -> bool + Send + Sync + 'static) -> Arc<Self> {
        Arc::new(Self {
            mode,
            send: Box::new(send),
            state: Mutex::default(),
        })
    }

    /// The `on_message` of the websocket, which ends the stream once it returns false.
    pub(crate) fn forwarder(self: &Arc<Self>) -> impl Fn(InvokeResponseBody) -> bool + Send + 'static {
        let flow = Arc::clone(self);
        move |body| flow.push(body)
    }

    /// Lets the frontend acknowledge the messages of the websocket `id`. The flow is
    /// kept alive by the websocket reader only.
    pub(crate) fn register(self: &Arc<Self>, id: WsConnectionId) {
        FLOWS.retain(|_, flow| flow.strong_count() > 0);
        FLOWS.insert(id, Arc::downgrade(self));
    }

    fn state(&self) -> MutexGuard<'_, FlowState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn push(&self, body: InvokeResponseBody) -> bool {
        let mut state = self.state();
        if state.closed {
            return false;
        }
        match state.in_flight_since {
            None => {
                state.in_flight_since = Some(Instant::now());
                drop(state);
                let body = match self.mode {
                    FlowMode::Latest => body,
                    FlowMode::Batch => batch_body([body_text(body)]),
                };
                self.deliver(body)
            }
            Some(since) if since.elapsed() >= STALL_TIMEOUT => {
                state.closed = true;
                drop(state);
                log::debug!("frontend stopped acknowledging websocket messages, ending the stream");
                let _ = (self.send)(stalled_body());
                false
            }
            Some(_) => {
                match self.mode {
                    FlowMode::Latest => state.latest = Some(body),
                    FlowMode::Batch => {
                        if state.events.len() >= MAX_PENDING_EVENTS {
                            state.events.pop_front();
                        }
                        state.events.push_back(body_text(body));
                    }
                }
                true
            }
        }
    }

    fn ack(&self) {
        let mut state = self.state();
        if state.closed {
            return;
        }
        let next = match self.mode {
            FlowMode::Latest => state.latest.take(),
            FlowMode::Batch => (!state.events.is_empty()).then(|| batch_body(state.events.drain(..))),
        };
        match next {
            Some(body) => {
                state.in_flight_since = Some(Instant::now());
                drop(state);
                self.deliver(body);
            }
            None => state.in_flight_since = None,
        }
    }

    fn deliver(&self, body: InvokeResponseBody) -> bool {
        if (self.send)(body) {
            return true;
        }
        self.state().closed = true;
        false
    }
}

// The reader is gone once the stream ended, and no acknowledgement can release what
// waits then, such as the error that tells the frontend to reconnect.
impl Drop for ChannelFlow {
    fn drop(&mut self) {
        let state = self.state.get_mut().unwrap_or_else(PoisonError::into_inner);
        if state.closed {
            return;
        }
        let pending = match self.mode {
            FlowMode::Latest => state.latest.take(),
            FlowMode::Batch => (!state.events.is_empty()).then(|| batch_body(state.events.drain(..))),
        };
        if let Some(body) = pending {
            let _ = (self.send)(body);
        }
    }
}

/// The frontend took in the message in flight of the websocket `id`.
pub(crate) fn ack(id: WsConnectionId) {
    if let Some(flow) = FLOWS.get(&id).and_then(|flow| flow.upgrade()) {
        flow.ack();
    }
}

fn body_text(body: InvokeResponseBody) -> String {
    match body {
        InvokeResponseBody::Json(text) => text,
        InvokeResponseBody::Raw(bytes) => serde_json::json!(String::from_utf8_lossy(&bytes)).to_string(),
    }
}

fn batch_body(items: impl IntoIterator<Item = String>) -> InvokeResponseBody {
    let mut text = String::from("[");
    for (i, item) in items.into_iter().enumerate() {
        if i > 0 {
            text.push(',');
        }
        text.push_str(&item);
    }
    text.push(']');
    InvokeResponseBody::Json(text)
}

fn stalled_body() -> InvokeResponseBody {
    let message = Error::Websocket("the frontend stopped taking messages".into()).to_string();
    InvokeResponseBody::Json(serde_json::json!(message).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn recording_flow(mode: FlowMode) -> (Arc<ChannelFlow>, Arc<Mutex<Vec<String>>>) {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let flow = ChannelFlow::new(mode, {
            let sent = Arc::clone(&sent);
            move |body| {
                sent.lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(body_text(body));
                true
            }
        });
        (flow, sent)
    }

    fn json(text: &str) -> InvokeResponseBody {
        InvokeResponseBody::Json(text.to_string())
    }

    fn sent_texts(sent: &Mutex<Vec<String>>) -> Vec<String> {
        sent.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }

    #[test]
    fn latest_snapshot_waits_for_the_acknowledgement() {
        let (flow, sent) = recording_flow(FlowMode::Latest);
        assert!(flow.push(json("1")));
        assert!(flow.push(json("2")));
        assert!(flow.push(json("3")));
        assert_eq!(sent_texts(&sent), ["1"]);

        flow.ack();
        assert_eq!(sent_texts(&sent), ["1", "3"]);
        flow.ack();
        assert!(flow.push(json("4")));
        assert_eq!(sent_texts(&sent), ["1", "3", "4"]);
    }

    #[test]
    fn events_arrive_in_bounded_batches() {
        let (flow, sent) = recording_flow(FlowMode::Batch);
        assert!(flow.push(json("0")));
        for i in 1..=MAX_PENDING_EVENTS + 5 {
            assert!(flow.push(json(&i.to_string())));
        }
        flow.ack();

        let texts = sent_texts(&sent);
        assert_eq!(texts[0], "[0]");
        let batch: Vec<usize> = serde_json::from_str(&texts[1]).unwrap_or_default();
        assert_eq!(batch.len(), MAX_PENDING_EVENTS);
        assert_eq!(batch.first(), Some(&6));
        assert_eq!(batch.last(), Some(&(MAX_PENDING_EVENTS + 5)));
    }

    #[test]
    fn stalled_frontend_ends_the_stream_with_an_error() {
        let (flow, sent) = recording_flow(FlowMode::Latest);
        assert!(flow.push(json("1")));
        flow.state().in_flight_since = Instant::now().checked_sub(STALL_TIMEOUT);
        assert!(!flow.push(json("2")));

        let texts = sent_texts(&sent);
        assert_eq!(texts.len(), 2);
        let error: String = serde_json::from_str(&texts[1]).unwrap_or_default();
        assert!(error.starts_with("Websocket error"), "{error}");
        assert!(!flow.push(json("3")));
    }

    #[test]
    fn last_message_goes_out_when_the_stream_ends() {
        for mode in [FlowMode::Latest, FlowMode::Batch] {
            let (flow, sent) = recording_flow(mode);
            let forward = flow.forwarder();
            drop(flow);
            assert!(forward(json("1")));
            assert!(forward(json("\"Websocket error: connection closed\"")));
            // the reader ends without an acknowledgement for the message in flight
            drop(forward);

            let texts = sent_texts(&sent);
            assert_eq!(texts.len(), 2);
            assert!(texts[1].contains("Websocket error"), "{}", texts[1]);
        }
    }

    #[test]
    fn closed_channel_ends_the_stream() {
        let flow = ChannelFlow::new(FlowMode::Latest, |_| false);
        assert!(!flow.push(json("1")));
        assert!(!flow.push(json("2")));
    }

    #[test]
    fn registered_flow_is_released_with_its_reader() {
        let (flow, sent) = recording_flow(FlowMode::Latest);
        let id = uuid::Uuid::new_v4();
        flow.register(id);
        let forward = flow.forwarder();
        drop(flow);

        assert!(forward(json("1")));
        assert!(forward(json("2")));
        ack(id);
        assert_eq!(sent_texts(&sent), ["1", "2"]);

        drop(forward);
        assert!(FLOWS.get(&id).is_some_and(|flow| flow.upgrade().is_none()));
    }
}
