//! The idea behind this is to provide a "reverse proxy" for ACP agents to connect to
//! the Tidewave Web tools (browser_eval, restart_app_server).
//!
//! When the browser tells the ACP agent what MCP servers to connect to, it provides
//! the session_id. The agent will then send POST requests to the `/socket/mcp-remote-client`
//! endpoint. When receiving such a POST request, we look up the registered browsers
//! and forward the raw MCP message to the browser. The response is routed back to the agent.

use axum::{
    body::Bytes,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
};
use dashmap::DashMap;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::sync::{mpsc::error::SendError, mpsc::UnboundedSender, oneshot};
use tracing::{debug, error, info, warn};

use crate::phoenix::{InitResult, PhxMessage};

use super::ChannelSender;

#[derive(Deserialize)]
pub struct McpParams {
    #[serde(rename = "sessionId")]
    pub session_id: Option<String>,
}

// ============================================================================
// State Types
// ============================================================================

#[derive(Clone)]
pub struct McpChannelState {
    /// Registry mapping session_id to the current channel for that session.
    pub sessions: Arc<DashMap<String, McpSession>>,
    /// Pending responses waiting for answers from the browser
    pub awaiting_answers: Arc<DashMap<(String, ChannelId, Value), AwaitingAnswer>>,
    /// Monotonic channel id generator for distinguishing reconnect ownership.
    pub next_channel_id: Arc<AtomicU64>,
    /// Monotonic proxy ID generator for correlating browser responses.
    next_proxy_id: Arc<AtomicU64>,
}

pub type ChannelId = u64;

#[derive(Clone)]
pub struct McpSession {
    pub channel_id: ChannelId,
    pub sender: ChannelSender,
}

pub struct AwaitingAnswer {
    client_id: Value,
    response_tx: oneshot::Sender<Value>,
}

struct PendingAnswerCleanup {
    awaiting_answers: Arc<DashMap<(String, ChannelId, Value), AwaitingAnswer>>,
    key: (String, ChannelId, Value),
}

impl Drop for PendingAnswerCleanup {
    fn drop(&mut self) {
        self.awaiting_answers.remove(&self.key);
    }
}

impl McpChannelState {
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(DashMap::new()),
            awaiting_answers: Arc::new(DashMap::new()),
            next_channel_id: Arc::new(AtomicU64::new(1)),
            next_proxy_id: Arc::new(AtomicU64::new(1)),
        }
    }
}

impl Default for McpChannelState {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Channel Init
// ============================================================================

/// Initialize an `mcp:{session_id}` channel.
///
/// Registers the browser connection for the given session and runs a forwarding
/// loop until the channel is left (incoming_rx closed).
pub async fn init(
    state: &McpChannelState,
    msg: &PhxMessage,
    outgoing_tx: UnboundedSender<PhxMessage>,
    mut incoming_rx: tokio::sync::mpsc::UnboundedReceiver<PhxMessage>,
) -> InitResult {
    // Extract session_id from topic "mcp:{session_id}"
    let session_id = match msg.topic.strip_prefix("mcp:") {
        Some(id) => id.to_string(),
        None => {
            return InitResult::Error(
                "invalid topic format, expected mcp:{session_id}".to_string(),
            );
        }
    };

    let channel_id = state.next_channel_id.fetch_add(1, Ordering::SeqCst);

    debug!(
        "MCP channel join for session_id: {}, channel_id: {}",
        session_id, channel_id
    );

    let sender = ChannelSender {
        tx: outgoing_tx.clone(),
        topic: msg.topic.clone(),
        join_ref: msg.join_ref.clone(),
    };

    // Register this channel's sender for the session
    state
        .sessions
        .insert(session_id.clone(), McpSession { channel_id, sender });

    // Send success reply
    let _ = outgoing_tx.send(PhxMessage::ok_reply(msg, json!({})));

    // Main loop: handle incoming messages from the browser
    loop {
        match incoming_rx.recv().await {
            Some(phx_msg) => {
                if phx_msg.event == "mcp_message" {
                    let json_rpc_message = phx_msg.payload.into_json();

                    // Only responses can complete a pending request. Browser-originated
                    // JSON-RPC requests also have IDs and must not consume an entry with a
                    // coincidentally matching proxy ID.
                    if let Some(id) = json_rpc_response_id(&json_rpc_message) {
                        let key = (session_id.clone(), channel_id, id.clone());
                        if let Some((_, answer)) = state.awaiting_answers.remove(&key) {
                            let mut client_response = json_rpc_message.clone();
                            if let Some(response) = client_response.as_object_mut() {
                                response.insert("id".to_string(), answer.client_id);
                            }
                            if answer.response_tx.send(client_response).is_err() {
                                warn!("Failed to send response to waiting request");
                            }
                            continue;
                        }
                    }

                    // This is a notification or unexpected message
                    if json_rpc_message.get("id").is_some() {
                        error!(
                            "Did not expect a reply (or request) for session {}: {:?}",
                            session_id, json_rpc_message
                        );
                    } else {
                        info!("Ignoring notification from browser: {:?}", json_rpc_message);
                    }
                } else {
                    warn!("Unknown event in MCP channel: {}", phx_msg.event);
                }
            }
            None => {
                // Channel was left/disconnected
                break;
            }
        }
    }

    debug!(
        "MCP channel terminating for session_id: {}, channel_id: {}",
        session_id, channel_id
    );
    state
        .sessions
        .remove_if(&session_id, |_, session| session.channel_id == channel_id);

    // Clean up any pending responses for this session
    cleanup_pending_answers(
        state,
        &session_id,
        channel_id,
        json!({
            "error": {
                "code": -32000,
                "message": "Channel connection closed"
            }
        }),
    );

    InitResult::Done
}

// ============================================================================
// HTTP Handler for Agent Requests
// ============================================================================

/// HTTP POST handler for MCP remote client requests.
/// This is called by the ACP agent to send MCP messages to the browser.
pub async fn mcp_channel_client_handler(
    Query(params): Query<McpParams>,
    State(state): State<McpChannelState>,
    body: Bytes,
) -> Result<Response, StatusCode> {
    let session_id = params.session_id.ok_or(StatusCode::BAD_REQUEST)?;

    debug!("MCP channel client request for session: {}", session_id);

    // Parse the JSON-RPC message from the request body
    let json_rpc_message: Value = serde_json::from_slice(&body).map_err(|e| {
        error!("Failed to parse JSON-RPC message: {}", e);
        StatusCode::BAD_REQUEST
    })?;

    // Basic validation that this looks like a JSON-RPC message
    if json_rpc_message.get("jsonrpc").is_none() {
        error!("Invalid JSON-RPC message: missing 'jsonrpc' field");
        return Err(StatusCode::BAD_REQUEST);
    }

    let mut pending_cleanup = None;
    let mut response_rx = None;
    let mut failed_channel_id = None;
    let mut browser_message = json_rpc_message.clone();

    {
        // Keep the session entry borrowed until after the send. This prevents a
        // reconnect from swapping the registered channel between lookup and push.
        let session = match state.sessions.get(&session_id) {
            Some(session) => session,
            None => {
                error!("Session not found: {}", session_id);
                return Ok(
                    browser_disconnected_response(json_rpc_message.get("id")).into_response()
                );
            }
        };

        // Only requests receive responses. Notifications and responses are forwarded
        // without installing a pending answer.
        if let Some(id) = json_rpc_request_id(&json_rpc_message) {
            let proxy_id = Value::String(format!(
                "tw-app-{}",
                state.next_proxy_id.fetch_add(1, Ordering::SeqCst)
            ));
            let key = (session_id.clone(), session.channel_id, proxy_id.clone());
            let (tx, rx) = oneshot::channel();
            state.awaiting_answers.insert(
                key.clone(),
                AwaitingAnswer {
                    client_id: id.clone(),
                    response_tx: tx,
                },
            );
            pending_cleanup = Some(PendingAnswerCleanup {
                awaiting_answers: state.awaiting_answers.clone(),
                key,
            });
            response_rx = Some(rx);
            if let Some(message) = browser_message.as_object_mut() {
                message.insert("id".to_string(), proxy_id);
            }
        }

        // Push the message directly to the browser
        if try_push_mcp_message(&session, browser_message).is_err() {
            failed_channel_id = Some(session.channel_id);
        }
    }

    if let Some(channel_id) = failed_channel_id {
        state
            .sessions
            .remove_if(&session_id, |_, current| current.channel_id == channel_id);
        return Ok(browser_disconnected_response(json_rpc_message.get("id")).into_response());
    }

    // If this is a request, wait for the response and return JSON
    if let Some(response_rx) = response_rx {
        let result = match response_rx.await {
            Ok(response) => {
                debug!("Got response: {:?}", response);
                Ok(Json(response).into_response())
            }
            Err(_) => {
                error!("Response channel closed unexpectedly");
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        };
        drop(pending_cleanup);
        result
    } else {
        // This is a notification or response - return HTTP 202 Accepted with no body
        Ok(Response::builder()
            .status(StatusCode::ACCEPTED)
            .body(axum::body::Body::empty())
            .unwrap())
    }
}

fn browser_disconnected_response(request_id: Option<&Value>) -> Json<Value> {
    Json(json!({
        "jsonrpc": "2.0",
        "id": request_id.cloned().unwrap_or(Value::Null),
        "error": {
            "code": -32000,
            "message": "Browser is not connected. Abort any generation until a manual user retry."
        }
    }))
}

fn try_push_mcp_message(session: &McpSession, payload: Value) -> Result<(), SendError<PhxMessage>> {
    let mut phx = PhxMessage::new(&session.sender.topic, "mcp_message", payload);
    phx.join_ref = session.sender.join_ref.clone();
    session.sender.tx.send(phx)
}

fn json_rpc_request_id(message: &Value) -> Option<&Value> {
    message.get("method").and_then(|_| message.get("id"))
}

fn json_rpc_response_id(message: &Value) -> Option<&Value> {
    let has_result = message.get("result").is_some();
    let has_error = message.get("error").is_some();

    if message.get("method").is_none() && has_result != has_error {
        message.get("id")
    } else {
        None
    }
}

fn cleanup_pending_answers(
    state: &McpChannelState,
    session_id: &str,
    channel_id: ChannelId,
    response: Value,
) {
    let keys_to_remove: Vec<_> = state
        .awaiting_answers
        .iter()
        .filter(|entry| entry.key().0 == session_id && entry.key().1 == channel_id)
        .map(|entry| entry.key().clone())
        .collect();

    for key in keys_to_remove {
        if let Some((_, answer)) = state.awaiting_answers.remove(&key) {
            let mut client_response = response.clone();
            if let Some(response) = client_response.as_object_mut() {
                response
                    .entry("jsonrpc".to_string())
                    .or_insert_with(|| Value::String("2.0".to_string()));
                response.insert("id".to_string(), answer.client_id);
            }
            let _ = answer.response_tx.send(client_response);
        }
    }
}
