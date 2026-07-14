mod common;

use axum::body::{to_bytes, Bytes};
use axum::extract::{Query, State};
use common::{create_fake_phoenix_socket, recv_phoenix_msg, send_phoenix_msg, wait_for_event};
use serde_json::{json, Value};
use tidewave_core::phoenix::PhxMessage;
use tidewave_core::ws::connection::unit_testable_ws_handler;
use tidewave_core::ws::mcp::{mcp_channel_client_handler, McpChannelState, McpParams, McpSession};
use tidewave_core::ws::{ChannelSender, WsState};
use tokio::sync::mpsc;

#[tokio::test]
async fn test_old_mcp_channel_disconnect_does_not_remove_reconnected_session() {
    let mcp_state = McpChannelState::new();
    let session_id = "sess_reconnect";
    let topic = format!("mcp:{session_id}");

    let ws_state1 = WsState::new().with_mcp_state(mcp_state.clone());
    let ws_state2 = WsState::new().with_mcp_state(mcp_state.clone());

    let (out_tx1, mut out_rx1, in_tx1, in_rx1) = create_fake_phoenix_socket();
    let (out_tx2, mut out_rx2, in_tx2, in_rx2) = create_fake_phoenix_socket();

    tokio::spawn(async move {
        unit_testable_ws_handler(out_tx1, in_rx1, ws_state1, uuid::Uuid::new_v4()).await;
    });
    tokio::spawn(async move {
        unit_testable_ws_handler(out_tx2, in_rx2, ws_state2, uuid::Uuid::new_v4()).await;
    });

    let old_join = PhxMessage::new(&topic, "phx_join", json!({}))
        .with_ref("1")
        .with_join_ref("old");
    send_phoenix_msg(&in_tx1, &old_join);
    let old_reply = recv_phoenix_msg(&mut out_rx1)
        .await
        .expect("old channel should join");
    assert_eq!(old_reply.event, "phx_reply");
    assert_eq!(old_reply.payload.as_json()["status"], "ok");

    let new_join = PhxMessage::new(&topic, "phx_join", json!({}))
        .with_ref("1")
        .with_join_ref("new");
    send_phoenix_msg(&in_tx2, &new_join);
    let new_reply = recv_phoenix_msg(&mut out_rx2)
        .await
        .expect("new channel should join");
    assert_eq!(new_reply.event, "phx_reply");
    assert_eq!(new_reply.payload.as_json()["status"], "ok");

    let old_leave = PhxMessage::new(&topic, "phx_leave", json!({}))
        .with_ref("2")
        .with_join_ref("old");
    send_phoenix_msg(&in_tx1, &old_leave);
    let old_close = wait_for_event(&mut out_rx1, "phx_close", 250)
        .await
        .expect("old channel should close");
    assert_eq!(old_close.topic, topic);

    let request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/list"
    });
    let handler_state = mcp_state.clone();
    let handler = tokio::spawn(async move {
        mcp_channel_client_handler(
            Query(McpParams {
                session_id: Some(session_id.to_string()),
            }),
            State(handler_state),
            Bytes::from(serde_json::to_vec(&request).unwrap()),
        )
        .await
        .expect("handler should return a response")
    });

    let forwarded = wait_for_event(&mut out_rx2, "mcp_message", 250)
        .await
        .expect("new channel should receive MCP request");
    assert_eq!(forwarded.topic, topic);
    assert_eq!(forwarded.payload.as_json()["method"], "tools/list");
    let proxy_id = forwarded.payload.as_json()["id"].clone();

    let browser_response = PhxMessage::new(
        &topic,
        "mcp_message",
        json!({
            "jsonrpc": "2.0",
            "id": proxy_id,
            "result": { "tools": [] }
        }),
    )
    .with_ref("2")
    .with_join_ref("new");
    send_phoenix_msg(&in_tx2, &browser_response);

    let response = handler.await.expect("handler task should not panic");
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should be readable");
    let response_json: Value = serde_json::from_slice(&body).expect("response should be JSON");
    assert_eq!(response_json["result"]["tools"], json!([]));
}

#[tokio::test]
async fn test_mcp_http_request_returns_error_when_registered_sender_is_closed() {
    let mcp_state = McpChannelState::new();
    let session_id = "sess_closed_sender";
    let topic = format!("mcp:{session_id}");
    let (tx, rx) = mpsc::unbounded_channel();
    drop(rx);

    mcp_state.sessions.insert(
        session_id.to_string(),
        McpSession {
            channel_id: 1,
            sender: ChannelSender {
                tx,
                topic,
                join_ref: Some("closed".to_string()),
            },
        },
    );

    let request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/list"
    });

    let response = tokio::time::timeout(
        tokio::time::Duration::from_millis(250),
        mcp_channel_client_handler(
            Query(McpParams {
                session_id: Some(session_id.to_string()),
            }),
            State(mcp_state.clone()),
            Bytes::from(serde_json::to_vec(&request).unwrap()),
        ),
    )
    .await
    .expect("handler should not hang when channel sender is closed")
    .expect("handler should return JSON-RPC error");

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should be readable");
    let response_json: Value = serde_json::from_slice(&body).expect("response should be JSON");
    assert_eq!(response_json["id"], 1);
    assert_eq!(response_json["error"]["code"], -32000);
    assert!(
        mcp_state.awaiting_answers.is_empty(),
        "closed sender should not leave pending answers behind"
    );
}

#[tokio::test]
async fn test_abandoned_mcp_http_request_does_not_capture_reused_request_id() {
    let mcp_state = McpChannelState::new();
    let session_id = "sess_abandoned";
    let topic = format!("mcp:{session_id}");

    let ws_state = WsState::new().with_mcp_state(mcp_state.clone());
    let (out_tx, mut out_rx, in_tx, in_rx) = create_fake_phoenix_socket();

    tokio::spawn(async move {
        unit_testable_ws_handler(out_tx, in_rx, ws_state, uuid::Uuid::new_v4()).await;
    });

    let join = PhxMessage::new(&topic, "phx_join", json!({}))
        .with_ref("1")
        .with_join_ref("join");
    send_phoenix_msg(&in_tx, &join);
    let join_reply = recv_phoenix_msg(&mut out_rx)
        .await
        .expect("channel should join");
    assert_eq!(join_reply.payload.as_json()["status"], "ok");

    let request = json!({
        "jsonrpc": "2.0",
        "id": "client-request",
        "method": "tools/list"
    });
    let first_handler_state = mcp_state.clone();
    let first_request = request.clone();
    let first_handler = tokio::spawn(async move {
        mcp_channel_client_handler(
            Query(McpParams {
                session_id: Some(session_id.to_string()),
            }),
            State(first_handler_state),
            Bytes::from(serde_json::to_vec(&first_request).unwrap()),
        )
        .await
    });

    let first_forwarded = wait_for_event(&mut out_rx, "mcp_message", 250)
        .await
        .expect("browser should receive first MCP request");
    let first_proxy_id = first_forwarded.payload.as_json()["id"].clone();
    assert!(first_proxy_id
        .as_str()
        .is_some_and(|id| id.starts_with("tw-app-")));

    first_handler.abort();
    assert!(first_handler
        .await
        .expect_err("aborted handler should not complete")
        .is_cancelled());
    assert!(
        mcp_state.awaiting_answers.is_empty(),
        "abandoned request should be removed immediately"
    );

    let second_handler_state = mcp_state.clone();
    let second_handler = tokio::spawn(async move {
        mcp_channel_client_handler(
            Query(McpParams {
                session_id: Some(session_id.to_string()),
            }),
            State(second_handler_state),
            Bytes::from(serde_json::to_vec(&request).unwrap()),
        )
        .await
        .expect("second handler should return a response")
    });

    let second_forwarded = wait_for_event(&mut out_rx, "mcp_message", 250)
        .await
        .expect("browser should receive second MCP request");
    let second_proxy_id = second_forwarded.payload.as_json()["id"].clone();
    assert_ne!(second_proxy_id, first_proxy_id);

    let late_first_response = PhxMessage::new(
        &topic,
        "mcp_message",
        json!({
            "jsonrpc": "2.0",
            "id": first_proxy_id,
            "result": { "request": "first" }
        }),
    )
    .with_ref("2")
    .with_join_ref("join");
    send_phoenix_msg(&in_tx, &late_first_response);
    tokio::task::yield_now().await;
    assert!(
        !second_handler.is_finished(),
        "late response must not complete the retried request"
    );

    let browser_request_with_same_id = PhxMessage::new(
        &topic,
        "mcp_message",
        json!({
            "jsonrpc": "2.0",
            "id": second_proxy_id,
            "method": "sampling/createMessage",
            "params": {}
        }),
    )
    .with_ref("3")
    .with_join_ref("join");
    send_phoenix_msg(&in_tx, &browser_request_with_same_id);
    tokio::task::yield_now().await;
    assert!(
        !second_handler.is_finished(),
        "browser request must not be mistaken for a response"
    );

    let browser_response = PhxMessage::new(
        &topic,
        "mcp_message",
        json!({
            "jsonrpc": "2.0",
            "id": second_proxy_id,
            "result": { "request": "second" }
        }),
    )
    .with_ref("2")
    .with_join_ref("join");
    send_phoenix_msg(&in_tx, &browser_response);

    let response = second_handler
        .await
        .expect("second handler task should not panic");
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should be readable");
    let response_json: Value = serde_json::from_slice(&body).expect("response should be JSON");
    assert_eq!(response_json["id"], "client-request");
    assert_eq!(response_json["result"]["request"], "second");
    assert!(
        mcp_state.awaiting_answers.is_empty(),
        "completed request should not leave pending answers behind"
    );
}
