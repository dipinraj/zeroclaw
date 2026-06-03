//! Prompt endpoint handler — bridges Synadia streaming to `Agent::turn_streamed`.

use std::sync::Arc;

use async_nats::client::Client;
use async_nats::service::Request;
use tokio_util::sync::CancellationToken;
use zeroclaw_api::agent::TurnEvent;
use zeroclaw_config::schema::NatsAgentConfig;

use crate::envelope::parse_prompt_payload;
use crate::sessions::SessionStore;
use crate::stream::{
    encode_query, encode_response_delta, publish_chunk, respond_client_error, respond_server_error,
    send_ack, send_terminator,
};

pub async fn handle_prompt(
    request: Request,
    client: Client,
    nats_cfg: &NatsAgentConfig,
    sessions: Arc<SessionStore>,
    max_payload_bytes: usize,
) {
    let payload = request.message.payload.as_ref();
    if payload.len() > max_payload_bytes {
        let _ = respond_client_error(&request, "prompt exceeds max_payload").await;
        return;
    }

    let envelope = match parse_prompt_payload(payload, nats_cfg.attachments_ok) {
        Ok(e) => e,
        Err(e) => {
            let _ = respond_client_error(&request, e.to_string()).await;
            return;
        }
    };

    let reply = match request.message.reply.as_deref() {
        Some(r) if !r.is_empty() => r.to_string(),
        _ => {
            let _ = respond_client_error(&request, "missing reply subject").await;
            return;
        }
    };

    let session_key = crate::sessions::session_key_from_request(
        nats_cfg.session.as_deref(),
        Some(reply.as_str()),
    );

    let agent_arc = match sessions.agent_for_session(&session_key).await {
        Ok(a) => a,
        Err(e) => {
            let _ = respond_server_error(&request, format!("agent init failed: {e}")).await;
            return;
        }
    };

    if let Err(e) = send_ack(&client, &reply).await {
        let _ = respond_server_error(&request, format!("failed to send ack: {e}")).await;
        return;
    }

    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel::<TurnEvent>(128);
    let cancel_token = CancellationToken::new();
    let cancel_for_loop = cancel_token.clone();
    let prompt_text = if envelope.attachments.is_empty() {
        envelope.prompt
    } else {
        format!(
            "{}\n\n[{} attachment(s) received — decode in a future revision]",
            envelope.prompt,
            envelope.attachments.len()
        )
    };

    let turn_handle = {
        let agent_arc = agent_arc.clone();
        tokio::spawn(async move {
            let mut agent = agent_arc.lock().await;
            agent
                .turn_streamed(&prompt_text, event_tx, Some(cancel_token))
                .await
        })
    };

    while let Some(event) = event_rx.recv().await {
        if matches!(event, TurnEvent::Usage { .. }) {
            continue;
        }

        let chunk = match &event {
            TurnEvent::Chunk { delta } | TurnEvent::Thinking { delta } => {
                encode_response_delta(delta)
            }
            TurnEvent::ToolCall { name, .. } => {
                encode_response_delta(&format!("\n[tool: {name}]\n"))
            }
            TurnEvent::ToolResult { name, output, .. } => {
                let preview: String = output.chars().take(500).collect();
                encode_response_delta(&format!("\n[tool result {name}]: {preview}\n"))
            }
            TurnEvent::ApprovalRequest {
                request_id,
                tool_name,
                arguments_summary,
                ..
            } => encode_query(&serde_json::json!({
                "kind": "approval",
                "request_id": request_id,
                "tool": tool_name,
                "summary": arguments_summary,
            })),
            TurnEvent::Usage { .. } => unreachable!(),
        };

        if let Err(e) = publish_chunk(&client, &reply, chunk).await {
            let _ = respond_server_error(&request, format!("stream publish failed: {e}")).await;
            cancel_for_loop.cancel();
            break;
        }
    }

    match turn_handle.await {
        Ok(Ok(_)) => {
            let _ = send_terminator(&request).await;
        }
        Ok(Err(e)) => {
            let _ = publish_chunk(
                &client,
                &reply,
                encode_response_delta(&format!("\n[error]: {e}\n")),
            )
            .await;
            let _ = send_terminator(&request).await;
        }
        Err(e) => {
            let _ = respond_server_error(&request, format!("agent task panicked: {e}")).await;
        }
    }
}
