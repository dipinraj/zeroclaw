//! Synadia stream chunk encoding and reply publishing.

use anyhow::Result;
use async_nats::client::Client;
use async_nats::service::error::Error as ServiceError;
use async_nats::service::Request;
use bytes::Bytes;
use serde::Serialize;

#[derive(Serialize)]
struct StatusChunk<'a> {
    #[serde(rename = "type")]
    chunk_type: &'static str,
    data: &'a str,
}

#[derive(Serialize)]
struct ResponseChunk<'a> {
    #[serde(rename = "type")]
    chunk_type: &'static str,
    data: &'a str,
}

#[derive(Serialize)]
struct QueryChunk<'a> {
    #[serde(rename = "type")]
    chunk_type: &'static str,
    data: &'a serde_json::Value,
}

pub fn encode_status_ack() -> Bytes {
    Bytes::from(
        serde_json::to_string(&StatusChunk {
            chunk_type: "status",
            data: "ack",
        })
        .expect("status ack serializes"),
    )
}

pub fn encode_response_delta(delta: &str) -> Bytes {
    Bytes::from(
        serde_json::to_string(&ResponseChunk {
            chunk_type: "response",
            data: delta,
        })
        .expect("response chunk serializes"),
    )
}

pub fn encode_query(data: &serde_json::Value) -> Bytes {
    Bytes::from(
        serde_json::to_string(&QueryChunk {
            chunk_type: "query",
            data,
        })
        .expect("query chunk serializes"),
    )
}

/// Publish an intermediate chunk on the request reply subject (streaming).
pub async fn publish_chunk(client: &Client, reply: &str, payload: Bytes) -> Result<()> {
    client
        .publish(reply.to_string(), payload)
        .await
        .map_err(|e| anyhow::anyhow!("failed to publish stream chunk: {e}"))?;
    Ok(())
}

/// Empty-body, no-headers stream terminator (§6.5).
pub async fn send_terminator(request: &Request) -> Result<()> {
    request
        .respond(Ok(Bytes::new()))
        .await
        .map_err(|e| anyhow::anyhow!("failed to send stream terminator: {e}"))?;
    Ok(())
}

fn service_error(code: usize, message: impl Into<String>) -> ServiceError {
    ServiceError {
        code,
        status: message.into(),
    }
}

pub async fn respond_client_error(request: &Request, message: impl Into<String>) -> Result<()> {
    request
        .respond(Err(service_error(400, message)))
        .await
        .map_err(|e| anyhow::anyhow!("failed to respond with 400: {e}"))?;
    Ok(())
}

pub async fn respond_server_error(request: &Request, message: impl Into<String>) -> Result<()> {
    request
        .respond(Err(service_error(500, message)))
        .await
        .map_err(|e| anyhow::anyhow!("failed to respond with 500: {e}"))?;
    Ok(())
}

/// Publish ack chunk then allow further chunks via [`publish_chunk`].
pub async fn send_ack(client: &Client, reply: &str) -> Result<()> {
    publish_chunk(client, reply, encode_status_ack()).await
}
