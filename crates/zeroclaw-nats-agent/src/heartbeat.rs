//! §8.3 heartbeat payload (pub/sub and status endpoint).

use crate::subject::{AgentSubject, PROTOCOL_VERSION};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct HeartbeatPayload {
    pub agent: String,
    pub owner: String,
    pub name: String,
    pub instance_id: String,
    pub protocol_version: String,
    pub heartbeat_interval_s: u64,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session: Option<String>,
}

pub fn build_heartbeat_payload(
    subject: &AgentSubject,
    heartbeat_interval_s: u64,
    instance_id: impl Into<String>,
    session: Option<String>,
) -> HeartbeatPayload {
    HeartbeatPayload {
        agent: subject.agent.clone(),
        owner: subject.owner.clone(),
        name: subject.name.clone(),
        instance_id: instance_id.into(),
        protocol_version: PROTOCOL_VERSION.to_string(),
        heartbeat_interval_s,
        status: "ready".to_string(),
        session,
    }
}

pub fn encode_heartbeat_payload(payload: &HeartbeatPayload) -> Vec<u8> {
    serde_json::to_vec(payload).expect("heartbeat payload serializes")
}
