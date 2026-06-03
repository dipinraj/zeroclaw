//! Verb-first subject layout (Synadia Agent Protocol v0.3).

/// NATS microservice name for agent discovery.
pub const SERVICE_NAME: &str = "agents";

/// Queue group shared by `prompt` and `status` endpoints.
pub const AGENTS_QUEUE_GROUP: &str = "agents";

pub const PROMPT_ENDPOINT_NAME: &str = "prompt";
pub const STATUS_ENDPOINT_NAME: &str = "status";

/// Protocol version advertised in service metadata.
pub const PROTOCOL_VERSION: &str = "0.3";

/// Canonical harness identifier for ZeroClaw.
pub const DEFAULT_HARNESS_AGENT: &str = "zeroclaw";

#[derive(Debug, Clone)]
pub struct AgentSubject {
    pub agent: String,
    pub owner: String,
    pub name: String,
    pub prompt: String,
    pub status: String,
    pub heartbeat: String,
}

impl AgentSubject {
    pub fn new(agent: impl Into<String>, owner: impl Into<String>, name: impl Into<String>) -> Self {
        let agent: String = agent.into();
        let agent = if agent.is_empty() {
            DEFAULT_HARNESS_AGENT.to_string()
        } else {
            agent
        };
        let owner = owner.into();
        let name = name.into();
        Self {
            prompt: format!("agents.prompt.{agent}.{owner}.{name}"),
            status: format!("agents.status.{agent}.{owner}.{name}"),
            heartbeat: format!("agents.hb.{agent}.{owner}.{name}"),
            agent,
            owner,
            name,
        }
    }
}
