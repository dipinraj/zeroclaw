//! Per-session Agent instances for NATS prompt handling.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use parking_lot::Mutex;
use zeroclaw_config::schema::Config;
use zeroclaw_runtime::agent::Agent;

const NATS_SESSION_PREFIX: &str = "nats_";

pub struct SessionStore {
    config: Config,
    agent_alias: String,
    sessions: Mutex<HashMap<String, Arc<tokio::sync::Mutex<Agent>>>>,
}

impl SessionStore {
    pub fn new(config: Config, agent_alias: String) -> Self {
        Self {
            config,
            agent_alias,
            sessions: Mutex::new(HashMap::new()),
        }
    }

    pub async fn agent_for_session(&self, session_key: &str) -> Result<Arc<tokio::sync::Mutex<Agent>>> {
        if let Some(existing) = self.sessions.lock().get(session_key).cloned() {
            return Ok(existing);
        }

        let session_cwd = std::env::current_dir().unwrap_or_else(|_| self.config.data_dir.clone());
        let agent = Agent::from_config_with_session_cwd_and_mcp_backchannel(
            &self.config,
            &self.agent_alias,
            Some(&session_cwd),
            true,
            false,
            None,
            None,
            None,
        )
        .await?;

        let memory_id =
            zeroclaw_api::session_keys::sanitize_session_key(session_key.strip_prefix(NATS_SESSION_PREFIX).unwrap_or(session_key));
        let mut agent = agent;
        agent.set_memory_session_id(Some(memory_id));

        let arc = Arc::new(tokio::sync::Mutex::new(agent));
        self.sessions
            .lock()
            .insert(session_key.to_string(), arc.clone());
        Ok(arc)
    }
}

pub fn session_key_from_request(
    configured_session: Option<&str>,
    reply: Option<&str>,
) -> String {
    if let Some(session) = configured_session.filter(|s| !s.is_empty()) {
        return format!("{NATS_SESSION_PREFIX}{session}");
    }
    let seed = reply.unwrap_or("anonymous");
    format!("{NATS_SESSION_PREFIX}{seed}")
}
