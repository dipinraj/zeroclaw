//! NATS connection, microservice registration, and supervisor loop.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use async_nats::service::ServiceExt;
use async_nats::ConnectOptions;
use futures_util::StreamExt;
use tokio_util::sync::CancellationToken;
use zeroclaw_config::schema::Config;

use crate::heartbeat::{build_heartbeat_payload, encode_heartbeat_payload};
use crate::prompt::handle_prompt;
use crate::sessions::SessionStore;
use crate::subject::{
    AgentSubject, AGENTS_QUEUE_GROUP, PROMPT_ENDPOINT_NAME, PROTOCOL_VERSION, SERVICE_NAME,
    STATUS_ENDPOINT_NAME,
};

pub async fn run(config: Config, cancel: CancellationToken) -> Result<()> {
    let nats_cfg = config.nats_agent.clone();
    if !nats_cfg.enabled {
        anyhow::bail!("nats_agent.enabled is false");
    }
    nats_cfg.validate(&config)?;

    let subject = AgentSubject::new(
        &nats_cfg.agent,
        &nats_cfg.owner,
        &nats_cfg.name,
    );

    let servers: Vec<String> = if nats_cfg.servers.is_empty() {
        vec!["nats://127.0.0.1:4222".to_string()]
    } else {
        nats_cfg.servers.clone()
    };

    let server_list = servers.join(",");
    let client = {
        let mut connect = ConnectOptions::new();
        if let (Some(user), Some(pass)) = (&nats_cfg.username, &nats_cfg.password) {
            connect = connect.user_and_password(user.clone(), pass.clone());
        }
        if let Some(token) = &nats_cfg.token {
            connect = connect.token(token.clone());
        }
        if nats_cfg.username.is_some() || nats_cfg.password.is_some() || nats_cfg.token.is_some() {
            connect
                .connect(server_list)
                .await
                .context("failed to connect to NATS")?
        } else {
            async_nats::connect(server_list)
                .await
                .context("failed to connect to NATS")?
        }
    };

    let server_max = 0usize;
    let max_payload_bytes = nats_cfg.effective_max_payload_bytes(server_max);

    let mut metadata = HashMap::from([
        ("agent".to_string(), subject.agent.clone()),
        ("owner".to_string(), subject.owner.clone()),
        (
            "protocol_version".to_string(),
            PROTOCOL_VERSION.to_string(),
        ),
    ]);
    if let Some(session) = nats_cfg.session.as_ref().filter(|s| !s.is_empty()) {
        metadata.insert("session".to_string(), session.clone());
    }

    let endpoint_meta = HashMap::from([
        (
            "max_payload".to_string(),
            nats_cfg.advertised_max_payload(server_max),
        ),
        (
            "attachments_ok".to_string(),
            if nats_cfg.attachments_ok {
                "true"
            } else {
                "false"
            }
            .to_string(),
        ),
    ]);

    let service = client
        .service_builder()
        .description(nats_cfg.description.clone())
        .metadata(metadata)
        .queue_group(AGENTS_QUEUE_GROUP)
        .start(SERVICE_NAME, env!("CARGO_PKG_VERSION"))
        .await
        .map_err(|e| anyhow::anyhow!("failed to register NATS agents service: {e}"))?;

    let instance_id = service.info().await.id.clone();

    let mut prompt_endpoint = service
        .endpoint_builder()
        .name(PROMPT_ENDPOINT_NAME)
        .queue_group(AGENTS_QUEUE_GROUP)
        .metadata(endpoint_meta.clone())
        .add(subject.prompt.clone())
        .await
        .map_err(|e| anyhow::anyhow!("failed to add prompt endpoint: {e}"))?;

    let status_subject = subject.status.clone();
    let status_interval = nats_cfg.heartbeat_interval_secs;
    let status_session = nats_cfg.session.clone();
    let status_agent = subject.clone();
    let status_instance_id = instance_id.clone();

    let mut status_endpoint = service
        .endpoint_builder()
        .name(STATUS_ENDPOINT_NAME)
        .queue_group(AGENTS_QUEUE_GROUP)
        .add(status_subject)
        .await
        .map_err(|e| anyhow::anyhow!("failed to add status endpoint: {e}"))?;

    let sessions = Arc::new(SessionStore::new(
        config.clone(),
        nats_cfg.agent_alias.clone(),
    ));
    let nats_cfg = Arc::new(nats_cfg);

    ::zeroclaw_log::record!(
        INFO,
        ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note)
            .with_attrs(serde_json::json!({
                "instance_id": instance_id,
                "prompt": subject.prompt,
                "status": subject.status,
                "heartbeat": subject.heartbeat,
                "agent_alias": nats_cfg.agent_alias,
            })),
        "Synadia NATS agent listening"
    );

    zeroclaw_runtime::health::mark_component_ok("nats_agent");

    let hb_client = client.clone();
    let hb_subject = subject.heartbeat.clone();
    let hb_agent = subject.clone();
    let hb_instance = instance_id.clone();
    let hb_session = nats_cfg.session.clone();
    let hb_cancel = cancel.clone();

    let heartbeat_handle = tokio::spawn(async move {
        let publish = || {
            let payload = build_heartbeat_payload(
                &hb_agent,
                status_interval,
                &hb_instance,
                hb_session.clone().filter(|s| !s.is_empty()),
            );
            let bytes = encode_heartbeat_payload(&payload);
            let client = hb_client.clone();
            let subject = hb_subject.clone();
            async move {
                let _ = client.publish(subject, bytes.into()).await;
            }
        };
        publish().await;
        let mut interval =
            tokio::time::interval(Duration::from_secs(status_interval.max(1)));
        loop {
            tokio::select! {
                _ = hb_cancel.cancelled() => break,
                _ = interval.tick() => {
                    publish().await;
                }
            }
        }
    });

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            req = prompt_endpoint.next() => {
                let Some(req) = req else { continue };
                let sessions = sessions.clone();
                let cfg = nats_cfg.clone();
                let max = max_payload_bytes;
                let prompt_client = client.clone();
                tokio::spawn(async move {
                    handle_prompt(req, prompt_client, &cfg, sessions, max).await;
                });
            }
            req = status_endpoint.next() => {
                let Some(req) = req else { continue };
                let payload = build_heartbeat_payload(
                    &status_agent,
                    status_interval,
                    &status_instance_id,
                    status_session.clone().filter(|s| !s.is_empty()),
                );
                let bytes = encode_heartbeat_payload(&payload);
                let _ = req.respond(Ok(bytes.into())).await;
            }
        }
    }

    heartbeat_handle.abort();
    service
        .stop()
        .await
        .map_err(|e| anyhow::anyhow!("failed to stop NATS service: {e}"))?;
    client
        .drain()
        .await
        .map_err(|e| anyhow::anyhow!("failed to drain NATS client: {e}"))?;
    Ok(())
}
