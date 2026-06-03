//! Prompt request envelope parsing (plain text or JSON).

use anyhow::{Context, Result};
use serde::Deserialize;
use zeroclaw_api::media::MediaAttachment;

#[derive(Debug, Clone)]
pub struct PromptEnvelope {
    pub prompt: String,
    pub attachments: Vec<MediaAttachment>,
}

#[derive(Debug, Deserialize)]
struct JsonEnvelope {
    prompt: String,
    #[serde(default)]
    attachments: Vec<JsonAttachment>,
}

#[derive(Debug, Deserialize)]
struct JsonAttachment {
    filename: String,
    content: String,
}

pub fn parse_prompt_payload(payload: &[u8], attachments_ok: bool) -> Result<PromptEnvelope> {
    if payload.is_empty() {
        anyhow::bail!("empty prompt payload");
    }

    let text = std::str::from_utf8(payload).context("prompt must be valid UTF-8")?;
    let trimmed = text.trim();

    if trimmed.starts_with('{') {
        let json: JsonEnvelope = serde_json::from_str(trimmed).context("invalid JSON prompt envelope")?;
        let mut attachments = Vec::new();
        if !json.attachments.is_empty() {
            if !attachments_ok {
                anyhow::bail!("attachments not supported by this agent");
            }
            for att in json.attachments {
                let bytes = base64::Engine::decode(
                    &base64::engine::general_purpose::STANDARD,
                    att.content.as_bytes(),
                )
                .context("attachment content must be base64")?;
                attachments.push(MediaAttachment {
                    file_name: att.filename,
                    mime_type: None,
                    data: bytes,
                });
            }
        }
        Ok(PromptEnvelope {
            prompt: json.prompt,
            attachments,
        })
    } else {
        Ok(PromptEnvelope {
            prompt: text.to_string(),
            attachments: vec![],
        })
    }
}
