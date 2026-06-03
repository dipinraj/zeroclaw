//! Synadia Agent Protocol for NATS — host implementation for ZeroClaw.

pub(crate) mod envelope;
mod heartbeat;
mod host;
mod prompt;
mod sessions;
mod stream;
pub(crate) mod subject;

pub use host::run;
pub use subject::{AgentSubject, SERVICE_NAME};

#[cfg(test)]
mod tests {
    use crate::envelope::parse_prompt_payload;
    use crate::subject::AgentSubject;

    #[test]
    fn subject_verb_first_layout() {
        let s = AgentSubject::new("zeroclaw", "demo", "main");
        assert_eq!(s.prompt, "agents.prompt.zeroclaw.demo.main");
        assert_eq!(s.status, "agents.status.zeroclaw.demo.main");
        assert_eq!(s.heartbeat, "agents.hb.zeroclaw.demo.main");
    }

    #[test]
    fn parse_plain_and_json_prompt() {
        let plain = parse_prompt_payload(b"hello", true).unwrap();
        assert_eq!(plain.prompt, "hello");

        let json = parse_prompt_payload(
            br#"{"prompt":"hi","attachments":[{"filename":"a.txt","content":"aGk="}]}"#,
            true,
        )
        .unwrap();
        assert_eq!(json.prompt, "hi");
        assert_eq!(json.attachments.len(), 1);
        assert_eq!(json.attachments[0].file_name, "a.txt");
    }
}
