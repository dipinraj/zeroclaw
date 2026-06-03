# NATS agent (Synadia protocol)

ZeroClaw can register as a discoverable NATS microservice named `agents` and serve the [Synadia Agent Protocol for NATS](https://github.com/synadia-ai/synadia-agents) directly from the `zeroclaw` binary.

Build with the feature enabled:

```bash
cargo build -p zeroclawlabs --features nats-agent
```

## Configuration

Add to `config.toml`:

```toml
[nats_agent]
enabled = true
servers = ["nats://127.0.0.1:4222"]
agent = "zeroclaw"
owner = "default"
name = "main"
description = "ZeroClaw on NATS"
agent_alias = "default"   # must match [agents.default]
heartbeat_interval_secs = 30
max_payload = "8MB"
attachments_ok = true
```

## Run standalone

```bash
zeroclaw nats serve
```

## Run with the daemon

When `nats_agent.enabled = true`, `zeroclaw daemon` supervises the NATS host alongside the gateway and channels.

## Discovery and prompt (NATS CLI)

```bash
nats req '$SRV.INFO.agents' '' --replies=0 --timeout=2s
nats sub 'agents.hb.zeroclaw.default.main'
nats req 'agents.prompt.zeroclaw.default.main' 'hello' \
  --replies=0 --reply-timeout=30s --timeout=60s
```

Stream shape: leading `{"type":"status","data":"ack"}`, then `{"type":"response","data":"..."}` chunks, then an **empty-body** terminator.

## Environment overrides

Use the standard `ZEROCLAW_` prefix, for example:

- `ZEROCLAW_nats_agent__enabled=true`
- `ZEROCLAW_nats_agent__servers__0=nats://localhost:4222`
- `ZEROCLAW_nats_agent__agent_alias=default`

## Troubleshooting `cargo check` / index errors

### `no matching package named 'cc' found`

This is **not** a missing ZeroClaw dependency — `cc` is a normal crates.io crate used by `rusqlite` / SQLite. Cargo usually prints this when the **crates.io sparse index is corrupt or incomplete** (often after a timed-out `Updating crates.io index`).

1. Close other Cargo processes (IDE rust-analyzer, background `cargo` builds).
2. Clear the local index cache:

```powershell
Remove-Item -Recurse -Force "$env:USERPROFILE\.cargo\registry\index\*" -ErrorAction SilentlyContinue
```

3. Retry with a longer HTTP timeout and refresh the lockfile (needed once for `async-nats`):

```powershell
$env:CARGO_HTTP_TIMEOUT = "600"
cd C:\Users\dipin\nats\zeroclaw_nats_integration\zeroclaw
cargo update -p async-nats --precise 0.46.0
cargo check --workspace --exclude zeroclaw-desktop -p zeroclaw-nats-agent
```

If index downloads keep failing, use the git protocol for this shell session (slower, more reliable on flaky networks):

```powershell
$env:CARGO_REGISTRIES_CRATES_IO_PROTOCOL = "git"
```

### Build the CLI with NATS support

```powershell
cargo build -p zeroclawlabs --features nats-agent
```
