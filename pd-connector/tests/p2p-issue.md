# P2P Connector — Feature Design

The P2P Connector generalizes the [PD Connector](PD-issue.md) into a fully symmetric peer-to-peer mode. It reuses the same secondary-tier architecture (CPU KV cache in canonical layout, NIXL data path, ZMQ control path, unified bi-directional `P2PSession`) but drops the Prefiller / Decoder role distinction.

Every vLLM instance is a **peer**. For any given request, a peer plays one of two roles:

- **Consumer** (a.k.a. client): pulls KV blocks for the request from a remote peer's CPU cache instead of computing them locally.
- **Producer** (a.k.a. server): serves KV blocks from its CPU cache when a remote consumer asks for them.

A single peer can act as consumer for some requests and producer for others at the same time over the same session.

The orchestration layer indicates P2P **on the consumer side only**, on the request itself. The producer is **implicit**: it has no per-request flag; it serves whatever block hashes it currently holds in CPU cache.


## Component Diagram

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {
  'primaryColor': '#1565c0',
  'primaryBorderColor': '#ffffff',
  'primaryTextColor': '#ffffff',
  'lineColor': '#ffffff',
  'edgeLabelBackground': '#1565c0',
  'tertiaryColor': '#1565c0'
}}}%%
graph TD
    OC[OffloadingConnector] --> TM[TieringManager]
    TM -->|GPU↔CPU offload| PT["PrimaryTier<br/>CPU Manager"]
    TM -->|submit_load / lookup| ST["SecondaryTier<br/>P2P Connector"]
    TM -->|get_finished| ST
    ST -->|CTRL: lookup_fetch by block hashes| Remote[Remote Peer P2P]
    ST -->|"NIXL Transfer (READ/WRITE)"| Remote
```


## Sequence Diagram

```mermaid

sequenceDiagram
    participant Cons_TM as Consumer TieringManager
    participant Cons_P2P as Consumer P2P Tier
    participant Prod_P2P as Producer P2P Tier
    participant Prod_TM as Producer TieringManager

    Cons_P2P->>Cons_P2P: Open listener thread
    Prod_P2P->>Prod_P2P: Open listener thread

    Note over Cons_TM,Prod_TM: ── Producer has previously cached blocks for this prompt ──

    Cons_TM->>Cons_P2P: submit_load(job_metadata, kv_transfer_params)
    Note right of Cons_P2P: kv_transfer_params:<br/>kv_request_id, do_p2p_fetch=true,<br/>remote_host, remote_port

    Cons_P2P->>Prod_P2P: 𝗖𝗧𝗥𝗟: lookup(kv_request_id, block_hashes)

    Note right of Prod_P2P: Match block_hashes against<br/>local CPU cache

    Prod_P2P--)Cons_P2P: 𝗖𝗧𝗥𝗟: lookup_resp(kv_request_id, hit_indexes)

    Prod_P2P-)Cons_P2P: 𝗗𝗔𝗧𝗔: NIXL.Transfer(WRITE, src_descs, dst_descs) <br/> for the subset of hashes that hit
    Prod_P2P-->>Cons_P2P: TransferDone(kv_request_id, served_indexes)

    Cons_TM->>Cons_P2P: get_finished()
    Note right of Cons_P2P: Hits → loaded into GPU as a normal cache hit<br/>Misses → recomputed by the engine
```

## Orchestration-Level Protocol

`kv_transfer_params` is set on the consumer's request only. The producer requires no orchestration signal beyond having offloading enabled and having previously processed work that left the relevant blocks in its CPU KV cache.

### Consumer

Set on every request whose KV blocks should be pulled from a remote peer.

| Field | Type | Required | Description |
|---|---|---|---|
| `kv_request_id` | `str` | Yes | Unique ID for this transfer transaction (allocated by the orchestrator) |
| `do_p2p_fetch` | `bool` | Yes | Indicates that KV blocks should be pulled from a remote peer |
| `remote_host` | `str` | Yes | IP address / hostname of the producer peer |
| `remote_port` | `int` or `str` | Yes | Listening port of the producer peer |


## Minimal Example

```python
# Consumer request — pull KV for this prompt from a peer that has it cached.
kv_transfer_params = {
    "kv_request_id": "<unique-transfer-id>",
    "do_p2p_fetch": True,
    "remote_host": "<producer-ip>",
    "remote_port": <producer-port>,
}

# Producer request — no kv_transfer_params needed.
# The peer simply needs offloading enabled and the relevant blocks already
# resident in its CPU cache (e.g., from a previous request the orchestrator
# routed there).
```
