# Prefill-Decode Disaggregation

```mermaid
sequenceDiagram
    participant Prefiller_OC as Prefiller OC
    participant Prefiller_CPU_Cache as Prefiller CPU Cache
    participant Prefiller_PD as Prefiller PD
    participant Decoder_PD as Decoder PD
    participant Decoder_CPU_Cache as Decoder CPU Cache
    participant Decoder_OC as Decoder OC

    par
        Prefiller_PD->>Prefiller_PD: Open listener thread
    and
        Decoder_PD->>Decoder_PD: Open listener thread
    end

    Note over Prefiller_OC,Decoder_OC: ── Init time ──

    Prefiller_OC->>Prefiller_PD: save(blocks, D identity)
    Note right of Prefiller_PD: If no connection to D exists,<br/>do handshake and create connection
    Prefiller_PD->>Decoder_PD: AllocateFetch(blocks_hash, local_block_desc)
    Decoder_PD->>Decoder_CPU_Cache: PrepareLoad(block_hashs)
    Decoder_PD->>Prefiller_PD: NIXL.Transfer(READ, local_block_descs, remote_block_descs)
    Decoder_CPU_Cache-->>Decoder_PD: done
    Prefiller_PD-->>Prefiller_OC: done
    Decoder_OC->>Decoder_CPU_Cache: load(block_hashs)
```
