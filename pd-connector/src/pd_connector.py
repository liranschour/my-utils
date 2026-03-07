"""
PDConnector — a SecondaryPillar that transfers KV cache blocks between
Prefiller and Decoder nodes via ZMQ control channel + NIXL data transfer.

Roles:
  Prefiller side: save() registers block descriptors; the listener thread
                  handles incoming lookup_fetch requests and sends lookup_ack.
                  On save(), if a matching lookup_fetch exists, initiates NIXL
                  WRITE and sends transfer_done per chunk when complete.
  Decoder side:   load() connects to the Prefiller peer, sends lookup_fetch,
                  and waits for lookup_ack within a deadline. Accumulates
                  transfer_done messages until all blocks received → DONE.
"""

import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
import torch

from nixl._api import nixl_agent, nixl_agent_config

from src.secondary_pillar import BlockDesc, SecondaryPillar
from src.zmq_ctrl_transport import ZmqCtrlTransport


# ---------------------------------------------------------------------------
# NIXL transport abstraction (real impl wraps nixl_wrapper)
# ---------------------------------------------------------------------------

class NixlTransport(ABC):

    @abstractmethod
    def write(
        self,
        transfer_id: str,
        local_block_descs: list[BlockDesc],
        remote_block_descs: list[BlockDesc],
        peer_id: str,
    ) -> None:
        """Start an async NIXL WRITE from local → remote."""
        ...

    @abstractmethod
    def get_finished(self, transfer_ids: list[str]) -> list[str]:
        """Return the subset of transfer_ids that have completed."""
        ...

    @abstractmethod
    def cancel(self, transfer_id: str) -> None: ...


# ---------------------------------------------------------------------------
# Internal job state
# ---------------------------------------------------------------------------

class _JobState(Enum):
    PENDING   = auto()
    TRANSFER  = auto()
    DONE      = auto()
    ABORTED   = auto()


@dataclass
class _SaveJob:
    job_id: str
    hash_to_desc: dict[int, BlockDesc] = field(default_factory=dict)
    state: _JobState = _JobState.PENDING


@dataclass
class _LoadJob:
    job_id: str
    block_hashes: list[int]
    block_indexes: list[int]    # Decoder's kv_block slot for each hash (same order)
    peer_id: str
    ack_deadline: float = 0.0   # exceeded with no ack → control failure
    xfer_deadline: float = 0.0  # set on ack; exceeded → transfer failure
    state: _JobState = _JobState.PENDING
    received_hashes: set[int] = field(default_factory=set)


@dataclass
class _PendingFetch:
    """Prefiller-side state for a lookup_fetch request from the Decoder."""
    job_id: str
    decoder_peer_id: str
    hash_to_remote_idx: dict[int, int]  # block_hash → Decoder kv_block slot
    remaining_hashes: set[int]          # hashes not yet transferred


# ---------------------------------------------------------------------------
# PDConnector
# ---------------------------------------------------------------------------

class PDConnector(SecondaryPillar):

    def __init__(
        self,
        peer_id: str,
        listen_port: int,
        nixl: NixlTransport,  # TODO: remove once all methods use self._agent directly
        kv_blocks: list[torch.Tensor] | None = None,
        heartbeat_ivl_ms:     int = 2000,
        heartbeat_timeout_ms: int = 10000,
    ) -> None:
        self._peer_id  = peer_id
        self._nixl     = nixl
        self._kv_blocks: list[torch.Tensor] = kv_blocks if kv_blocks is not None else []

        # NIXL agent — register all KV blocks up front and prep a local descriptor list
        # so future transfers can be initiated using only block indices.
        self._agent = nixl_agent(peer_id, nixl_agent_config(backends=["UCX"]))
        if self._kv_blocks:
            self._reg         = self._agent.register_memory(self._kv_blocks)
            self._local_dlist = self._agent.prep_xfer_dlist("NIXL_INIT_AGENT", self._kv_blocks)
        else:
            self._reg         = None
            self._local_dlist = None

        # Pre-compute wire-format block descriptors so _connect() has nothing to calculate.
        self._block_descs_wire: list[tuple[int, int, int]] = [
            (t.data_ptr(), t.numel() * t.element_size(), max(t.get_device(), 0))
            for t in self._kv_blocks
        ]

        # addr → local kv_block index (for fast lookup during _try_transfer)
        self._addr_to_idx: dict[int, int] = {
            t.data_ptr(): i for i, t in enumerate(self._kv_blocks)
        }

        self._lock       = threading.Lock()
        self._save_jobs: dict[str, _SaveJob] = {}
        self._load_jobs: dict[str, _LoadJob] = {}

        # Prefiller-side pending fetch state
        self._pending_fetches: dict[str, _PendingFetch] = {}  # job_id → _PendingFetch

        # In-flight NIXL transfer handles: (handle, job_id, chunk_hashes, decoder_peer_id)
        self._xfer_handles: list[tuple] = []

        # Connection state
        self._connections:    set[str]                    = set()
        self._connect_events: dict[str, threading.Event] = {}
        self._remote_dlists:  dict[str, object]          = {}  # peer_id → nixl_prepped_dlist_handle

        self._ctrl = ZmqCtrlTransport(
            peer_id, listen_port,
            heartbeat_ivl_ms=heartbeat_ivl_ms,
            heartbeat_timeout_ms=heartbeat_timeout_ms,
        )
        self._ctrl.set_peer_down_callback(self._on_peer_down)

        self._listener = threading.Thread(target=self._listen, daemon=True)
        self._listener.start()

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def connect(self, peer_id: str, host: str, port: int) -> None:
        """Connect to a remote peer's PDConnector."""
        self._ctrl.connect(peer_id, host, port)

    def disconnect(self, peer_id: str) -> None:
        self._ctrl.disconnect(peer_id)

    def close(self) -> None:
        self._ctrl.close()
        with self._lock:
            remote_dlists = list(self._remote_dlists.values())
            self._remote_dlists.clear()
            xfer_handles = [h for h, *_ in self._xfer_handles]
            self._xfer_handles.clear()
        for dlist in remote_dlists:
            self._agent.release_dlist_handle(dlist)
        for handle in xfer_handles:
            self._agent.release_xfer_handle(handle)
        if self._local_dlist is not None:
            self._agent.release_dlist_handle(self._local_dlist)
        if self._reg is not None:
            self._agent.deregister_memory(self._reg)

    # ------------------------------------------------------------------
    # SecondaryPillar interface
    # ------------------------------------------------------------------

    def save(self, job_id: str, block_descs: list[BlockDesc]) -> None:
        """Prefiller: register blocks as ready to serve lookup_fetch.

        Blocks are accumulated incrementally (streaming support). After
        updating, attempt to transfer any matching blocks to waiting Decoders.
        """
        with self._lock:
            job = self._save_jobs.setdefault(job_id, _SaveJob(job_id=job_id))
            for desc in block_descs:
                job.hash_to_desc[desc.block_hash] = desc

        self._try_transfer(job_id)

    def load(self, job_id: str, block_hashes: list[int], block_indexes: list[int], peer_id: str) -> None:
        """Decoder: connect to Prefiller if needed, then send lookup_fetch.

        peer_id must be in "host:port" format pointing to the remote ZMQ listener.
        block_hashes[i] identifies block i; block_indexes[i] is the local kv_block
        slot where the Prefiller should write it.
        Raises TimeoutError if the connection handshake fails.
        """
        self._ensure_connected(peer_id)

        job = _LoadJob(
            job_id=job_id,
            block_hashes=block_hashes,
            block_indexes=block_indexes,
            peer_id=peer_id,
            ack_deadline=time.monotonic() + self._LOOKUP_ACK_TIMEOUT_S,
        )
        with self._lock:
            self._load_jobs[job_id] = job

        self._ctrl.send(peer_id, {
            "type":          "lookup_fetch",
            "job_id":        job_id,
            "block_hashes":  block_hashes,
            "block_indexes": block_indexes,
            "peer_id":       self._peer_id,
        })

    def get_finished(self, job_ids: list[str]) -> list[str]:
        self._poll_xfer_handles()

        now = time.monotonic()
        with self._lock:
            for job in self._load_jobs.values():
                if job.state == _JobState.PENDING and now > job.ack_deadline:
                    job.state = _JobState.ABORTED  # control failure: no lookup_ack received
                elif job.state == _JobState.TRANSFER and now > job.xfer_deadline:
                    job.state = _JobState.ABORTED  # transfer failure: data did not arrive

            return [jid for jid in job_ids if self._is_done(jid)]

    def abort(self, job_id: str) -> None:
        with self._lock:
            if job_id in self._load_jobs:
                self._load_jobs[job_id].state = _JobState.ABORTED
            if job_id in self._save_jobs:
                self._save_jobs[job_id].state = _JobState.ABORTED

    # ------------------------------------------------------------------
    # Connection establishment
    # ------------------------------------------------------------------

    _CONNECT_TIMEOUT_S    = 10.0
    _LOOKUP_ACK_TIMEOUT_S = 10.0
    _TRANSFER_TIMEOUT_S   = 30.0

    def _ensure_connected(self, peer_id: str) -> None:
        """Connect and complete NIXL handshake with peer_id if not already done."""
        with self._lock:
            if peer_id in self._connections:
                return
            first = peer_id not in self._connect_events
            if first:
                self._connect_events[peer_id] = threading.Event()
            event = self._connect_events[peer_id]

        if first:
            self._connect(peer_id)

        if not event.wait(timeout=self._CONNECT_TIMEOUT_S):
            raise TimeoutError(f"Handshake with {peer_id} timed out")

    def _connect(self, peer_id: str) -> None:
        """Open ZMQ channel to peer and send connect handshake message."""
        host, port_str = peer_id.rsplit(":", 1)
        self._ctrl.connect(peer_id, host, int(port_str))
        self._ctrl.send(peer_id, {
            "type":           "connect",
            "peer_id":        self._peer_id,
            "agent_metadata": self._agent.get_agent_metadata(),
            "block_descs":    self._block_descs_wire,
        })

    # ------------------------------------------------------------------
    # NIXL transfer (Prefiller side)
    # ------------------------------------------------------------------

    def _try_transfer(self, job_id: str) -> None:
        """Initiate NIXL WRITE for any blocks that are both saved and pending fetch.

        Called from save() after accumulating new blocks. Matches saved hashes
        against the pending fetch's remaining_hashes, builds index lists, starts
        the transfer, and removes matched hashes from remaining_hashes.
        """
        with self._lock:
            save_job = self._save_jobs.get(job_id)
            fetch    = self._pending_fetches.get(job_id)
            if save_job is None or fetch is None:
                return
            if save_job.state == _JobState.ABORTED:
                return

            # Find hashes that are both saved and still waiting to be transferred
            ready = fetch.remaining_hashes & save_job.hash_to_desc.keys()
            if not ready:
                return

            # Build parallel local/remote index lists
            local_indices  = []
            remote_indices = []
            chunk_hashes   = []
            for h in ready:
                desc = save_job.hash_to_desc[h]
                local_idx = self._addr_to_idx.get(desc.addr)
                if local_idx is None:
                    continue
                remote_idx = fetch.hash_to_remote_idx[h]
                local_indices.append(local_idx)
                remote_indices.append(remote_idx)
                chunk_hashes.append(h)

            if not chunk_hashes:
                return

            remote_dlist   = self._remote_dlists.get(fetch.decoder_peer_id)
            decoder_peer_id = fetch.decoder_peer_id

            fetch.remaining_hashes -= set(chunk_hashes)

        if remote_dlist is None or self._local_dlist is None:
            return

        handle = self._agent.make_prepped_xfer(
            "NIXL_WRITE",
            self._local_dlist,
            local_indices,
            remote_dlist,
            remote_indices,
        )
        state = self._agent.transfer(handle)

        with self._lock:
            if state == "ERR":
                self._agent.release_xfer_handle(handle)
            else:
                self._xfer_handles.append((handle, job_id, chunk_hashes, decoder_peer_id))

    def _poll_xfer_handles(self) -> None:
        """Check all in-flight NIXL transfers; send transfer_done for completed ones."""
        with self._lock:
            pending = list(self._xfer_handles)

        still_in_flight = []
        for entry in pending:
            handle, job_id, chunk_hashes, decoder_peer_id = entry
            state = self._agent.check_xfer_state(handle)
            if state == "PROC":
                still_in_flight.append(entry)
                continue

            self._agent.release_xfer_handle(handle)

            if state == "DONE":
                self._ctrl.send(decoder_peer_id, {
                    "type":        "transfer_done",
                    "job_id":      job_id,
                    "block_hashes": chunk_hashes,
                })
            # ERR: transfer failed — Decoder's xfer_deadline will fire eventually

        with self._lock:
            # Replace list preserving any entries added concurrently
            completed_handles = {id(e[0]) for e in pending if e not in still_in_flight}
            self._xfer_handles = [
                e for e in self._xfer_handles
                if id(e[0]) not in completed_handles
            ]

    # ------------------------------------------------------------------
    # Listener — handles messages from both Prefiller and Decoder
    # ------------------------------------------------------------------

    def _listen(self) -> None:
        while True:
            sender_id, msg = self._ctrl.recv()
            mtype = msg.get("type")
            if mtype == "connect":
                self._handle_connect(sender_id, msg)
            elif mtype == "connect_ack":
                self._handle_connect_ack(sender_id, msg)
            elif mtype == "lookup_fetch":
                self._handle_lookup_fetch(sender_id, msg)
            elif mtype == "lookup_ack":
                self._handle_lookup_ack(sender_id, msg)
            elif mtype == "transfer_done":
                self._handle_transfer_done(sender_id, msg)

    def _handle_connect(self, sender_id: str, msg: dict) -> None:
        """Prefiller side: receive connect from Decoder, prep remote dlist, reply with ack."""
        self._agent.add_remote_agent(msg["agent_metadata"])

        block_descs = [tuple(d) for d in msg["block_descs"]]
        remote_dlist = self._agent.prep_xfer_dlist(sender_id, block_descs, mem_type="cpu")

        with self._lock:
            self._remote_dlists[sender_id] = remote_dlist
            self._connections.add(sender_id)

        # Connect back so we can reply (and for future NIXL transfers)
        decoder_peer_id = msg["peer_id"]
        host, port_str = decoder_peer_id.rsplit(":", 1)
        self._ctrl.connect(decoder_peer_id, host, int(port_str))

        self._ctrl.send(decoder_peer_id, {
            "type":    "connect_ack",
            "peer_id": self._peer_id,
            # No agent_metadata: Decoder is the WRITE target, not the initiator,
            # so it does not need to call add_remote_agent on the Prefiller.
        })

    def _handle_connect_ack(self, sender_id: str, _msg: dict) -> None:
        """Decoder side: receive connect_ack and signal handshake done.
        No add_remote_agent call: Decoder is the WRITE target, not the initiator."""
        with self._lock:
            self._connections.add(sender_id)
            event = self._connect_events.get(sender_id)
        if event:
            event.set()

    def _handle_lookup_fetch(self, sender_id: str, msg: dict) -> None:
        """Prefiller side: receive lookup_fetch, create PendingFetch, send ack.

        The ack is sent immediately — it confirms message receipt, not data transfer.
        If blocks are already saved, _try_transfer initiates the NIXL write right away.
        """
        job_id       = msg["job_id"]
        block_hashes = msg["block_hashes"]
        block_indexes = msg["block_indexes"]
        decoder_peer_id = msg["peer_id"]

        fetch = _PendingFetch(
            job_id=job_id,
            decoder_peer_id=decoder_peer_id,
            hash_to_remote_idx=dict(zip(block_hashes, block_indexes)),
            remaining_hashes=set(block_hashes),
        )
        with self._lock:
            self._pending_fetches[job_id] = fetch

        self._ctrl.send(decoder_peer_id, {
            "type":   "lookup_ack",
            "job_id": job_id,
        })

        # If blocks are already saved, initiate transfer immediately
        self._try_transfer(job_id)

    def _handle_lookup_ack(self, sender_id: str, msg: dict) -> None:
        """Decoder side: receive lookup_ack — confirms Prefiller received the job.
        Advances from PENDING to TRANSFER and starts the transfer deadline.
        Does NOT mark DONE: data transfer has not occurred yet."""
        job_id = msg["job_id"]
        with self._lock:
            job = self._load_jobs.get(job_id)
            if job and job.state == _JobState.PENDING:
                job.xfer_deadline = time.monotonic() + self._TRANSFER_TIMEOUT_S
                job.state = _JobState.TRANSFER

    def _handle_transfer_done(self, _sender_id: str, msg: dict) -> None:
        """Decoder side: accumulate received block hashes; mark DONE when all received."""
        job_id       = msg["job_id"]
        chunk_hashes = set(msg["block_hashes"])
        with self._lock:
            job = self._load_jobs.get(job_id)
            if job is None or job.state != _JobState.TRANSFER:
                return
            job.received_hashes |= chunk_hashes
            if job.received_hashes >= set(job.block_hashes):
                job.state = _JobState.DONE

    def _on_peer_down(self, peer_id: str) -> None:
        with self._lock:
            self._connections.discard(peer_id)
            dlist = self._remote_dlists.pop(peer_id, None)
            affected = [
                jid for jid, job in self._load_jobs.items()
                if job.peer_id == peer_id
                and job.state not in (_JobState.DONE, _JobState.ABORTED)
            ]
        if dlist is not None:
            self._agent.release_dlist_handle(dlist)
        for jid in affected:
            self.abort(jid)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _is_done(self, job_id: str) -> bool:
        job = self._load_jobs.get(job_id) or self._save_jobs.get(job_id)
        return job is not None and job.state == _JobState.DONE
