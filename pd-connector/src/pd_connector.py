"""
PDConnector — a SecondaryPillar that transfers KV cache blocks between
Prefiller and Decoder nodes via a control channel + NIXL data transfer.

Roles:
  Prefiller side: save() registers block descriptors; the listener thread
                  handles incoming lookup_fetch requests and initiates NIXL WRITE.
  Decoder side:   load() sends lookup_fetch to the Prefiller and waits for the
                  NIXL transfer to complete.
"""

import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto

from src.secondary_pillar import BlockDesc, SecondaryPillar


# ---------------------------------------------------------------------------
# Transport abstraction (real impl will wrap NIXL / zmq / etc.)
# ---------------------------------------------------------------------------

class CtrlTransport(ABC):
    """Send/receive control messages between PD peers."""

    @abstractmethod
    def send(self, peer_id: str, msg: dict) -> None: ...

    @abstractmethod
    def recv(self) -> tuple[str, dict]:
        """Block until a message arrives. Returns (sender_peer_id, msg)."""
        ...


class NixlTransport(ABC):
    """Initiate and poll one-sided NIXL transfers."""

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
    PENDING   = auto()   # registered, transfer not yet started
    TRANSFER  = auto()   # NIXL transfer in progress
    DONE      = auto()
    ABORTED   = auto()


@dataclass
class _SaveJob:
    job_id: str
    block_descs: list[BlockDesc]
    state: _JobState = _JobState.PENDING


@dataclass
class _LoadJob:
    job_id: str
    block_hashes: list[int]
    peer_id: str
    local_block_descs: list[BlockDesc] = field(default_factory=list)
    state: _JobState = _JobState.PENDING


# ---------------------------------------------------------------------------
# PDConnector
# ---------------------------------------------------------------------------

class PDConnector(SecondaryPillar):

    def __init__(
        self,
        peer_id: str,
        ctrl: CtrlTransport,
        nixl: NixlTransport,
    ) -> None:
        self._peer_id  = peer_id
        self._ctrl     = ctrl
        self._nixl     = nixl

        self._lock           = threading.Lock()
        self._save_jobs:  dict[str, _SaveJob] = {}
        self._load_jobs:  dict[str, _LoadJob] = {}
        # Maps nixl transfer_id → job_id (for load jobs)
        self._transfer_to_job: dict[str, str] = {}

        self._listener = threading.Thread(target=self._listen, daemon=True)
        self._listener.start()

    # ------------------------------------------------------------------
    # SecondaryPillar interface
    # ------------------------------------------------------------------

    def save(self, job_id: str, block_descs: list[BlockDesc]) -> None:
        """Prefiller: register blocks as ready to serve lookup_fetch."""
        with self._lock:
            self._save_jobs[job_id] = _SaveJob(job_id, block_descs)

    def load(self, job_id: str, block_hashes: list[int], peer_id: str) -> None:
        """Decoder: send lookup_fetch to Prefiller and await NIXL transfer."""
        with self._lock:
            job = _LoadJob(job_id, block_hashes, peer_id)
            self._load_jobs[job_id] = job

        self._ctrl.send(peer_id, {
            "type":         "lookup_fetch",
            "job_id":       job_id,
            "block_hashes": block_hashes,
            "peer_id":      self._peer_id,
        })

    def get_finished(self, job_ids: list[str]) -> list[str]:
        """Return job_ids whose transfer has completed."""
        # Drain any newly finished NIXL transfers
        with self._lock:
            transfer_ids = list(self._transfer_to_job.keys())

        finished_transfers = self._nixl.get_finished(transfer_ids)

        with self._lock:
            for tid in finished_transfers:
                jid = self._transfer_to_job.pop(tid, None)
                if jid and jid in self._load_jobs:
                    self._load_jobs[jid].state = _JobState.DONE
                if jid and jid in self._save_jobs:
                    self._save_jobs[jid].state = _JobState.DONE

            return [
                jid for jid in job_ids
                if self._is_done(jid)
            ]

    def abort(self, job_id: str) -> None:
        with self._lock:
            if job_id in self._load_jobs:
                self._load_jobs[job_id].state = _JobState.ABORTED
            if job_id in self._save_jobs:
                self._save_jobs[job_id].state = _JobState.ABORTED
            # Cancel in-flight transfer if any
            for tid, jid in list(self._transfer_to_job.items()):
                if jid == job_id:
                    self._nixl.cancel(tid)
                    del self._transfer_to_job[tid]

    # ------------------------------------------------------------------
    # Listener — runs on Prefiller side
    # ------------------------------------------------------------------

    def _listen(self) -> None:
        while True:
            sender_id, msg = self._ctrl.recv()
            if msg.get("type") == "lookup_fetch":
                self._handle_lookup_fetch(sender_id, msg)

    def _handle_lookup_fetch(self, decoder_peer_id: str, msg: dict) -> None:
        job_id      = msg["job_id"]
        block_hashes = msg["block_hashes"]

        with self._lock:
            save_job = self._save_jobs.get(job_id)
            if save_job is None or save_job.state == _JobState.ABORTED:
                # TODO: send NACK back to decoder
                return

            local_descs  = save_job.block_descs
            remote_descs = [
                BlockDesc(block_hash=h, addr=0, size=0)  # filled by NIXL reg
                for h in block_hashes
            ]
            save_job.state = _JobState.TRANSFER
            transfer_id = f"{job_id}:nixl"
            self._transfer_to_job[transfer_id] = job_id

        self._nixl.write(transfer_id, local_descs, remote_descs, decoder_peer_id)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _is_done(self, job_id: str) -> bool:
        job = self._load_jobs.get(job_id) or self._save_jobs.get(job_id)
        return job is not None and job.state == _JobState.DONE
