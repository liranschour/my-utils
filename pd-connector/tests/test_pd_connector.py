import time
import pytest
import torch

from src.secondary_pillar import BlockDesc
from src.pd_connector import NixlTransport, PDConnector

BASE_PORT = 15300


class MockNixl(NixlTransport):
    def __init__(self):
        self.writes: list = []
        self._finished: set[str] = set()
        self.cancelled: list[str] = []

    def write(self, transfer_id, local_block_descs, remote_block_descs, peer_id):
        self.writes.append((transfer_id, local_block_descs, remote_block_descs, peer_id))

    def get_finished(self, transfer_ids: list[str]) -> list[str]:
        return [t for t in transfer_ids if t in self._finished]

    def cancel(self, transfer_id: str) -> None:
        self.cancelled.append(transfer_id)

    def mark_finished(self, transfer_id: str) -> None:
        self._finished.add(transfer_id)


def _peer_id(port: int) -> str:
    return f"127.0.0.1:{port}"


def _make_pair(port_p: int, port_d: int):
    """Create a Prefiller / Decoder PDConnector pair. Connections are established on demand via load()."""
    nixl_p = MockNixl()
    nixl_d = MockNixl()
    kv_p = [torch.zeros(1024, dtype=torch.float32)]
    kv_d = [torch.zeros(1024, dtype=torch.float32)]
    prefiller = PDConnector(_peer_id(port_p), port_p, nixl_p, kv_blocks=kv_p,
                             heartbeat_ivl_ms=200, heartbeat_timeout_ms=1000)
    decoder   = PDConnector(_peer_id(port_d), port_d, nixl_d, kv_blocks=kv_d,
                             heartbeat_ivl_ms=200, heartbeat_timeout_ms=1000)
    return prefiller, nixl_p, decoder, nixl_d


def test_kv_blocks_stored_at_init():
    blocks = [torch.zeros(128, dtype=torch.float16) for _ in range(4)]
    nixl = MockNixl()
    connector = PDConnector("node0", BASE_PORT - 2, nixl, kv_blocks=blocks)
    try:
        assert connector._kv_blocks is blocks
        assert len(connector._kv_blocks) == 4
    finally:
        connector.close()


def test_kv_blocks_defaults_to_empty():
    nixl = MockNixl()
    connector = PDConnector("node0", BASE_PORT - 1, nixl)
    try:
        assert connector._kv_blocks == []
    finally:
        connector.close()


def test_nixl_registration_at_init():
    blocks = [torch.zeros(1024, dtype=torch.float32) for _ in range(2)]
    nixl = MockNixl()
    connector = PDConnector("regnode", BASE_PORT - 4, nixl, kv_blocks=blocks)
    try:
        assert connector._reg is not None
        assert connector._local_dlist is not None
    finally:
        connector.close()


def test_nixl_registration_skipped_when_no_blocks():
    nixl = MockNixl()
    connector = PDConnector("regnode2", BASE_PORT - 3, nixl)
    try:
        assert connector._reg is None
        assert connector._local_dlist is None
    finally:
        connector.close()


def test_save_registers_job():
    prefiller, nixl_p, decoder, nixl_d = _make_pair(BASE_PORT, BASE_PORT + 1)
    try:
        descs = [BlockDesc(block_hash=1, addr=0x1000, size=4096)]
        prefiller.save("job1", descs)
        assert "job1" in prefiller._save_jobs
    finally:
        prefiller.close()
        decoder.close()


def test_load_sends_lookup_fetch_and_triggers_nixl_write():
    port_p, port_d = BASE_PORT + 2, BASE_PORT + 3
    prefiller, nixl_p, decoder, nixl_d = _make_pair(port_p, port_d)
    try:
        descs = [BlockDesc(block_hash=1, addr=0x1000, size=4096)]
        prefiller.save("job1", descs)

        decoder.load("job1", [1], _peer_id(port_p))

        # Give listener time to process handshake + lookup_fetch
        time.sleep(0.3)

        assert len(nixl_p.writes) == 1
        transfer_id, local, remote, peer = nixl_p.writes[0]
        assert peer == _peer_id(port_d)
        assert local == descs
    finally:
        prefiller.close()
        decoder.close()


def test_get_finished_returns_completed_jobs():
    port_p, port_d = BASE_PORT + 4, BASE_PORT + 5
    prefiller, nixl_p, decoder, nixl_d = _make_pair(port_p, port_d)
    try:
        descs = [BlockDesc(block_hash=1, addr=0x1000, size=4096)]
        prefiller.save("job1", descs)
        decoder.load("job1", [1], _peer_id(port_p))
        time.sleep(0.3)

        assert prefiller.get_finished(["job1"]) == []

        nixl_p.mark_finished("job1:nixl")
        assert prefiller.get_finished(["job1"]) == ["job1"]
    finally:
        prefiller.close()
        decoder.close()


def test_abort_cancels_transfer():
    port_p, port_d = BASE_PORT + 6, BASE_PORT + 7
    prefiller, nixl_p, decoder, nixl_d = _make_pair(port_p, port_d)
    try:
        descs = [BlockDesc(block_hash=1, addr=0x1000, size=4096)]
        prefiller.save("job1", descs)
        decoder.load("job1", [1], _peer_id(port_p))
        time.sleep(0.3)

        prefiller.abort("job1")

        assert "job1:nixl" in nixl_p.cancelled
        assert prefiller.get_finished(["job1"]) == []
    finally:
        prefiller.close()
        decoder.close()


def test_load_auto_connects_and_handshakes():
    port_p, port_d = BASE_PORT + 10, BASE_PORT + 11
    prefiller, nixl_p, decoder, nixl_d = _make_pair(port_p, port_d)
    try:
        prefiller.save("job1", [BlockDesc(block_hash=1, addr=0x1000, size=4096)])
        decoder.load("job1", [1], _peer_id(port_p))  # triggers auto-connect + handshake
        time.sleep(0.3)

        assert _peer_id(port_p) in decoder._connections
        assert _peer_id(port_d) in prefiller._connections
        assert _peer_id(port_d) in prefiller._remote_dlists
    finally:
        prefiller.close()
        decoder.close()


def test_peer_down_aborts_load_jobs():
    port_p, port_d = BASE_PORT + 8, BASE_PORT + 9
    prefiller, nixl_p, decoder, nixl_d = _make_pair(port_p, port_d)
    try:
        decoder.load("job1", [1], _peer_id(port_p))
        time.sleep(0.3)

        # Simulate prefiller going down — decoder's on_peer_down fires
        prefiller.close()
        time.sleep(1.5)  # wait for heartbeat timeout

        with decoder._lock:
            job = decoder._load_jobs.get("job1")
        assert job is not None
        from src.pd_connector import _JobState
        assert job.state == _JobState.ABORTED
    finally:
        decoder.close()
