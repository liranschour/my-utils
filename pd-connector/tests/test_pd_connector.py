import time
import pytest

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


def _make_pair(port_p: int, port_d: int):
    """Create a connected Prefiller / Decoder PDConnector pair."""
    nixl_p = MockNixl()
    nixl_d = MockNixl()
    prefiller = PDConnector("prefiller", port_p, nixl_p,
                             heartbeat_ivl_ms=200, heartbeat_timeout_ms=1000)
    decoder   = PDConnector("decoder",   port_d, nixl_d,
                             heartbeat_ivl_ms=200, heartbeat_timeout_ms=1000)
    prefiller.connect("decoder",   "127.0.0.1", port_d)
    decoder.connect("prefiller", "127.0.0.1", port_p)
    time.sleep(0.1)
    return prefiller, nixl_p, decoder, nixl_d


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
    prefiller, nixl_p, decoder, nixl_d = _make_pair(BASE_PORT + 2, BASE_PORT + 3)
    try:
        descs = [BlockDesc(block_hash=1, addr=0x1000, size=4096)]
        prefiller.save("job1", descs)

        decoder.load("job1", [1], "prefiller")

        # Give listener time to process lookup_fetch
        time.sleep(0.15)

        assert len(nixl_p.writes) == 1
        transfer_id, local, remote, peer = nixl_p.writes[0]
        assert peer == "decoder"
        assert local == descs
    finally:
        prefiller.close()
        decoder.close()


def test_get_finished_returns_completed_jobs():
    prefiller, nixl_p, decoder, nixl_d = _make_pair(BASE_PORT + 4, BASE_PORT + 5)
    try:
        descs = [BlockDesc(block_hash=1, addr=0x1000, size=4096)]
        prefiller.save("job1", descs)
        decoder.load("job1", [1], "prefiller")
        time.sleep(0.15)

        assert prefiller.get_finished(["job1"]) == []

        nixl_p.mark_finished("job1:nixl")
        assert prefiller.get_finished(["job1"]) == ["job1"]
    finally:
        prefiller.close()
        decoder.close()


def test_abort_cancels_transfer():
    prefiller, nixl_p, decoder, nixl_d = _make_pair(BASE_PORT + 6, BASE_PORT + 7)
    try:
        descs = [BlockDesc(block_hash=1, addr=0x1000, size=4096)]
        prefiller.save("job1", descs)
        decoder.load("job1", [1], "prefiller")
        time.sleep(0.15)

        prefiller.abort("job1")

        assert "job1:nixl" in nixl_p.cancelled
        assert prefiller.get_finished(["job1"]) == []
    finally:
        prefiller.close()
        decoder.close()


def test_peer_down_aborts_load_jobs():
    prefiller, nixl_p, decoder, nixl_d = _make_pair(BASE_PORT + 8, BASE_PORT + 9)
    try:
        decoder.load("job1", [1], "prefiller")
        time.sleep(0.1)

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
