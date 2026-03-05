import threading
import time
import pytest

from src.secondary_pillar import BlockDesc
from src.pd_connector import (
    CtrlTransport,
    NixlTransport,
    PDConnector,
)


# ---------------------------------------------------------------------------
# Mock transports
# ---------------------------------------------------------------------------

class MockCtrl(CtrlTransport):
    def __init__(self):
        self._inbox: list[tuple[str, dict]] = []
        self._sent:  list[tuple[str, dict]] = []
        self._cond = threading.Condition()

    def send(self, peer_id: str, msg: dict) -> None:
        self._sent.append((peer_id, msg))

    def deliver(self, sender_id: str, msg: dict) -> None:
        """Inject a message as if received from a remote peer."""
        with self._cond:
            self._inbox.append((sender_id, msg))
            self._cond.notify()

    def recv(self) -> tuple[str, dict]:
        with self._cond:
            while not self._inbox:
                self._cond.wait()
            return self._inbox.pop(0)


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


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_save_registers_job():
    ctrl, nixl = MockCtrl(), MockNixl()
    connector = PDConnector("prefiller-1", ctrl, nixl)

    descs = [BlockDesc(block_hash=1, addr=0x1000, size=4096)]
    connector.save("job1", descs)

    assert "job1" in connector._save_jobs


def test_load_sends_lookup_fetch():
    ctrl, nixl = MockCtrl(), MockNixl()
    connector = PDConnector("decoder-1", ctrl, nixl)

    connector.load("job1", [1, 2, 3], "prefiller-1")

    assert len(ctrl._sent) == 1
    peer, msg = ctrl._sent[0]
    assert peer == "prefiller-1"
    assert msg["type"] == "lookup_fetch"
    assert msg["job_id"] == "job1"
    assert msg["block_hashes"] == [1, 2, 3]


def test_lookup_fetch_triggers_nixl_write():
    ctrl, nixl = MockCtrl(), MockNixl()
    connector = PDConnector("prefiller-1", ctrl, nixl)

    descs = [BlockDesc(block_hash=1, addr=0x1000, size=4096)]
    connector.save("job1", descs)

    # Simulate decoder sending lookup_fetch
    ctrl.deliver("decoder-1", {
        "type":         "lookup_fetch",
        "job_id":       "job1",
        "block_hashes": [1],
        "peer_id":      "decoder-1",
    })

    # Give listener thread time to process
    time.sleep(0.05)

    assert len(nixl.writes) == 1
    transfer_id, local, remote, peer = nixl.writes[0]
    assert peer == "decoder-1"
    assert local == descs


def test_get_finished_returns_completed_jobs():
    ctrl, nixl = MockCtrl(), MockNixl()
    connector = PDConnector("prefiller-1", ctrl, nixl)

    descs = [BlockDesc(block_hash=1, addr=0x1000, size=4096)]
    connector.save("job1", descs)

    ctrl.deliver("decoder-1", {
        "type":         "lookup_fetch",
        "job_id":       "job1",
        "block_hashes": [1],
        "peer_id":      "decoder-1",
    })
    time.sleep(0.05)

    # Not finished yet
    assert connector.get_finished(["job1"]) == []

    nixl.mark_finished("job1:nixl")
    assert connector.get_finished(["job1"]) == ["job1"]


def test_abort_cancels_transfer():
    ctrl, nixl = MockCtrl(), MockNixl()
    connector = PDConnector("prefiller-1", ctrl, nixl)

    descs = [BlockDesc(block_hash=1, addr=0x1000, size=4096)]
    connector.save("job1", descs)

    ctrl.deliver("decoder-1", {
        "type":         "lookup_fetch",
        "job_id":       "job1",
        "block_hashes": [1],
        "peer_id":      "decoder-1",
    })
    time.sleep(0.05)

    connector.abort("job1")

    assert "job1:nixl" in nixl.cancelled
    assert connector.get_finished(["job1"]) == []
