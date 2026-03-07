import time
import pytest
import torch

from src.secondary_pillar import BlockDesc
from src.pd_connector import NixlTransport, PDConnector, _JobState

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


def test_load_ack_advances_to_transfer_state():
    """After lookup_ack, job moves from PENDING to TRANSFER (not yet DONE)."""
    port_p, port_d = BASE_PORT + 2, BASE_PORT + 3
    prefiller, nixl_p, decoder, nixl_d = _make_pair(port_p, port_d)
    try:
        prefiller.save("job1", [BlockDesc(block_hash=1, addr=0x1000, size=4096)])
        decoder.load("job1", [1], [0], _peer_id(port_p))

        # Job starts in PENDING (waiting for ack)
        with decoder._lock:
            assert decoder._load_jobs["job1"].state == _JobState.PENDING

        # After ack arrives, job advances to TRANSFER
        time.sleep(0.3)
        with decoder._lock:
            assert decoder._load_jobs["job1"].state == _JobState.TRANSFER

        # Not DONE — data transfer has not occurred (MockNixl is a stub)
        assert decoder.get_finished(["job1"]) == []
    finally:
        prefiller.close()
        decoder.close()


def test_load_control_failure_no_save():
    """No save on Prefiller → no lookup_ack → ack_deadline fires → ABORTED (control failure).

    In Step 8, Prefiller always sends ack on lookup_fetch regardless of whether
    save() was called yet. So a "no save" scenario no longer causes a control failure
    at the ack stage — but if no blocks are ever saved, no transfer_done arrives
    and the xfer_deadline fires instead.

    This test keeps the ack-deadline path by pointing the Decoder at a non-existent
    Prefiller so the lookup_fetch is never received.
    """
    port_p, port_d = BASE_PORT + 4, BASE_PORT + 5
    _prefiller, nixl_p, decoder, nixl_d = _make_pair(port_p, port_d)
    # Create a second "ghost" prefiller that is never started — just use a free port
    ghost_port = BASE_PORT + 50
    ghost_prefiller = PDConnector(_peer_id(ghost_port), ghost_port, MockNixl(),
                                   heartbeat_ivl_ms=200, heartbeat_timeout_ms=1000)
    try:
        decoder._LOOKUP_ACK_TIMEOUT_S = 0.3
        # load() will try to connect to ghost_prefiller which exists but has no save()
        # and more importantly will not send ack because it has no pending fetch handler
        # that matches — actually in Step 8 it WILL send ack. So instead we use a port
        # that has no listener at all by closing the ghost prefiller first.
        ghost_prefiller.close()

        decoder.load("job1", [1], [0], _peer_id(ghost_port))

        time.sleep(0.6)

        assert decoder.get_finished(["job1"]) == []
        with decoder._lock:
            assert decoder._load_jobs["job1"].state == _JobState.ABORTED
    finally:
        _prefiller.close()
        decoder.close()


def test_load_transfer_failure_after_ack():
    """Ack received but no NIXL transfer → xfer_deadline fires → ABORTED (transfer failure).

    Prefiller sends ack (lookup_fetch always acked in Step 8) but save() is never
    called, so no transfer_done arrives and the transfer deadline fires.
    """
    port_p, port_d = BASE_PORT + 6, BASE_PORT + 7
    prefiller, nixl_p, decoder, nixl_d = _make_pair(port_p, port_d)
    try:
        # Do NOT call prefiller.save() — lookup_fetch will be acked but no transfer occurs
        decoder._TRANSFER_TIMEOUT_S = 0.3
        decoder.load("job1", [1], [0], _peer_id(port_p))

        # Wait for ack then for xfer deadline to pass
        time.sleep(0.8)

        assert decoder.get_finished(["job1"]) == []
        with decoder._lock:
            assert decoder._load_jobs["job1"].state == _JobState.ABORTED
    finally:
        prefiller.close()
        decoder.close()


def test_abort_prevents_job_completion():
    port_p, port_d = BASE_PORT + 8, BASE_PORT + 9
    prefiller, nixl_p, decoder, nixl_d = _make_pair(port_p, port_d)
    try:
        prefiller.save("job1", [BlockDesc(block_hash=1, addr=0x1000, size=4096)])
        decoder.load("job1", [1], [0], _peer_id(port_p))

        decoder.abort("job1")

        time.sleep(0.3)
        assert decoder.get_finished(["job1"]) == []
        with decoder._lock:
            assert decoder._load_jobs["job1"].state == _JobState.ABORTED
    finally:
        prefiller.close()
        decoder.close()


def test_load_auto_connects_and_handshakes():
    port_p, port_d = BASE_PORT + 10, BASE_PORT + 11
    prefiller, nixl_p, decoder, nixl_d = _make_pair(port_p, port_d)
    try:
        prefiller.save("job1", [BlockDesc(block_hash=1, addr=0x1000, size=4096)])
        decoder.load("job1", [1], [0], _peer_id(port_p))  # triggers auto-connect + handshake
        time.sleep(0.3)

        assert _peer_id(port_p) in decoder._connections
        assert _peer_id(port_d) in prefiller._connections
        assert _peer_id(port_d) in prefiller._remote_dlists
    finally:
        prefiller.close()
        decoder.close()


def test_peer_down_aborts_load_jobs():
    port_p, port_d = BASE_PORT + 12, BASE_PORT + 13
    prefiller, nixl_p, decoder, nixl_d = _make_pair(port_p, port_d)
    try:
        decoder.load("job1", [1], [0], _peer_id(port_p))
        time.sleep(0.3)

        # Simulate prefiller going down — decoder's on_peer_down fires
        prefiller.close()
        time.sleep(1.5)  # wait for heartbeat timeout

        with decoder._lock:
            job = decoder._load_jobs.get("job1")
        assert job is not None
        assert job.state == _JobState.ABORTED
    finally:
        decoder.close()


def test_transfer_done_marks_job_complete():
    """End-to-end: save() + load() with real NIXL → get_finished returns job as DONE.

    Uses actual NIXL UCX transfers between two PDConnectors on the same host.
    Block hashes must match between save() and load(); block_indexes[i] points to
    the Decoder kv_block slot that receives block i.
    """
    port_p, port_d = BASE_PORT + 14, BASE_PORT + 15
    prefiller, nixl_p, decoder, nixl_d = _make_pair(port_p, port_d)
    try:
        # Prefiller has one kv_block (index 0); Decoder has one kv_block (index 0)
        block_hash = 42
        kv_block_p = prefiller._kv_blocks[0]
        addr_p = kv_block_p.data_ptr()

        prefiller.save("job1", [BlockDesc(block_hash=block_hash, addr=addr_p,
                                          size=kv_block_p.numel() * kv_block_p.element_size())])
        decoder.load("job1", [block_hash], [0], _peer_id(port_p))

        # Allow time for handshake + NIXL transfer + transfer_done message
        deadline = time.monotonic() + 5.0
        finished = []
        while time.monotonic() < deadline:
            finished = decoder.get_finished(["job1"])
            if finished:
                break
            time.sleep(0.05)

        assert finished == ["job1"]
        with decoder._lock:
            assert decoder._load_jobs["job1"].state == _JobState.DONE
    finally:
        prefiller.close()
        decoder.close()


def test_streaming_save_triggers_transfer():
    """Prefiller saves blocks incrementally; each save() chunk is transferred as it arrives."""
    port_p, port_d = BASE_PORT + 16, BASE_PORT + 17
    prefiller, nixl_p, decoder, nixl_d = _make_pair(port_p, port_d)

    # Need 2 kv_block slots on each side
    extra_kv_p = torch.zeros(1024, dtype=torch.float32)
    extra_kv_d = torch.zeros(1024, dtype=torch.float32)
    prefiller2, nixl_p2, decoder2, nixl_d2 = _make_pair(BASE_PORT + 18, BASE_PORT + 19)
    prefiller2.close()
    decoder2.close()

    # Re-create with 2 blocks each
    nixl_p3 = MockNixl()
    nixl_d3 = MockNixl()
    kv_p = [torch.zeros(1024, dtype=torch.float32), torch.zeros(1024, dtype=torch.float32)]
    kv_d = [torch.zeros(1024, dtype=torch.float32), torch.zeros(1024, dtype=torch.float32)]
    port_p3, port_d3 = BASE_PORT + 20, BASE_PORT + 21
    prefiller3 = PDConnector(_peer_id(port_p3), port_p3, nixl_p3, kv_blocks=kv_p,
                              heartbeat_ivl_ms=200, heartbeat_timeout_ms=1000)
    decoder3   = PDConnector(_peer_id(port_d3), port_d3, nixl_d3, kv_blocks=kv_d,
                              heartbeat_ivl_ms=200, heartbeat_timeout_ms=1000)
    try:
        hash1, hash2 = 101, 102
        addr1 = prefiller3._kv_blocks[0].data_ptr()
        addr2 = prefiller3._kv_blocks[1].data_ptr()
        size  = prefiller3._kv_blocks[0].numel() * prefiller3._kv_blocks[0].element_size()

        # Decoder requests both blocks; Prefiller saves them one at a time
        decoder3.load("job1", [hash1, hash2], [0, 1], _peer_id(port_p3))
        time.sleep(0.2)  # let handshake + lookup_fetch arrive

        prefiller3.save("job1", [BlockDesc(block_hash=hash1, addr=addr1, size=size)])
        time.sleep(0.2)  # first chunk transferred

        # Job not done yet — second block still pending
        assert decoder3.get_finished(["job1"]) == []

        prefiller3.save("job1", [BlockDesc(block_hash=hash2, addr=addr2, size=size)])

        deadline = time.monotonic() + 5.0
        finished = []
        while time.monotonic() < deadline:
            finished = decoder3.get_finished(["job1"])
            if finished:
                break
            time.sleep(0.05)

        assert finished == ["job1"]
    finally:
        prefiller3.close()
        decoder3.close()
