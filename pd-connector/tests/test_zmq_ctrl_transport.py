import time
import pytest

from src.zmq_ctrl_transport import ZmqCtrlTransport

BASE_PORT = 15200


def _make_pair(port_a: int, port_b: int, heartbeat_ivl_ms=200, heartbeat_timeout_ms=1000):
    a = ZmqCtrlTransport("node-a", port_a,
                          heartbeat_ivl_ms=heartbeat_ivl_ms,
                          heartbeat_timeout_ms=heartbeat_timeout_ms)
    b = ZmqCtrlTransport("node-b", port_b,
                          heartbeat_ivl_ms=heartbeat_ivl_ms,
                          heartbeat_timeout_ms=heartbeat_timeout_ms)
    a.connect("node-b", "127.0.0.1", port_b)
    b.connect("node-a", "127.0.0.1", port_a)
    time.sleep(0.1)
    return a, b


def test_send_recv():
    a, b = _make_pair(BASE_PORT, BASE_PORT + 1)
    try:
        a.send("node-b", {"hello": "world"})
        sender, msg = b.recv()
        assert sender == "node-a"
        assert msg == {"hello": "world"}
    finally:
        a.close()
        b.close()


def test_bidirectional():
    a, b = _make_pair(BASE_PORT + 2, BASE_PORT + 3)
    try:
        a.send("node-b", {"from": "a"})
        b.send("node-a", {"from": "b"})
        s1, m1 = b.recv()
        s2, m2 = a.recv()
        assert s1 == "node-a" and m1 == {"from": "a"}
        assert s2 == "node-b" and m2 == {"from": "b"}
    finally:
        a.close()
        b.close()


def test_graceful_disconnect_fires_callback():
    a, b = _make_pair(BASE_PORT + 4, BASE_PORT + 5)
    downed = []
    b.set_peer_down_callback(lambda pid: downed.append(pid))
    try:
        a.disconnect("node-b")
        time.sleep(0.3)
        assert "node-a" in downed
    finally:
        a.close()
        b.close()


def test_abrupt_disconnect_fires_callback():
    """Closing a peer abruptly triggers EVENT_DISCONNECTED via ZMTP heartbeat."""
    a = ZmqCtrlTransport("node-a", BASE_PORT + 6,
                          heartbeat_ivl_ms=100, heartbeat_timeout_ms=400)
    b = ZmqCtrlTransport("node-b", BASE_PORT + 7,
                          heartbeat_ivl_ms=100, heartbeat_timeout_ms=400)
    a.connect("node-b", "127.0.0.1", BASE_PORT + 7)
    b.connect("node-a", "127.0.0.1", BASE_PORT + 6)
    time.sleep(0.1)

    downed = []
    b.set_peer_down_callback(lambda pid: downed.append(pid))

    # Close a abruptly — no disconnect message sent
    a.close()

    # ZMTP heartbeat detects dead connection within timeout + margin
    time.sleep(1.2)
    assert "node-a" in downed
    b.close()


def test_multiple_peers_independent():
    listener = ZmqCtrlTransport("server", BASE_PORT + 8)
    c1 = ZmqCtrlTransport("client-1", BASE_PORT + 9)
    c2 = ZmqCtrlTransport("client-2", BASE_PORT + 10)

    c1.connect("server", "127.0.0.1", BASE_PORT + 8)
    c2.connect("server", "127.0.0.1", BASE_PORT + 8)
    listener.connect("client-1", "127.0.0.1", BASE_PORT + 9)
    listener.connect("client-2", "127.0.0.1", BASE_PORT + 10)
    time.sleep(0.1)

    try:
        c1.send("server", {"msg": "from-c1"})
        c2.send("server", {"msg": "from-c2"})

        received = {}
        for _ in range(2):
            sender, msg = listener.recv()
            received[sender] = msg

        assert received["client-1"] == {"msg": "from-c1"}
        assert received["client-2"] == {"msg": "from-c2"}
    finally:
        c1.close()
        c2.close()
        listener.close()
