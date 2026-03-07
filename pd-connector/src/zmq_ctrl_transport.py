"""
ZmqCtrlTransport — ZMQ-backed CtrlTransport.

Topology:
  Listener:  ROUTER socket (one per node), accepts N DEALER peers.
  Connector: DEALER socket (one per remote peer), connects to remote ROUTER.

Peer liveness:
  Uses ZMQ built-in ZMTP heartbeat (HEARTBEAT_IVL / HEARTBEAT_TIMEOUT) on
  every socket — no application-level ping thread needed. When ZMQ closes a
  dead connection it fires EVENT_DISCONNECTED on the DEALER's monitor socket.
  A single monitor thread polls all DEALER monitor sockets and calls
  on_peer_down(peer_id) accordingly.

  Graceful disconnect: a _MSG_DISCONNECT application message triggers
  on_peer_down immediately without waiting for the heartbeat timeout.
"""

import queue
import threading
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass

import msgspec
import zmq
import zmq.utils.monitor

_MSG_DISCONNECT = "disconnect"
_MSG_DATA       = "data"


class CtrlTransport(ABC):
    """Send/receive control messages between PD peers."""

    @abstractmethod
    def send(self, peer_id: str, msg: dict) -> None: ...

    @abstractmethod
    def recv(self) -> tuple[str, dict]:
        """Block until a message arrives. Returns (sender_peer_id, msg)."""
        ...

    def set_peer_down_callback(self, cb: Callable[[str], None]) -> None:
        """Register a callback invoked with peer_id when a peer goes down."""
        ...


@dataclass
class _DealerEntry:
    sock:    zmq.Socket
    monitor: zmq.Socket  # PAIR socket reading from sock's monitor


class ZmqCtrlTransport(CtrlTransport):

    def __init__(
        self,
        peer_id: str,
        listen_port: int,
        heartbeat_ivl_ms:     int = 2000,
        heartbeat_timeout_ms: int = 10000,
    ) -> None:
        self._peer_id             = peer_id
        self._listen_port         = listen_port
        self._heartbeat_ivl_ms     = heartbeat_ivl_ms
        self._heartbeat_timeout_ms = heartbeat_timeout_ms
        self._peer_down_cb: Callable[[str], None] | None = None

        self._recv_queue: queue.Queue[tuple[str, dict]] = queue.Queue()
        self._stop_event  = threading.Event()
        self._lock        = threading.Lock()

        # peer_id → DealerEntry
        self._dealers: dict[str, _DealerEntry] = {}

        self._ctx = zmq.Context()

        # ROUTER socket — receives from all peers
        self._router: zmq.Socket = self._ctx.socket(zmq.ROUTER)
        self._router.setsockopt(zmq.LINGER, 0)
        self._router.setsockopt(zmq.HEARTBEAT_IVL,     heartbeat_ivl_ms)
        self._router.setsockopt(zmq.HEARTBEAT_TIMEOUT, heartbeat_timeout_ms)
        self._router.setsockopt(zmq.HEARTBEAT_TTL,     heartbeat_timeout_ms)
        self._router.bind(f"tcp://*:{listen_port}")

        self._encoder = msgspec.msgpack.Encoder()
        self._decoder = msgspec.msgpack.Decoder(dict)

        self._ready = threading.Event()
        self._listener_t = threading.Thread(
            target=self._listener_loop, daemon=True, name="zmq-ctrl-listener")
        self._monitor_t = threading.Thread(
            target=self._monitor_loop, daemon=True, name="zmq-ctrl-monitor")

        self._listener_t.start()
        self._monitor_t.start()
        self._ready.wait()

    # ------------------------------------------------------------------
    # CtrlTransport interface
    # ------------------------------------------------------------------

    def set_peer_down_callback(self, cb: Callable[[str], None]) -> None:
        self._peer_down_cb = cb

    def send(self, peer_id: str, msg: dict) -> None:
        with self._lock:
            entry = self._dealers.get(peer_id)
        if entry is None:
            raise RuntimeError(f"Not connected to peer {peer_id}")
        payload = self._encoder.encode({
            "type": _MSG_DATA, "src": self._peer_id, "body": msg})
        entry.sock.send(payload)

    def recv(self) -> tuple[str, dict]:
        return self._recv_queue.get()

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def connect(self, peer_id: str, host: str, port: int) -> None:
        """Connect a DEALER to a remote peer's ROUTER, with monitor."""
        with self._lock:
            if peer_id in self._dealers:
                return

            dealer = self._ctx.socket(zmq.DEALER)
            dealer.setsockopt(zmq.LINGER, 0)
            dealer.setsockopt_string(zmq.IDENTITY, self._peer_id)
            dealer.setsockopt(zmq.HEARTBEAT_IVL,     self._heartbeat_ivl_ms)
            dealer.setsockopt(zmq.HEARTBEAT_TIMEOUT, self._heartbeat_timeout_ms)
            dealer.setsockopt(zmq.HEARTBEAT_TTL,     self._heartbeat_timeout_ms)

            # Attach monitor before connecting so no events are missed
            monitor_addr = f"inproc://monitor-{self._peer_id}-{peer_id}"
            dealer.monitor(monitor_addr, zmq.EVENT_DISCONNECTED)
            mon = self._ctx.socket(zmq.PAIR)
            mon.setsockopt(zmq.LINGER, 0)
            mon.connect(monitor_addr)

            dealer.connect(f"tcp://{host}:{port}")
            self._dealers[peer_id] = _DealerEntry(sock=dealer, monitor=mon)

    def disconnect(self, peer_id: str) -> None:
        """Send graceful disconnect then fire on_peer_down immediately."""
        with self._lock:
            entry = self._dealers.get(peer_id)
        if entry:
            payload = self._encoder.encode({
                "type": _MSG_DISCONNECT, "src": self._peer_id})
            entry.sock.send(payload)
            with self._lock:
                self._dealers.pop(peer_id, None)
        self._fire_peer_down(peer_id)

    def close(self) -> None:
        self._stop_event.set()
        # Threads poll with 200ms timeout so they exit within ~200ms
        self._listener_t.join(timeout=2.0)
        self._monitor_t.join(timeout=2.0)
        self._router.setsockopt(zmq.LINGER, 0)
        self._router.close()
        with self._lock:
            for entry in self._dealers.values():
                entry.sock.setsockopt(zmq.LINGER, 0)
                entry.sock.disable_monitor()
                entry.sock.close()
                entry.monitor.setsockopt(zmq.LINGER, 0)
                entry.monitor.close()
            self._dealers.clear()
        self._ctx.term()

    # ------------------------------------------------------------------
    # Background threads
    # ------------------------------------------------------------------

    def _listener_loop(self) -> None:
        self._router.setsockopt(zmq.RCVTIMEO, 200)
        self._ready.set()
        while not self._stop_event.is_set():
            try:
                parts = self._router.recv_multipart()
            except zmq.Again:
                continue
            except zmq.ZMQError:
                break

            # DEALER→ROUTER: [identity, data]
            # REQ→ROUTER:    [identity, empty, data]
            if len(parts) == 2:
                identity, raw = parts
            elif len(parts) == 3:
                identity, _, raw = parts
            else:
                continue

            try:
                msg = self._decoder.decode(raw)
            except (msgspec.DecodeError, msgspec.ValidationError):
                continue

            src   = msg.get("src", identity.decode(errors="replace"))
            mtype = msg.get("type")

            if mtype == _MSG_DISCONNECT:
                self._fire_peer_down(src)
            elif mtype == _MSG_DATA:
                self._recv_queue.put((src, msg.get("body", {})))

    def _monitor_loop(self) -> None:
        """Poll all DEALER monitor sockets for EVENT_DISCONNECTED."""
        poller  = zmq.Poller()
        tracked: dict[zmq.Socket, str] = {}  # monitor_sock → peer_id

        while not self._stop_event.is_set():
            # Sync poller with the current dealer set
            with self._lock:
                current = {e.monitor: pid for pid, e in self._dealers.items()}

            for mon, pid in current.items():
                if mon not in tracked:
                    poller.register(mon, zmq.POLLIN)
                    tracked[mon] = pid

            for mon in list(tracked):
                if mon not in current:
                    poller.unregister(mon)
                    del tracked[mon]

            try:
                events = dict(poller.poll(timeout=200))
            except zmq.ZMQError:
                break

            for mon, mask in events.items():
                if mask & zmq.POLLIN:
                    evt = zmq.utils.monitor.recv_monitor_message(mon)
                    if evt["event"] == zmq.EVENT_DISCONNECTED:
                        pid = tracked.get(mon)
                        if pid:
                            self._fire_peer_down(pid)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _fire_peer_down(self, peer_id: str) -> None:
        if self._peer_down_cb:
            self._peer_down_cb(peer_id)
