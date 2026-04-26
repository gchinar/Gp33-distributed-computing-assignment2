"""
dme.py
======
Distributed Mutual Exclusion middleware — Ricart & Agrawala (1981).

This module is INDEPENDENT of the chat application.  The chat client
imports it and calls:

        dme = RicartAgrawala(my_id, my_host, my_port, peers)
        dme.start()
        ...
        dme.acquire()       # blocks until this node holds the CS
        # << critical section: e.g. RPC `post` to file server >>
        dme.release()

Algorithm summary
-----------------
Each node maintains a Lamport logical clock and three pieces of state:

    state           : RELEASED | WANTED | HELD
    request_ts      : timestamp of *its own* outstanding request
    deferred[]      : peers whose REQUEST it received but has not yet
                      replied to (because it is HELD or has higher
                      priority WANTED)

To enter the CS:
    1. state <- WANTED
    2. clock += 1; request_ts <- (clock, my_id)
    3. broadcast REQUEST(request_ts) to all N-1 peers
    4. wait until N-1 REPLY messages have been collected
    5. state <- HELD; enter CS

On receiving REQUEST(ts_j, j) from peer j while in state s:
    * update clock = max(clock, ts_j) + 1
    * if s == HELD                                 -> defer REPLY
    * elif s == WANTED and (request_ts, my_id) < (ts_j, j)
                                                   -> defer REPLY
    * else                                          -> send REPLY immediately

To leave the CS:
    1. state <- RELEASED
    2. send REPLY to every peer in deferred[]; clear deferred[]

Ordering of timestamps is the standard Lamport tie-break:
    (t_a, id_a) < (t_b, id_b)  iff  t_a < t_b  or  (t_a == t_b and id_a < id_b)

Properties
----------
* Safety  : at most one node in CS at a time.
* Liveness: every request is eventually granted (no starvation), since
            every later request has a strictly larger (ts, id) pair.
* Messages per CS entry: 2(N-1)   — N-1 REQUESTs + N-1 REPLYs.
* No central coordinator, no token, no single point of failure for
  the mutual-exclusion logic itself.

Transport
---------
We use XML-RPC for peer-to-peer messages too — same library, same
deployment story as the file server.  Each node runs its own little
RPC server in a background thread that exposes `on_request` and
`on_reply`.
"""

import logging
import threading
import time
import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer
from socketserver import ThreadingMixIn

# ---------- logging ------------------------------------------------------- #

log = logging.getLogger("dme")

# ---------- constants ----------------------------------------------------- #

RELEASED = "RELEASED"
WANTED   = "WANTED"
HELD     = "HELD"

# ---------- threaded RPC server ------------------------------------------- #

class _ThreadedRPC(ThreadingMixIn, SimpleXMLRPCServer):
    daemon_threads = True
    allow_reuse_address = True


# ---------- main class ---------------------------------------------------- #

class RicartAgrawala:
    """
    Parameters
    ----------
    my_id   : str          Unique node id, e.g. "A" or "B".  Used as the
                           Lamport tie-breaker.
    my_host : str          Host/IP this node binds its DME RPC server on.
    my_port : int          Port for incoming DME messages.
    peers   : dict[str, tuple[str, int]]
                           id -> (host, port) of every OTHER participant
                           in the DME group.  Must be the same on all
                           nodes (modulo each node omitting itself).
    """

    def __init__(self, my_id, my_host, my_port, peers):
        self.my_id   = my_id
        self.my_host = my_host
        self.my_port = my_port
        self.peers   = dict(peers)             # {peer_id: (host, port)}

        # --- algorithm state ------------------------------------------- #
        self._clock        = 0                 # Lamport logical clock
        self._state        = RELEASED
        self._request_ts   = None              # (clock, id) of my request
        self._deferred     = set()             # peer ids whose REPLY we owe
        self._replies      = set()             # peer ids who've REPLY'd to me

        # one CV guards the whole state machine
        self._cv = threading.Condition()

        # Serialises acquire() calls *within this node*.  Ricart-Agrawala
        # assumes one outstanding request per node; if the application
        # has multiple threads, this lock makes them queue locally
        # before they enter the distributed protocol.
        self._local_acquire_lock = threading.Lock()

        # --- transport ------------------------------------------------- #
        self._rpc_srv = None
        self._rpc_thread = None

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    def start(self) -> None:
        """Bring the DME RPC endpoint up.  Non-blocking."""
        self._rpc_srv = _ThreadedRPC((self.my_host, self.my_port),
                                     allow_none=True,
                                     logRequests=False)
        self._rpc_srv.register_function(self._on_request, "on_request")
        self._rpc_srv.register_function(self._on_reply,   "on_reply")
        self._rpc_srv.register_function(self._ping,       "ping")

        self._rpc_thread = threading.Thread(
            target=self._rpc_srv.serve_forever,
            name="dme-rpc",
            daemon=True,
        )
        self._rpc_thread.start()
        log.info("[%s] DME endpoint up on %s:%d  peers=%s",
                 self.my_id, self.my_host, self.my_port,
                 list(self.peers.keys()))

    def stop(self) -> None:
        if self._rpc_srv is not None:
            self._rpc_srv.shutdown()
            self._rpc_srv.server_close()

    def wait_for_peers(self, timeout: float = 30.0) -> None:
        """
        Block until every peer's DME endpoint is reachable.  Useful so
        the user can start the three nodes in any order without races.
        """
        deadline = time.time() + timeout
        for pid, (host, port) in self.peers.items():
            url = f"http://{host}:{port}/"
            while True:
                try:
                    xmlrpc.client.ServerProxy(url, allow_none=True).ping()
                    log.info("[%s] peer %s @ %s reachable",
                             self.my_id, pid, url)
                    break
                except Exception:
                    if time.time() > deadline:
                        raise RuntimeError(
                            f"peer {pid} at {url} unreachable")
                    time.sleep(0.5)

    # ------------------------------------------------------------------ #
    # Public API used by the application
    # ------------------------------------------------------------------ #

    def acquire(self) -> None:
        """Block until this node holds the critical section."""
        # Per-node serialisation: Ricart-Agrawala has one outstanding
        # request per node.  If the host application is multi-threaded,
        # callers queue here before entering the distributed protocol.
        self._local_acquire_lock.acquire()
        try:
            with self._cv:
                self._clock += 1
                self._request_ts = (self._clock, self.my_id)
                self._state = WANTED
                self._replies.clear()
                log.info("[%s] REQUEST CS  ts=%s  -> broadcasting to %s",
                         self.my_id, self._request_ts,
                         list(self.peers.keys()))

            # broadcast OUTSIDE the lock so a slow peer can't stall us
            for pid in list(self.peers.keys()):
                self._send_request(pid, self._request_ts)

            # wait for replies from everyone
            with self._cv:
                while len(self._replies) < len(self.peers):
                    log.debug("[%s] waiting for REPLYs, have %s/%s",
                              self.my_id,
                              len(self._replies), len(self.peers))
                    self._cv.wait()
                self._state = HELD
                log.info("[%s] >>> ENTER CS  ts=%s  (got %d REPLYs)",
                         self.my_id, self._request_ts, len(self._replies))
        except Exception:
            # Don't strand the lock if anything blew up before release()
            self._local_acquire_lock.release()
            raise

    def release(self) -> None:
        """Leave the critical section and grant queued peers."""
        try:
            with self._cv:
                self._state = RELEASED
                self._request_ts = None
                to_grant = list(self._deferred)
                self._deferred.clear()
                log.info("[%s] <<< EXIT CS  -> sending deferred REPLYs to %s",
                         self.my_id, to_grant)

            for pid in to_grant:
                self._send_reply(pid)
        finally:
            self._local_acquire_lock.release()

    # ------------------------------------------------------------------ #
    # RPC handlers (called by remote peers)
    # ------------------------------------------------------------------ #

    def _ping(self) -> bool:
        return True

    def _on_request(self, ts_clock, ts_id, sender_id) -> bool:
        """Peer `sender_id` is requesting CS with timestamp (ts_clock, ts_id)."""
        their_ts = (ts_clock, ts_id)
        defer = False
        with self._cv:
            # Lamport clock update on receive
            self._clock = max(self._clock, ts_clock) + 1

            if self._state == HELD:
                defer = True
            elif (self._state == WANTED
                  and self._request_ts is not None
                  and self._request_ts < their_ts):
                # I asked first (smaller ts) -> make them wait
                defer = True

            if defer:
                self._deferred.add(sender_id)
                log.info("[%s] RECV REQUEST from %s ts=%s  state=%s "
                         "my_ts=%s  -> DEFER",
                         self.my_id, sender_id, their_ts,
                         self._state, self._request_ts)
            else:
                log.info("[%s] RECV REQUEST from %s ts=%s  state=%s "
                         "my_ts=%s  -> REPLY now",
                         self.my_id, sender_id, their_ts,
                         self._state, self._request_ts)

        if not defer:
            self._send_reply(sender_id)
        return True

    def _on_reply(self, sender_id) -> bool:
        with self._cv:
            self._clock += 1
            self._replies.add(sender_id)
            log.info("[%s] RECV REPLY from %s  (%d/%d collected)",
                     self.my_id, sender_id,
                     len(self._replies), len(self.peers))
            self._cv.notify_all()
        return True

    # ------------------------------------------------------------------ #
    # Outbound message helpers
    # ------------------------------------------------------------------ #

    def _proxy(self, peer_id):
        # Fresh proxy per call -- xmlrpc.client.ServerProxy is NOT
        # thread-safe; reusing one across threads can raise
        # http.client.CannotSendRequest.  Proxies are cheap to create.
        host, port = self.peers[peer_id]
        return xmlrpc.client.ServerProxy(f"http://{host}:{port}/",
                                         allow_none=True)

    def _send_request(self, peer_id, ts) -> None:
        try:
            self._proxy(peer_id).on_request(ts[0], ts[1], self.my_id)
            log.debug("[%s] SENT REQUEST ts=%s -> %s",
                      self.my_id, ts, peer_id)
        except Exception as e:
            log.error("[%s] SEND REQUEST -> %s failed: %s",
                      self.my_id, peer_id, e)

    def _send_reply(self, peer_id) -> None:
        try:
            self._proxy(peer_id).on_reply(self.my_id)
            log.info("[%s] SENT REPLY -> %s", self.my_id, peer_id)
        except Exception as e:
            log.error("[%s] SEND REPLY -> %s failed: %s",
                      self.my_id, peer_id, e)
