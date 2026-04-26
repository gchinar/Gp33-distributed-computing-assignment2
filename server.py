"""
server.py
=========
Chat-room file server. Runs on the SERVER node (VM-1).

Responsibilities
----------------
* Owns the single shared file (chat.txt) — the resource the clients
  contend for.
* Exposes two RPC methods (XML-RPC over HTTP):
      view()           -> str   (returns full file contents)
      post(entry)      -> bool  (appends one line to the file)
* Allows concurrent reads (multiple `view` at the same time).
* Serialises writes locally with a threading.Lock.  This is NOT the
  distributed mutual-exclusion mechanism — it merely protects the file
  descriptor from interleaved writes inside this single Python process.
  The real cross-node mutual exclusion is enforced by the clients
  themselves using the Ricart-Agrawala DME algorithm in dme.py BEFORE
  they ever call post().

Run
---
    python3 server.py <bind-host> <port>

Example:
    python3 server.py 0.0.0.0 9000
"""

import os
import sys
import threading
import logging
from xmlrpc.server import SimpleXMLRPCServer
from socketserver import ThreadingMixIn

# ---------- configuration ------------------------------------------------- #

CHAT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         "chat.txt")

# ---------- logging ------------------------------------------------------- #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SERVER] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("server")

# ---------- threaded XML-RPC server --------------------------------------- #

class ThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    """One thread per request -> concurrent `view` calls are possible."""
    daemon_threads = True
    allow_reuse_address = True


# Local lock guarding file APPEND operations only.  Reads do not need it
# because POSIX append-mode writes are atomic for small lines and reads
# of an open file just see whatever has been flushed.
_write_lock = threading.Lock()


def view() -> str:
    """Return the entire shared file as a string."""
    log.info("RPC view() received")
    if not os.path.exists(CHAT_FILE):
        return ""
    with open(CHAT_FILE, "r", encoding="utf-8") as f:
        data = f.read()
    log.info("RPC view() -> %d bytes", len(data))
    return data


def post(entry: str) -> bool:
    """
    Append one already-formatted entry to the shared file.

    The DME middleware on the client side guarantees that at most one
    client is in its critical section when this RPC is invoked, so the
    inner _write_lock is just defence-in-depth against accidental local
    concurrency.
    """
    log.info("RPC post() received: %r", entry)
    with _write_lock:
        with open(CHAT_FILE, "a", encoding="utf-8") as f:
            if not entry.endswith("\n"):
                entry += "\n"
            f.write(entry)
            f.flush()
            os.fsync(f.fileno())
    log.info("RPC post() committed")
    return True


# ---------- entry point --------------------------------------------------- #

def main() -> None:
    if len(sys.argv) != 3:
        print("usage: python3 server.py <bind-host> <port>")
        sys.exit(1)

    host = sys.argv[1]
    port = int(sys.argv[2])

    # Make sure the file exists so the first `view` doesn't crash.
    open(CHAT_FILE, "a", encoding="utf-8").close()

    srv = ThreadedXMLRPCServer((host, port),
                               allow_none=True,
                               logRequests=False)
    srv.register_function(view, "view")
    srv.register_function(post, "post")

    log.info("Chat-room file server listening on %s:%d", host, port)
    log.info("Shared file: %s", CHAT_FILE)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        log.info("Shutting down")


if __name__ == "__main__":
    main()
