"""
client.py
=========
Chat-room user application.  Runs on each user node (VM-2 and VM-3).

Separation of concerns
----------------------
This module ONLY implements the user-facing chat application:
    - parses a config file
    - presents the `view` / `post` shell
    - calls the file-server RPC (`view`, `post`) to read/write the
      shared file

It does NOT implement mutual exclusion itself.  For `post` it delegates
to the DME middleware in dme.py:

        dme.acquire()                         # << blocks if someone else
                                              #    is posting >>
        try:
            server.post(formatted_entry)
        finally:
            dme.release()

`view` does NOT take the DME lock — the spec explicitly allows
multiple simultaneous viewers.

Config file format
------------------
A simple INI file shared by all three nodes (or a per-node copy).
Example (config.ini):

    [server]
    host = 10.0.0.10
    port = 9000

    [me]
    id   = A
    user = Joel
    host = 0.0.0.0
    port = 9101

    [peer:B]
    host = 10.0.0.12
    port = 9102

You may list as many [peer:*] sections as you want; the script picks up
all of them automatically.

Run
---
    python3 client.py <config-file>
"""

import argparse
import configparser
import logging
import sys
import time
import xmlrpc.client
from datetime import datetime

from dme import RicartAgrawala

# ---------- logging ------------------------------------------------------- #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("client")


# ---------- helpers ------------------------------------------------------- #

def load_config(path):
    cfg = configparser.ConfigParser()
    if not cfg.read(path):
        raise FileNotFoundError(path)

    server = (cfg["server"]["host"], int(cfg["server"]["port"]))

    me = {
        "id":   cfg["me"]["id"],
        "user": cfg["me"]["user"],
        "host": cfg["me"]["host"],
        "port": int(cfg["me"]["port"]),
    }

    peers = {}
    for section in cfg.sections():
        if section.startswith("peer:"):
            pid = section.split(":", 1)[1]
            peers[pid] = (cfg[section]["host"], int(cfg[section]["port"]))

    return server, me, peers


def format_entry(user, text):
    """`12 Oct 9:01AM Lucy: Welcome ...`  (timestamp = client side)"""
    # %-d / %-I are POSIX, behave fine on Linux VMs
    ts = datetime.now().strftime("%-d %b %-I:%M%p")
    return f"{ts} {user}: {text}"


# ---------- main ---------------------------------------------------------- #

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("config", help="path to client config .ini")
    args = ap.parse_args()

    (srv_host, srv_port), me, peers = load_config(args.config)

    if not peers:
        sys.exit("error: no [peer:*] sections in config (DME needs peers)")

    # --- middleware ---------------------------------------------------- #
    dme = RicartAgrawala(me["id"], me["host"], me["port"], peers)
    dme.start()

    log.info("[%s] waiting for peers to come online ...", me["id"])
    dme.wait_for_peers(timeout=60)

    # --- file server proxy --------------------------------------------- #
    file_server = xmlrpc.client.ServerProxy(
        f"http://{srv_host}:{srv_port}/", allow_none=True)

    # quick sanity check
    try:
        file_server.view()
    except Exception as e:
        sys.exit(f"cannot reach file server at {srv_host}:{srv_port}: {e}")

    log.info("[%s] ready.  user=%s  type 'help' for commands.",
             me["id"], me["user"])

    # --- REPL ---------------------------------------------------------- #
    prompt = f"{me['user']}_machine> "
    try:
        while True:
            try:
                line = input(prompt).strip()
            except EOFError:
                print()
                break
            if not line:
                continue

            cmd, _, rest = line.partition(" ")
            cmd = cmd.lower()

            if cmd in ("quit", "exit"):
                break

            elif cmd == "help":
                print("commands:")
                print("  view              show all chat messages")
                print("  post <text>       append a message (DME-protected)")
                print("  quit              exit")

            elif cmd == "view":
                # No DME lock — concurrent reads are allowed.
                try:
                    contents = file_server.view()
                except Exception as e:
                    log.error("view RPC failed: %s", e)
                    continue
                if not contents:
                    print("(no messages yet)")
                else:
                    sys.stdout.write(contents)
                    if not contents.endswith("\n"):
                        sys.stdout.write("\n")

            elif cmd == "post":
                text = rest.strip()
                # strip optional surrounding quotes the user may type
                if (len(text) >= 2 and text[0] == text[-1]
                        and text[0] in ('"', "'")):
                    text = text[1:-1]
                if not text:
                    print("usage: post <text>")
                    continue

                entry = format_entry(me["user"], text)
                log.info("[%s] -> requesting CS for post", me["id"])

                t0 = time.time()
                dme.acquire()
                try:
                    log.info("[%s] in CS, calling server.post()", me["id"])
                    file_server.post(entry)
                    log.info("[%s] post committed on server", me["id"])
                finally:
                    dme.release()
                log.info("[%s] CS held for %.3fs",
                         me["id"], time.time() - t0)

            else:
                print(f"unknown command: {cmd!r} (try 'help')")
    finally:
        dme.stop()


if __name__ == "__main__":
    main()
