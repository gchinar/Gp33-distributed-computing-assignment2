"""
Integration test on a single host:
  - launches server
  - launches 2 clients (in-process, since client.py main() reads stdin)
  - has both clients fire post()s "simultaneously" via threads
  - verifies:
        (a) no two CSs overlap   (the killer mutual-exclusion invariant)
        (b) all messages reach the file
        (c) message bodies match exactly
"""
import os, sys, time, subprocess, threading, signal, re
import xmlrpc.client

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

# --- start server in subprocess --------------------------------------------
# fresh chat.txt each run
chat_file = os.path.join(HERE, "chat.txt")
if os.path.exists(chat_file):
    os.remove(chat_file)

server_proc = subprocess.Popen(
    [sys.executable, os.path.join(HERE, "server.py"), "127.0.0.1", "9000"],
    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
)

# wait for server to come up
for _ in range(50):
    try:
        xmlrpc.client.ServerProxy("http://127.0.0.1:9000/").view()
        break
    except Exception:
        time.sleep(0.1)
else:
    print("server did not start"); sys.exit(1)

# --- start 2 clients in-process via dme + direct calls ---------------------
from dme import RicartAgrawala
from datetime import datetime

# Client A
peers_A = {"B": ("127.0.0.1", 9102)}
dme_A = RicartAgrawala("A", "127.0.0.1", 9101, peers_A)
dme_A.start()

# Client B
peers_B = {"A": ("127.0.0.1", 9101)}
dme_B = RicartAgrawala("B", "127.0.0.1", 9102, peers_B)
dme_B.start()

dme_A.wait_for_peers(timeout=5)
dme_B.wait_for_peers(timeout=5)

server_proxy = xmlrpc.client.ServerProxy("http://127.0.0.1:9000/")

# track CS overlap
in_cs = {"A": False, "B": False}
overlap_detected = []
cs_lock = threading.Lock()

def do_post(dme, user, msg, results):
    dme.acquire()
    with cs_lock:
        # safety check: nobody else should be in CS right now
        for other, flag in in_cs.items():
            if other != dme.my_id and flag:
                overlap_detected.append((dme.my_id, other, msg))
        in_cs[dme.my_id] = True
    try:
        ts = datetime.now().strftime("%-d %b %-I:%M%p")
        entry = f"{ts} {user}: {msg}"
        # fresh proxy per call: ServerProxy isn't thread-safe
        xmlrpc.client.ServerProxy("http://127.0.0.1:9000/").post(entry)
        results.append(entry)
        # hold CS briefly to make overlap detection meaningful
        time.sleep(0.05)
    finally:
        with cs_lock:
            in_cs[dme.my_id] = False
        dme.release()

# fire off 10 concurrent posts from each client, interleaved
results_A, results_B = [], []
threads = []
for i in range(10):
    t1 = threading.Thread(target=do_post,
                          args=(dme_A, "Joel", f"A-msg-{i}", results_A))
    t2 = threading.Thread(target=do_post,
                          args=(dme_B, "Lucy", f"B-msg-{i}", results_B))
    threads += [t1, t2]
    t1.start(); t2.start()

for t in threads:
    t.join()

# --- verify -----------------------------------------------------------------
file_contents = server_proxy.view()
posted = results_A + results_B

print("\n=== chat.txt contents after concurrent run ===")
print(file_contents)

print(f"posted {len(posted)} messages, file has "
      f"{len([l for l in file_contents.splitlines() if l.strip()])} lines")

ok = True
if overlap_detected:
    print("FAIL: CS overlap detected:", overlap_detected)
    ok = False
else:
    print("PASS: no CS overlap (mutual exclusion held)")

for entry in posted:
    if entry not in file_contents:
        print(f"FAIL: missing entry in file: {entry!r}")
        ok = False

if ok:
    print("PASS: every posted message is present in the file")

# --- teardown --------------------------------------------------------------
dme_A.stop()
dme_B.stop()
server_proc.send_signal(signal.SIGINT)
server_proc.wait(timeout=5)

sys.exit(0 if ok else 1)
