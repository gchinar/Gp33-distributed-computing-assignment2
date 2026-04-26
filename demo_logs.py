"""Run a short scenario and print all DME logs to verify algorithm transitions."""
import logging, sys, os, time, threading, subprocess, signal
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
    force=True,
)

# server
HERE = os.path.dirname(os.path.abspath(__file__))
chat = os.path.join(HERE, "chat.txt")
if os.path.exists(chat): os.remove(chat)
sp = subprocess.Popen([sys.executable, "server.py", "127.0.0.1", "9000"],
                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(0.5)

import xmlrpc.client
from dme import RicartAgrawala

A = RicartAgrawala("A", "127.0.0.1", 9101, {"B": ("127.0.0.1", 9102)})
B = RicartAgrawala("B", "127.0.0.1", 9102, {"A": ("127.0.0.1", 9101)})
A.start(); B.start()
A.wait_for_peers(5); B.wait_for_peers(5)

print("\n--- scenario: A and B both want CS at almost the same time ---\n")

def post(node, label):
    node.acquire()
    print(f"   ### {node.my_id} doing work in CS ({label}) ###")
    xmlrpc.client.ServerProxy("http://127.0.0.1:9000/").post(f"{label} from {node.my_id}")
    time.sleep(0.1)
    node.release()

t1 = threading.Thread(target=post, args=(A, "msg-1"))
t2 = threading.Thread(target=post, args=(B, "msg-2"))
t1.start(); t2.start()
t1.join(); t2.join()

A.stop(); B.stop()
sp.send_signal(signal.SIGINT); sp.wait(timeout=3)
