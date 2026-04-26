# Distributed Chat Room — Assignment 2

Three-node distributed chat application with Ricart–Agrawala
distributed mutual exclusion.

## Files

| File          | Role                                                                                     |
| ------------- | ---------------------------------------------------------------------------------------- |
| `server.py`   | File server. Hosts the shared `chat.txt`. Exposes XML-RPC `view()` / `post()`.           |
| `dme.py`      | DME middleware (Ricart–Agrawala). Independent of the chat application.                   |
| `client.py`   | Chat application. Calls `dme.acquire()`/`dme.release()` around `post`.                   |
| `config_A.ini`| Config used by Client A (VM-2).                                                          |
| `config_B.ini`| Config used by Client B (VM-3).                                                          |

The DME algorithm and the chat application are in **separate
modules**, as required by the assignment. The application calls into
`RicartAgrawala.acquire()` / `release()` and is otherwise oblivious to
how mutual exclusion is achieved.

## Architecture

```
              ┌──────────────────────────────┐
              │     VM-1  SERVER             │
              │   server.py  port 9000       │
              │   shared file: chat.txt      │
              └──────────────┬───────────────┘
                             │ XML-RPC view/post
              ┌──────────────┴──────────────┐
              │                             │
   ┌──────────┴───────────┐     ┌───────────┴──────────┐
   │   VM-2  CLIENT A     │◄───►│   VM-3  CLIENT B     │
   │   client.py + dme.py │ DME │   client.py + dme.py │
   │   port 9101          │ RPC │   port 9102          │
   └──────────────────────┘     └──────────────────────┘
```

* The server **does not** participate in the DME protocol — that would
  be the "trivial centralised solution" the spec forbids. The server
  just blindly serves any `post` it receives. Mutual exclusion is
  enforced *between the clients* by Ricart–Agrawala before they ever
  call `post`.

## Why Ricart–Agrawala

* **Decentralised.** No coordinator, no token, no single point of
  failure for the locking decision.
* **Safe.** At most one node enters the critical section at a time —
  proof: a node only enters CS after it has received a REPLY from
  *every* peer; a peer only sends REPLY when its own state is
  RELEASED, or when its WANTED timestamp is larger than the
  requester's. So two nodes cannot both be HELD simultaneously.
* **Live (no starvation).** Lamport timestamps with the (clock, id)
  tie-break are a total order; every later request has a strictly
  larger timestamp, so an earlier request is always granted first.
* **Cheap.** Exactly `2(N-1)` messages per CS entry — for N=2 that is
  just one REQUEST and one REPLY.

## Run

### 1. Server (VM-1)

```bash
python3 server.py 0.0.0.0 9000
```

### 2. Edit the configs

Replace the IP placeholders in `config_A.ini` and `config_B.ini` with
the real IPs of the three VMs. Both clients need the server's IP and
each other's IP.

### 3. Client A (VM-2)

```bash
python3 client.py config_A.ini
```

### 4. Client B (VM-3)

```bash
python3 client.py config_B.ini
```

The clients will block on startup until they can `ping` each other —
this avoids start-order races, you can launch them in any order.

### 5. Use it

```
Joel_machine> post Welcome to the team project
Joel_machine> view
12 Oct 9:01AM Joel: Welcome to the team project
Joel_machine> post Thanks Lucy - hope to work together
```

## Demonstrating that DME actually works

Each node prints log lines for every algorithm event:

```
[A] REQUEST CS  ts=(5, 'A')  -> broadcasting to ['B']
[A] RECV REQUEST from B ts=(6, 'B')  state=WANTED my_ts=(5, 'A') -> DEFER
[A] RECV REPLY from B  (1/1 collected)
[A] >>> ENTER CS  ts=(5, 'A')  (got 1 REPLYs)
[A] in CS, calling server.post()
[A] post committed on server
[A] <<< EXIT CS  -> sending deferred REPLYs to ['B']
[A] SENT REPLY -> B
```

A good way to demonstrate mutual exclusion to the evaluator:

1. On Client A, run a tight `post` loop.
2. On Client B, run another tight `post` loop at the same time.
3. Watch the logs: every `>>> ENTER CS` on one node is preceded by a
   `<<< EXIT CS` on the other. Timestamps in the resulting `chat.txt`
   are strictly interleaved with no overlap.

A simple stress driver (optional):

```bash
# on each client, instead of typing manually:
for i in $(seq 1 20); do
    echo "post message-$i from $(hostname)"
done | python3 client.py config_A.ini
```

## Notes

* `view` does **not** acquire the DME lock — the spec explicitly allows
  unlimited concurrent viewers.
* `post` timestamps are taken on the **client side** (as required),
  not on the server.
* The local `threading.Lock` inside `server.py` only serialises
  appends within the single server process — it is *not* the
  distributed mutual-exclusion mechanism. If you remove the DME
  middleware, two simultaneous `post` calls would still both succeed;
  the whole point of the DME layer is to ensure the server only ever
  sees one client in its critical section at a time.
* No authentication, as the spec allows.
