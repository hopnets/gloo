# Peel — Next Steps

Three areas for future development, in priority order.

---

## 1. Replace Stop-and-Wait with a Proper Multicast Transport

### What exists today

`peel_transport.cc` implements a stop-and-wait reliability layer on top of AF_PACKET raw sockets:

- The sender splits the payload into fixed-size chunks (`PEEL_MAX_PAYLOAD`, currently ~8 KB).
- It sends one chunk, then blocks in `waitForAcks()` until every receiver has acknowledged that sequence number.
- Only then does it advance to the next chunk.
- On timeout (`rto_ms`) it retransmits the entire chunk and waits again.

This works but is fundamentally limited: the sender is stalled for one full RTT per chunk. For large messages on a high-bandwidth fabric the pipeline is almost always empty, so throughput is a fraction of what the NIC can sustain.

### Known reliability issues in the current layer

- `sendAck()` ignores the return value of `sendto()` — a full NIC transmit queue (ENOBUFS) silently drops ACKs.
- The linger duration after a FIN equals `rto_ms` exactly, creating a race: if the initial ACK is dropped and the sender retransmits after exactly `rto_ms`, the receiver's linger has just expired.
- Both issues combine deterministically on VM setups where multiple ranks share one physical NIC.

### Where to make the replacement

All reliability logic lives in these two methods of `PeelTransport`:

```
gloo/transport/tcp/peel/peel_transport.cc
  PeelTransport::send()          — chunking loop + waitForAcks call
  PeelTransport::waitForAcks()   — the stop-and-wait ACK collector
  PeelTransport::recv()          — receiver-side loop
  PeelTransport::sendAck()       — raw ACK frame constructor/sender
```

The framing format (sequence numbers, frame types SYN/DATA/ACK/FIN, checksum) is defined in:

```
gloo/transport/tcp/peel/peel_protocol.h
```

A replacement transport should keep the same external interface — `send(data, size)` / `recv(buf, size)` — and the same `PeelTransportConfig` input, so `PeelBroadcast` and `PeelContext` above it are unaffected.

### Reference implementation

We have already implemented a multicast TCP transport, which is, essentially, TCP Reno adapted to work in multicast environment.

Code:

```
https://github.com/hopnets/multicast-udp/tree/multicast_reno_testing
```
Design:
The design document was generated using AI agent, so make sure to go over code and functions (with their descriptions) to fully understand the process.

```
https://livejohnshopkins-my.sharepoint.com/:w:/g/personal/smahmo12_jh_edu/IQDd8NE00mbwSrF0RU-zoZB-AQD0yqgMZMpZ2tTbGliF9Uo?e=M6ACJq
```
---

## 2. Fit Peel into Gloo's Architecture

### The fundamental difference

Gloo is **unicast**: every pair of ranks shares one TCP connection. Its entire design — `transport::Device`, `transport::Pair`, `rendezvous::Context`, `Algorithm` — assumes that a rank can address any other rank directly.

Peel is **multicast**: one sender transmits to a set of receivers simultaneously via UDP multicast groups, and its own discovery layer (`PeelDiscovery`, `PeelRedis`, `PeelFullMesh`) exists precisely because Gloo's rendezvous is not designed for this. **None of Peel's internal components should be removed or replaced** — they do different jobs from their Gloo namesakes and both are necessary.

The task is therefore not to rewrite Peel to look like Gloo, but to identify the seams where Peel should be *added alongside* Gloo rather than bolted on through one-off hooks.

### What "bolted on" currently looks like

Peel is currently accessed by patching Gloo's TCP context with three ad-hoc methods:

```
gloo/transport/tcp/context.h   [MODIFIED]
  enablePeel()
  isPeelReady()
  peelBroadcast()
```

This is not how Gloo is designed to be extended. It makes Peel invisible to anything that holds a generic `gloo::Context*` or `gloo::transport::Device*`, and it couples the multicast path tightly to one specific transport backend (TCP). The goal is to make Peel a first-class, independent transport alongside TCP rather than a hidden extension of it.

### What Gloo's extension model looks like

To understand the interfaces involved, read these files before proceeding:

```
gloo/transport/device.h        — abstract Device interface (creates Pairs)
gloo/transport/pair.h          — abstract Pair (unicast send/recv between two ranks)
gloo/algorithm.h               — abstract Algorithm (takes a gloo::Context)
gloo/rendezvous/context.h      — creates a gloo::Context from a Store + Device
gloo/transport/tcp/device.h    — concrete example: how TCP implements Device
gloo/broadcast.h               — example new-style collective (BroadcastOptions API)
```

### Where Peel should plug in — without changing its internals

#### 1. `PeelTransport` stays under `gloo/transport/tcp/peel/`

Peel is multicast over the same network fabric as TCP — it is not an independent transport like ibverbs. `gloo/transport/tcp/peel/` is the correct location. No move is needed.

What *is* wrong is how Peel is currently accessed from outside: three ad-hoc methods were added directly to `gloo/transport/tcp/context.h`:

```cpp
// gloo/transport/tcp/context.h  [should not be here]
void enablePeel(...);
bool isPeelReady();
bool peelBroadcast(...);
```

This makes the TCP context aware of Peel, coupling two things that should be independent. The TCP context should have no knowledge of Peel. Peel should be constructed and used directly alongside a TCP context, not through it.

Note that `peel_tree.{h,cc}` is **not** transport code — see point 3 below.

#### 2. `PeelContext` as a parallel context, not a replacement

Gloo's `gloo::Context` carries unicast pairs between all ranks. `PeelContext` carries multicast transport groups. These two contexts need to **coexist** for the same job — the Gloo context handles barriers and any unicast coordination; the Peel context handles bulk multicast data.

The current approach (creating a standalone `PeelContext` beside a normal `gloo::Context`) is already correct. What's missing is a clean construction path — today `PeelContext` is constructed manually in benchmark code. Ideally `PeelContext` would be constructable from an existing `gloo::rendezvous::Context`-derived object, sharing the same `Store` for its peer-IP exchange step, so the two contexts are tied together at initialisation without duplicating the rendezvous. Read `gloo/rendezvous/context.h` to see what that construction currently looks like.

#### 3. `PeelBroadcast`, `PeelAllgather`, and `PeelTree` → implement `gloo::Algorithm`

Read `gloo/algorithm.h`. `gloo::Algorithm` is a thin base class:

```cpp
class Algorithm {
public:
    explicit Algorithm(std::shared_ptr<Context> context);
    virtual void run() = 0;
protected:
    std::shared_ptr<Context> context_;
};
```

`PeelBroadcast`, `PeelAllgather`, and `PeelTree` should all live at the algorithm level, not inside the transport directory. `PeelTree` builds the spanning tree and partitions it into subtrees — it decides *what* multicast groups exist and *who* is in each one. That is algorithm-level policy. The transport layer (`PeelTransport`) then executes those decisions. Keeping `PeelTree` in the transport directory conflates the two layers: the transport should not know how the tree was built, only which subtree it is responsible for.

`PeelBroadcast` and `PeelAllgather` should inherit from `Algorithm`. The `context_` member would be the standard `gloo::Context` (used for barriers/sync), while the multicast work continues to go through `PeelContext*` stored as an additional member. This makes the algorithms:

- Compatible with the existing `gloo::benchmark::Benchmark<T>` framework (no more special-case `PeelBroadcastBenchmark` in `main.cc` — they become regular benchmark classes like `AllreduceBenchmark`).
- Passable to anything in Gloo that takes a `gloo::Algorithm*`.
- Logically co-located with `gloo/broadcast.h` and `gloo/allgather.h` at the top level rather than buried in the transport directory.

#### 4. What should stay exactly where it is (inside the transport)

| Component | Reason |
|---|---|
| `PeelDiscovery` / `PeelRedis` | Gloo's `RedisStore` does unicast key-value; Peel's discovery publishes IPs for multicast group membership — different purpose |
| `PeelFullMesh` | The SYN/ACK multicast handshake has no Gloo equivalent; it establishes multicast group membership, not TCP pairs |
| `PeelTransport` internals | AF_PACKET, chunking, stop-and-wait — none of this maps to Gloo's `Pair` interface and shouldn't try to |
| `peel_protocol.h` | Wire format — internal detail of the transport |

### Summary: additions, not replacements

| What to do | Where | What it unlocks |
|---|---|---|
| Transport files stay at `gloo/transport/tcp/peel/` | — | Already the right location; no move needed |
| Move `peel_tree.{h,cc}` to `gloo/` | File tree | Tree sits at the algorithm layer where it belongs |
| `PeelBroadcast` inherits `gloo::Algorithm` | `gloo/broadcast_peel.h` | Works with standard Benchmark and Algorithm consumers |
| `PeelAllgather` inherits `gloo::Algorithm` | `gloo/allgather_peel.h` | Same |
| `PeelContext` constructed via shared `Store` | `peel_context.cc` init path | Ties Peel rendezvous to the same Redis instance as Gloo, no duplication |
| Remove `enablePeel()` / `peelBroadcast()` from `tcp/context.h` | `gloo/transport/tcp/context.{h,cc}` | TCP context is no longer aware of Peel; Peel stands alone |

---

## 3. Running Training with Peel via PyTorch

### Background

PyTorch's `torch.distributed` package uses Gloo as one of its communication backends through `ProcessGroupGloo`. `ProcessGroupGloo` accepts a `gloo::transport::Device` at construction time and uses it for all collective operations (allreduce, broadcast, allgather, etc.).

### High-level steps

1. **Make PeelTransport a proper Gloo transport device** (see task 2 above). `ProcessGroupGloo` calls `gloo::transport::Device::createPair()` internally — Peel needs to satisfy that interface.

2. **Instantiate ProcessGroupGloo with the Peel device** in your training script:

   ```python
   import torch.distributed as dist
   from torch.distributed.distributed_c10d import ProcessGroupGloo
   # (Gloo device factory must be exposed via Python bindings or a custom C++ extension)
   store = dist.TCPStore(...)
   pg = ProcessGroupGloo(store, rank, world_size, options=ProcessGroupGloo.Options(device=peel_device))
   ```

3. **Register the process group** so `dist.broadcast`, `dist.all_gather`, etc. route through it:

   ```python
   dist.init_process_group(backend="gloo", store=store, rank=rank, world_size=world_size)
   ```

4. **Launch training** normally — Peel will handle the underlying data movement. No changes to the model or optimizer code are required.

### Entry point in PyTorch source

The relevant PyTorch file is:

```
torch/csrc/distributed/c10d/ProcessGroupGloo.cpp
  ProcessGroupGloo::broadcast()
  ProcessGroupGloo::allgather()
```

These construct `gloo::BroadcastOptions` / `gloo::AllgatherOptions` and call the Gloo algorithm directly. Once Peel's algorithms implement the same options API (task 2), they slot in here with minimal changes.
