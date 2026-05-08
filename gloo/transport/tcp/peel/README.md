# Peel — Topology-Aware Multicast Transport for Gloo

Peel adds UDP multicast broadcast and allgather to Gloo using AF_PACKET raw sockets with stop-and-wait reliability. It discovers peers via Redis, optionally loads a physical topology file to build a spanning tree, and partitions that tree into subtrees — each subtree maps to one multicast group.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Build](#build)
3. [Topology File](#topology-file)
4. [Standalone Test Binaries](#standalone-test-binaries)
5. [Benchmark (gloo_benchmark)](#benchmark-gloo_benchmark)
6. [Common Recipes](#common-recipes)

---

## Prerequisites

### System packages

```bash
sudo apt-get install libhiredis-dev redis-server
```

### Redis

Redis is used as the rendezvous store — all ranks publish their IPs and synchronise through it. Start it on any node reachable by all participants:

```bash
redis-server --bind 0.0.0.0 --port 6379 --daemonize yes
redis-cli -h <REDIS_IP> ping   # should print PONG
```

### Network interface

Peel opens an AF_PACKET raw socket on a named interface. Find the right one with:

```bash
ip addr show
ip link show <IFACE> | grep MULTICAST   # flag must be present
```

### Capabilities / permissions

AF_PACKET requires either `CAP_NET_RAW` or running as root:

```bash
sudo setcap cap_net_raw=eip ./gloo_benchmark
# or just run with sudo (recommended)
```

---

## Build

### Standalone test binaries

```bash
cd ~/gloo
mkdir -p build && cd build
sudo cmake .. \
  -DUSE_REDIS=ON \
  -DHIREDIS_INCLUDE_DIRS=/usr/local/include/hiredis \
  -DHIREDIS_LIBRARIES=/usr/local/lib/libhiredis.so \
  -DCMAKE_EXE_LINKER_FLAGS="-L/usr/local/lib -Wl,-rpath,/usr/local/lib"
sudo make -j$(nproc)
```

### Benchmark binary

```bash
cd ~/gloo
mkdir -p build && cd build
sudo cmake .. \
  -DUSE_REDIS=ON \
  -DBUILD_BENCHMARK=ON \
  -DHIREDIS_INCLUDE_DIRS=/usr/local/include/hiredis \
  -DHIREDIS_LIBRARIES=/usr/local/lib/libhiredis.so \
  -DCMAKE_EXE_LINKER_FLAGS="-L/usr/local/lib -Wl,-rpath,/usr/local/lib" \
  -DCMAKE_SHARED_LINKER_FLAGS="-L/usr/local/lib -Wl,-rpath,/usr/local/lib"
sudo make -j$(nproc)
```

The build produces:

| Binary | Location |
|--------|----------|
| `benchmark` | `build/gloo/benchmark/` |
| `test_peel_broadcast` | `build/gloo/transport/tcp/peel/` |
| `test_peel_allgather` | `build/gloo/transport/tcp/peel/` |
| `test_tcp_broadcast` | `build/gloo/transport/tcp/peel/` |
| `test_peel_full_mesh` | `build/gloo/transport/tcp/peel/` |
| `test_peel_gloo_context` | `build/gloo/transport/tcp/peel/` |

---

## Topology File

For PEEL, we always need a topology file (even if it is set as optional) using `--peel-topology-file`. Peel reads an adjacency-list file that describes the physical switch/GPU graph. It builds a minimum spanning tree rooted at the sender rank and partitions it into subtrees — one per multicast group.

**Format:** each line is a pair of node IDs separated by whitespace. Node IDs are either a dotted-decimal IP (GPU/server) or `switch_N` (switch).

```
# Example: two ToR switches, four servers
switch_0  10.0.0.1
switch_0  10.0.0.2
switch_1  10.0.0.3
switch_1  10.0.0.4
switch_0  switch_1
```

Without a topology file, all ranks are placed in one flat transport (single multicast group, no tree).

Note: Either provide the complete address to the topology file (wherever it is located in your system), or copy it into the directory where your binary is created.

---

## Standalone Test Binaries

These are quick sanity-check programs that do not require the benchmark framework.

### test_peel_broadcast

Runs a single multicast broadcast from rank 0 to all other ranks and verifies the data.

```bash
./test_peel_broadcast <rank> <world_size> <redis_host> [redis_port] [iface] [mcast_group] [base_port] [topology_file]
```

| Argument | Default | Description |
|----------|---------|-------------|
| rank | — | This process's rank (0 = sender) |
| world_size | — | Total number of ranks |
| redis_host | — | Redis server IP |
| redis_port | 6379 | Redis server port |
| iface | auto | Network interface name (e.g. `ens18`) |
| mcast_group | 239.255.0.1 | Multicast group address |
| base_port | 50000 | UDP base port |
| topology_file | — | Path to topology adjacency file |

Example (rank 0 of 2):
```bash
./test_peel_broadcast 0 2 10.169.157.11 6379 ens18 239.255.0.1 80001 test_topo.txt
```

### test_peel_allgather

Runs one broadcast per rank so every rank ends up with every other rank's data.

```bash
./test_peel_allgather <rank> <world_size> <redis_host> [redis_port] [iface] [mcast_group] [base_port] [topology_file] [--parallel]
```

| Argument | Default | Description |
|----------|---------|-------------|
| rank | — | This process's rank |
| world_size | — | Total number of ranks |
| redis_host | — | Redis server IP |
| redis_port | 6379 | Redis server port |
| iface | auto | Network interface name |
| mcast_group | 239.255.0.1 | Multicast group address |
| base_port | 50000 | UDP base port |
| topology_file | — | Path to topology adjacency file |
| --parallel | off | Run all N broadcasts concurrently |

Example (rank 0 of 5, parallel):
```bash
./test_peel_allgather 0 5 10.169.157.11 6379 ens18 239.255.0.1 50000 test_topo.txt --parallel
```

### test_tcp_broadcast

Baseline: same data transfer using Gloo's standard TCP point-to-point. Useful for latency/throughput comparison.

```bash
./test_tcp_broadcast <rank> <world_size> <redis_host> [redis_port] [iface]
```

### test_peel_full_mesh / test_peel_gloo_context

Older integration tests for peer discovery and the TCP context wrapper. Run without arguments for usage.

---

## Benchmark (`benchmark`)

The `benchmark` binary exposes `peel_broadcast` and `peel_allgather` as named benchmarks. It handles iteration timing, warmup, and result reporting.

> **Invocation order:** the benchmark name (`peel_broadcast` / `peel_allgather`) goes at the **end** of the command line as a positional argument.

### Before every benchmark run — flush Redis

Redis keys from a previous run will block rendezvous. Always flush before starting:

```bash
redis-cli -h <REDIS_IP> FLUSHALL
```

### Peel-specific flags

| Flag | Default | Description |
|------|---------|-------------|
| `--peel-iface <name>` | *(required)* | Network interface for AF_PACKET socket |
| `--peel-topology-file <path>` | *(required)* | Path to topology adjacency file |
| `--peel-mcast-group <ip>` | `239.255.0.1` | Multicast group address |
| `--peel-base-port <n>` | `50000` | UDP base port |
| `--peel-ttl <n>` | `3` | IP TTL for multicast packets |
| `--peel-sender-rank <n>` | `0` | Root rank for broadcast (ignored for allgather) |
| `--peel-parallel` | off | Run allgather broadcasts concurrently |
| `--peel-rto-ms <n>` | `500` | Stop-and-wait retransmission timeout (ms) |

### Common benchmark flags

| Flag | Default | Description |
|------|---------|-------------|
| `--rank <n>` | — | This process's rank |
| `--transport <name>` | — | Transport to use (use `tcp`) |
| `--tcp-device <name>` | — | TCP network interface |
| `--size <n>` | — | Message size in bytes |
| `--elements <n>` | — | Number of elements (alternative to `--size` for allgather) |
| `--iteration-count <n>` | — | Fixed iteration count *(required for peel)* |
| `--warmup-iters <n>` | `5` | Warmup iterations before timing |
| `--verify` | on | Verify data correctness each iteration |
| `--prefix <str>` | `prefix` | Redis key prefix (use a unique value per run) |
| `--redis-host <ip>` | — | Redis rendezvous server |
| `--redis-port <n>` | `6379` | Redis port |

> **Note:** `--iteration-count` must be set explicitly for Peel benchmarks. The default auto-scaling mode runs a Gloo TCP broadcast internally, which will time out with asymmetric subtrees.

---

## Common Recipes

All examples assume:
- Redis is running at `10.169.156.14:6379`
- Interface is `ens18`
- Topology file is `test_topo.txt` (in the same directory as the binary, or provide full path)
- Run one of these commands **per node**, substituting the correct `--rank`.
- **Always flush Redis before each run:** `redis-cli -h 10.169.156.14 FLUSHALL`

### Broadcast, small message (33 bytes), 1 iteration, no warmup

```bash
sudo ./benchmark \
  --transport=tcp --tcp-device=ens18 \
  --rank=0 \
  --redis-host=10.169.156.14 --redis-port=6379 \
  --prefix=peel_bench_run1 \
  --peel-iface=ens18 \
  --peel-topology-file=test_topo.txt \
  --peel-mcast-group=239.255.0.1 \
  --peel-sender-rank=0 \
  --peel-ttl=64 \
  --size=33 \
  --iteration-count=1 \
  --warmup-iters=0 \
  peel_broadcast
```

### Broadcast, 1 MB, 100 iterations, no warmup

```bash
sudo ./benchmark \
  --transport=tcp --tcp-device=ens18 \
  --rank=0 \
  --redis-host=10.169.156.14 --redis-port=6379 \
  --prefix=peel_bench_run2 \
  --peel-iface=ens18 \
  --peel-topology-file=test_topo.txt \
  --peel-mcast-group=239.255.0.1 \
  --peel-sender-rank=0 \
  --peel-ttl=64 \
  --size=1048576 \
  --iteration-count=100 \
  --warmup-iters=0 \
  peel_broadcast
```

### Allgather, sequential, 5 million elements

```bash
./benchmark \
  --transport=tcp --tcp-device=ens18 \
  --rank=0 \
  --redis-host=10.169.156.14 --redis-port=6379 \
  --prefix=peel_ag_run1 \
  --peel-iface=ens18 \
  --peel-topology-file=test_topo.txt \
  --peel-mcast-group=239.255.0.1 \
  --peel-base-port=50000 \
  --peel-ttl=64 \
  --elements=5000000 \
  --iteration-count=1 \
  --warmup-iters=0 \
  peel_allgather
```

### Allgather, parallel broadcasts, 5 million elements

```bash
./benchmark \
  --transport=tcp --tcp-device=ens18 \
  --rank=0 \
  --redis-host=10.169.156.14 --redis-port=6379 \
  --prefix=peel_ag_run2 \
  --peel-iface=ens18 \
  --peel-topology-file=test_topo.txt \
  --peel-mcast-group=239.255.0.1 \
  --peel-base-port=50000 \
  --peel-ttl=64 \
  --elements=5000000 \
  --peel-parallel \
  --iteration-count=1 \
  --warmup-iters=0 \
  peel_allgather
```

### Adjusting the retransmission timeout

If you see frequent retransmissions on a lossy or high-latency network, increase `--peel-rto-ms`:

```bash
  --peel-rto-ms 1000
```

On a low-latency cluster where timeouts are causing unnecessary delays, reduce it:

```bash
  --peel-rto-ms 100
```

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `Operation not permitted` on socket open | Missing `CAP_NET_RAW` | `sudo` or `setcap cap_net_raw=eip` |
| Ranks hang at discovery | Redis unreachable or wrong prefix | Verify `redis-cli ping`; use a unique `--prefix` per run |
| `PeelContext init failed` | Interface name wrong or no MULTICAST flag | Check `ip link show <iface>` |
| 2 of N ranks fail consistently | ACK dropped during NIC saturation + linger race | Increase `--peel-rto-ms`; see NEXT_STEPS.md |
| Retransmission storm | `world_size` too large for single multicast group | Ensure topology file is correct so tree is partitioned |
