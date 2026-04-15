/**
 * test_peel_allgather.cc — end-to-end sanity test for Peel multicast allgather.
 *
 * What this test does (in order):
 *   1. Parses command-line args and prints config.
 *   2. Runs PeelDiscovery: each rank publishes its interface IP via Redis so
 *      every rank ends up with a complete rank→IP map.
 *   3. Creates N PeelContext objects (one per sender rank), each rooted at a
 *      different rank and with a non-overlapping port range.
 *   4. Each rank fills its own send buffer with a recognizable pattern:
 *        buf[r][i] = r * dataCount + i
 *      All other buffers are zeroed (will be overwritten by broadcast).
 *   5. Runs N sequential broadcasts — for each r = 0..world_size-1:
 *        contexts[r]->broadcast(r, bufs[r].data(), dataBytes)
 *      After the loop, every rank holds every other rank's data.
 *   6. Measures total wall time and computes aggregate throughput.
 *   7. All ranks verify every buffer matches the expected pattern.
 *
 * NOTE: This test uses PeelContext directly — no TCP context is involved.
 *       PeelDiscovery is the only rendezvous step; it uses Redis directly.
 *
 * Usage:
 *   ./test_peel_allgather <rank> <world_size> <redis_host>
 *                         [redis_port] [iface] [mcast_group] [base_port]
 *                         [topology_file]
 *
 * Arguments:
 *   rank           — this process's rank (0-based)
 *   world_size     — total number of ranks
 *   redis_host     — Redis server IP for rendezvous (e.g. 10.0.0.1)
 *   redis_port     — Redis port (default: 6379)
 *   iface          — network interface for AF_PACKET raw socket (e.g. eth0)
 *                    REQUIRED
 *   mcast_group    — IPv4 multicast group (default: 239.255.0.1)
 *   base_port      — base UDP port; formula: base + sender*W*W + subtree*W
 *                    (default: 50000)
 *   topology_file  — path to adjacency topology file; omit for flat mode
 *
 * Port isolation guarantee:
 *   Each sender r occupies ports [base + r*W*W, base + r*W*W + W*max_subtrees).
 *   With base=50000 and W=4: sender 0 → 50000-50015, sender 1 → 50016-50031, etc.
 *   All N contexts are open simultaneously — they never share a port.
 *
 * Examples:
 *   Flat mode (no topology):
 *     ./test_peel_allgather 0 4 10.0.0.1 6379 eth0
 *
 *   Tree mode (with topology):
 *     ./test_peel_allgather 0 4 10.0.0.1 6379 eth0 239.255.0.1 50000 topo.txt
 */

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "gloo/transport/tcp/peel/peel_context.h"
#include "gloo/transport/tcp/peel/peel_discovery.h"

using Clock = std::chrono::steady_clock;

static void printUsage(const char* prog) {
    std::cerr << "Usage: " << prog
              << " <rank> <world_size> <redis_host>"
                 " [redis_port] [iface] [mcast_group] [base_port] [topology_file]\n"
              << "\nDefaults:\n"
              << "  redis_port:    6379\n"
              << "  iface:         (required)\n"
              << "  mcast_group:   239.255.0.1\n"
              << "  base_port:     50000\n"
              << "  topology_file: (none — flat single-transport mode)\n"
              << "\nExamples:\n"
              << "  " << prog << " 0 4 10.0.0.1 6379 eth0\n"
              << "  " << prog << " 0 4 10.0.0.1 6379 eth0 239.255.0.1 50000 topo.txt\n";
}

int main(int argc, char** argv) {
    if (argc < 4) {
        printUsage(argv[0]);
        return 1;
    }

    // =========================================================================
    // Step 1: Parse arguments
    // =========================================================================
    int         rank         = std::atoi(argv[1]);
    int         worldSize    = std::atoi(argv[2]);
    std::string redisHost    = argv[3];
    int         redisPort    = (argc > 4) ? std::atoi(argv[4]) : 6379;
    std::string iface        = (argc > 5) ? argv[5] : "";
    std::string mcastGroup   = (argc > 6) ? argv[6] : "239.255.0.1";
    uint16_t    basePort     = (argc > 7) ? static_cast<uint16_t>(std::atoi(argv[7])) : 50000;
    std::string topologyFile = (argc > 8) ? argv[8] : "";

    std::cout << "\n[PEEL-AG] ====== STEP 1: Config ======\n"
              << "[PEEL-AG] Rank " << rank << "/" << worldSize
              << "  Redis=" << redisHost << ":" << redisPort << "\n"
              << "[PEEL-AG] iface=" << (iface.empty() ? "(not set)" : iface)
              << "  mcast=" << mcastGroup
              << "  base_port=" << basePort << "\n"
              << "[PEEL-AG] Topology: "
              << (topologyFile.empty() ? "FLAT (no file)" : topologyFile) << "\n";

    if (iface.empty()) {
        std::cerr << "[PEEL-AG] ERROR — iface argument is required for AF_PACKET\n";
        return 1;
    }

    // =========================================================================
    // Step 2: PeelDiscovery — build rank→IP map via Redis
    // No TCP context is needed; discovery talks to Redis directly.
    // =========================================================================
    std::cout << "\n[PEEL-AG] ====== STEP 2: PeelDiscovery ======\n";

    const std::string discoveryPrefix = "peel_allgather_test";

    gloo::transport::tcp::peel::PeelDiscoveryConfig discCfg;
    discCfg.rank         = rank;
    discCfg.world_size   = worldSize;
    discCfg.iface_name   = iface;
    discCfg.redis_host   = redisHost;
    discCfg.redis_port   = redisPort;
    discCfg.redis_prefix = discoveryPrefix;

    gloo::transport::tcp::peel::PeelDiscovery discovery(discCfg);
    if (!discovery.run()) {
        std::cerr << "[PEEL-AG] Rank " << rank << ": ERROR — discovery failed\n";
        return 1;
    }

    std::unordered_map<int, std::string> peerIps;
    for (int r = 0; r < worldSize; ++r)
        peerIps[r] = discovery.getIp(r);

    std::cout << "[PEEL-AG] Rank " << rank << ": Discovery complete — rank→IP map:\n";
    for (int r = 0; r < worldSize; ++r)
        std::cout << "[PEEL-AG]   rank " << r << " -> " << peerIps[r] << "\n";

    // =========================================================================
    // Step 3: Create N PeelContext objects — one per sender rank
    //
    // Each context is rooted at a different rank (sender_rank = r).
    // The port formula base + sender_rank*W*W + subtree_id*W ensures every
    // context uses a completely separate port range, so all N can be open
    // and initialized simultaneously without any port conflicts.
    // =========================================================================
    std::cout << "\n[PEEL-AG] ====== STEP 3: Initializing " << worldSize
              << " PeelContext(s) ======\n";

    std::vector<std::unique_ptr<gloo::transport::tcp::peel::PeelContext>> contexts;
    contexts.reserve(worldSize);

    for (int r = 0; r < worldSize; ++r) {
        gloo::transport::tcp::peel::PeelContextConfig cfg;
        cfg.rank          = rank;
        cfg.world_size    = worldSize;
        cfg.sender_rank   = r;         // root of this broadcast tree
        cfg.peer_ips      = peerIps;
        cfg.base_port     = basePort;  // formula in PeelTree handles isolation
        cfg.iface_name    = iface;
        cfg.mcast_group   = mcastGroup;
        cfg.topology_file = topologyFile;

        auto ctx = std::make_unique<gloo::transport::tcp::peel::PeelContext>(cfg);
        if (!ctx->init()) {
            std::cerr << "[PEEL-AG] Rank " << rank
                      << ": ERROR — PeelContext init failed for sender_rank=" << r << "\n";
            return 1;
        }
        if (!ctx->isReady()) {
            std::cerr << "[PEEL-AG] Rank " << rank
                      << ": ERROR — PeelContext not ready for sender_rank=" << r << "\n";
            return 1;
        }

        std::cout << "[PEEL-AG] Rank " << rank
                  << ": context[sender=" << r << "] ready ✓\n";
        contexts.push_back(std::move(ctx));
    }

    // =========================================================================
    // Step 4: Prepare data buffers
    //
    // bufs[r] is the data that rank r will broadcast to everyone.
    // Pattern: bufs[r][i] = r * dataCount + i
    //   → rank 0 broadcasts 0, 1, 2, ...
    //   → rank 1 broadcasts 1000000, 1000001, ...  (with dataCount=1M)
    //   → etc.
    // Each rank fills only its own buffer; all others start zeroed and are
    // overwritten by the corresponding broadcast.
    // =========================================================================
    std::cout << "\n[PEEL-AG] ====== STEP 4: Prepare data ======\n";

    const size_t dataCount = 256 * 1024;             // 256 K uint32 elements = 1 MB
    const size_t dataBytes = dataCount * sizeof(uint32_t);

    std::vector<std::vector<uint32_t>> bufs(
        worldSize, std::vector<uint32_t>(dataCount, 0));

    // Fill this rank's send buffer with its unique pattern.
    for (size_t i = 0; i < dataCount; ++i)
        bufs[rank][i] = static_cast<uint32_t>(rank * dataCount + i);

    std::cout << "[PEEL-AG] Rank " << rank << ": buffer[" << rank
              << "] filled with pattern (rank*" << dataCount << " + i).  "
              << "All other buffers zeroed.\n"
              << "[PEEL-AG] Buffer size per rank: "
              << dataBytes / 1024 << " KB  |  total allgather: "
              << (dataBytes * worldSize) / (1024 * 1024) << " MB\n";

    // =========================================================================
    // Step 5: Allgather — N sequential broadcasts
    //
    // For each sender r:
    //   contexts[r]->broadcast(r, bufs[r].data(), dataBytes)
    //
    // After all N iterations, bufs[r] on every rank contains exactly what
    // rank r filled in Step 4.
    //
    // Why sequential here: this is a correctness test; the stop-and-wait ACK
    // protocol inside each transport already handles ordering within one
    // broadcast.  Parallel broadcasts across contexts would also work (their
    // port ranges don't overlap), but sequential is easier to debug.
    // =========================================================================
    std::cout << "\n[PEEL-AG] ====== STEP 5: Allgather (" << worldSize
              << " broadcasts) ======\n";

    auto totalStart = Clock::now();

    for (int r = 0; r < worldSize; ++r) {
        std::cout << "[PEEL-AG] Rank " << rank
                  << ": broadcast from sender " << r << " ...\n";

        auto t0 = Clock::now();
        bool ok = contexts[r]->broadcast(r, bufs[r].data(), dataBytes);
        auto t1 = Clock::now();

        if (!ok) {
            std::cerr << "[PEEL-AG] Rank " << rank
                      << ": ERROR — broadcast failed for sender_rank=" << r << "\n";
            return 1;
        }

        auto us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
        std::cout << "[PEEL-AG] Rank " << rank
                  << ": broadcast[sender=" << r << "] done in "
                  << us / 1000.0 << " ms  ("
                  << static_cast<double>(dataBytes) / (us / 1e6) / (1024.0 * 1024.0)
                  << " MB/s)\n";
    }

    auto totalEnd = Clock::now();

    // =========================================================================
    // Step 6: Timing summary
    // =========================================================================
    auto totalUs       = std::chrono::duration_cast<std::chrono::microseconds>(
                             totalEnd - totalStart).count();
    double totalMs     = totalUs / 1000.0;
    double totalBytes  = static_cast<double>(dataBytes) * worldSize;
    double throughput  = totalBytes / (totalUs / 1e6) / (1024.0 * 1024.0);

    std::cout << "\n[PEEL-AG] ====== STEP 6: Timing ======\n"
              << "[PEEL-AG] Rank " << rank << ": allgather completed in "
              << totalMs << " ms  (aggregate throughput: "
              << throughput << " MB/s over " << worldSize << " broadcasts)\n";

    // =========================================================================
    // Step 7: Verify
    //
    // Every rank checks all N buffers.
    // bufs[r][i] must equal r * dataCount + i.
    // =========================================================================
    std::cout << "\n[PEEL-AG] ====== STEP 7: Verify ======\n";

    bool allOk = true;
    for (int r = 0; r < worldSize; ++r) {
        bool bufOk = true;
        for (size_t i = 0; i < dataCount; ++i) {
            uint32_t expected = static_cast<uint32_t>(r * dataCount + i);
            if (bufs[r][i] != expected) {
                std::cerr << "[PEEL-AG] Rank " << rank
                          << ": FAILED — buf[sender=" << r << "][" << i << "] = "
                          << bufs[r][i] << ", expected " << expected
                          << "  (first mismatch)\n";
                bufOk = false;
                allOk = false;
                break;
            }
        }
        if (bufOk)
            std::cout << "[PEEL-AG] Rank " << rank
                      << ": buf[sender=" << r << "] — OK ✓\n";
    }

    if (allOk)
        std::cout << "\n[PEEL-AG] Rank " << rank
                  << ": SUCCESS — all " << worldSize << " buffers verified ✓\n\n";
    else
        std::cerr << "\n[PEEL-AG] Rank " << rank << ": FAILED\n\n";

    return allOk ? 0 : 1;
}
