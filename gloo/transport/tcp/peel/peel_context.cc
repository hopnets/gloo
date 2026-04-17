// gloo/transport/tcp/peel/peel_context.cc

#include "peel_context.h"
#include "peel_tree.h"

#include <cstring>
#include <iostream>

namespace gloo {
namespace transport {
namespace tcp {
namespace peel {

PeelContext::PeelContext(const PeelContextConfig& config)
    : config_(config) {}

PeelContext::~PeelContext() {
    cleanup();
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Fills a PeelTransportConfig with the fields that are common across all
// subtrees (identity, multicast group, timing, buffer sizes).
static PeelTransportConfig makeBaseTransportConfig(const PeelContextConfig& c) {
    PeelTransportConfig tc;
    tc.rank           = c.rank;
    tc.world_size     = c.world_size;
    tc.mcast_group    = c.mcast_group;
    tc.iface_name     = c.iface_name;
    tc.ttl            = c.ttl;
    tc.rcvbuf         = c.rcvbuf;
    tc.rto_ms         = c.rto_ms;
    tc.timeout_ms     = c.timeout_ms;
    tc.max_chunk_size = c.max_chunk_size;
    tc.dscp           = c.dscp;
    tc.sender_rank    = c.sender_rank;
    return tc;
}

// ---------------------------------------------------------------------------
// init() — topology-aware path
// ---------------------------------------------------------------------------

bool PeelContext::init() {
    // Fall back to flat single-transport mode when no topology information
    // is available. This preserves the pre-PeelTree behaviour and keeps the
    // test binary working without a topology file.
    if (config_.topology_file.empty() || config_.peer_ips.empty()) {
        std::cout << "peel_context[" << config_.rank
                  << "]: no topology/peer_ips — using flat single transport\n";
        return initSingleTransport();
    }

    // ── Build spanning tree ──────────────────────────────────────────────────
    PeelTreeConfig treeConfig;
    treeConfig.rank          = config_.rank;
    treeConfig.world_size    = config_.world_size;
    treeConfig.sender_rank   = config_.sender_rank;
    treeConfig.peer_ips      = config_.peer_ips;
    treeConfig.base_port     = config_.base_port;
    treeConfig.topology_file = config_.topology_file;

    PeelTree tree(treeConfig);

    if (!tree.loadTopology(config_.topology_file)) {
        std::cerr << "peel_context[" << config_.rank
                  << "]: topology load failed\n";
        return false;
    }
    if (!tree.build()) {
        std::cerr << "peel_context[" << config_.rank
                  << "]: tree build failed\n";
        return false;
    }

    const std::vector<PeelSubtree>& subtrees = tree.subtrees();
    if (subtrees.empty()) {
        std::cerr << "peel_context[" << config_.rank
                  << "]: tree produced no subtrees\n";
        return false;
    }

    // ── Create one transport per relevant subtree ────────────────────────────
    // Rank 0 (source) sends on every subtree → creates a transport for each.
    // Every other rank receives on exactly one subtree → creates one transport.
    for (const PeelSubtree& sub : subtrees) {
        if (config_.rank != config_.sender_rank && sub.subtree_id != tree.mySubtreeId())
            continue;

        PeelTransportConfig tc  = makeBaseTransportConfig(config_);
        tc.base_port            = sub.base_port;
        tc.participant_ranks    = sub.receiver_ranks;
        tc.use_cidr_rules_mac   = true;
        std::memcpy(tc.cidr_rules_mac, sub.cidr_rules_mac, 6);

        auto t = std::make_unique<PeelTransport>(tc);
        if (!t->init()) {
            std::cerr << "peel_context[" << config_.rank
                      << "]: transport init failed for subtree "
                      << sub.subtree_id << "\n";
            return false;
        }
        transports_.push_back(std::move(t));
    }

    if (transports_.empty()) {
        std::cerr << "peel_context[" << config_.rank
                  << "]: no transports created (rank not in any subtree?)\n";
        return false;
    }

    // ── Wire up PeelBroadcast ────────────────────────────────────────────────
    std::vector<PeelTransport*> raw;
    raw.reserve(transports_.size());
    for (auto& t : transports_) raw.push_back(t.get());
    broadcast_ = std::make_unique<PeelBroadcast>(std::move(raw));

    std::cout << "peel_context[" << config_.rank << "]: initialized ("
              << transports_.size() << " transport(s) across "
              << subtrees.size() << " subtree(s))\n";
    return true;
}

// ---------------------------------------------------------------------------
// initSingleTransport() — flat fallback (no topology file)
// ---------------------------------------------------------------------------

bool PeelContext::initSingleTransport() {
    PeelTransportConfig tc = makeBaseTransportConfig(config_);
    tc.base_port = config_.base_port;
    // participant_ranks left empty → PeelFullMesh uses all world_size ranks.
    // use_cidr_rules_mac stays false → standard derived multicast MAC.

    auto t = std::make_unique<PeelTransport>(tc);
    if (!t->init()) {
        std::cerr << "peel_context[" << config_.rank
                  << "]: transport init failed\n";
        return false;
    }

    std::vector<PeelTransport*> raw = { t.get() };
    transports_.push_back(std::move(t));
    broadcast_ = std::make_unique<PeelBroadcast>(std::move(raw));

    std::cout << "peel_context[" << config_.rank
              << "]: initialized (1 flat transport)\n";
    return true;
}

bool PeelContext::initRing() {
    ring_transports_.clear();
    broadcast_ring_.reset();

    std::vector<PeelRingHop> hops;
    hops.reserve(config_.world_size);

    if (config_.world_size <= 1) {
        broadcast_ring_ = std::make_unique<PeelBroadcastRing>(std::move(hops));
        return true;
    }

    for (int sender = 0; sender < config_.world_size; ++sender) {
        const int receiver = (sender + 1) % config_.world_size;

        PeelRingHop hop;
        hop.sender = sender;
        hop.receiver = receiver;

        // Only the sender and receiver of this logical edge participate in the
        // corresponding 2-rank Peel mesh. Other ranks keep a null transport for
        // this hop and skip it in PeelBroadcastRing::run().
        if (config_.rank == sender || config_.rank == receiver) {
            PeelTransportConfig tc = makeBaseTransportConfig(config_);
            tc.participant_ranks = {sender, receiver};
            tc.sender_rank = sender;
            tc.base_port = static_cast<uint16_t>(
                config_.base_port + sender * config_.world_size);

            auto t = std::make_unique<PeelTransport>(tc);
            if (!t->init()) {
                std::cerr << "peel_context[" << config_.rank
                          << "]: ring hop transport init failed for "
                          << sender << " -> " << receiver << "\n";
                return false;
            }

            hop.transport = t.get();
            ring_transports_.push_back(std::move(t));
        }

        hops.push_back(hop);
    }

    broadcast_ring_ = std::make_unique<PeelBroadcastRing>(std::move(hops));

    std::cout << "peel_context[" << config_.rank
              << "]: initialized ring transports ("
              << ring_transports_.size() << " local hop transport(s) out of "
              << config_.world_size << " ring edge(s))\n";
    return true;
}
bool PeelContext::broadcastRing(int root, void* data, size_t size) {
    if (config_.world_size <= 1) return true;
    if (!broadcast_ring_) return false;
    return broadcast_ring_->run(root, data, size);
}
// ---------------------------------------------------------------------------
// isReady / broadcast / cleanup
// ---------------------------------------------------------------------------

bool PeelContext::isReady() const {
    bool haveTransport = false;

    if (!transports_.empty()) {
        haveTransport = true;
        for (const auto& t : transports_)
            if (!t->isReady()) return false;
    }

    if (!ring_transports_.empty()) {
        haveTransport = true;
        for (const auto& t : ring_transports_)
            if (!t->isReady()) return false;
    }

    return haveTransport || config_.world_size <= 1;
}

bool PeelContext::broadcast(int root, void* data, size_t size) {
    if (!broadcast_) return false;
    return broadcast_->run(root, data, size);
}

void PeelContext::cleanup() {
    // Reset broadcast first: it holds raw pointers into transports_.
    broadcast_.reset();
    for (auto& t : transports_)
        t->cleanup();
    transports_.clear();
	
	broadcast_ring_.reset();
    for (auto& t : ring_transports_)
        t->cleanup();
    ring_transports_.clear();
}

} // namespace peel
} // namespace tcp
} // namespace transport
} // namespace gloo
