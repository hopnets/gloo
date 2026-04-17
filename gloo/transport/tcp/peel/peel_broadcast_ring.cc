#include "peel_broadcast_ring.h"

#include <iostream>

namespace gloo {
namespace transport {
namespace tcp {
namespace peel {

PeelBroadcastRing::PeelBroadcastRing(std::vector<PeelRingHop> hops)
    : hops_(std::move(hops)) {}

bool PeelBroadcastRing::run(int root, void* data, size_t size) {
    if (hops_.empty()) {
        std::cerr << "peel_broadcast_ring: no hop transports\n";
        return false;
    }

    // Validate all hops before starting.
    for (const auto& hop : hops_) {
        if (!hop.transport || !hop.transport->isReady()) {
            std::cerr << "peel_broadcast_ring: a hop transport is not ready\n";
            return false;
        }
    }

    // Execute the ring in virtual-root order.
    const int worldSize = static_cast<int>(hops_.size());
    for (int step = 0; step < worldSize - 1; ++step) {
        const int idx = (root + step) % worldSize;
        const auto& hop = hops_[idx];

        if (!hop.transport->broadcast(hop.sender, data, size)) {
		    std::cerr << "peel_broadcast_ring: hop failed: "
                      << hop.sender << " -> " << hop.receiver << "\n";
            return false;
        }
    }

    return true;
}

} // namespace peel
} // namespace tcp
} // namespace transport
} // namespace gloo