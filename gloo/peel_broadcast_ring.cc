#include "gloo/peel_broadcast_ring.h"

#include <stdexcept>

namespace gloo {

void peel_broadcast_ring(PeelBroadcastRingOptions& opts) {
  if (!opts.tcpContext) {
    throw std::runtime_error("peel_broadcast_ring: tcpContext is null");
  }

  if (!opts.tcpContext->isPeelReady()) {
    throw std::runtime_error(
        "peel_broadcast_ring: Peel not initialized. Call enablePeel() first.");
  }

  if (!opts.ptr || opts.size == 0) {
    throw std::runtime_error("peel_broadcast_ring: invalid buffer");
  }

  if (opts.root < 0 || opts.root >= opts.tcpContext->size) {
    throw std::runtime_error("peel_broadcast_ring: invalid root rank");
  }

  bool success =
      opts.tcpContext->peelBroadcastRing(opts.root, opts.ptr, opts.size);
  if (!success) {
    throw std::runtime_error("peel_broadcast_ring: broadcast failed");
  }
}

void peel_broadcast_ring(
    transport::tcp::Context* tcpContext,
    int root,
    void* data,
    size_t size) {
  PeelBroadcastRingOptions opts;
  opts.tcpContext = tcpContext;
  opts.root = root;
  opts.ptr = data;
  opts.size = size;
  peel_broadcast_ring(opts);
}

} // namespace gloo