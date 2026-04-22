#include "gloo/peel_broadcast_stop_and_wait.h"

#include <stdexcept>

namespace gloo {

void peel_broadcast_stop_and_wait(PeelBroadcastStopAndWaitOptions& opts) {
  if (!opts.tcpContext) {
    throw std::runtime_error(
        "peel_broadcast_stop_and_wait: tcpContext is null");
  }

  if (!opts.tcpContext->isPeelReady()) {
    throw std::runtime_error(
        "peel_broadcast_stop_and_wait: Peel not initialized. Call enablePeel() first.");
  }

  if (!opts.ptr || opts.size == 0) {
    throw std::runtime_error(
        "peel_broadcast_stop_and_wait: invalid buffer");
  }

  if (opts.root < 0 || opts.root >= opts.tcpContext->size) {
    throw std::runtime_error(
        "peel_broadcast_stop_and_wait: invalid root rank");
  }

  const bool success =
      opts.tcpContext->peelBroadcastStopAndWait(opts.root, opts.ptr, opts.size);
  if (!success) {
    throw std::runtime_error(
        "peel_broadcast_stop_and_wait: broadcast failed");
  }
}

void peel_broadcast_stop_and_wait(
    transport::tcp::Context* tcpContext,
    int root,
    void* data,
    size_t size) {
  PeelBroadcastStopAndWaitOptions opts;
  opts.tcpContext = tcpContext;
  opts.root = root;
  opts.ptr = data;
  opts.size = size;
  peel_broadcast_stop_and_wait(opts);
}

} // namespace gloo
