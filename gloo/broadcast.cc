/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "gloo/broadcast.h"

#include <algorithm>
#include <cerrno>
#include <climits>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>

#include "gloo/common/logging.h"
#include "gloo/config.h"
#include "gloo/math.h"
#include "gloo/types.h"

#if GLOO_HAVE_TRANSPORT_PEEL
#include "gloo/transport/peel/peel_context.h"
#endif

namespace gloo {
namespace {

enum class BroadcastAlgorithm {
  DEFAULT,
  RING,
  PEEL_BROADCAST_RING,
};

std::string getEnv(const char* name) {
  const char* value = std::getenv(name);
  return value == nullptr ? std::string() : std::string(value);
}

long parseLongEnv(
    const char* name,
    long defaultValue,
    long minValue,
    long maxValue) {
  const auto value = getEnv(name);
  if (value.empty()) {
    return defaultValue;
  }

  errno = 0;
  char* end = nullptr;
  const long parsed = std::strtol(value.c_str(), &end, 10);
  GLOO_ENFORCE(
      errno == 0 && end != value.c_str() && *end == '\0',
      "Invalid integer value for ",
      name,
      ": ",
      value);
  GLOO_ENFORCE(
      parsed >= minValue && parsed <= maxValue,
      name,
      " must be between ",
      minValue,
      " and ",
      maxValue,
      "; got ",
      parsed);
  return parsed;
}

BroadcastAlgorithm getBroadcastAlgorithm() {
  const auto value = getEnv("GLOO_BROADCAST_ALGORITHM");
  if (value.empty() || value == "default" || value == "broadcast") {
    return BroadcastAlgorithm::DEFAULT;
  }
  if (value == "ring" || value == "broadcast_ring") {
    return BroadcastAlgorithm::RING;
  }
  if (value == "peel" || value == "peel_broadcast_ring") {
    return BroadcastAlgorithm::PEEL_BROADCAST_RING;
  }

  GLOO_ENFORCE(false, "Unsupported GLOO_BROADCAST_ALGORITHM: ", value);
  return BroadcastAlgorithm::DEFAULT;
}

#if GLOO_HAVE_TRANSPORT_PEEL

std::string getPeelInterface() {
  auto iface = getEnv("GLOO_PEEL_IFACE");
  if (!iface.empty()) {
    return iface;
  }

  iface = getEnv("GLOO_SOCKET_IFNAME");
  const auto separator = iface.find(',');
  if (separator != std::string::npos) {
    iface.resize(separator);
  }
  return iface;
}

class PeelBroadcastRingRuntime {
 public:
  static PeelBroadcastRingRuntime& instance() {
    static PeelBroadcastRingRuntime runtime;
    return runtime;
  }

  void run(
      const std::shared_ptr<Context>& glooContext,
      int root,
      void* data,
      size_t size,
      std::chrono::milliseconds timeout) {
    initialize(glooContext, timeout);

    std::unique_lock<std::mutex> operationLock(
        operationMutex_, std::try_to_lock);
    GLOO_ENFORCE(
        operationLock.owns_lock(),
        "Concurrent peel_broadcast_ring operations are not supported");
    GLOO_ENFORCE(
        peelContext_->broadcastRing(root, data, size),
        "peel_broadcast_ring failed");
  }

 private:
  void initialize(
      const std::shared_ptr<Context>& glooContext,
      std::chrono::milliseconds timeout) {
    std::lock_guard<std::mutex> lock(initializationMutex_);

    if (peelContext_) {
      GLOO_ENFORCE_EQ(rank_, glooContext->rank);
      GLOO_ENFORCE_EQ(worldSize_, glooContext->size);
      return;
    }

    const auto iface = getPeelInterface();
    GLOO_ENFORCE(
        !iface.empty(),
        "peel_broadcast_ring requires GLOO_PEEL_IFACE or "
        "GLOO_SOCKET_IFNAME");

    const auto basePort = parseLongEnv(
        "GLOO_PEEL_BASE_PORT", 50000, 1, 65535);
    const long maxPort =
        basePort + static_cast<long>(glooContext->size) * glooContext->size - 1;
    GLOO_ENFORCE(
        maxPort <= 65535,
        "GLOO_PEEL_BASE_PORT and world size require ports through ",
        maxPort,
        ", which exceeds 65535");

    const auto timeoutCount = timeout.count();
    const long defaultTimeout = timeoutCount > 0
        ? std::min<long long>(timeoutCount, INT_MAX)
        : 300000;

    transport::peel::PeelContextConfig config;
    config.rank = glooContext->rank;
    config.world_size = glooContext->size;
    config.mcast_group = getEnv("GLOO_PEEL_MCAST_GROUP");
    if (config.mcast_group.empty()) {
      config.mcast_group = "239.255.0.1";
    }
    config.base_port = static_cast<uint16_t>(basePort);
    config.iface_name = iface;
    config.ttl = static_cast<int>(
        parseLongEnv("GLOO_PEEL_TTL", 64, 1, 255));
    config.rto_ms = static_cast<int>(
        parseLongEnv("GLOO_PEEL_RTO_MS", 500, 1, INT_MAX));
    config.timeout_ms = static_cast<int>(parseLongEnv(
        "GLOO_PEEL_TIMEOUT_MS", defaultTimeout, 1, INT_MAX));
    config.rcvbuf = static_cast<int>(parseLongEnv(
        "GLOO_PEEL_RCVBUF", 32 * 1024 * 1024, 1, INT_MAX));
    config.max_chunk_size = static_cast<size_t>(parseLongEnv(
        "GLOO_PEEL_MAX_PAYLOAD", 0, 0, INT_MAX));
    config.dscp = static_cast<uint8_t>(
        parseLongEnv("GLOO_PEEL_DSCP", 7, 0, 63));

    auto context = std::make_unique<transport::peel::PeelContext>(config);
    GLOO_ENFORCE(context->initRing(), "PeelContext ring initialization failed");

    rank_ = glooContext->rank;
    worldSize_ = glooContext->size;
    peelContext_ = std::move(context);
  }

  std::mutex initializationMutex_;
  std::mutex operationMutex_;
  int rank_ = -1;
  int worldSize_ = -1;
  std::unique_ptr<transport::peel::PeelContext> peelContext_;
};

#endif

} // namespace

void broadcast(BroadcastOptions& opts) {
  const auto algorithm = getBroadcastAlgorithm();
  if (algorithm == BroadcastAlgorithm::RING) {
    broadcast_ring(opts);
    return;
  }

  if (algorithm == BroadcastAlgorithm::PEEL_BROADCAST_RING) {
#if GLOO_HAVE_TRANSPORT_PEEL
    const auto& context = opts.context;
    transport::UnboundBuffer* in = opts.in.get();
    transport::UnboundBuffer* out = opts.out.get();

    GLOO_ENFORCE(opts.elementSize > 0);
    GLOO_ENFORCE(opts.root >= 0 && opts.root < context->size);
    GLOO_ENFORCE(out);

    if (context->rank == opts.root) {
      if (in) {
        GLOO_ENFORCE_EQ(in->size, out->size);
      } else {
        in = out;
      }
    } else {
      GLOO_ENFORCE(!in, "Non-root may not specify input");
      in = out;
    }

    if (context->rank == opts.root && in != out) {
      memcpy(out->ptr, in->ptr, out->size);
    }

    if (context->size == 1 || out->size == 0) {
      return;
    }

    PeelBroadcastRingRuntime::instance().run(
        context, opts.root, out->ptr, out->size, opts.timeout);
    return;
#else
    GLOO_ENFORCE(
        false,
        "peel_broadcast_ring was requested but Gloo was built without Peel");
#endif
  }

  const auto& context = opts.context;
  transport::UnboundBuffer* in = opts.in.get();
  transport::UnboundBuffer* out = opts.out.get();
  const auto slot = Slot::build(kBroadcastSlotPrefix, opts.tag);

  // Sanity checks
  GLOO_ENFORCE(opts.elementSize > 0);
  GLOO_ENFORCE(opts.root >= 0 && opts.root < context->size);
  GLOO_ENFORCE(out);
  if (context->rank == opts.root) {
    if (in) {
      GLOO_ENFORCE_EQ(in->size, out->size);
    } else {
      // Broadcast in place
      in = out;
    }
  } else {
    GLOO_ENFORCE(!in, "Non-root may not specify input");

    // Broadcast in place (for forwarding)
    in = out;
  }

  // Map rank to new rank where root process has rank 0.
  const size_t vsize = context->size;
  const size_t vrank = (context->rank + vsize - opts.root) % vsize;
  const size_t dim = log2ceil(vsize);

  // Track number of pending send operations.
  // Send operations can complete asynchronously because there is dependency
  // between iterations. This unlike recv operations that must complete
  // before any send operations can be queued.
  size_t numSends = 0;

  // Create mask with all 1's where we progressively set bits to 0
  // starting with the LSB. When the mask applied to the virtual rank
  // equals 0 we know the process must participate. This results in
  // exponential participation starting with virtual ranks 0 and 1.
  size_t mask = (1 << dim) - 1;

  for (size_t i = 0; i < dim; i++) {
    // Clear bit `i`. In the first iteration, virtual ranks 0 and 1 participate.
    // In the second iteration 0, 1, 2, and 3 participate, and so on.
    mask ^= (1 << i);
    if ((vrank & mask) != 0) {
      continue;
    }

    // The virtual rank of the peer in this iteration has opposite bit `i`.
    auto vpeer = vrank ^ (1 << i);
    if (vpeer >= vsize) {
      continue;
    }

    // Map virtual rank of peer to actual rank of peer.
    auto peer = (vpeer + opts.root) % vsize;
    if ((vrank & (1 << i)) == 0) {
      in->send(peer, slot);
      numSends++;
    } else {
      out->recv(peer, slot);
      out->waitRecv(opts.timeout);
    }
  }

  // Copy local input to output if applicable.
  if (context->rank == opts.root && in != out) {
    memcpy(out->ptr, in->ptr, out->size);
  }

  // Wait on pending sends.
  for (auto i = 0; i < numSends; i++) {
    in->waitSend(opts.timeout);
  }
}

} // namespace gloo
