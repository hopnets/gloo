#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <utility>
#include <vector>

#include "gloo/algorithm.h"
#include "gloo/transport/peel/peel_broadcast_ring.h"

namespace gloo {
namespace transport {
namespace peel {

class PeelAllreduceRingGeneric {
 public:
  using ReduceFunction =
      std::function<void(void*, const void*, const void*, size_t)>;

  PeelAllreduceRingGeneric(
      int rank,
      std::vector<PeelRingHop> hops,
      std::vector<void*> ptrs,
      size_t elements,
      size_t elementSize,
      ReduceFunction reduce)
      : rank_(rank),
        hops_(std::move(hops)),
        ptrs_(std::move(ptrs)),
        elements_(elements),
        bytes_(elements_ * elementSize),
        reduce_(std::move(reduce)),
        inbox_(bytes_),
        outbox_(bytes_) {}

  bool run() {
    if (ptrs_.empty()) {
      std::cerr << "peel_allreduce_ring: no input pointers\n";
      return false;
    }
    if (!reduce_) {
      std::cerr << "peel_allreduce_ring: reduction function is not set\n";
      return false;
    }
    if (elements_ == 0 || bytes_ == 0) {
      return true;
    }
    for (auto* ptr : ptrs_) {
      if (ptr == nullptr) {
        std::cerr << "peel_allreduce_ring: null input pointer\n";
        return false;
      }
    }

    for (size_t i = 1; i < ptrs_.size(); ++i) {
      reduce_(ptrs_[0], ptrs_[0], ptrs_[i], elements_);
    }

    const int worldSize = static_cast<int>(hops_.size());
    if (worldSize <= 1) {
      copyResultToLocalPointers();
      return true;
    }

    if (rank_ < 0 || rank_ >= worldSize) {
      std::cerr << "peel_allreduce_ring: invalid local rank " << rank_
                << " for world size " << worldSize << "\n";
      return false;
    }

    const int sendHopIdx = rank_;
    const int recvHopIdx = (rank_ + worldSize - 1) % worldSize;
    auto* sendTransport = hops_[sendHopIdx].transport;
    auto* recvTransport = hops_[recvHopIdx].transport;

    if (sendTransport == nullptr || recvTransport == nullptr) {
      std::cerr << "peel_allreduce_ring: missing local hop transport(s) for rank "
                << rank_ << " sendHop=" << sendHopIdx
                << " recvHop=" << recvHopIdx << "\n";
      return false;
    }
    if (!sendTransport->isReady() || !recvTransport->isReady()) {
      std::cerr << "peel_allreduce_ring: transport not ready for rank "
                << rank_ << "\n";
      return false;
    }

    std::memcpy(outbox_.data(), ptrs_[0], bytes_);

    const int numRounds = worldSize - 1;
    for (int round = 0; round < numRounds; ++round) {
      recvTransport->submitWork(
          hops_[recvHopIdx].sender, inbox_.data(), bytes_);
      sendTransport->submitWork(
          hops_[sendHopIdx].sender, outbox_.data(), bytes_);

      if (!recvTransport->waitResult()) {
        std::cerr << "peel_allreduce_ring: recv hop failed in round " << round
                  << ": " << hops_[recvHopIdx].sender << " -> "
                  << hops_[recvHopIdx].receiver << "\n";
        return false;
      }

      reduce_(ptrs_[0], ptrs_[0], inbox_.data(), elements_);

      if (!sendTransport->waitResult()) {
        std::cerr << "peel_allreduce_ring: send hop failed in round " << round
                  << ": " << hops_[sendHopIdx].sender << " -> "
                  << hops_[sendHopIdx].receiver << "\n";
        return false;
      }

      if (round < numRounds - 1) {
        std::memcpy(outbox_.data(), inbox_.data(), bytes_);
      }
    }

    copyResultToLocalPointers();
    return true;
  }

 private:
  void copyResultToLocalPointers() {
    for (size_t i = 1; i < ptrs_.size(); ++i) {
      std::memcpy(ptrs_[i], ptrs_[0], bytes_);
    }
  }

  int rank_;
  std::vector<PeelRingHop> hops_;
  std::vector<void*> ptrs_;
  size_t elements_;
  size_t bytes_;
  ReduceFunction reduce_;
  std::vector<uint8_t> inbox_;
  std::vector<uint8_t> outbox_;
};

template <typename T>
class PeelAllreduceRing {
 public:
  PeelAllreduceRing(
      int rank,
      std::vector<PeelRingHop> hops,
      const std::vector<T*>& ptrs,
      int count,
      const ReductionFunction<T>* fn = ReductionFunction<T>::sum)
      : impl_(
            rank,
            std::move(hops),
            toVoidPointers(ptrs),
            static_cast<size_t>(count),
            sizeof(T),
            [fn](
                void* output,
                const void* input1,
                const void* input2,
                size_t elements) {
              if (output != input1) {
                std::memcpy(output, input1, elements * sizeof(T));
              }
              fn->call(
                  static_cast<T*>(output),
                  static_cast<const T*>(input2),
                  elements);
            }) {}

  bool run() {
    return impl_.run();
  }

 private:
  static std::vector<void*> toVoidPointers(const std::vector<T*>& ptrs) {
    std::vector<void*> result;
    result.reserve(ptrs.size());
    for (auto* ptr : ptrs) {
      result.push_back(static_cast<void*>(ptr));
    }
    return result;
  }

  PeelAllreduceRingGeneric impl_;
};

} // namespace peel
} // namespace transport
} // namespace gloo
