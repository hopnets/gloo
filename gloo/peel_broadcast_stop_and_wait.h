#pragma once

#include <cstddef>
#include <vector>

#include "gloo/transport/tcp/context.h"
#include "gloo/transport/tcp/peel/peel_context.h"

namespace gloo {

struct PeelBroadcastStopAndWaitOptions {
  transport::tcp::Context* tcpContext = nullptr;
  int root = 0;
  void* ptr = nullptr;
  size_t size = 0;

  template <typename T>
  void setOutput(T* p, size_t count) {
    ptr = static_cast<void*>(p);
    size = count * sizeof(T);
  }

  template <typename T>
  void setOutput(std::vector<T>& v) {
    setOutput(v.data(), v.size());
  }
};

void peel_broadcast_stop_and_wait(PeelBroadcastStopAndWaitOptions& opts);

void peel_broadcast_stop_and_wait(
    transport::tcp::Context* tcpContext,
    int root,
    void* data,
    size_t size);

template <typename T>
void peel_broadcast_stop_and_wait(
    transport::tcp::Context* tcpContext,
    int root,
    std::vector<T>& data) {
  peel_broadcast_stop_and_wait(
      tcpContext, root, data.data(), data.size() * sizeof(T));
}

} // namespace gloo
