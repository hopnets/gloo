/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 */

#include "gloo/broadcast.h"

#include <cstring>
#include <string>

#include "gloo/common/logging.h"
#include "gloo/types.h"

namespace gloo {

void broadcast_ring(BroadcastOptions& opts) {
  const auto& context = opts.context;
  transport::UnboundBuffer* in = opts.in.get();
  transport::UnboundBuffer* out = opts.out.get();

  const auto slot = Slot::build(kBroadcastSlotPrefix, opts.tag);

  // Same sanity checks as broadcast().
  GLOO_ENFORCE(opts.elementSize > 0);
  GLOO_ENFORCE(opts.root >= 0 && opts.root < context->size);
  GLOO_ENFORCE(out);

  if (context->rank == opts.root) {
    if (in) {
      GLOO_ENFORCE_EQ(in->size, out->size);
    } else {
      // In-place broadcast on root.
      in = out;
    }
  } else {
    GLOO_ENFORCE(!in, "Non-root may not specify input");

    // Non-root receives into out, then forwards out.
    in = out;
  }

  // Single-rank case: just copy root input to output if needed.
  if (context->size == 1) {
    if (context->rank == opts.root && in != out) {
      memcpy(out->ptr, in->ptr, out->size);
    }
    return;
  }

  // Empty payload: nothing to move.
  if (out->size == 0) {
    return;
  }

  const int size = context->size;
  const int rank = context->rank;

  // Virtual rank maps root to 0, root+1 to 1, ..., wrapping around.
  const int vrank = (rank + size - opts.root) % size;

  // Ring order is:
  //   root -> root+1 -> root+2 -> ... -> root-1
  const int prev = (rank + size - 1) % size;
  const int next = (rank + 1) % size;

  // Validate required ring connections.
  if (vrank > 0) {
    GLOO_ENFORCE(
        context->getPair(prev),
        "missing connection between rank ",
        rank,
        " and previous ring rank ",
        prev);
  }

  if (vrank + 1 < size) {
    GLOO_ENFORCE(
        context->getPair(next),
        "missing connection between rank ",
        rank,
        " and next ring rank ",
        next);
  }

  if (vrank == 0) {
    // Root starts the chain.
    in->send(next, slot);

    // Preserve normal broadcast semantics if root uses separate input/output.
    if (in != out) {
      memcpy(out->ptr, in->ptr, out->size);
    }

    in->waitSend(opts.timeout);
  } else {
    // Everyone else receives from predecessor.
    out->recv(prev, slot);
    out->waitRecv(opts.timeout);

    // Forward unless this is the last virtual rank.
    if (vrank + 1 < size) {
      out->send(next, slot);
      out->waitSend(opts.timeout);
    }
  }
}

} // namespace gloo