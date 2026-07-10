#!/usr/bin/env python3

import argparse
import datetime
import os

import torch
import torch.distributed as dist


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--elements", type=int, default=4096)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--timeout-seconds", type=int, default=60)
    return parser.parse_args()


def expected_tensor(elements: int, iteration: int, root: int) -> torch.Tensor:
    offset = iteration * 1_000_000 + root * 100_000
    return torch.arange(elements, dtype=torch.int64) + offset


def main() -> None:
    args = parse_args()
    if args.elements <= 0:
        raise ValueError("--elements must be positive")
    if args.iterations <= 0:
        raise ValueError("--iterations must be positive")

    torch.set_num_threads(1)
    dist.init_process_group(
        backend="gloo",
        timeout=datetime.timedelta(seconds=args.timeout_seconds),
    )

    rank = dist.get_rank()
    world_size = dist.get_world_size()
    algorithm = os.environ.get("GLOO_BROADCAST_ALGORITHM", "default")

    try:
        for iteration in range(args.iterations):
            root = iteration % world_size
            if rank == root:
                tensor = expected_tensor(args.elements, iteration, root)
            else:
                tensor = torch.full((args.elements,), -1, dtype=torch.int64)

            dist.broadcast(tensor, src=root)

            expected = expected_tensor(args.elements, iteration, root)
            if not torch.equal(tensor, expected):
                mismatch = torch.nonzero(tensor != expected, as_tuple=False)
                first = int(mismatch[0].item()) if mismatch.numel() else -1
                raise RuntimeError(
                    f"rank={rank} iteration={iteration} root={root} "
                    f"first_mismatch={first}"
                )

            dist.barrier()

        print(
            f"PASS rank={rank} world_size={world_size} "
            f"algorithm={algorithm} elements={args.elements} "
            f"iterations={args.iterations}",
            flush=True,
        )
    finally:
        dist.destroy_process_group()


if __name__ == "__main__":
    main()
