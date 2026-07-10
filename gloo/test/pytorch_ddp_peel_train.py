#!/usr/bin/env python3

import argparse
import hashlib
import os
import socket
from datetime import timedelta

import torch
import torch.distributed as dist
from torch import nn
from torch.nn.parallel import DistributedDataParallel as DDP


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--steps", type=int, default=6)
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--input-size", type=int, default=16)
    parser.add_argument("--hidden-size", type=int, default=32)
    parser.add_argument("--classes", type=int, default=4)
    parser.add_argument("--learning-rate", type=float, default=0.05)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument("--seed", type=int, default=20260710)
    return parser.parse_args()


def flatten_parameters(model: nn.Module) -> torch.Tensor:
    return torch.cat([parameter.detach().reshape(-1) for parameter in model.parameters()])


def tensor_digest(tensor: torch.Tensor) -> str:
    data = tensor.detach().contiguous().cpu().numpy().tobytes()
    return hashlib.sha256(data).hexdigest()[:16]


def main() -> None:
    args = parse_args()
    rank = int(os.environ["RANK"])
    world_size = int(os.environ["WORLD_SIZE"])

    torch.set_num_threads(1)
    torch.manual_seed(args.seed)

    dist.init_process_group(
        backend="gloo",
        timeout=timedelta(seconds=args.timeout_seconds),
    )

    algorithm = os.environ.get("GLOO_ALLREDUCE_ALGORITHM", "default")
    if rank == 0:
        print(
            f"START host={socket.gethostname()} world_size={world_size} "
            f"allreduce={algorithm}"
        )

    probe = torch.tensor([float(rank + 1)], dtype=torch.float32)
    dist.all_reduce(probe, op=dist.ReduceOp.SUM)
    expected_probe = world_size * (world_size + 1) / 2
    if probe.item() != expected_probe:
        raise RuntimeError(
            f"rank {rank}: allreduce probe={probe.item()} expected={expected_probe}"
        )
    print(f"ALLREDUCE_PROBE_PASS rank={rank} value={probe.item():.1f}")

    model = nn.Sequential(
        nn.Linear(args.input_size, args.hidden_size),
        nn.ReLU(),
        nn.Linear(args.hidden_size, args.classes),
    )
    ddp_model = DDP(
        model,
        broadcast_buffers=False,
        bucket_cap_mb=100,
        find_unused_parameters=False,
    )
    optimizer = torch.optim.SGD(ddp_model.parameters(), lr=args.learning_rate)
    criterion = nn.CrossEntropyLoss()

    initial_parameters = flatten_parameters(ddp_model.module).clone()

    generator = torch.Generator().manual_seed(args.seed + 1000 + rank)
    inputs = torch.randn(
        args.batch_size,
        args.input_size,
        generator=generator,
    )
    labels = torch.randint(
        0,
        args.classes,
        (args.batch_size,),
        generator=generator,
    )

    first_loss = None
    final_loss = None
    for step in range(args.steps):
        optimizer.zero_grad(set_to_none=True)
        outputs = ddp_model(inputs)
        loss = criterion(outputs, labels)
        loss.backward()
        optimizer.step()

        current_loss = float(loss.detach())
        if first_loss is None:
            first_loss = current_loss
        final_loss = current_loss
        print(f"TRAIN rank={rank} step={step} loss={current_loss:.6f}")

    final_parameters = flatten_parameters(ddp_model.module)
    parameter_change = torch.linalg.vector_norm(
        final_parameters - initial_parameters
    ).item()
    if parameter_change == 0.0:
        raise RuntimeError(f"rank {rank}: model parameters did not change")

    gathered = [torch.empty_like(final_parameters) for _ in range(world_size)]
    dist.all_gather(gathered, final_parameters)
    max_difference = max(
        torch.max(torch.abs(gathered[0] - other)).item()
        for other in gathered[1:]
    ) if world_size > 1 else 0.0

    if max_difference > 1e-6:
        raise RuntimeError(
            f"rank {rank}: model parameters diverged; max difference={max_difference}"
        )

    print(
        f"TRAINING_PASS rank={rank} first_loss={first_loss:.6f} "
        f"final_loss={final_loss:.6f} parameter_change={parameter_change:.6f} "
        f"parameter_sum={final_parameters.sum().item():.9f} "
        f"parameter_norm={torch.linalg.vector_norm(final_parameters).item():.9f} "
        f"digest={tensor_digest(final_parameters)}"
    )

    dist.barrier()
    if rank == 0:
        print(
            f"DDP_PEEL_VERIFICATION_PASS world_size={world_size} "
            f"steps={args.steps} max_parameter_difference={max_difference:.3e}"
        )
    dist.destroy_process_group()


if __name__ == "__main__":
    main()
