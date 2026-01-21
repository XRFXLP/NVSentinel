#!/usr/bin/env python3
"""
NCCL All-Reduce Benchmark for Preflight Checks

Tests multi-node GPU communication bandwidth using NCCL backend.
Reports results to stdout in JSON format for parsing.

Environment Variables (set by torchrun):
  - RANK: Global rank of this process
  - LOCAL_RANK: Local GPU index on this node
  - WORLD_SIZE: Total number of processes

Environment Variables (set by preflight checker):
  - BW_THRESHOLD_GBPS: Minimum acceptable bus bandwidth (default: 100)
  - MESSAGE_SIZES: Comma-separated message sizes to test (default: 4GB,8GB)
"""
import os
import sys
import json
import time
import torch
import torch.distributed as dist


def format_size(size_bytes):
    """Format bytes to human readable string."""
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} TB"


def parse_size(size_str):
    """Parse size string like '4GB', '4G', or '4294967296' to bytes."""
    size_str = size_str.strip().upper()
    # Support both short (G, M, K) and long (GB, MB, KB) suffixes
    multipliers = [
        ('TB', 1024**4),
        ('GB', 1024**3),
        ('MB', 1024**2),
        ('KB', 1024),
        ('T', 1024**4),
        ('G', 1024**3),
        ('M', 1024**2),
        ('K', 1024),
        ('B', 1),
    ]
    for suffix, mult in multipliers:
        if size_str.endswith(suffix):
            num_part = size_str[:-len(suffix)].strip()
            if num_part:
                return int(float(num_part) * mult)
    # No suffix - assume bytes
    return int(size_str)


def benchmark_allreduce(size_bytes, iters=20, warmup=5):
    """
    Run all-reduce benchmark with given data size.
    
    Args:
        size_bytes: Size of tensor in bytes
        iters: Number of benchmark iterations
        warmup: Number of warmup iterations
        
    Returns:
        Tuple of (avg_time, algorithm_bandwidth, bus_bandwidth)
    """
    local_rank = int(os.environ.get("LOCAL_RANK", 0))
    num_elements = size_bytes // 4  # float32 = 4 bytes
    
    tensor = torch.randn(num_elements, dtype=torch.float32, 
                        device=f"cuda:{local_rank}")
    
    # Warmup iterations (not timed)
    for _ in range(warmup):
        dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
    torch.cuda.synchronize()
    
    # Timed benchmark iterations
    start = time.perf_counter()
    for _ in range(iters):
        dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
    torch.cuda.synchronize()
    elapsed = time.perf_counter() - start
    
    # Calculate bandwidth metrics
    world_size = dist.get_world_size()
    algo_bw = (size_bytes * iters) / elapsed / 1e9  # GB/s
    # Bus bandwidth accounts for the ring/tree algorithm overhead
    bus_bw = algo_bw * (2 * (world_size - 1) / world_size)
    
    return elapsed / iters, algo_bw, bus_bw


def main():
    # Debug: Print NCCL env vars before init
    rank_for_debug = int(os.environ.get("RANK", 0))
    if rank_for_debug == 0:
        print("=== NCCL Environment Variables ===", file=sys.stderr)
        for k, v in sorted(os.environ.items()):
            if k.startswith("NCCL_") or "IB" in k:
                print(f"  {k}={v}", file=sys.stderr)
        print("===================================", file=sys.stderr)
    
    # Get configuration from environment
    threshold = float(os.environ.get("BW_THRESHOLD_GBPS", "100"))
    size_strs = os.environ.get("MESSAGE_SIZES", "4GB,8GB").split(",")
    test_sizes = [parse_size(s) for s in size_strs]
    
    # Initialize distributed process group with NCCL backend
    dist.init_process_group(backend="nccl")
    
    rank = dist.get_rank()
    world_size = dist.get_world_size()
    local_rank = int(os.environ.get("LOCAL_RANK", 0))
    num_nodes = world_size // int(os.environ.get("NPROCS_PER_NODE", 8))
    
    torch.cuda.set_device(local_rank)
    
    results = {
        "world_size": world_size,
        "num_nodes": num_nodes,
        "gpus_per_node": world_size // num_nodes if num_nodes > 0 else world_size,
        "threshold_gbps": threshold,
        "tests": [],
        "passed": True,
        "min_bus_bw": float('inf'),
    }
    
    if rank == 0:
        print(f"\n{'='*70}", file=sys.stderr)
        print(f"NCCL All-Reduce Preflight Benchmark", file=sys.stderr)
        print(f"{'='*70}", file=sys.stderr)
        print(f"Nodes: {num_nodes}, GPUs per node: {results['gpus_per_node']}, Total: {world_size}", file=sys.stderr)
        print(f"Threshold: {threshold} GB/s bus bandwidth", file=sys.stderr)
        print(f"{'='*70}", file=sys.stderr)
        print(f"{'Size':>12} {'Time (ms)':>12} {'AlgoBW (GB/s)':>14} {'BusBW (GB/s)':>14} {'Status':>10}", file=sys.stderr)
    
    for size in test_sizes:
        avg_time, algo_bw, bus_bw = benchmark_allreduce(size)
        
        passed = bus_bw >= threshold
        status = "PASS" if passed else "FAIL"
        
        test_result = {
            "size_bytes": size,
            "size_human": format_size(size),
            "time_ms": avg_time * 1000,
            "algo_bw_gbps": algo_bw,
            "bus_bw_gbps": bus_bw,
            "passed": passed,
        }
        results["tests"].append(test_result)
        
        if bus_bw < results["min_bus_bw"]:
            results["min_bus_bw"] = bus_bw
        
        if not passed:
            results["passed"] = False
        
        if rank == 0:
            print(f"{format_size(size):>12} {avg_time*1000:>12.2f} "
                  f"{algo_bw:>14.2f} {bus_bw:>14.2f} {status:>10}", file=sys.stderr)
    
    if rank == 0:
        print(f"{'='*70}", file=sys.stderr)
        overall = "PASSED" if results["passed"] else "FAILED"
        print(f"Overall: {overall} (min bus BW: {results['min_bus_bw']:.2f} GB/s, threshold: {threshold} GB/s)", file=sys.stderr)
        print(f"{'='*70}\n", file=sys.stderr)
        
        # Output JSON result to stdout for parsing
        print(json.dumps(results))
    
    dist.destroy_process_group()
    
    # Exit with appropriate code
    sys.exit(0 if results["passed"] else 1)


if __name__ == "__main__":
    main()

