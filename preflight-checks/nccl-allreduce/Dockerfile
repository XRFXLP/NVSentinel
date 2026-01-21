# Preflight NCCL All-Reduce Checker
#
# This container runs NCCL all-reduce benchmarks across gang members
# to verify cross-node GPU communication before workloads start.
#
# Build from repository root:
#   docker build -t preflight-nccl-allreduce -f preflight-checks/nccl-allreduce/Dockerfile .

# Use PyTorch image with NCCL support - no Go build needed for shell-based POC
FROM nvcr.io/nvidia/pytorch:24.01-py3

# Copy scripts
COPY preflight-checks/nccl-allreduce/scripts/run.sh /scripts/run.sh
COPY preflight-checks/nccl-allreduce/scripts/bench.py /scripts/bench.py

# Make scripts executable
RUN chmod +x /scripts/run.sh /scripts/bench.py

ENTRYPOINT ["/scripts/run.sh"]
