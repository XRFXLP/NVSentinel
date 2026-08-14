"""log.py — Timestamped logging for benchmark scripts."""
import time


def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)
