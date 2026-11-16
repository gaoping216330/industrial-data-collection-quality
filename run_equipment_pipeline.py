"""Start equipment training only after a cache extractor reports completion."""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--extract-log", required=True)
    parser.add_argument("--train-script", required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--base-cache", required=True)
    parser.add_argument("--old-context-cache", required=True)
    parser.add_argument("--new-context-cache", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--log", required=True)
    parser.add_argument("--poll-seconds", type=int, default=300)
    args = parser.parse_args()

    extract_log = Path(args.extract_log)
    pipeline_log = Path(args.log)
    pipeline_log.parent.mkdir(parents=True, exist_ok=True)
    while True:
        text = extract_log.read_text(encoding="utf-8", errors="replace") if extract_log.exists() else ""
        if "all requested cache chunks complete" in text:
            break
        time.sleep(args.poll_seconds)

    command = [
        sys.executable, args.train_script,
        "--scope", args.scope,
        "--cache", args.base_cache,
        "--context-cache", args.old_context_cache,
        "--context-cache", args.new_context_cache,
        "--output", args.output,
        "--threads", "4",
        "--memory-limit", "20GB",
        "--model-group-column", "model_cell_id",
        "--cache-group-column", "cache_group_id",
        "--model-group-name-column", "model_cell_name",
    ]
    with pipeline_log.open("a", encoding="utf-8") as handle:
        handle.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} cache complete; starting training\n")
        handle.flush()
        completed = subprocess.run(command, stdout=handle, stderr=subprocess.STDOUT, env=os.environ.copy())
        handle.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} training exit={completed.returncode}\n")
    raise SystemExit(completed.returncode)


if __name__ == "__main__":
    main()
