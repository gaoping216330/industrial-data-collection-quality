from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from online_model_detector import OnlineDetector, load_json, setup_logging


def read_recent_group_data(path: Path, window_seconds: int) -> pd.DataFrame:
    parquet = pq.ParquetFile(path)
    ts_index = parquet.schema.names.index("ts_bin")
    maximum = max(
        parquet.metadata.row_group(i).column(ts_index).statistics.max
        for i in range(parquet.num_row_groups)
        if parquet.metadata.row_group(i).column(ts_index).statistics is not None
    )
    cutoff = int(maximum) - window_seconds
    row_groups = []
    for i in range(parquet.num_row_groups):
        statistics = parquet.metadata.row_group(i).column(ts_index).statistics
        if statistics is None or statistics.max >= cutoff:
            row_groups.append(i)
    table = parquet.read_row_groups(row_groups, columns=["ts_bin", "eid", "value"])
    frame = table.to_pandas()
    frame = frame[pd.to_numeric(frame["ts_bin"], errors="coerce") >= cutoff]
    return frame.rename(columns={"ts_bin": "ts", "value": "v"})


def main() -> int:
    parser = argparse.ArgumentParser(description="使用已有验证片段做一次离线回放，不写数据库")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--input", action="append", default=[])
    parser.add_argument("--output", default="offline_test_output")
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    config = load_json(config_path)
    setup_logging(config_path.parent, config)
    detector = OnlineDetector(config_path, config)

    inputs = [Path(value).resolve() for value in args.input]
    frames = []
    if inputs:
        for path in inputs:
            if path.exists():
                frames.append(pd.read_parquet(path, columns=["ts", "eid", "v"]))
    else:
        enhanced = set(detector.registry.manifest.get("bool_enhanced_groups", []))
        for group in detector.registry.groups:
            cache_name = "bool_cache_10s" if group.group_id in enhanced else "cache_10s"
            path = (
                detector.project_root
                / "outputs"
                / "full_ptf"
                / cache_name
                / f"group_id={group.group_id}"
                / "data.parquet"
            )
            if not path.exists() and cache_name == "bool_cache_10s":
                path = (
                    detector.project_root
                    / "outputs"
                    / "full_ptf"
                    / "cache_10s"
                    / f"group_id={group.group_id}"
                    / "data.parquet"
                )
            if path.exists():
                frames.append(read_recent_group_data(path, detector.cache_minutes * 60))
    if not frames:
        raise FileNotFoundError("没有找到离线验证片段")
    data = pd.concat(frames, ignore_index=True)
    data["eid"] = data["eid"].astype(str)
    data = data[data["eid"].isin(detector.registry.required_eids)]
    for row in data.itertuples(index=False):
        detector.cache.add(row.eid, row.v, True, row.ts)

    latest_timestamp = int(pd.to_numeric(data["ts"], errors="coerce").max())
    results = detector.run_detection_once(
        now_epoch=latest_timestamp + detector.registry.bin_seconds,
        write_database=False,
    )
    output_dir = Path(args.output).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(
        [
            {
                "eid": row.eid,
                "result": row.result,
                "status_code": row.status_code,
                "detected_at": row.detected_at,
                "message": row.message,
            }
            for row in results
        ]
    )
    frame.to_csv(output_dir / "offline_model_results.csv", index=False, encoding="utf-8-sig")
    summary = {
        "input_rows": int(len(data)),
        "ready_models": len(detector.registry.ready_eids),
        "results": int(len(frame)),
        "result_counts": frame["result"].value_counts().to_dict(),
        "status_code_counts": frame["status_code"].value_counts().to_dict(),
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if len(frame) != len(detector.registry.ready_eids):
        raise RuntimeError("离线回放结果没有覆盖全部ready模型")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
