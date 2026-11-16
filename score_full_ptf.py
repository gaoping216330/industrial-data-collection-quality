from __future__ import annotations

import argparse
import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from sensor_quality.full import infer_group_states, score_target


def read_long_input(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        frame = pd.read_csv(path)
    elif path.suffix.lower() in {".json", ".jsonl"}:
        frame = pd.read_json(path, lines=path.suffix.lower() == ".jsonl")
    else:
        frame = pd.read_parquet(path)
    required = {"ts", "eid", "v"}
    if not required.issubset(frame.columns):
        raise ValueError(f"输入需要长表字段 {sorted(required)}，实际字段={list(frame.columns)}")
    frame["eid"] = frame["eid"].astype(str)
    if np.issubdtype(frame["ts"].dtype, np.number):
        frame["ts"] = pd.to_datetime(frame["ts"], unit="s", utc=True).dt.tz_convert("Asia/Shanghai")
    else:
        frame["ts"] = pd.to_datetime(frame["ts"], utc=True).dt.tz_convert("Asia/Shanghai")
    frame["v"] = pd.to_numeric(frame["v"], errors="coerce")
    return frame


def main() -> None:
    parser = argparse.ArgumentParser(description="使用全量压力/温度/流量模型判断一段当前数据")
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--input", required=True, help="长表CSV/JSON/Parquet，字段ts,eid,v")
    parser.add_argument("--output", required=True)
    parser.add_argument("--latest-only", action="store_true")
    parser.add_argument("--eid", action="append", default=[], help="只输出指定EID；可重复或用逗号分隔")
    parser.add_argument("--allow-unvalidated", action="store_true", help="允许待校准模型输出正常/异常结论")
    args = parser.parse_args()

    manifest_path = Path(args.manifest)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    summary_path = manifest_path.parent / "model_summary.csv"
    deployment_map: dict[str, str] = {}
    if summary_path.exists():
        model_summary = pd.read_csv(summary_path, encoding="utf-8-sig", dtype={"eid": str})
        if "deployment_status" in model_summary:
            deployment_map = model_summary.set_index("eid")["deployment_status"].fillna("").to_dict()
    requested_eids = {
        eid.strip() for item in args.eid for eid in item.split(",") if eid.strip()
    }
    step = int(manifest["bin_seconds"])
    long = read_long_input(Path(args.input))
    epoch = (
        (long["ts"].dt.tz_convert("UTC") - pd.Timestamp("1970-01-01", tz="UTC"))
        .dt.total_seconds()
        .astype("int64")
    )
    long["ts_bin"] = (epoch // step) * step
    aggregate = long.groupby(["ts_bin", "eid"], observed=True)["v"].mean().reset_index()
    output_rows = []

    for group_id, model_path in manifest["group_models"].items():
        bundle = joblib.load(model_path)
        group_long = aggregate[aggregate["eid"].isin(bundle.all_eids)]
        if group_long.empty:
            continue
        data = group_long.pivot(index="ts_bin", columns="eid", values="v").sort_index()
        full_index = np.arange(data.index.min(), data.index.max() + step, step, dtype="int64")
        data = data.reindex(full_index).reindex(columns=bundle.all_eids)
        data = data.interpolate(limit=3, limit_direction="both")
        data.index = pd.to_datetime(data.index, unit="s", utc=True).tz_convert("Asia/Shanghai")
        states = infer_group_states(bundle, data)
        for eid, model in bundle.target_models.items():
            if requested_eids and eid not in requested_eids:
                continue
            scored = score_target(model, data, states)
            peer_coverage = (
                data[model.peer_eids].notna().mean(axis=1)
                if model.peer_eids
                else data[eid].notna().astype(float)
            )
            selected = scored.tail(1) if args.latest_only else scored
            for timestamp, row in selected.iterrows():
                coverage = float(peer_coverage.loc[timestamp])
                if pd.isna(row["actual"]) or coverage < 0.50 or pd.isna(row["predicted"]):
                    status = "无法判断"
                elif bool(row["confirmed_anomaly"]):
                    status = "确认异常"
                elif bool(row["raw_anomaly"]):
                    status = "初步异常"
                else:
                    status = "正常"
                deployment_status = deployment_map.get(eid, "needs_recalibration")
                if not args.allow_unvalidated and status != "无法判断":
                    if deployment_status == "observation_only":
                        status = "观察模式"
                    elif deployment_status != "ready":
                        status = "模型待校准"
                output_rows.append(
                    {
                        "ts": timestamp,
                        "eid": eid,
                        "name": model.name,
                        "type": model.sensor_type,
                        "status": status,
                        "confirmed": bool(row["confirmed_anomaly"]),
                        "confidence_grade": model.confidence_grade,
                        "deployment_status": deployment_status,
                        "actual": None if pd.isna(row["actual"]) else float(row["actual"]),
                        "predicted": None if pd.isna(row["predicted"]) else float(row["predicted"]),
                        "normal_lower": None if pd.isna(row["normal_lower"]) else float(row["normal_lower"]),
                        "normal_upper": None if pd.isna(row["normal_upper"]) else float(row["normal_upper"]),
                        "health_score": None if pd.isna(row["health_score"]) else float(row["health_score"]),
                        "production_state": int(row["state"]),
                        "related_data_coverage": coverage,
                        "reason": row["reason"] if status != "无法判断" else "相关数据或历史窗口不足",
                        "model_kind": model.model_kind,
                        "group_id": group_id,
                        "group_name": bundle.group_name,
                    }
                )

    result = pd.DataFrame(output_rows)
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    result.to_parquet(output / "sensor_health.parquet", compression="zstd", index=False)
    result.to_csv(output / "sensor_health.csv", index=False, encoding="utf-8-sig")
    summary = {
        "rows": len(result),
        "latest_time": str(result["ts"].max()) if len(result) else None,
        "status_counts": result["status"].value_counts().to_dict() if len(result) else {},
    }
    (output / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
