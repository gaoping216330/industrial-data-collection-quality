from __future__ import annotations

import argparse
import gc
import json
import time
import warnings
from pathlib import Path

import duckdb
import joblib
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", message="An ill-conditioned matrix detected.*")

from sensor_quality.full import (
    GroupHealthBundle,
    build_peer_correlation_matrix,
    fit_group_states,
    score_target,
    train_target_model,
)


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_path(path: Path) -> str:
    return str(path.resolve()).replace("\\", "/").replace("'", "''")


def load_group_wide(
    connection: duckdb.DuckDBPyConnection,
    cache_roots: list[Path],
    group_id: str,
    eids: list[str],
    bin_seconds: int,
    boolean_eids: set[str],
    pivot_days: int = 7,
) -> pd.DataFrame:
    patterns = []
    for cache_root in cache_roots:
        group_dir = cache_root / f"group_id={group_id}"
        if group_dir.exists() and any(group_dir.glob("*.parquet")):
            patterns.append(sql_path(group_dir / "*.parquet"))
        for source_dir in cache_root.glob("source_table=*"):
            # Chunked extractors insert a time-chunk directory between the
            # source-table and group partitions; retain support for the old
            # one-level layout too.
            for nested_group in source_dir.rglob(f"group_id={group_id}"):
                if nested_group.is_dir() and any(nested_group.glob("*.parquet")):
                    patterns.append(sql_path(nested_group / "*.parquet"))
    if not patterns:
        return pd.DataFrame()
    source = sql_quote(patterns[0]) if len(patterns) == 1 else "[" + ",".join(sql_quote(path) for path in patterns) + "]"
    eid_list = ",".join(sql_quote(eid) for eid in eids)
    bounds = connection.execute(
        f"SELECT min(ts_bin), max(ts_bin) FROM read_parquet({source}, union_by_name=true) "
        f"WHERE eid IN ({eid_list})"
    ).fetchone()
    if not bounds or bounds[0] is None:
        return pd.DataFrame()
    chunk_seconds = max(int(pivot_days), 1) * 86400
    frames: list[pd.DataFrame] = []
    start = (int(bounds[0]) // chunk_seconds) * chunk_seconds
    end_limit = int(bounds[1]) + 1
    while start < end_limit:
        end = min(start + chunk_seconds, end_limit)
        query = f"""
            PIVOT (
                SELECT ts_bin, eid, value
                FROM read_parquet({source}, union_by_name=true)
                WHERE eid IN ({eid_list}) AND ts_bin >= {start} AND ts_bin < {end}
            )
            ON eid IN ({eid_list})
            USING first(value)
            GROUP BY ts_bin
            ORDER BY ts_bin
        """
        chunk = connection.execute(query).fetch_df()
        if not chunk.empty:
            frames.append(chunk)
        start = end
    frame = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if frame.empty:
        return frame
    frame["ts_bin"] = frame["ts_bin"].astype("int64")
    frame = frame.set_index("ts_bin")
    full_index = np.arange(frame.index.min(), frame.index.max() + bin_seconds, bin_seconds, dtype="int64")
    frame = frame.reindex(full_index)
    for eid in eids:
        if eid not in frame:
            frame[eid] = np.nan
    frame = frame[eids].astype("float32")
    analog_eids = [eid for eid in eids if eid not in boolean_eids]
    bool_columns = [eid for eid in eids if eid in boolean_eids]
    if analog_eids:
        frame[analog_eids] = frame[analog_eids].interpolate(limit=3, limit_direction="both")
    if bool_columns:
        frame[bool_columns] = frame[bool_columns].ffill(limit=3).bfill(limit=3)
    frame.index = pd.to_datetime(frame.index, unit="s", utc=True).tz_convert("Asia/Shanghai")
    return frame


def main() -> None:
    parser = argparse.ArgumentParser(description="训练全部压力、温度、流量传感器模型")
    parser.add_argument("--cache", default=r"D:\python_work\数据中台数据质量判定\outputs\full_ptf\cache_10s")
    parser.add_argument("--context-cache", action="append", default=[], help="Optional additional cache root; may be repeated")
    parser.add_argument("--scope", default=r"D:\python_work\数据中台数据质量判定\outputs\full_ptf\training_scope.csv")
    parser.add_argument("--output", default=r"D:\python_work\数据中台数据质量判定\outputs\full_ptf\models")
    parser.add_argument("--bin-seconds", type=int, default=10)
    parser.add_argument("--threads", type=int, default=6)
    parser.add_argument("--memory-limit", default="20GB")
    parser.add_argument("--pivot-days", type=int, default=7)
    parser.add_argument("--resume", action="store_true", default=True)
    parser.add_argument("--model-group-column", default="group_id")
    parser.add_argument("--cache-group-column", default="group_id")
    parser.add_argument("--model-group-name-column", default="group_name")
    parser.add_argument("--only-group", default="")
    args = parser.parse_args()

    started = time.time()
    cache_root = Path(args.cache)
    cache_roots = [cache_root]
    cache_roots.extend(Path(value) for value in args.context_cache if value)
    output = Path(args.output)
    group_dir = output / "groups"
    report_dir = output / "group_reports"
    group_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)
    scope = pd.read_csv(args.scope, encoding="utf-8-sig", dtype=str).fillna("")
    for column in (args.model_group_column, args.cache_group_column, args.model_group_name_column):
        if column not in scope:
            raise SystemExit(f"Scope is missing required column: {column}")
    if args.only_group:
        scope = scope[scope[args.model_group_column].eq(args.only_group)]
    connection = duckdb.connect()
    connection.execute(f"SET threads={int(args.threads)}")
    connection.execute(f"SET memory_limit='{args.memory_limit}'")
    connection.execute("SET preserve_insertion_order=false")

    all_summaries: list[dict[str, object]] = []
    group_manifest: dict[str, str] = {}
    groups = list(scope.groupby(args.model_group_column, sort=True))
    for group_index, (group_id, group_scope) in enumerate(groups, start=1):
        group_started = time.time()
        group_name = group_scope[args.model_group_name_column].iloc[0]
        cache_group_id = group_scope[args.cache_group_column].iloc[0]
        # Some catalog entries can appear in both local and process context.
        # Keep one column per EID for DuckDB PIVOT and model feature selection.
        eids = list(dict.fromkeys(group_scope["eid"].tolist()))
        group_scope = group_scope.drop_duplicates("eid", keep="first").copy()
        bundle_path = group_dir / f"{group_id}.joblib"
        completed_report_path = report_dir / f"{group_id}.json"
        if args.resume and bundle_path.exists() and completed_report_path.exists():
            completed = json.loads(completed_report_path.read_text(encoding="utf-8"))
            all_summaries.extend(completed.get("targets", []))
            group_manifest[group_id] = str(bundle_path)
            print(f"[{group_index}/{len(groups)}] 跳过已完成 {group_name}", flush=True)
            continue
        has_cache = (cache_root / f"group_id={cache_group_id}").exists() or any(
            any(source_dir.rglob(f"group_id={cache_group_id}"))
            for source_dir in cache_root.glob("source_table=*")
        )
        if not has_cache:
            for row in group_scope.loc[
                group_scope["target_role"].isin(["target", "cumulative_target"])
            ].itertuples(index=False):
                all_summaries.append(
                    {"eid": row.eid, "point": row.point, "type": row.type, "group_id": group_id, "group_name": group_name,
                     "target_role": row.target_role, "status": "untrainable", "reason": "训练缓存中没有该设备组"}
                )
            continue
        print(f"[{group_index}/{len(groups)}] 读取 {group_name} ({len(eids)}点)", flush=True)
        boolean_eids = set(group_scope.loc[group_scope["target_role"].isin(["bool_context", "bool_local_context", "bool_process_context"]), "eid"])
        data = load_group_wide(
            connection, cache_roots, cache_group_id, eids, int(args.bin_seconds), boolean_eids, int(args.pivot_days)
        )
        if data.empty:
            for row in group_scope.loc[
                group_scope["target_role"].isin(["target", "cumulative_target"])
            ].itertuples(index=False):
                summary = {
                    "eid": row.eid,
                    "point": row.point,
                    "type": row.type,
                    "group_id": group_id,
                    "group_name": group_name,
                    "target_role": row.target_role,
                    "status": "untrainable",
                    "reason": "该设备组在20天历史文件中没有匹配数据",
                    "availability": 0.0,
                }
                all_summaries.append(summary)
            continue
        train_end = int(len(data) * 0.65)
        print("  计算组内共享相关矩阵", flush=True)
        correlation_matrix = build_peer_correlation_matrix(data, train_end)
        state_eids, state_fill, state_scaler, state_model, states, state_report = fit_group_states(
            data, train_end, preferred_eids=boolean_eids
        )
        # The bundle contains the full department/process/equipment line
        # context.  ``analog_context`` points are feature candidates only;
        # select_peers still admits them only when history shows a sufficiently
        # strong relationship to the target.
        context_eids = set(
            group_scope.loc[
                group_scope["target_role"].isin(["context_only", "analog_context", "bool_context", "bool_local_context", "bool_process_context"]),
                "eid",
            ]
        )
        metadata = group_scope.set_index("eid").to_dict("index")
        target_models = {}
        target_reports = []
        targets = group_scope.loc[group_scope["target_role"].isin(["target", "cumulative_target"]), "eid"].tolist()
        for target_index, target in enumerate(targets, start=1):
            model, report = train_target_model(
                data, target, metadata[target], states, context_eids,
                correlation_matrix=correlation_matrix,
            )
            summary = {
                "eid": target,
                "point": metadata[target].get("point", target),
                "type": metadata[target].get("type", ""),
                "group_id": group_id,
                "group_name": group_name,
                "target_role": metadata[target].get("target_role", "target"),
                **report,
            }
            if model is not None:
                target_models[target] = model
                scored = score_target(model, data, states)
                test = scored.iloc[int(len(scored) * 0.80):]
                summary.update(
                    {
                        "test_mae": float(test["residual"].abs().mean()),
                        "test_raw_anomaly_rate": float(test["raw_anomaly"].mean()),
                        "test_confirmed_anomaly_rate": float(test["confirmed_anomaly"].mean()),
                        "history_confirmed_points": int(scored["confirmed_anomaly"].sum()),
                    }
                )
            target_reports.append(summary)
            all_summaries.append(summary)
            if target_index % 25 == 0 or target_index == len(targets):
                print(f"  已处理 {target_index}/{len(targets)}", flush=True)

        bundle = GroupHealthBundle(
            group_id=group_id,
            group_name=group_name,
            bin_seconds=int(args.bin_seconds),
            all_eids=eids,
            state_eids=state_eids,
            state_fill_values=state_fill,
            state_scaler=state_scaler,
            state_model=state_model,
            target_models=target_models,
            catalog=metadata,
        )
        joblib.dump(bundle, bundle_path, compress=3)
        group_manifest[group_id] = str(bundle_path)
        group_report = {
            "group_id": group_id,
            "group_name": group_name,
            "rows": len(data),
            "start": str(data.index.min()),
            "end": str(data.index.max()),
            "catalog_eids": len(eids),
            "target_eids": len(targets),
            "trained_targets": len(target_models),
            "state_model": state_report,
            "elapsed_seconds": time.time() - group_started,
            "targets": target_reports,
        }
        (report_dir / f"{group_id}.json").write_text(json.dumps(group_report, ensure_ascii=False, indent=2), encoding="utf-8")
        del data, bundle, target_models
        gc.collect()

    summary_frame = pd.DataFrame(all_summaries)
    trained = summary_frame["status"].eq("trained") if "status" in summary_frame else pd.Series(dtype=bool)
    confirmed_rate = pd.to_numeric(
        summary_frame.get("test_confirmed_anomaly_rate", pd.Series(index=summary_frame.index)), errors="coerce"
    )
    confidence = summary_frame.get("confidence_grade", pd.Series(index=summary_frame.index, dtype=str)).fillna("")
    ready = trained & confidence.isin(["A", "B"]) & confirmed_rate.le(0.05)
    observe = trained & ~ready & confirmed_rate.le(0.20)
    summary_frame["deployment_status"] = "untrainable"
    summary_frame.loc[trained, "deployment_status"] = "needs_recalibration"
    summary_frame.loc[observe, "deployment_status"] = "observation_only"
    summary_frame.loc[ready, "deployment_status"] = "ready"
    summary_frame.to_csv(output / "model_summary.csv", index=False, encoding="utf-8-sig")
    manifest = {
        "version": 1,
        "created_at": pd.Timestamp.now(tz="Asia/Shanghai").isoformat(),
        "bin_seconds": int(args.bin_seconds),
        "scope_path": str(Path(args.scope)),
        "cache_path": str(cache_root),
        "context_cache_paths": [str(path) for path in cache_roots[1:]],
        "bool_context_eids": int(scope["target_role"].isin(["bool_context", "bool_local_context", "bool_process_context"]).sum()),
        "group_models": group_manifest,
        "scope_targets": int(len(summary_frame)),
        "trained_targets": int(trained.sum()) if len(trained) else 0,
        "untrainable_targets": int((~trained).sum()) if len(trained) else 0,
        "by_type": summary_frame.loc[trained].groupby("type").size().to_dict() if len(trained) else {},
        "by_confidence": (
            summary_frame.loc[trained].groupby("confidence_grade").size().to_dict()
            if len(trained) and "confidence_grade" in summary_frame.columns and trained.any()
            else {}
        ),
        "by_deployment_status": summary_frame.groupby("deployment_status").size().to_dict(),
        "elapsed_seconds": time.time() - started,
    }
    (output / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(manifest, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
