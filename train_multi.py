from __future__ import annotations

import argparse
import copy
import json
import time
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from sensor_quality.io import load_config, save_json
from sensor_quality.model import synthetic_fault_evaluation, train_bundle
from sensor_quality.multi import combine_multi_sensor_scores, fit_process_novelty


def main() -> None:
    parser = argparse.ArgumentParser(description="训练同设备全部有效模拟量的联合诊断模型")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--aligned", default="")
    parser.add_argument("--profile", default="")
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    started = time.time()
    config = load_config(args.config)
    base_output = Path(config["output_dir"])
    aligned_path = Path(args.aligned) if args.aligned else base_output / "aligned_timeseries.parquet"
    profile_path = Path(args.profile) if args.profile else base_output / "signal_profile.csv"
    output = Path(args.output) if args.output else base_output.parent / "new_bar_8h_multi"
    model_dir = output / "models"
    report_dir = output / "reports"
    model_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    data = pd.read_parquet(aligned_path)
    profile = pd.read_csv(profile_path, dtype={"eid": str})
    boolean = profile.loc[
        profile["eid"].isin(data.columns) & profile["inferred_type"].eq("boolean"), "eid"
    ].tolist()
    analog = [column for column in data.columns if column not in boolean]
    names = dict(zip(profile["eid"], profile["name"]))
    if len(analog) < 2:
        raise ValueError("至少需要两个有效模拟量才能进行留一预测")

    process_bundle, process_score, process_report = fit_process_novelty(data, boolean, analog, config)
    joblib.dump(process_bundle, output / "process_state_model.joblib", compress=3)
    save_json(process_report, output / "process_state_report.json")

    target_scores: dict[str, pd.DataFrame] = {}
    summary_rows = []
    manifest_models = {}
    for index, target in enumerate(analog, start=1):
        target_config = copy.deepcopy(config)
        target_config["target"] = {"eid": target, "name": names.get(target, target)}
        bundle, scored, report = train_bundle(data, boolean, analog, target_config)
        model_path = model_dir / f"{target}.joblib"
        report_path = report_dir / f"{target}.json"
        joblib.dump(bundle, model_path, compress=3)
        test_rows = int(report["test_rows"])
        test = scored.iloc[-test_rows:]
        synthetic = synthetic_fault_evaluation(bundle, data.iloc[-min(20_000, len(data)):])
        report["synthetic_fault_evaluation"] = synthetic
        save_json(report, report_path)
        target_scores[target] = scored
        manifest_models[target] = str(model_path)
        summary_rows.append(
            {
                "eid": target,
                "name": names.get(target, target),
                "selected_models": "|".join(sorted(set(report["selected_regressors"].values()))),
                "test_mae": float(test["residual"].abs().mean()),
                "test_p99_abs_error": float(test["residual"].abs().quantile(0.99)),
                "test_raw_anomaly_rate": float(test["raw_anomaly"].mean()),
                "test_confirmed_anomaly_rate": float(test["confirmed_anomaly"].mean()),
                "history_confirmed_points": int(scored["confirmed_anomaly"].sum()),
                "synthetic_bias_recall": synthetic["bias"]["recall"],
                "synthetic_drift_recall": synthetic["drift"]["recall"],
                "synthetic_stuck_recall": synthetic["stuck"]["recall"],
                "synthetic_noise_recall": synthetic["noise"]["recall"],
            }
        )
        print(f"[{index}/{len(analog)}] {target} {names.get(target, target)} 完成")

    consensus, detail = combine_multi_sensor_scores(target_scores, process_score)
    summary = pd.DataFrame(summary_rows).sort_values("eid")
    summary.to_csv(output / "target_summary.csv", index=False, encoding="utf-8-sig")
    consensus.to_parquet(output / "system_judgement.parquet", compression="zstd")
    consensus.tail(20_000).to_csv(output / "latest_system_judgement.csv", encoding="utf-8-sig")
    detail.to_parquet(output / "sensor_judgement_long.parquet", compression="zstd")

    manifest = {
        "version": 1,
        "created_at": pd.Timestamp.now(tz="Asia/Shanghai").isoformat(),
        "aligned_columns": data.columns.tolist(),
        "boolean_eids": boolean,
        "analog_target_eids": analog,
        "target_names": names,
        "process_model": str(output / "process_state_model.joblib"),
        "models": manifest_models,
        "rows": len(data),
        "elapsed_seconds": time.time() - started,
        "status_counts": consensus["system_status"].value_counts().to_dict(),
    }
    save_json(manifest, output / "manifest.json")
    print(f"联合训练完成: {output}")
    print(json.dumps(manifest["status_counts"], ensure_ascii=False))


if __name__ == "__main__":
    main()
