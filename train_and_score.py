from __future__ import annotations

import argparse
import time
from pathlib import Path

import joblib
import pandas as pd

from sensor_quality.io import (
    build_wide_timeseries,
    load_config,
    load_sensor_catalog,
    prepare_timeseries,
    save_json,
    validate_config_against_catalog,
)
from sensor_quality.model import synthetic_fault_evaluation, train_bundle


def main() -> None:
    parser = argparse.ArgumentParser(description="训练无监督工况与模拟量传感器健康模型")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--reuse-aligned", action="store_true", help="复用已生成的对齐宽表，跳过43GB原始文件扫描")
    args = parser.parse_args()

    started = time.time()
    config = load_config(args.config)
    output = Path(config["output_dir"])
    output.mkdir(parents=True, exist_ok=True)

    catalog = load_sensor_catalog(config["sensor_csv_path"])
    validate_config_against_catalog(config, catalog)
    aligned_path = output / "aligned_timeseries.parquet"
    profile_path = output / "signal_profile.csv"
    if args.reuse_aligned and aligned_path.exists() and profile_path.exists():
        profile = pd.read_csv(profile_path, dtype={"eid": str})
        data = pd.read_parquet(aligned_path)
        boolean = profile.loc[
            profile["eid"].isin(data.columns) & profile["inferred_type"].eq("boolean"), "eid"
        ].tolist()
        analog = [column for column in data.columns if column not in boolean]
    else:
        wide, profile = build_wide_timeseries(config)
        profile.to_csv(profile_path, index=False, encoding="utf-8-sig")
        data, boolean, analog = prepare_timeseries(wide, profile, config)
        data.to_parquet(aligned_path, compression="zstd")

    bundle, scored, diagnostics = train_bundle(data, boolean, analog, config)
    joblib.dump(bundle, output / "model.joblib", compress=3)
    scored.to_parquet(output / "historical_judgement.parquet", compression="zstd")
    scored.tail(20_000).to_csv(output / "latest_judgement.csv", encoding="utf-8-sig")
    diagnostics["signal_types"] = {"boolean": boolean, "analog": analog}
    diagnostics["elapsed_seconds"] = time.time() - started
    diagnostics["synthetic_fault_evaluation"] = synthetic_fault_evaluation(bundle, data.iloc[-min(20_000, len(data)):])
    save_json(diagnostics, output / "training_report.json")
    print(f"完成。输出目录: {output}")
    print(f"有效对齐行数: {len(data):,}; 自动工况数: {bundle.state_model.n_clusters}")
    print(f"累计耗时: {time.time() - started:.1f} 秒")


if __name__ == "__main__":
    main()
