from __future__ import annotations

import argparse
import json
from pathlib import Path

import joblib
import pandas as pd

from sensor_quality.multi import score_multi_bundles


def main() -> None:
    parser = argparse.ArgumentParser(description="加载多传感器模型并生成联合诊断结论")
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--input", required=True, help="与训练列结构一致的对齐宽表Parquet")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    manifest_path = Path(args.manifest)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    data = pd.read_parquet(args.input)
    missing = sorted(set(manifest["aligned_columns"]) - set(data.columns))
    if missing:
        raise ValueError(f"输入数据缺少训练列: {missing}")
    data = data[manifest["aligned_columns"]]
    models = {eid: joblib.load(path) for eid, path in manifest["models"].items()}
    process_bundle = joblib.load(manifest["process_model"])
    consensus, detail, _ = score_multi_bundles(models, process_bundle, data)

    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    consensus.to_parquet(output / "system_judgement.parquet", compression="zstd")
    detail.to_parquet(output / "sensor_judgement_long.parquet", compression="zstd")
    consensus.tail(20_000).to_csv(output / "latest_system_judgement.csv", encoding="utf-8-sig")
    print(f"评分完成: {output}; 行数={len(consensus):,}")
    print(consensus["system_status"].value_counts().to_string())


if __name__ == "__main__":
    main()
