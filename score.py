from __future__ import annotations

import argparse
from pathlib import Path

import joblib
import pandas as pd

from sensor_quality.model import score_prepared_data


def main() -> None:
    parser = argparse.ArgumentParser(description="使用已训练模型判断对齐后的当前/新增数据")
    parser.add_argument("--model", required=True)
    parser.add_argument("--input", required=True, help="宽表Parquet；索引为时间，列名为eid")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    bundle = joblib.load(args.model)
    data = pd.read_parquet(args.input)
    result = score_prepared_data(bundle, data)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.suffix.lower() == ".csv":
        result.to_csv(output, encoding="utf-8-sig")
    else:
        result.to_parquet(output, compression="zstd")
    print(f"完成: {output}; 行数={len(result):,}; 确认异常={int(result['confirmed_anomaly'].sum()):,}")


if __name__ == "__main__":
    main()
