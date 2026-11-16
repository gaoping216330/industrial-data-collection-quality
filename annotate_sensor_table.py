from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


HISTORY_COL = "20天历史有数据"
SIGNAL_COL = "历史数据状态"
MODEL_COL = "模型状态"


def classify_signal(row: pd.Series) -> str:
    if row[HISTORY_COL] != "Y":
        return "无历史数据"

    finite = int(row.get("finite_rows", 0) or 0)
    if finite < 2_000:
        return "有效样本不足"

    minimum = pd.to_numeric(row.get("minimum"), errors="coerce")
    maximum = pd.to_numeric(row.get("maximum"), errors="coerce")
    std = pd.to_numeric(row.get("standard_deviation"), errors="coerce")
    if pd.notna(minimum) and pd.notna(maximum):
        scale = max(abs(float(minimum)), abs(float(maximum)), 1.0)
        if abs(float(maximum) - float(minimum)) <= 1e-12 * scale:
            return "恒值/不变化"
    if pd.notna(std) and float(std) <= 1e-12:
        return "恒值/不变化"

    # BOOL naturally has only two values; do not call a healthy switching BOOL signal
    # "low variation" merely because its distinct-value count is small.
    value_type = str(row.get("value_type", "")).strip().lower()
    tag_type = str(row.get("tag_type", "")).strip().lower()
    distinct = pd.to_numeric(row.get("approximate_distinct_values"), errors="coerce")
    is_bool = value_type in {"bool", "boolean"} or tag_type in {"bool", "boolean"}
    if not is_bool and pd.notna(distinct) and int(distinct) <= 3:
        return "变化过少"
    return "有效"


def main() -> None:
    parser = argparse.ArgumentParser(description="Add history and model labels to sensor.csv")
    parser.add_argument("--sensor", required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--model-summary", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    sensor = pd.read_csv(args.sensor, encoding="gb18030", dtype=str).fillna("")
    profile = pd.read_csv(args.profile, encoding="utf-8-sig", dtype={"eid": str})
    models = pd.read_csv(args.model_summary, encoding="utf-8-sig", dtype=str).fillna("")

    if sensor["eid"].duplicated().any():
        # Multiple sensor-table rows may legitimately refer to one EID. Map, rather
        # than merge, so row count and order remain exactly unchanged.
        pass
    if profile["eid"].duplicated().any():
        raise ValueError("History profile contains duplicate EIDs")
    if models["eid"].duplicated().any():
        raise ValueError("Model summary contains duplicate EIDs")

    profile_map = profile.set_index("eid")
    for column in profile.columns:
        if column != "eid":
            sensor[column] = sensor["eid"].map(profile_map[column])

    sensor[HISTORY_COL] = np.where(sensor["raw_rows"].notna(), "Y", "")
    sensor[SIGNAL_COL] = sensor.apply(classify_signal, axis=1)

    deployment_to_label = {
        "ready": "Y",
        "observation_only": "观察模式",
        "needs_recalibration": "继续校准",
        "untrainable": "继续校准",
    }
    deployment_map = models.set_index("eid")["deployment_status"]
    sensor[MODEL_COL] = (
        sensor["eid"].map(deployment_map).map(deployment_to_label).fillna("")
    )

    # A model target cannot be marked as generated if no historical data exists.
    sensor.loc[(sensor[HISTORY_COL] == "") & (sensor[MODEL_COL] != ""), MODEL_COL] = "继续校准"

    profile_columns = [column for column in profile.columns if column != "eid"]
    final_columns = [
        column
        for column in sensor.columns
        if column not in profile_columns + [HISTORY_COL, SIGNAL_COL, MODEL_COL]
    ] + [HISTORY_COL, SIGNAL_COL, MODEL_COL]
    annotated = sensor[final_columns]

    utf8_path = output_dir / "sensor_历史模型标记_UTF8.csv"
    gb_path = output_dir / "sensor_历史模型标记_GB18030.csv"
    annotated.to_csv(utf8_path, index=False, encoding="utf-8-sig")
    annotated.to_csv(gb_path, index=False, encoding="gb18030")

    summary = {
        "rows": int(len(annotated)),
        "columns": int(len(annotated.columns)),
        "history": annotated[HISTORY_COL].value_counts(dropna=False).to_dict(),
        "signal_status": annotated[SIGNAL_COL].value_counts(dropna=False).to_dict(),
        "model_status": annotated[MODEL_COL].value_counts(dropna=False).to_dict(),
        "outputs": {"utf8": str(utf8_path), "gb18030": str(gb_path)},
    }
    (output_dir / "sensor_annotation_report.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
