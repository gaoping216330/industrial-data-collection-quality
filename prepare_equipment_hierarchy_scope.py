"""Build an equipment-centred training scope from the sensor catalog."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path

import pandas as pd


TARGET_TYPES = {"温度", "压力", "流量"}
SKIP_TABLES = {"rolling", "string"}
NON_SENSOR = re.compile(r"总电量|电度|累计运行时间|运行小时|运行时间|累计值|累积值")
PROCESS_BOOL = re.compile(r"运行|停止|启动|自动|手动|使能|就绪|允许|有钢|咬钢|过钢|空转")
PROCESS_BOOL_NEGATIVE = re.compile(r"故障|急停|超时|报警")
CONTEXT = re.compile(r"给定|设定|目标|调节阀|控制输出|输出值|限幅")


def digest(*parts: str) -> str:
    text = "|".join(part or "未知" for part in parts)
    return "group_" + hashlib.sha1(text.encode("utf-8")).hexdigest()[:10]


def equipment_key(row: pd.Series) -> str:
    for field in ("equipment", "device_name", "group"):
        value = str(row.get(field, "")).strip()
        if value:
            return value
    point = str(row.get("point", "")).strip()
    return point[:8] if point else "未归类设备"


def main() -> None:
    parser = argparse.ArgumentParser(description="Create equipment-centred sensor training scope")
    parser.add_argument("--sensor", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--local-analog-cap", type=int, default=60)
    parser.add_argument("--local-bool-cap", type=int, default=24)
    parser.add_argument("--process-bool-cap", type=int, default=10)
    parser.add_argument("--existing-scope", default="", help="Existing cached EID scope; used to emit only missing EIDs")
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    catalog = pd.read_excel(args.sensor, dtype=str).fillna("")
    catalog.columns = [str(column).strip() for column in catalog.columns]
    for column in ("eid", "table", "department", "process", "equipment", "group", "point", "type", "tag_type", "device_name"):
        if column not in catalog:
            catalog[column] = ""
    catalog = catalog[catalog["eid"].ne("") & catalog["table"].ne("")].copy()
    catalog = catalog[~catalog["table"].isin(SKIP_TABLES)].drop_duplicates("eid", keep="last").copy()
    catalog["equipment_key"] = catalog.apply(equipment_key, axis=1)
    catalog["cache_group_id"] = [digest(a, b) for a, b in zip(catalog.department, catalog.process)]
    catalog["model_cell_id"] = [digest(a, b, c) for a, b, c in zip(catalog.department, catalog.process, catalog.equipment_key)]
    catalog["model_cell_name"] = catalog.department + " / " + catalog.process + " / " + catalog.equipment_key

    targets = catalog[catalog["type"].isin(TARGET_TYPES)].copy()
    target_text = targets[[name for name in ("point", "tag_name", "tag_description") if name in targets]].agg(" ".join, axis=1)
    targets["target_role"] = "target"
    targets.loc[target_text.str.contains(NON_SENSOR, na=False), "target_role"] = "context_only"
    targets.loc[target_text.str.contains(CONTEXT, na=False), "target_role"] = "context_only"
    targets = targets[targets["target_role"].eq("target")].copy()

    # All target types on the same physical equipment are candidate analogue
    # context, plus a bounded selection of other local analogue signals.
    numeric = catalog[~catalog["tag_type"].str.lower().eq("boolean")].copy()
    numeric["local_score"] = 0
    numeric.loc[numeric["type"].isin(TARGET_TYPES), "local_score"] += 8
    text = numeric[[name for name in ("point", "tag_name", "tag_description", "tags") if name in numeric]].agg(" ".join, axis=1)
    numeric.loc[text.str.contains(CONTEXT, na=False), "local_score"] += 4
    bools = catalog[catalog["tag_type"].str.lower().eq("boolean")].copy()
    bool_text = bools[[name for name in ("point", "tag_name", "tag_description", "tags") if name in bools]].agg(" ".join, axis=1)
    bools["local_score"] = bool_text.str.count(PROCESS_BOOL) * 4 - bool_text.str.count(PROCESS_BOOL_NEGATIVE) * 5

    rows = []
    for cell_id, target_part in targets.groupby("model_cell_id", sort=True):
        first = target_part.iloc[0]
        mask = (
            numeric["table"].eq(first.table)
            & numeric["department"].eq(first.department)
            & numeric["process"].eq(first.process)
            & numeric["equipment_key"].eq(first.equipment_key)
        )
        local_analog = numeric.loc[mask & ~numeric["eid"].isin(target_part["eid"])]
        local_analog = local_analog.sort_values(["local_score", "eid"], ascending=[False, True]).head(args.local_analog_cap).copy()
        local_analog["target_role"] = "analog_context"
        bool_mask = (
            bools["table"].eq(first.table)
            & bools["department"].eq(first.department)
            & bools["process"].eq(first.process)
            & bools["equipment_key"].eq(first.equipment_key)
        )
        local_bool = bools.loc[bool_mask & bools["local_score"].gt(0)]
        local_bool = local_bool.sort_values(["local_score", "eid"], ascending=[False, True]).head(args.local_bool_cap).copy()
        local_bool["target_role"] = "bool_local_context"
        process_bool = bools.loc[
            bools["table"].eq(first.table)
            & bools["department"].eq(first.department)
            & bools["process"].eq(first.process)
            & bools["local_score"].gt(0)
        ]
        process_bool = process_bool.sort_values(["local_score", "eid"], ascending=[False, True]).head(args.process_bool_cap).copy()
        process_bool["target_role"] = "bool_process_context"
        part = pd.concat([target_part, local_analog, local_bool, process_bool], ignore_index=True, sort=False)
        part = part.drop_duplicates("eid", keep="first")
        rows.append(part)
    training = pd.concat(rows, ignore_index=True, sort=False)
    keep = ["eid", "point", "type", "table", "tag_type", "unit", "department", "process", "equipment", "equipment_key", "group", "model_cell_id", "model_cell_name", "cache_group_id", "target_role", "tag_name", "tag_description"]
    for column in keep:
        if column not in training:
            training[column] = ""
    training = training[keep].sort_values(["department", "process", "equipment_key", "target_role", "eid"])
    training.to_csv(output / "training_scope.csv", index=False, encoding="utf-8-sig")
    extract = training[["eid", "table", "cache_group_id"]].drop_duplicates().rename(columns={"cache_group_id": "group_id"})
    extract.to_csv(output / "extract_scope.csv", index=False, encoding="utf-8")
    missing = extract
    if args.existing_scope:
        existing = pd.read_csv(args.existing_scope, dtype=str).fillna("")
        existing_eids = set(existing.get("eid", pd.Series(dtype=str)))
        missing = extract[~extract["eid"].isin(existing_eids)].copy()
    missing.to_csv(output / "missing_extract_scope.csv", index=False, encoding="utf-8")
    summary = {
        "model_cells": int(training["model_cell_id"].nunique()),
        "target_eids": int(training.loc[training["target_role"].eq("target"), "eid"].nunique()),
        "training_rows": int(len(training)),
        "extract_eids": int(len(extract)),
        "missing_extract_eids": int(len(missing)),
        "local_analog_context_rows": int(training["target_role"].eq("analog_context").sum()),
        "local_bool_context_rows": int(training["target_role"].eq("bool_local_context").sum()),
        "process_bool_context_rows": int(training["target_role"].eq("bool_process_context").sum()),
    }
    (output / "scope_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
