from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path

import pandas as pd


TARGET_TYPES = {"温度", "压力", "流量"}
SKIP_TABLES_DEFAULT = {"rolling", "string"}
CONTEXT_PATTERN = re.compile(r"给定|设定|目标|调节阀|控制输出|输出值|限幅")
CUMULATIVE_PATTERN = re.compile(r"累计|累积")
NON_SENSOR_TARGET_PATTERN = re.compile(r"总电量|电度|累计运行时间|运行小时|运行时间|累计值|累积值")
BOOL_PATTERN = re.compile(r"运行|停止|启动|自动|手动|使能|就绪|允许|有钢|咬钢|过钢|空转|合闸|工作")
BOOL_NEGATIVE = re.compile(r"故障|急停|超时|报警")


def group_id(department: str, process: str) -> str:
    """A model bundle represents one department/process production context.

    Equipment, group and point stay in the catalog as point-level attributes;
    they do not split the bundle.  This lets a model learn relationships across
    the complete process while still selecting only historically useful peers.
    """
    text = "|".join([
        department or "未知部门",
        process or "未知工序",
    ])
    return "group_" + hashlib.sha1(text.encode("utf-8")).hexdigest()[:10]


def main() -> None:
    parser = argparse.ArgumentParser(description="生成非轧钢全厂模拟量和BOOL工况训练范围")
    parser.add_argument("--sensor", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--exclude-tables", default=",".join(sorted(SKIP_TABLES_DEFAULT)))
    parser.add_argument("--bool-cap", type=int, default=30)
    parser.add_argument("--analog-context-cap", type=int, default=160)
    args = parser.parse_args()

    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    catalog = pd.read_excel(args.sensor, dtype=str).fillna("")
    catalog.columns = [str(c).strip() for c in catalog.columns]
    for column in ("eid", "table", "type", "process", "equipment", "group", "point", "tag_type"):
        if column not in catalog:
            catalog[column] = ""
    excluded = {x.strip() for x in args.exclude_tables.split(",") if x.strip()}
    catalog = catalog[catalog["eid"].ne("") & catalog["table"].ne("")].drop_duplicates("eid", keep="last").copy()
    catalog = catalog[~catalog["table"].isin(excluded)].copy()
    catalog["group_id"] = [group_id(a, b) for a, b in zip(catalog.department, catalog.process)]
    catalog["group_name"] = (
        catalog.department.where(catalog.department.ne(""), "未知部门") + " / "
        + catalog.process.where(catalog.process.ne(""), "未知工序")
    )

    targets = catalog[catalog["type"].isin(TARGET_TYPES)].copy()
    text = targets[[c for c in ("point", "tag_name", "tag_description") if c in targets]].agg(" ".join, axis=1)
    targets["target_role"] = "target"
    targets.loc[text.str.contains(CONTEXT_PATTERN, na=False), "target_role"] = "context_only"
    # These values are meters/counters despite occasionally being mislabelled
    # as pressure or flow in the source catalog.  Keep them as line context,
    # but do not judge them with an instantaneous sensor-health model.
    targets.loc[text.str.contains(NON_SENSOR_TARGET_PATTERN, na=False), "target_role"] = "context_only"
    targets.loc[targets["type"].eq("流量") & text.str.contains(CUMULATIVE_PATTERN, na=False), "target_role"] = "cumulative_target"

    active_groups = targets[["table", "department", "process", "group_id", "group_name"]].drop_duplicates()

    # Keep other analogue measurements on the same production line as feature
    # candidates.  They are never judged as targets here, but historical
    # correlation decides whether they help predict a temperature/pressure/
    # flow target.  A bounded per-line selection keeps a very large equipment
    # line from producing an impractically wide training matrix.
    analogs = catalog[~catalog["tag_type"].str.lower().eq("boolean")].copy()
    analogs = analogs.merge(active_groups, on=["table", "department", "process", "group_id", "group_name"], how="inner")
    analogs = analogs[~analogs["eid"].isin(targets["eid"])].copy()
    analog_text = analogs[[c for c in ("point", "tag_name", "tag_description", "tags") if c in analogs]].agg(" ".join, axis=1)
    analogs["context_score"] = 0
    analogs.loc[analog_text.str.contains(CONTEXT_PATTERN, na=False), "context_score"] += 8
    analogs.loc[analogs["group"].ne(""), "context_score"] += 2
    analogs["target_role"] = "analog_context"
    selected_analogs = [
        part.sort_values(["context_score", "eid"], ascending=[False, True]).head(args.analog_context_cap)
        for _, part in analogs.groupby("group_id", sort=True)
    ]
    analogs = pd.concat(selected_analogs, ignore_index=True) if selected_analogs else analogs.head(0)
    bools = catalog[catalog["tag_type"].str.lower().eq("boolean")].copy()
    bools = bools.merge(active_groups, on=["table", "department", "process", "group_id", "group_name"], how="inner")
    bool_text = bools[[c for c in ("point", "tag_name", "tag_description", "tags") if c in bools]].agg(" ".join, axis=1)
    bools["context_score"] = bool_text.str.count(BOOL_PATTERN) * 4 - bool_text.str.count(BOOL_NEGATIVE) * 5
    bools = bools[bools["context_score"].gt(0)].copy()
    selected = [part.sort_values(["context_score", "eid"], ascending=[False, True]).head(args.bool_cap) for _, part in bools.groupby("group_id", sort=True)]
    bools = pd.concat(selected, ignore_index=True) if selected else bools.head(0)
    bools["type"] = "BOOL"
    bools["target_role"] = "bool_context"

    all_scope = pd.concat([targets, analogs, bools], ignore_index=True, sort=False).drop_duplicates("eid", keep="first")
    keep = ["eid", "point", "type", "table", "tag_type", "unit", "department", "process", "equipment", "group", "group_id", "group_name", "target_role", "tag_name", "tag_description"]
    for col in keep:
        if col not in all_scope:
            all_scope[col] = ""
    all_scope = all_scope[keep].sort_values(["table", "department", "process", "equipment", "group", "type", "eid"])
    all_scope.to_csv(output / "training_scope.csv", index=False, encoding="utf-8-sig")
    all_scope[["eid", "table", "group_id", "group_name", "type", "target_role"]].to_csv(output / "scope_eids.csv", index=False, encoding="utf-8")
    summary = {
        "catalog_eids": int(len(catalog)), "scope_eids": int(len(all_scope)),
        "target_eids": int(targets["eid"].nunique()),
        "analog_context_eids": int(analogs["eid"].nunique()), "bool_context_eids": int(bools["eid"].nunique()),
        "groups": int(all_scope["group_id"].nunique()), "excluded_tables": sorted(excluded),
        "by_table": {str(k): int(v) for k, v in targets.groupby("table")["eid"].nunique().items()},
    }
    (output / "scope_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
