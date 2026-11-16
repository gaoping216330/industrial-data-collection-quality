from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path

import pandas as pd


TARGET_TYPES = {"温度", "压力", "流量"}
CONTEXT_PATTERN = re.compile(r"给定|设定|目标|调节阀|控制输出|输出值|限幅")
CUMULATIVE_PATTERN = re.compile(r"累计|累积")


def load_catalog(path: Path) -> pd.DataFrame:
    for encoding in ("gb18030", "utf-8-sig", "utf-8"):
        try:
            return pd.read_csv(path, encoding=encoding, dtype=str, on_bad_lines="warn").fillna("")
        except UnicodeDecodeError:
            continue
    raise ValueError(f"无法识别目录编码: {path}")


def safe_group_id(process: str, equipment: str, group: str) -> str:
    label = "|".join([process or "未知工序", equipment or group or "未知设备"])
    digest = hashlib.sha1(label.encode("utf-8")).hexdigest()[:10]
    return f"group_{digest}"


def main() -> None:
    parser = argparse.ArgumentParser(description="生成压力、温度、流量全量训练范围")
    parser.add_argument("--sensor-csv", default=r"D:\python_work\数据中台数据质量判定\sensor.csv")
    parser.add_argument("--output", default=r"D:\python_work\数据中台数据质量判定\outputs\full_ptf")
    args = parser.parse_args()

    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    catalog = load_catalog(Path(args.sensor_csv))
    scope = catalog[catalog["type"].isin(TARGET_TYPES)].copy()
    scope = scope[scope["eid"].ne("")].drop_duplicates("eid", keep="last")
    text = scope["point"].fillna("") + " " + scope.get("tag_description", "").fillna("")
    scope["target_role"] = "target"
    scope.loc[text.str.contains(CONTEXT_PATTERN), "target_role"] = "context_only"
    cumulative = scope["type"].eq("流量") & text.str.contains(CUMULATIVE_PATTERN)
    scope.loc[cumulative, "target_role"] = "cumulative_target"
    scope["group_id"] = [
        safe_group_id(process, equipment, group)
        for process, equipment, group in zip(scope["process"], scope["equipment"], scope["group"])
    ]
    scope["group_name"] = scope["process"].replace("", "未知工序") + " / " + (
        scope["equipment"].where(scope["equipment"].ne(""), scope["group"]).replace("", "未知设备")
    )
    scope["catalog_data_type"] = scope.get("tag_type", "").where(
        scope.get("tag_type", "").ne(""), scope.get("data_type", "")
    )
    scope["scope_reason"] = "实测压力/温度/流量"
    scope.loc[scope["target_role"].eq("context_only"), "scope_reason"] = "设定/目标/调节量，仅作上下文"
    scope.loc[scope["target_role"].eq("cumulative_target"), "scope_reason"] = "累计流量，使用计数器专用规则"

    keep_columns = [
        "eid", "point", "type", "catalog_data_type", "unit", "department", "process", "equipment", "group",
        "group_id", "group_name", "target_role", "scope_reason", "device_enable", "tag_address", "tag_description",
    ]
    for column in keep_columns:
        if column not in scope:
            scope[column] = ""
    scope = scope[keep_columns].sort_values(["process", "equipment", "type", "eid"])
    scope.to_csv(output / "training_scope.csv", index=False, encoding="utf-8-sig")
    scope[["eid", "group_id", "group_name", "type", "target_role"]].to_csv(
        output / "scope_eids.csv", index=False, encoding="utf-8"
    )

    groups = (
        scope.groupby(["group_id", "group_name", "type", "target_role"], dropna=False)
        .size()
        .rename("count")
        .reset_index()
    )
    groups.to_csv(output / "scope_groups.csv", index=False, encoding="utf-8-sig")
    summary = {
        "catalog_rows": int(len(catalog)),
        "scope_rows": int(len(scope)),
        "unique_eids": int(scope["eid"].nunique()),
        "by_type": {str(key): int(value) for key, value in scope["type"].value_counts().items()},
        "by_role": {str(key): int(value) for key, value in scope["target_role"].value_counts().items()},
        "group_count": int(scope["group_id"].nunique()),
    }
    (output / "scope_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
