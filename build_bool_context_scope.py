from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


STRONG = r"运行/停止|运行信号|运行状态|启动信号|启动状态|允许轧钢|轧钢允许|有钢|咬钢|过钢|空转|生产状态|停机|开机|电机运行|泵运行|自动状态|手动状态|就绪"
GENERAL = r"运行|启动|停止|自动|手动|使能|就绪|允许|有钢|咬钢|过钢|空转|合闸|工作"
NEGATIVE = r"故障|急停|超时|快熔|报警|联锁投用"
LOW_VALUE = r"风扇|开到位|关到位|阀开|阀关"


def main() -> None:
    parser = argparse.ArgumentParser(description="从sensor.csv筛选用于工况识别的BOOL上下文")
    parser.add_argument("--sensor", required=True)
    parser.add_argument("--scope", required=True, help="现有温度/压力/流量training_scope.csv")
    parser.add_argument("--output", required=True)
    parser.add_argument("--default-cap", type=int, default=30)
    parser.add_argument("--large-cap", type=int, default=80)
    args = parser.parse_args()

    sensor = pd.read_csv(args.sensor, encoding="gb18030", dtype=str).fillna("")
    scope = pd.read_csv(args.scope, encoding="utf-8-sig", dtype=str).fillna("")
    groups = scope[["process", "equipment", "group_id", "group_name"]].drop_duplicates()
    boolean = sensor[sensor["tag_type"].str.lower().eq("boolean") & sensor["eid"].ne("")].copy()
    boolean = boolean.merge(groups, on=["process", "equipment"], how="inner")
    text_columns = [column for column in ["point", "tag_name", "tag_description", "tags", "model"] if column in boolean]
    text = boolean[text_columns].agg(" ".join, axis=1)
    score = (
        text.str.count(STRONG) * 10
        + text.str.count(GENERAL) * 2
        - text.str.count(NEGATIVE) * 8
        - text.str.count(LOW_VALUE) * 3
    )
    boolean["context_score"] = score
    boolean["context_text"] = text
    boolean = boolean[boolean["context_score"] > 0].copy()
    selected = []
    for _, group in boolean.groupby("group_id", sort=True):
        cap = args.large_cap if len(group) >= 500 else args.default_cap
        selected.append(group.sort_values(["context_score", "eid"], ascending=[False, True]).head(cap))
    result = pd.concat(selected, ignore_index=True) if selected else boolean.head(0)
    result["type"] = "BOOL"
    result["target_role"] = "bool_context"
    keep = [
        "eid", "point", "type", "tag_type", "data_type", "process", "equipment",
        "group_id", "group_name", "target_role", "context_score", "tag_name", "tag_description",
    ]
    result = result[[column for column in keep if column in result]].drop_duplicates("eid")
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output, index=False, encoding="utf-8-sig")
    print(f"selected_bool={len(result)}, groups={result['group_id'].nunique()}")
    print(result.groupby(["process", "equipment"]).size().sort_values(ascending=False).to_string())


if __name__ == "__main__":
    main()
