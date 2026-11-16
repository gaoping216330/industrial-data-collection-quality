from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd


def sql_path(path: Path) -> str:
    return str(path.resolve()).replace("\\", "/").replace("'", "''")


def main() -> None:
    parser = argparse.ArgumentParser(description="分析BOOL上下文历史覆盖与切换次数")
    parser.add_argument("--scope", required=True)
    parser.add_argument("--cache", required=True)
    parser.add_argument("--base-scope", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--min-rows", type=int, default=10_000)
    parser.add_argument("--min-switches", type=int, default=10)
    args = parser.parse_args()

    scope = pd.read_csv(args.scope, encoding="utf-8-sig", dtype=str).fillna("")
    connection = duckdb.connect()
    cache_glob = sql_path(Path(args.cache) / "group_id=*" / "*.parquet")
    profile = connection.execute(
        f"""
        WITH ordered AS (
            SELECT
                eid, ts_bin, value,
                lag(value) OVER (PARTITION BY eid ORDER BY ts_bin) AS previous_value
            FROM read_parquet('{cache_glob}')
        )
        SELECT
            eid,
            count(*)::BIGINT AS rows,
            avg(value) AS on_share,
            min(value) AS minimum,
            max(value) AS maximum,
            sum(CASE WHEN previous_value IS NOT NULL AND abs(value - previous_value) >= 0.5 THEN 1 ELSE 0 END)::BIGINT AS switches
        FROM ordered
        GROUP BY eid
        ORDER BY eid
        """
    ).fetch_df()
    result = scope.merge(profile, on="eid", how="left")
    result[["rows", "switches"]] = result[["rows", "switches"]].fillna(0).astype("int64")
    result["usable_bool"] = (
        result["rows"].ge(args.min_rows)
        & result["switches"].ge(args.min_switches)
        & (result["maximum"] - result["minimum"]).ge(0.5)
    )
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    result.to_csv(output / "bool_context_profile.csv", index=False, encoding="utf-8-sig")
    usable = result[result["usable_bool"]].copy()
    usable.to_csv(output / "usable_bool_context.csv", index=False, encoding="utf-8-sig")
    base = pd.read_csv(args.base_scope, encoding="utf-8-sig", dtype=str).fillna("")
    columns = list(dict.fromkeys([*base.columns, *usable.columns]))
    extended = pd.concat(
        [base.reindex(columns=columns, fill_value=""), usable.reindex(columns=columns, fill_value="")],
        ignore_index=True,
    )
    extended.to_csv(output / "training_scope_with_bool.csv", index=False, encoding="utf-8-sig")
    print(f"candidate={len(result)}, usable={len(usable)}, groups={usable['group_id'].nunique()}")
    if len(usable):
        print(usable.groupby(["process", "equipment"]).size().sort_values(ascending=False).to_string())


if __name__ == "__main__":
    main()
