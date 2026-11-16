from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import duckdb
import pandas as pd


def sql_path(path: Path) -> str:
    return str(path.resolve()).replace("\\", "/").replace("'", "''")


def main() -> None:
    parser = argparse.ArgumentParser(description="统计sensor.csv全部EID在历史Parquet中的覆盖和变化")
    parser.add_argument("--sensor", required=True)
    parser.add_argument("--parquet", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--memory-limit", default="12GB")
    args = parser.parse_args()

    started = time.time()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    sensor = pd.read_csv(args.sensor, encoding="gb18030", dtype=str).fillna("")
    eids = sensor.loc[sensor["eid"].ne(""), ["eid"]].drop_duplicates().sort_values("eid")
    scope_path = output / "all_sensor_eids.csv"
    eids.to_csv(scope_path, index=False, encoding="utf-8-sig")

    connection = duckdb.connect(str(output / "history_profile.duckdb"))
    connection.execute(f"SET threads={int(args.threads)}")
    connection.execute(f"SET memory_limit='{args.memory_limit}'")
    connection.execute(f"SET temp_directory='{sql_path(output / 'duckdb_temp')}'")
    connection.execute(
        f"""
        CREATE OR REPLACE TABLE scope AS
        SELECT eid, encode(eid) AS eid_blob
        FROM read_csv('{sql_path(scope_path)}', header=true, all_varchar=true)
        """
    )
    parquet = sql_path(Path(args.parquet))
    profile = connection.execute(
        f"""
        SELECT
            s.eid,
            count(*)::UBIGINT AS raw_rows,
            count(p.v)::UBIGINT AS non_null_rows,
            sum(CASE WHEN isfinite(CAST(p.v AS DOUBLE)) THEN 1 ELSE 0 END)::UBIGINT AS finite_rows,
            min(CAST(p.v AS DOUBLE)) FILTER (WHERE isfinite(CAST(p.v AS DOUBLE))) AS minimum,
            max(CAST(p.v AS DOUBLE)) FILTER (WHERE isfinite(CAST(p.v AS DOUBLE))) AS maximum,
            stddev_pop(CAST(p.v AS DOUBLE)) FILTER (WHERE isfinite(CAST(p.v AS DOUBLE))) AS standard_deviation,
            approx_count_distinct(CAST(p.v AS DOUBLE)) FILTER (WHERE isfinite(CAST(p.v AS DOUBLE)))::UBIGINT AS approximate_distinct_values,
            min(p.ts)::UBIGINT AS first_ts,
            max(p.ts)::UBIGINT AS last_ts
        FROM read_parquet('{parquet}') AS p
        INNER JOIN scope AS s ON p.eid = s.eid_blob
        GROUP BY s.eid
        ORDER BY s.eid
        """
    ).fetch_df()
    profile.to_csv(output / "all_eid_history_profile.csv", index=False, encoding="utf-8-sig")
    report = {
        "sensor_rows": int(len(sensor)),
        "sensor_unique_nonblank_eids": int(len(eids)),
        "history_matched_eids": int(len(profile)),
        "history_unmatched_eids": int(len(eids) - len(profile)),
        "matched_raw_rows": int(profile["raw_rows"].sum()),
        "elapsed_seconds": time.time() - started,
    }
    (output / "profile_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
