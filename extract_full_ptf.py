from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import duckdb


def sql_path(path: Path) -> str:
    return str(path.resolve()).replace("\\", "/").replace("'", "''")


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def main() -> None:
    parser = argparse.ArgumentParser(description="按设备组生成压力/温度/流量训练缓存，支持断点续跑")
    parser.add_argument("--parquet", default=r"D:\数据中台数据\out.parquet")
    parser.add_argument("--scope", default=r"D:\python_work\数据中台数据质量判定\outputs\full_ptf\scope_eids.csv")
    parser.add_argument("--output", default=r"D:\python_work\数据中台数据质量判定\outputs\full_ptf\cache_10s")
    parser.add_argument("--bin-seconds", type=int, default=10)
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--memory-limit", default="12GB")
    parser.add_argument("--limit-eids", type=int, default=0, help="仅用于小规模验证")
    parser.add_argument("--known-matched-raw-rows", type=int, default=0, help="已完成预检时可复用匹配行数")
    args = parser.parse_args()

    parquet_path = Path(args.parquet)
    scope_path = Path(args.scope)
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    temp_dir = output.parent / "duckdb_temp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    database = output.parent / "extract.duckdb"
    started = time.time()

    connection = duckdb.connect(str(database))
    connection.execute(f"SET threads={int(args.threads)}")
    connection.execute(f"SET memory_limit='{args.memory_limit}'")
    connection.execute(f"SET temp_directory='{sql_path(temp_dir)}'")
    connection.execute("DROP TABLE IF EXISTS scope")
    connection.execute(
        f"""
        CREATE TABLE scope AS
        SELECT eid, encode(eid) AS eid_blob, group_id, group_name, type AS sensor_type, target_role
        FROM read_csv('{sql_path(scope_path)}', header=true, auto_detect=true, all_varchar=true)
        """
    )
    if args.limit_eids > 0:
        connection.execute(
            f"CREATE OR REPLACE TEMP TABLE active_scope AS SELECT * FROM scope ORDER BY eid LIMIT {int(args.limit_eids)}"
        )
        scope_table = "active_scope"
    else:
        scope_table = "scope"
    scope_count = connection.execute(f"SELECT count(*) FROM {scope_table}").fetchone()[0]
    matched = int(args.known_matched_raw_rows)
    if matched <= 0:
        matched = connection.execute(
            f"""
            SELECT count(*)
            FROM read_parquet('{sql_path(parquet_path)}') AS p
            SEMI JOIN {scope_table} AS s ON p.eid = s.eid_blob
            """
        ).fetchone()[0]
    print(json.dumps({"scope_eids": scope_count, "matched_raw_rows": matched}, ensure_ascii=False), flush=True)
    if matched == 0:
        raise ValueError("目录EID与Parquet未匹配，请检查BLOB/VARCHAR编码")

    step = int(args.bin_seconds)
    groups = connection.execute(
        f"SELECT group_id, any_value(group_name), count(*) FROM {scope_table} GROUP BY group_id ORDER BY group_id"
    ).fetchall()
    aggregate_rows = 0
    completed_groups = 0
    for group_index, (group_id, group_name, eid_count) in enumerate(groups, start=1):
        group_dir = output / f"group_id={group_id}"
        group_file = group_dir / "data.parquet"
        group_dir.mkdir(parents=True, exist_ok=True)
        if group_file.exists() and group_file.stat().st_size > 0:
            rows = connection.execute(
                f"SELECT count(*) FROM read_parquet('{sql_path(group_file)}')"
            ).fetchone()[0]
            aggregate_rows += int(rows)
            completed_groups += 1
            print(f"[{group_index}/{len(groups)}] 复用 {group_name}: {rows}行", flush=True)
            continue
        if group_file.exists():
            group_file.unlink()
        group_started = time.time()
        print(f"[{group_index}/{len(groups)}] 抽取 {group_name}: {eid_count}个EID", flush=True)
        result = connection.execute(
            f"""
            COPY (
                SELECT
                    any_value(s.group_name) AS group_name,
                    any_value(s.sensor_type) AS sensor_type,
                    any_value(s.target_role) AS target_role,
                    ((CAST(p.ts AS UBIGINT) // {step}) * {step})::UBIGINT AS ts_bin,
                    s.eid,
                    avg(CAST(p.v AS DOUBLE))::FLOAT AS value,
                    count(*)::UINTEGER AS source_samples
                FROM read_parquet('{sql_path(parquet_path)}') AS p
                INNER JOIN {scope_table} AS s
                    ON p.eid = s.eid_blob AND s.group_id = {sql_quote(str(group_id))}
                GROUP BY s.eid, ts_bin
            ) TO '{sql_path(group_file)}'
            (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
            """
        ).fetchone()
        rows = int(result[0]) if result else 0
        aggregate_rows += rows
        completed_groups += 1
        print(
            f"  完成: {rows}行, {group_file.stat().st_size / (1024 ** 2):.1f}MB, "
            f"{time.time() - group_started:.1f}秒",
            flush=True,
        )
    report = {
        "scope_eids": int(scope_count),
        "matched_raw_rows": int(matched),
        "aggregate_rows": int(aggregate_rows),
        "groups": len(groups),
        "completed_groups": completed_groups,
        "bin_seconds": step,
        "elapsed_seconds": time.time() - started,
        "output": str(output),
    }
    (output.parent / "extraction_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
