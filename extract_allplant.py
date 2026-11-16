from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import duckdb


def path_sql(path: Path) -> str:
    return str(path.resolve()).replace("\\", "/").replace("'", "''")


def quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def main() -> None:
    parser = argparse.ArgumentParser(description="按历史分表一次扫描并输出全厂设备组10秒训练缓存")
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--bin-seconds", type=int, default=10)
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--memory-limit", default="20GB")
    parser.add_argument(
        "--only-tables",
        default="",
        help="Comma-separated source tables to extract; use for safe resume.",
    )
    args = parser.parse_args()
    data_dir, scope_path, output = Path(args.data_dir), Path(args.scope), Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    temp = output.parent / "duckdb_temp"
    temp.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(output.parent / "extract.duckdb"))
    con.execute(f"SET threads={int(args.threads)}")
    con.execute(f"SET memory_limit='{args.memory_limit}'")
    con.execute(f"SET temp_directory='{path_sql(temp)}'")
    con.execute("DROP TABLE IF EXISTS scope")
    con.execute(f"CREATE TABLE scope AS SELECT eid, encode(eid) eid_blob, \"table\" source_table, group_id, group_name, type sensor_type, target_role FROM read_csv('{path_sql(scope_path)}', header=true, all_varchar=true)")
    tables = [row[0] for row in con.execute("SELECT DISTINCT source_table FROM scope ORDER BY source_table").fetchall()]
    if args.only_tables:
        requested = {value.strip() for value in args.only_tables.split(",") if value.strip()}
        tables = [table for table in tables if table in requested]
        if not tables:
            raise SystemExit("No requested source tables are present in the scope.")
    totals = {}
    started = time.time()
    for i, table in enumerate(tables, 1):
        parquet = data_dir / f"collect_{table}.parquet"
        if not parquet.exists():
            print(f"[{i}/{len(tables)}] skip missing {parquet.name}", flush=True); continue
        source_output = output / f"source_table={table}"
        source_output.mkdir(parents=True, exist_ok=True)
        count = con.execute(
            f"SELECT count(*) FROM read_parquet('{path_sql(parquet)}') p "
            f"SEMI JOIN (SELECT eid_blob FROM scope WHERE source_table={quote(table)}) s ON p.eid=s.eid_blob"
        ).fetchone()[0]
        print(f"[{i}/{len(tables)}] {table}: matched_raw_rows={count}", flush=True)
        if count == 0: continue
        con.execute(f"COPY (SELECT s.group_id, s.eid, ((CAST(p.ts AS UBIGINT)//{int(args.bin_seconds)})*{int(args.bin_seconds)})::UBIGINT AS ts_bin, avg(CAST(p.v AS DOUBLE))::FLOAT AS \"value\", count(*)::UINTEGER AS source_samples FROM read_parquet('{path_sql(parquet)}') p INNER JOIN scope s ON p.eid=s.eid_blob AND s.source_table={quote(table)} GROUP BY s.group_id,s.eid,ts_bin) TO '{path_sql(source_output)}' (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (group_id), OVERWRITE_OR_IGNORE true)")
        totals[table] = int(count)
    report = {"matched_raw_rows": totals, "elapsed_seconds": time.time()-started, "output": str(output)}
    (output.parent / "extraction_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
