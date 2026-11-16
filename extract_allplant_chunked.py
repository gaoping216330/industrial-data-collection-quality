"""Restartable time-chunk cache extraction for full-factory sensor training.

Each successful source-table/time-window query creates a checkpoint.  A later
run skips those checkpoints, so an interrupted multi-day extraction never
throws away completed cache data.
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import duckdb


def sql_path(path: Path) -> str:
    return str(path.resolve()).replace("\\", "/").replace("'", "''")


def quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def write_checkpoint(path: Path, payload: dict[str, object]) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Restartable full-factory 10-second cache extraction")
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--tables", default="ironworks,others,steelmaking")
    parser.add_argument("--bin-seconds", type=int, default=10)
    parser.add_argument("--chunk-hours", type=int, default=24)
    parser.add_argument("--threads", type=int, default=6)
    parser.add_argument("--memory-limit", default="10GB")
    args = parser.parse_args()

    if args.chunk_hours <= 0:
        raise SystemExit("--chunk-hours must be positive")
    data_dir = Path(args.data_dir)
    scope_path = Path(args.scope)
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    checkpoint_dir = output.parent / "cache_checkpoints"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = output.parent / "duckdb_temp_chunked"
    temp_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(output.parent / "extract_chunked.duckdb"))
    con.execute(f"SET threads={int(args.threads)}")
    con.execute(f"SET memory_limit='{args.memory_limit}'")
    con.execute(f"SET temp_directory='{sql_path(temp_dir)}'")
    con.execute("DROP TABLE IF EXISTS scope")
    con.execute(
        "CREATE TABLE scope AS "
        f"SELECT eid, encode(eid) AS eid_blob, \"table\" AS source_table, group_id "
        f"FROM read_csv('{sql_path(scope_path)}', header=true, all_varchar=true)"
    )

    requested_tables = [part.strip() for part in args.tables.split(",") if part.strip()]
    chunk_seconds = int(args.chunk_hours) * 3600
    for table in requested_tables:
        parquet = data_dir / f"collect_{table}.parquet"
        if not parquet.exists():
            print(f"skip missing source: {parquet}", flush=True)
            continue
        scoped = con.execute(
            f"SELECT count(*) FROM scope WHERE source_table={quote(table)}"
        ).fetchone()[0]
        if not scoped:
            print(f"skip {table}: no scoped EIDs", flush=True)
            continue

        minimum, maximum = con.execute(
            f"SELECT min(ts), max(ts) FROM read_parquet('{sql_path(parquet)}')"
        ).fetchone()
        if minimum is None or maximum is None:
            print(f"skip {table}: no source rows", flush=True)
            continue
        start = (int(minimum) // chunk_seconds) * chunk_seconds
        end_limit = int(maximum) + 1
        print(
            f"{table}: source_ts=[{minimum},{maximum}], chunk_hours={args.chunk_hours}, scoped_eids={scoped}",
            flush=True,
        )
        while start < end_limit:
            end = min(start + chunk_seconds, end_limit)
            checkpoint = checkpoint_dir / f"{table}_{start}_{end}.json"
            if checkpoint.exists():
                print(f"skip complete {table} [{start},{end})", flush=True)
                start = end
                continue
            chunk_name = f"chunk_{start}_{end}"
            destination = output / f"source_table={table}" / chunk_name
            destination.mkdir(parents=True, exist_ok=True)
            began = time.time()
            print(f"begin {table} [{start},{end})", flush=True)
            con.execute(
                "COPY ("
                "SELECT s.group_id, s.eid, "
                f"((CAST(p.ts AS UBIGINT)//{int(args.bin_seconds)})*{int(args.bin_seconds)})::UBIGINT AS ts_bin, "
                "avg(CAST(p.v AS DOUBLE))::FLOAT AS \"value\", count(*)::UINTEGER AS source_samples "
                f"FROM read_parquet('{sql_path(parquet)}') p "
                "INNER JOIN scope s ON p.eid=s.eid_blob "
                f"AND s.source_table={quote(table)} "
                f"WHERE p.ts >= {start} AND p.ts < {end} "
                "GROUP BY s.group_id, s.eid, ts_bin"
                f") TO '{sql_path(destination)}' "
                "(FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (group_id), OVERWRITE_OR_IGNORE true)"
            )
            write_checkpoint(
                checkpoint,
                {
                    "table": table,
                    "start_ts": start,
                    "end_ts": end,
                    "completed_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                    "elapsed_seconds": round(time.time() - began, 3),
                    "destination": str(destination),
                },
            )
            print(f"complete {table} [{start},{end}) in {time.time()-began:.1f}s", flush=True)
            start = end
    print("all requested cache chunks complete", flush=True)


if __name__ == "__main__":
    main()
