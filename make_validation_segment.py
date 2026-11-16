from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def sql_path(path: Path) -> str:
    return str(path.resolve()).replace("\\", "/").replace("'", "''")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--context-input", default="")
    parser.add_argument("--output", required=True)
    parser.add_argument("--seconds", type=int, default=1800)
    args = parser.parse_args()
    source = Path(args.input)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    source_sql = sql_path(source)
    output_sql = sql_path(output)
    sources = [source_sql]
    if args.context_input:
        context_source = Path(args.context_input)
        if context_source.exists() and context_source.stat().st_size > 0:
            sources.append(sql_path(context_source))
    read_source = "'" + sources[0] + "'" if len(sources) == 1 else "[" + ",".join("'" + item + "'" for item in sources) + "]"
    connection.execute(
        f"""
        COPY (
            SELECT ts_bin AS ts, eid, value AS v
            FROM read_parquet({read_source}, union_by_name=true)
            WHERE ts_bin >= (
                SELECT max(ts_bin) - {int(args.seconds)} FROM read_parquet({read_source}, union_by_name=true)
            )
        ) TO '{output_sql}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    print(
        connection.execute(
            f"SELECT count(*), count(distinct eid), min(ts), max(ts) FROM read_parquet('{output_sql}')"
        ).fetchone()
    )


if __name__ == "__main__":
    main()
