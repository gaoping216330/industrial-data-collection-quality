from __future__ import annotations

import argparse
from pathlib import Path
import subprocess

import pymysql

from online_model_detector import load_json


def process_status(root: Path) -> tuple[bool, str]:
    pid_file = root / "online_model_detector.pid"
    if not pid_file.exists():
        return False, "没有PID文件"
    try:
        pid = int(pid_file.read_text(encoding="ascii").strip())
    except (OSError, ValueError):
        return False, "PID文件无效"
    completed = subprocess.run(
        ["tasklist.exe", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
        capture_output=True,
        text=True,
        encoding="mbcs",
        errors="replace",
        check=False,
    )
    running = f'"{pid}"' in completed.stdout
    return running, f"PID={pid}"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.json")
    args = parser.parse_args()
    config_path = Path(args.config).resolve()
    root = config_path.parent
    config = load_json(config_path)

    running, process_detail = process_status(root)
    print("=" * 72)
    print("传感器模型在线检测服务状态")
    print("=" * 72)
    print(f"后台进程：{'运行中' if running else '未运行'}（{process_detail}）")
    print(f"程序目录：{root}")
    print(f"运行日志：{root / 'logs' / 'online_model_detector.log'}")

    database = config["database"]
    connection = pymysql.connect(
        host=database["host"],
        port=int(database.get("port", 3306)),
        user=database["user"],
        password=database.get("password", ""),
        database=database.get("database", "gpwinccoa"),
        charset="utf8mb4",
        connect_timeout=int(database.get("connect_timeout_seconds", 8)),
    )
    try:
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(
                """
                SELECT modeltestresult, COUNT(*) AS count
                FROM autosensor
                WHERE havemodel='Y'
                GROUP BY modeltestresult
                """
            )
            counts = {row["modeltestresult"] or "尚未检测": int(row["count"]) for row in cursor.fetchall()}
            cursor.execute(
                """
                SELECT MAX(modeltesttime) AS latest_time, COUNT(modeltesttime) AS tested
                FROM autosensor
                WHERE havemodel='Y'
                """
            )
            timing = cursor.fetchone()
            cursor.execute(
                """
                SELECT eid, point, type, modeltestresult, modelmseeage
                FROM autosensor
                WHERE havemodel='Y' AND modeltestresult IN ('异常', '预警')
                ORDER BY modeltestresult, eid
                LIMIT 100
                """
            )
            abnormal = cursor.fetchall()
    finally:
        connection.close()

    print("-" * 72)
    print(f"模型点位：{sum(counts.values())} 个")
    print(f"最新检测：{timing['latest_time'] or '尚未产生结果'}")
    print(f"已有结果：{int(timing['tested'])} 个")
    print(
        "结果汇总："
        f"正常={counts.get('正常', 0)}，"
        f"预警={counts.get('预警', 0)}，"
        f"异常={counts.get('异常', 0)}，"
        f"无法判断={counts.get('无法判断', 0)}，"
        f"尚未检测={counts.get('尚未检测', 0)}"
    )
    if abnormal:
        print("-" * 72)
        print("当前预警/异常点位：")
        for row in abnormal:
            point = str(row.get("point") or "")[:24]
            message = str(row.get("modelmseeage") or "")[:100]
            print(
                f"[{row['modeltestresult']}] EID={row['eid']} "
                f"类型={row.get('type') or '-'} 点位={point} | {message}"
            )
    print("=" * 72)
    return 0 if running else 1


if __name__ == "__main__":
    raise SystemExit(main())
