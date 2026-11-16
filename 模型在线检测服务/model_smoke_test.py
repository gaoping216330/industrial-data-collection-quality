from __future__ import annotations

import argparse
import json
from pathlib import Path

from online_model_detector import OnlineDetector, load_json, setup_logging


def main() -> int:
    parser = argparse.ArgumentParser(description="为全部ready模型构造完整缓存，检查推理链路，不写数据库")
    parser.add_argument("--config", default="config.json")
    args = parser.parse_args()
    config_path = Path(args.config).resolve()
    config = load_json(config_path)
    setup_logging(config_path.parent, config)
    detector = OnlineDetector(config_path, config)

    step = detector.registry.bin_seconds
    bins = detector.cache_minutes * 60 // step
    end_bin = 1_800_000_000
    values: dict[str, float] = {}
    for group in detector.registry.groups:
        values.update({str(eid): float(value) for eid, value in group.bundle.state_fill_values.items()})
        for model in group.target_models.values():
            for eid, value in model.peer_fill_values.items():
                values.setdefault(str(eid), float(value))
            low, high = model.history_bounds
            values[str(model.eid)] = float((low + high) / 2.0)
        for eid in group.required_eids:
            values.setdefault(str(eid), 0.0)

    for position in range(bins):
        timestamp = end_bin - (bins - 1 - position) * step
        for eid, value in values.items():
            detector.cache.add(eid, value, True, timestamp)

    results = detector.run_detection_once(now_epoch=end_bin + step, write_database=False)
    code_counts: dict[str, int] = {}
    for row in results:
        code_counts[row.status_code] = code_counts.get(row.status_code, 0) + 1
    bad = [row for row in results if row.status_code in {"MODEL_ERROR", "GROUP_ERROR", "UNAVAILABLE"}]
    summary = {
        "ready_models": len(detector.registry.ready_eids),
        "results": len(results),
        "status_code_counts": code_counts,
        "pipeline_failures": len(bad),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if len(results) != 390 or bad:
        raise RuntimeError(f"模型冒烟测试失败: {summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
