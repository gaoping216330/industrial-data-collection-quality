from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def main() -> None:
    parser = argparse.ArgumentParser(description="补充全量模型部署门槛")
    parser.add_argument("--models", required=True)
    args = parser.parse_args()
    root = Path(args.models)
    summary_path = root / "model_summary.csv"
    manifest_path = root / "manifest.json"
    frame = pd.read_csv(summary_path, encoding="utf-8-sig", dtype={"eid": str})
    trained = frame["status"].eq("trained")
    rate = pd.to_numeric(frame.get("test_confirmed_anomaly_rate"), errors="coerce")
    confidence = frame.get("confidence_grade", pd.Series(index=frame.index, dtype=str)).fillna("")
    ready = trained & confidence.isin(["A", "B"]) & rate.le(0.05)
    observe = trained & ~ready & rate.le(0.20)
    frame["deployment_status"] = "untrainable"
    frame.loc[trained, "deployment_status"] = "needs_recalibration"
    frame.loc[observe, "deployment_status"] = "observation_only"
    frame.loc[ready, "deployment_status"] = "ready"
    frame.to_csv(summary_path, index=False, encoding="utf-8-sig")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["model_summary_path"] = str(summary_path)
    manifest["by_deployment_status"] = {
        str(key): int(value) for key, value in frame["deployment_status"].value_counts().items()
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(manifest["by_deployment_status"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
