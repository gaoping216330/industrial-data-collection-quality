from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

import pandas as pd


def main() -> None:
    parser = argparse.ArgumentParser(description="按设备组合并基础模型与BOOL增强模型")
    parser.add_argument("--base", required=True)
    parser.add_argument("--bool", required=True, dest="bool_models")
    parser.add_argument("--output", required=True)
    parser.add_argument("--bool-groups", required=True, help="采用BOOL增强模型的group_id，逗号分隔")
    args = parser.parse_args()

    base = Path(args.base)
    enhanced = Path(args.bool_models)
    output = Path(args.output)
    selected = {item.strip() for item in args.bool_groups.split(",") if item.strip()}
    group_dir = output / "groups"
    report_dir = output / "group_reports"
    group_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    base_manifest = json.loads((base / "manifest.json").read_text(encoding="utf-8"))
    bool_manifest = json.loads((enhanced / "manifest.json").read_text(encoding="utf-8"))
    group_models: dict[str, str] = {}
    for group_id in base_manifest["group_models"]:
        source_root = enhanced if group_id in selected else base
        source_model = source_root / "groups" / f"{group_id}.joblib"
        target_model = group_dir / source_model.name
        shutil.copy2(source_model, target_model)
        source_report = source_root / "group_reports" / f"{group_id}.json"
        if source_report.exists():
            shutil.copy2(source_report, report_dir / source_report.name)
        group_models[group_id] = str(target_model)

    old = pd.read_csv(base / "model_summary.csv", encoding="utf-8-sig", dtype={"eid": str})
    new = pd.read_csv(enhanced / "model_summary.csv", encoding="utf-8-sig", dtype={"eid": str})
    production = old[~old["group_id"].isin(selected)].copy()
    production = pd.concat([production, new[new["group_id"].isin(selected)]], ignore_index=True)
    production["model_source"] = production["group_id"].map(
        lambda group_id: "bool_enhanced" if group_id in selected else "base"
    )
    production.to_csv(output / "model_summary.csv", index=False, encoding="utf-8-sig")

    compare = old.merge(new, on="eid", suffixes=("_base", "_bool"), how="outer")
    compare["selected_source"] = compare["group_id_base"].fillna(compare["group_id_bool"]).map(
        lambda group_id: "bool_enhanced" if group_id in selected else "base"
    )
    compare.to_csv(output / "model_comparison.csv", index=False, encoding="utf-8-sig")

    trained = production["status"].eq("trained")
    manifest = dict(base_manifest)
    manifest.update(
        {
            "created_at": pd.Timestamp.now(tz="Asia/Shanghai").isoformat(),
            "scope_path": bool_manifest.get("scope_path", base_manifest.get("scope_path")),
            "group_models": group_models,
            "model_summary_path": str(output / "model_summary.csv"),
            "model_strategy": "hybrid_by_group_validation",
            "bool_enhanced_groups": sorted(selected),
            "bool_context_eids": 21,
            "context_cache_path": bool_manifest.get("context_cache_path"),
            "scope_targets": int(len(production)),
            "trained_targets": int(trained.sum()),
            "untrainable_targets": int((~trained).sum()),
            "by_type": production.loc[trained].groupby("type").size().to_dict(),
            "by_confidence": production.loc[trained].groupby("confidence_grade").size().to_dict(),
            "by_deployment_status": production["deployment_status"].value_counts().to_dict(),
        }
    )
    (output / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(manifest["by_deployment_status"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
