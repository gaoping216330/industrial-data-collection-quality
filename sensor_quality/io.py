from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.compute as pc
import pyarrow.dataset as ds


def load_config(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_sensor_catalog(path: str | Path) -> pd.DataFrame:
    last_error: Exception | None = None
    for encoding in ("gb18030", "utf-8-sig", "utf-8"):
        try:
            return pd.read_csv(path, encoding=encoding, dtype=str, on_bad_lines="warn").fillna("")
        except UnicodeDecodeError as exc:
            last_error = exc
    raise ValueError(f"无法识别 sensor.csv 编码: {last_error}")


def validate_config_against_catalog(config: dict[str, Any], catalog: pd.DataFrame) -> None:
    if "eid" not in catalog.columns:
        raise ValueError("sensor.csv 缺少 eid 列")
    lookup = catalog.drop_duplicates("eid").set_index("eid")
    missing = [s["eid"] for s in config["signals"] if s["eid"] not in lookup.index]
    if missing:
        raise ValueError(f"配置中的 eid 不在 sensor.csv 中: {missing}")
    target = config["target"]["eid"]
    if sum(s["eid"] == target for s in config["signals"]) != 1:
        raise ValueError("目标 eid 必须在 signals 中恰好出现一次")


def _aggregate_filtered_batches(config: dict[str, Any]) -> pd.DataFrame:
    parquet_path = Path(config["parquet_path"])
    eids = [s["eid"] for s in config["signals"]]
    wanted = [eid.encode("ascii") for eid in eids]
    eid_to_code = {eid: code for code, eid in enumerate(wanted)}
    code_to_eid = {code: eid for code, eid in enumerate(eids)}
    step = int(config["resample_seconds"])
    dataset = ds.dataset(parquet_path, format="parquet")
    scanner = dataset.scanner(
        columns=["ts", "v", "eid"],
        filter=pc.field("eid").isin(wanted),
        batch_size=524_288,
        use_threads=True,
    )

    partials: list[pd.DataFrame] = []
    for batch_number, batch in enumerate(scanner.to_batches(), start=1):
        if batch.num_rows == 0:
            continue
        frame = batch.to_pandas()
        frame["eid_code"] = frame["eid"].map(eid_to_code).astype("int16")
        frame["ts_bin"] = (frame["ts"].astype("int64") // step) * step
        grouped = (
            frame.groupby(["ts_bin", "eid_code"], sort=False, observed=True)["v"]
            .agg(["sum", "count"])
            .reset_index()
        )
        partials.append(grouped)
        if len(partials) >= 6:
            partials = [_merge_partials(partials)]

    if not partials:
        raise ValueError("Parquet 中没有找到配置的 eid")
    result = _merge_partials(partials)
    result["eid"] = result["eid_code"].map(code_to_eid)
    return result.drop(columns="eid_code")


def _merge_partials(parts: list[pd.DataFrame]) -> pd.DataFrame:
    merged = pd.concat(parts, ignore_index=True)
    return (
        merged.groupby(["ts_bin", "eid_code"], sort=False, observed=True)
        .agg({"sum": "sum", "count": "sum"})
        .reset_index()
    )


def build_wide_timeseries(config: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame]:
    aggregate = _aggregate_filtered_batches(config)
    aggregate["value"] = aggregate["sum"] / aggregate["count"].clip(lower=1)
    wide = aggregate.pivot(index="ts_bin", columns="eid", values="value").sort_index()
    wide.index = pd.to_datetime(wide.index, unit="s", utc=True).tz_convert("Asia/Shanghai")

    profiles = []
    for signal in config["signals"]:
        eid = signal["eid"]
        series = wide[eid] if eid in wide else pd.Series(dtype=float)
        available = series.dropna()
        binary_ratio = float(available.isin([0.0, 1.0]).mean()) if len(available) else 0.0
        role = signal.get("role", "auto")
        if role == "boolean":
            inferred_type = "boolean"
        elif role in {"target", "analog"}:
            inferred_type = "analog"
        else:
            bounded_binary = len(available) > 0 and available.min() >= 0.0 and available.max() <= 1.0
            inferred_type = "boolean" if bounded_binary or binary_ratio >= config["binary_ratio_threshold"] else "analog"
        profiles.append(
            {
                "eid": eid,
                "name": signal.get("name", eid),
                "configured_role": role,
                "inferred_type": inferred_type,
                "rows": int(len(available)),
                "availability": float(series.notna().mean()),
                "binary_ratio": binary_ratio,
                "min": float(available.min()) if len(available) else np.nan,
                "max": float(available.max()) if len(available) else np.nan,
                "mean": float(available.mean()) if len(available) else np.nan,
                "std": float(available.std()) if len(available) else np.nan,
            }
        )
    profile_frame = pd.DataFrame(profiles)
    return wide, profile_frame


def prepare_timeseries(
    wide: pd.DataFrame, profile: pd.DataFrame, config: dict[str, Any]
) -> tuple[pd.DataFrame, list[str], list[str]]:
    target = config["target"]["eid"]
    informative = profile["std"].fillna(0.0).abs() > 1e-12
    usable = profile.loc[
        (profile["availability"] >= 0.20) & (informative | profile["eid"].eq(target)), "eid"
    ].tolist()
    if target not in usable:
        raise ValueError("目标传感器有效数据不足20%")
    data = wide[usable].copy()
    boolean = profile.loc[
        profile["eid"].isin(usable) & profile["inferred_type"].eq("boolean"), "eid"
    ].tolist()
    analog = [eid for eid in usable if eid not in boolean]

    limit = int(config["max_interpolation_bins"])
    if analog:
        data[analog] = data[analog].interpolate(limit=limit, limit_direction="both")
    if boolean:
        data[boolean] = data[boolean].ffill(limit=max(3, limit * 3)).bfill(limit=1)
        data[boolean] = (data[boolean] >= 0.5).astype(float)

    peer_columns = [c for c in data.columns if c != target]
    minimum_peers = max(1, min(3, len(peer_columns)))
    keep = data[target].notna() & (data[peer_columns].notna().sum(axis=1) >= minimum_peers)
    data = data.loc[keep]
    if len(data) < 2_000:
        raise ValueError(f"对齐后只有{len(data)}行，无法可靠训练")
    return data, boolean, analog


def save_json(value: Any, path: str | Path) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with Path(path).open("w", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2, default=str)
