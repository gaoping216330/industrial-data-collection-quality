from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from sklearn.cluster import MiniBatchKMeans
from sklearn.preprocessing import RobustScaler

from .model import (
    ModelBundle,
    build_state_features,
    fit_states,
    score_prepared_data,
)


@dataclass
class ProcessNoveltyBundle:
    config: dict[str, Any]
    boolean_columns: list[str]
    analog_columns: list[str]
    feature_columns: list[str]
    feature_medians: dict[str, float]
    scaler: RobustScaler
    state_model: MiniBatchKMeans
    distance_limits: dict[int, float]
    fallback_limit: float


def fit_process_novelty(
    data: pd.DataFrame,
    boolean: list[str],
    analog: list[str],
    config: dict[str, Any],
) -> tuple[ProcessNoveltyBundle, pd.DataFrame, dict[str, Any]]:
    n = len(data)
    train_end = int(n * config["train_fraction"])
    validation_end = int(n * (config["train_fraction"] + config["validation_fraction"]))
    features = build_state_features(data, "", boolean, analog)
    scaler, state_model, medians, states, diagnostics = fit_states(features, train_end, config)
    filled = features.fillna(medians).fillna(0.0)
    scaled = np.clip(scaler.transform(filled), -12.0, 12.0)
    distances = state_model.transform(scaled)[np.arange(n), states]

    validation = distances[train_end:validation_end]
    fallback = float(np.quantile(validation, config["residual_quantile"]))
    limits: dict[int, float] = {}
    for state in range(state_model.n_clusters):
        state_values = validation[states[train_end:validation_end] == state]
        limits[state] = float(np.quantile(state_values, config["residual_quantile"])) if len(state_values) >= 100 else fallback
        limits[state] = max(limits[state], 1e-6)

    bundle = ProcessNoveltyBundle(
        config=config,
        boolean_columns=boolean,
        analog_columns=analog,
        feature_columns=features.columns.tolist(),
        feature_medians={key: float(value) for key, value in medians.items()},
        scaler=scaler,
        state_model=state_model,
        distance_limits=limits,
        fallback_limit=max(fallback, 1e-6),
    )
    scored = score_process_novelty(bundle, data)
    diagnostics.update(
        {
            "train_rows": train_end,
            "validation_rows": validation_end - train_end,
            "test_rows": n - validation_end,
            "distance_limits": {str(key): value for key, value in limits.items()},
            "test_raw_novelty_rate": float(scored["process_raw_novelty"].iloc[validation_end:].mean()),
            "test_confirmed_novelty_rate": float(scored["process_confirmed_novelty"].iloc[validation_end:].mean()),
        }
    )
    return bundle, scored, diagnostics


def score_process_novelty(bundle: ProcessNoveltyBundle, data: pd.DataFrame) -> pd.DataFrame:
    features = build_state_features(data, "", bundle.boolean_columns, bundle.analog_columns)
    features = features.reindex(columns=bundle.feature_columns).fillna(bundle.feature_medians).fillna(0.0)
    scaled = np.clip(bundle.scaler.transform(features), -12.0, 12.0)
    states = bundle.state_model.predict(scaled)
    distances = bundle.state_model.transform(scaled)[np.arange(len(data)), states]
    limits = np.array([bundle.distance_limits.get(int(state), bundle.fallback_limit) for state in states])
    raw = distances > limits
    persistence = pd.Series(raw, index=data.index).rolling(
        int(bundle.config["persistence_window"]), min_periods=1
    ).sum()
    confirmed = persistence >= int(bundle.config["persistence_required"])
    novelty_score = np.clip(distances / np.maximum(limits, 1e-6), 0.0, 10.0)
    return pd.DataFrame(
        {
            "process_state": states,
            "process_distance": distances,
            "process_distance_limit": limits,
            "process_novelty_score": novelty_score,
            "process_raw_novelty": raw,
            "process_confirmed_novelty": confirmed.to_numpy(),
        },
        index=data.index,
    )


def combine_multi_sensor_scores(
    target_scores: dict[str, pd.DataFrame], process_score: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not target_scores:
        raise ValueError("没有可合并的目标传感器结果")
    confirmed = pd.DataFrame(
        {eid: frame["confirmed_anomaly"].astype(bool) for eid, frame in target_scores.items()}
    )
    raw = pd.DataFrame({eid: frame["raw_anomaly"].astype(bool) for eid, frame in target_scores.items()})
    health = pd.DataFrame({eid: frame["health_score"] for eid, frame in target_scores.items()})
    count = confirmed.sum(axis=1).astype("int16")
    raw_count = raw.sum(axis=1).astype("int16")
    target_count = len(target_scores)
    common_mode_count = max(2, int(np.ceil(target_count * 0.40)))
    process_novel = process_score["process_confirmed_novelty"].astype(bool)

    status = np.full(len(count), "正常", dtype=object)
    status[(count == 1) & ~process_novel] = "单传感器疑似异常"
    status[(count >= 2) & (count < common_mode_count) & ~process_novel] = "多传感器不一致"
    status[(count == 0) & process_novel] = "未知或新工况"
    status[(count == 1) & process_novel] = "传感器异常与新工况并存"
    status[(count >= common_mode_count) | ((count >= 2) & process_novel)] = "工艺或共模异常"

    suspect = pd.Series("", index=count.index, dtype=object)
    active_rows = count[count > 0].index
    if len(active_rows):
        suspect.loc[active_rows] = confirmed.loc[active_rows].apply(
            lambda row: "|".join(row.index[row.to_numpy()].tolist()), axis=1
        )

    consensus = process_score.copy()
    consensus["target_count"] = target_count
    consensus["raw_sensor_count"] = raw_count
    consensus["confirmed_sensor_count"] = count
    consensus["min_sensor_health"] = health.min(axis=1)
    consensus["mean_sensor_health"] = health.mean(axis=1)
    consensus["suspect_eids"] = suspect
    consensus["system_status"] = status

    long_frames = []
    for eid, frame in target_scores.items():
        part = frame.copy()
        part.insert(0, "eid", eid)
        part.insert(0, "ts", part.index)
        long_frames.append(part.reset_index(drop=True))
    detail = pd.concat(long_frames, ignore_index=True)
    return consensus, detail


def score_multi_bundles(
    models: dict[str, ModelBundle],
    process_bundle: ProcessNoveltyBundle,
    data: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, pd.DataFrame]]:
    target_scores = {eid: score_prepared_data(bundle, data) for eid, bundle in models.items()}
    process_score = score_process_novelty(process_bundle, data)
    consensus, detail = combine_multi_sensor_scores(target_scores, process_score)
    return consensus, detail, target_scores
