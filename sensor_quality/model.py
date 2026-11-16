from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from sklearn.cluster import MiniBatchKMeans
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, silhouette_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import RobustScaler
from sklearn.preprocessing import StandardScaler


@dataclass
class ModelBundle:
    config: dict[str, Any]
    boolean_columns: list[str]
    analog_columns: list[str]
    feature_columns: list[str]
    feature_medians: dict[str, float]
    state_columns: list[str]
    state_medians: dict[str, float]
    state_scaler: RobustScaler
    state_model: MiniBatchKMeans
    regressors: dict[int, Any]
    fallback_regressor: Any
    residual_limits: dict[int, tuple[float, float]]
    fallback_limits: tuple[float, float]
    train_target_bounds: tuple[float, float]
    derivative_limit: float


def _even_sample(frame: pd.DataFrame, maximum: int) -> pd.DataFrame:
    if len(frame) <= maximum:
        return frame
    positions = np.linspace(0, len(frame) - 1, maximum, dtype=int)
    return frame.iloc[positions]


def build_state_features(
    data: pd.DataFrame, target: str, boolean: list[str], analog: list[str]
) -> pd.DataFrame:
    peers = [c for c in data.columns if c != target]
    result = data[peers].copy()
    for column in [c for c in analog if c != target]:
        result[f"{column}__diff"] = data[column].diff()
        result[f"{column}__mean6"] = data[column].rolling(6, min_periods=1).mean()
    for column in boolean:
        if column in data:
            result[f"{column}__switch"] = data[column].diff().abs().fillna(0.0)
    return result.replace([np.inf, -np.inf], np.nan)


def fit_states(
    state_features: pd.DataFrame, train_end: int, config: dict[str, Any]
) -> tuple[RobustScaler, MiniBatchKMeans, dict[str, float], np.ndarray, dict[str, Any]]:
    train = state_features.iloc[:train_end]
    medians = train.median().fillna(0.0).to_dict()
    filled = state_features.fillna(medians).fillna(0.0)
    fit_frame = _even_sample(filled.iloc[:train_end], int(config["state_fit_rows"]))
    scaler = RobustScaler(quantile_range=(10.0, 90.0)).fit(fit_frame)
    fit_scaled = np.clip(scaler.transform(fit_frame), -12.0, 12.0)

    rng = np.random.default_rng(config["random_seed"])
    score_size = min(20_000, len(fit_scaled))
    score_index = rng.choice(len(fit_scaled), score_size, replace=False)
    score_sample = fit_scaled[score_index]
    candidates: list[tuple[float, int, float]] = []
    candidate_models: dict[int, MiniBatchKMeans] = {}
    for k in range(int(config["state_clusters_min"]), int(config["state_clusters_max"]) + 1):
        probe = MiniBatchKMeans(
            n_clusters=k,
            random_state=config["random_seed"],
            batch_size=4096,
            n_init=5,
        ).fit(fit_scaled)
        candidate_models[k] = probe
        labels = probe.predict(score_sample)
        score = silhouette_score(score_sample, labels, sample_size=min(8_000, len(labels)), random_state=config["random_seed"])
        shares = np.bincount(labels, minlength=k) / len(labels)
        candidates.append((float(score), k, float(shares.min())))
    stable_candidates = [item for item in candidates if item[2] >= 0.005]
    best_score, best_k, _ = max(stable_candidates or candidates, key=lambda item: item[0])
    model = candidate_models[best_k]
    all_scaled = np.clip(scaler.transform(filled), -12.0, 12.0)
    raw_states = model.predict(all_scaled)
    states = smooth_short_states(raw_states, minimum_duration=3)
    diagnostics = {
        "selected_clusters": best_k,
        "selected_silhouette": best_score,
        "candidate_scores": [
            {"clusters": k, "silhouette": s, "smallest_cluster_fraction": share}
            for s, k, share in sorted(candidates, key=lambda x: x[1])
        ],
    }
    return scaler, model, medians, states, diagnostics


def smooth_short_states(states: np.ndarray, minimum_duration: int = 3) -> np.ndarray:
    result = states.copy()
    start = 0
    while start < len(result):
        end = start + 1
        while end < len(result) and result[end] == result[start]:
            end += 1
        if end - start < minimum_duration:
            replacement = result[start - 1] if start > 0 else (result[end] if end < len(result) else result[start])
            result[start:end] = replacement
        start = end
    return result


def build_regression_features(
    data: pd.DataFrame, target: str, analog: list[str], states: np.ndarray, n_states: int
) -> pd.DataFrame:
    peers = [c for c in data.columns if c != target]
    features = data[peers].copy()
    for column in [c for c in analog if c != target]:
        features[f"{column}__lag1"] = data[column].shift(1)
        features[f"{column}__diff1"] = data[column].diff()
        features[f"{column}__mean6"] = data[column].rolling(6, min_periods=1).mean()
    for state in range(n_states):
        features[f"state_{state}"] = (states == state).astype(float)
    return features.replace([np.inf, -np.inf], np.nan)


def _new_tree_regressor(seed: int) -> HistGradientBoostingRegressor:
    return HistGradientBoostingRegressor(
        learning_rate=0.06,
        max_iter=220,
        max_leaf_nodes=31,
        min_samples_leaf=35,
        l2_regularization=1.0,
        early_stopping=True,
        validation_fraction=0.12,
        random_state=seed,
    )


def _new_linear_regressor() -> Any:
    return make_pipeline(StandardScaler(), Ridge(alpha=0.1))


def _new_regressor(kind: str, seed: int) -> Any:
    return _new_linear_regressor() if kind == "linear" else _new_tree_regressor(seed)


def _robust_fit(x: pd.DataFrame, y: pd.Series, seed: int) -> Any:
    split = max(500, int(len(x) * 0.80))
    split = min(split, len(x) - 100)
    candidate_errors: dict[str, float] = {}
    if split > 0:
        for kind in ("linear", "tree"):
            candidate = _new_regressor(kind, seed).fit(x.iloc[:split], y.iloc[:split])
            candidate_errors[kind] = mean_absolute_error(y.iloc[split:], candidate.predict(x.iloc[split:]))
    selected_kind = min(candidate_errors, key=candidate_errors.get) if candidate_errors else "tree"
    first = _new_regressor(selected_kind, seed).fit(x, y)
    residual = y.to_numpy() - first.predict(x)
    cutoff = np.quantile(np.abs(residual), 0.98)
    keep = np.abs(residual) <= cutoff
    if keep.sum() < 500:
        return first
    return _new_regressor(selected_kind, seed).fit(x.iloc[keep], y.iloc[keep])


def train_bundle(
    data: pd.DataFrame, boolean: list[str], analog: list[str], config: dict[str, Any]
) -> tuple[ModelBundle, pd.DataFrame, dict[str, Any]]:
    target = config["target"]["eid"]
    n = len(data)
    train_end = int(n * config["train_fraction"])
    validation_end = int(n * (config["train_fraction"] + config["validation_fraction"]))
    state_features = build_state_features(data, target, boolean, analog)
    scaler, state_model, state_medians, states, state_diag = fit_states(state_features, train_end, config)
    n_states = state_model.n_clusters
    features = build_regression_features(data, target, analog, states, n_states)
    medians = features.iloc[:train_end].median().fillna(0.0).to_dict()
    features = features.fillna(medians).fillna(0.0)

    regressors: dict[int, Any] = {}
    max_rows = int(config["regression_fit_rows_per_state"])
    for state in range(n_states):
        mask = states[:train_end] == state
        x_state = features.iloc[:train_end].loc[mask]
        y_state = data[target].iloc[:train_end].loc[mask]
        if len(x_state) < 500:
            continue
        sample = _even_sample(pd.concat([x_state, y_state.rename("__target")], axis=1), max_rows)
        regressors[state] = _robust_fit(sample[x_state.columns], sample["__target"], config["random_seed"] + state)

    fallback_sample = _even_sample(
        pd.concat([features.iloc[:train_end], data[target].iloc[:train_end].rename("__target")], axis=1),
        max_rows,
    )
    fallback = _robust_fit(fallback_sample[features.columns], fallback_sample["__target"], config["random_seed"])
    predicted = predict_with_states(features, states, regressors, fallback)
    residual = data[target].to_numpy() - predicted

    validation_slice = slice(train_end, validation_end)
    fallback_limits = tuple(np.quantile(residual[validation_slice], [1 - config["residual_quantile"], config["residual_quantile"]]))
    target_iqr = float(np.subtract(*np.quantile(data[target].iloc[:train_end], [0.75, 0.25])))
    residual_floor = max(target_iqr * 0.001, 1e-6)
    limits: dict[int, tuple[float, float]] = {}
    for state in range(n_states):
        mask = states[validation_slice] == state
        values = residual[validation_slice][mask]
        raw_limits = tuple(np.quantile(values, [1 - config["residual_quantile"], config["residual_quantile"]])) if len(values) >= 100 else fallback_limits
        limits[state] = (min(float(raw_limits[0]), -residual_floor), max(float(raw_limits[1]), residual_floor))

    train_target = data[target].iloc[:train_end]
    q_low, q_high = np.quantile(train_target, [0.001, 0.999])
    margin = max((q_high - q_low) * 0.10, 1e-6)
    derivative_limit = float(np.quantile(train_target.diff().abs().dropna(), 0.9995))
    bundle = ModelBundle(
        config=config,
        boolean_columns=boolean,
        analog_columns=analog,
        feature_columns=features.columns.tolist(),
        feature_medians={k: float(v) for k, v in medians.items()},
        state_columns=state_features.columns.tolist(),
        state_medians={k: float(v) for k, v in state_medians.items()},
        state_scaler=scaler,
        state_model=state_model,
        regressors=regressors,
        fallback_regressor=fallback,
        residual_limits=limits,
        fallback_limits=(float(fallback_limits[0]), float(fallback_limits[1])),
        train_target_bounds=(float(q_low - margin), float(q_high + margin)),
        derivative_limit=derivative_limit,
    )
    scored = score_prepared_data(bundle, data)
    diagnostics = {
        "rows": n,
        "train_rows": train_end,
        "validation_rows": validation_end - train_end,
        "test_rows": n - validation_end,
        "train_start": str(data.index[0]),
        "train_end": str(data.index[train_end - 1]),
        "validation_end": str(data.index[validation_end - 1]),
        "test_end": str(data.index[-1]),
        "state_model": state_diag,
        "state_counts": pd.Series(states).value_counts().sort_index().to_dict(),
        "selected_regressors": {
            str(state): type(model.steps[-1][1]).__name__ if hasattr(model, "steps") else type(model).__name__
            for state, model in regressors.items()
        },
        "fallback_regressor": type(fallback.steps[-1][1]).__name__ if hasattr(fallback, "steps") else type(fallback).__name__,
        "state_profiles": {
            str(state): {
                "rows": int((states == state).sum()),
                "medians": {
                    column: float(value)
                    for column, value in data.loc[states == state].median().to_dict().items()
                },
            }
            for state in range(n_states)
        },
        "residual_limits": {str(k): list(v) for k, v in limits.items()},
        "fallback_limits": list(fallback_limits),
        "residual_floor": residual_floor,
        "test_raw_anomaly_rate": float(scored["raw_anomaly"].iloc[validation_end:].mean()),
        "test_confirmed_anomaly_rate": float(scored["confirmed_anomaly"].iloc[validation_end:].mean()),
    }
    return bundle, scored, diagnostics


def predict_with_states(
    features: pd.DataFrame,
    states: np.ndarray,
    regressors: dict[int, Any],
    fallback: Any,
) -> np.ndarray:
    prediction = fallback.predict(features)
    for state, model in regressors.items():
        positions = np.flatnonzero(states == state)
        if len(positions):
            prediction[positions] = model.predict(features.iloc[positions])
    return prediction


def infer_states(bundle: ModelBundle, data: pd.DataFrame) -> np.ndarray:
    target = bundle.config["target"]["eid"]
    features = build_state_features(data, target, bundle.boolean_columns, bundle.analog_columns)
    features = features.reindex(columns=bundle.state_columns).fillna(bundle.state_medians).fillna(0.0)
    scaled = np.clip(bundle.state_scaler.transform(features), -12.0, 12.0)
    return smooth_short_states(bundle.state_model.predict(scaled), minimum_duration=3)


def score_prepared_data(bundle: ModelBundle, data: pd.DataFrame) -> pd.DataFrame:
    target = bundle.config["target"]["eid"]
    states = infer_states(bundle, data)
    features = build_regression_features(data, target, bundle.analog_columns, states, bundle.state_model.n_clusters)
    features = features.reindex(columns=bundle.feature_columns).fillna(bundle.feature_medians).fillna(0.0)
    predicted = predict_with_states(features, states, bundle.regressors, bundle.fallback_regressor)
    actual = data[target].to_numpy()
    residual = actual - predicted
    lower = np.array([bundle.residual_limits.get(int(s), bundle.fallback_limits)[0] for s in states])
    upper = np.array([bundle.residual_limits.get(int(s), bundle.fallback_limits)[1] for s in states])
    raw = (residual < lower) | (residual > upper)
    persistence = pd.Series(raw, index=data.index).rolling(
        int(bundle.config["persistence_window"]), min_periods=1
    ).sum().to_numpy()
    confirmed = persistence >= int(bundle.config["persistence_required"])
    width = np.maximum(np.maximum(np.abs(lower), np.abs(upper)), 1e-6)
    score = np.clip(np.abs(residual) / width, 0.0, 5.0)
    health = np.clip(100.0 - score * 25.0, 0.0, 100.0)

    derivative = pd.Series(actual, index=data.index).diff().abs().to_numpy()
    spike = derivative > bundle.derivative_limit
    out_of_history = (actual < bundle.train_target_bounds[0]) | (actual > bundle.train_target_bounds[1])
    reason = np.full(len(data), "正常", dtype=object)
    reason[raw] = "与相关传感器不一致"
    reason[out_of_history] = "超出历史鲁棒范围"
    reason[spike] = "突变"
    reason[confirmed] = "持续性相关残差异常"

    return pd.DataFrame(
        {
            "actual": actual,
            "predicted": predicted,
            "prediction_lower": predicted + lower,
            "prediction_upper": predicted + upper,
            "residual": residual,
            "state": states,
            "raw_anomaly": raw,
            "confirmed_anomaly": confirmed,
            "health_score": health,
            "reason": reason,
        },
        index=data.index,
    )


def synthetic_fault_evaluation(bundle: ModelBundle, data: pd.DataFrame) -> dict[str, Any]:
    baseline = score_prepared_data(bundle, data)
    target = bundle.config["target"]["eid"]
    n = len(data)
    segment = max(20, min(120, n // 10))
    starts = np.linspace(max(10, n // 10), max(11, n - segment - 1), 4, dtype=int)
    predicted_variation = baseline["predicted"].rolling(segment, min_periods=segment).std()
    if predicted_variation.notna().any():
        stuck_end = int(np.nanargmax(predicted_variation.to_numpy()))
        starts[2] = max(0, stuck_end - segment + 1)
    width = max(abs(bundle.fallback_limits[0]), abs(bundle.fallback_limits[1]), data[target].std() * 0.05, 1e-6)
    cases: dict[str, Any] = {}
    for fault, start in zip(("bias", "drift", "stuck", "noise"), starts):
        changed = data.copy()
        stop = min(n, start + segment)
        length = stop - start
        if fault == "bias":
            changed.iloc[start:stop, changed.columns.get_loc(target)] += 4.0 * width
        elif fault == "drift":
            changed.iloc[start:stop, changed.columns.get_loc(target)] += np.linspace(0.0, 6.0 * width, length)
        elif fault == "stuck":
            changed.iloc[start:stop, changed.columns.get_loc(target)] = changed[target].iloc[start]
        else:
            rng = np.random.default_rng(bundle.config["random_seed"])
            changed.iloc[start:stop, changed.columns.get_loc(target)] += rng.normal(0.0, 4.0 * width, length)
        scored = score_prepared_data(bundle, changed)
        injected = scored["confirmed_anomaly"].iloc[start:stop]
        cases[fault] = {
            "segment_rows": int(length),
            "detected_rows": int(injected.sum()),
            "recall": float(injected.mean()),
            "first_detection_delay_rows": int(np.argmax(injected.to_numpy())) if injected.any() else None,
        }
    cases["baseline_confirmed_rate"] = float(baseline["confirmed_anomaly"].mean())
    return cases
