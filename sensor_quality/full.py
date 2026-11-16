from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from sklearn.cluster import MiniBatchKMeans
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, silhouette_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import RobustScaler, StandardScaler


@dataclass
class TargetHealthModel:
    eid: str
    name: str
    sensor_type: str
    model_kind: str
    peer_eids: list[str]
    peer_fill_values: dict[str, float]
    estimator: Any
    residual_limits: dict[int, tuple[float, float]]
    fallback_limits: tuple[float, float]
    history_bounds: tuple[float, float]
    derivative_limit: float
    resolution: float
    persistence_window: int
    persistence_required: int
    history_window: int
    confidence_grade: str
    availability: float
    validation_mae: float
    # A target is considered stuck only while its selected peer signals are
    # actively moving.  This prevents normal stop/idle plateaus from being
    # treated as sensor faults.
    stuck_peer_activity_threshold: float | None = None


@dataclass
class GroupHealthBundle:
    group_id: str
    group_name: str
    bin_seconds: int
    all_eids: list[str]
    state_eids: list[str]
    state_fill_values: dict[str, float]
    state_scaler: RobustScaler | None
    state_model: MiniBatchKMeans | None
    target_models: dict[str, TargetHealthModel]
    catalog: dict[str, dict[str, str]]


def _sample_positions(length: int, maximum: int) -> np.ndarray:
    if length <= maximum:
        return np.arange(length)
    return np.linspace(0, length - 1, maximum, dtype=int)


def _fill_frame(frame: pd.DataFrame, medians: dict[str, float]) -> pd.DataFrame:
    return frame.replace([np.inf, -np.inf], np.nan).fillna(medians).fillna(0.0)


def fit_group_states(
    data: pd.DataFrame, train_end: int, seed: int = 42, preferred_eids: set[str] | None = None
) -> tuple[list[str], dict[str, float], RobustScaler | None, MiniBatchKMeans | None, np.ndarray, dict[str, Any]]:
    train = data.iloc[:train_end]
    availability = train.notna().mean()
    variance = train.var().replace([np.inf, -np.inf], np.nan).fillna(0.0)
    candidates = [column for column in data.columns if availability[column] >= 0.50 and variance[column] > 1e-12]
    if not candidates:
        return [], {}, None, None, np.zeros(len(data), dtype=int), {"state_count": 1, "reason": "没有可用状态特征"}
    preferred_eids = preferred_eids or set()
    preferred = sorted(
        [column for column in candidates if column in preferred_eids],
        key=lambda column: (availability[column], variance[column]),
        reverse=True,
    )[:12]
    ordinary = sorted(
        [column for column in candidates if column not in preferred_eids],
        key=lambda column: (availability[column], variance[column]),
        reverse=True,
    )
    ranking = preferred + ordinary[: max(0, 20 - len(preferred))]
    medians = train[ranking].median().fillna(0.0).to_dict()
    features = _fill_frame(data[ranking], medians)
    fit_positions = _sample_positions(train_end, 80_000)
    fit = features.iloc[fit_positions]
    scaler = RobustScaler(quantile_range=(10.0, 90.0)).fit(fit)
    scaled_fit = np.clip(scaler.transform(fit), -12.0, 12.0)
    if len(fit) < 2_000:
        return ranking, {k: float(v) for k, v in medians.items()}, scaler, None, np.zeros(len(data), dtype=int), {"state_count": 1, "reason": "样本不足"}

    rng = np.random.default_rng(seed)
    score_size = min(12_000, len(scaled_fit))
    score_index = rng.choice(len(scaled_fit), score_size, replace=False)
    score_sample = scaled_fit[score_index]
    options = []
    models: dict[int, MiniBatchKMeans] = {}
    for k in range(2, 6):
        model = MiniBatchKMeans(n_clusters=k, random_state=seed, n_init=5, batch_size=4096).fit(scaled_fit)
        labels = model.predict(score_sample)
        shares = np.bincount(labels, minlength=k) / len(labels)
        # A steady device can legitimately collapse to one observed state.
        # Silhouette is undefined in that case; treat it as a single-state
        # context instead of aborting the complete factory training run.
        if np.unique(labels).size < 2:
            continue
        score = silhouette_score(score_sample, labels, sample_size=min(5_000, len(labels)), random_state=seed)
        options.append((float(score), k, float(shares.min())))
        models[k] = model
    if not options:
        return ranking, {k: float(v) for k, v in medians.items()}, scaler, None, np.zeros(len(data), dtype=int), {
            "state_count": 1,
            "reason": "历史数据仅识别到单一稳定工况",
            "features": ranking,
        }
    stable = [option for option in options if option[2] >= 0.005]
    best_score, best_k, _ = max(stable or options, key=lambda item: item[0])
    state_model = models[best_k]
    all_scaled = np.clip(scaler.transform(features), -12.0, 12.0)
    states = state_model.predict(all_scaled)
    report = {
        "state_count": best_k,
        "silhouette": best_score,
        "features": ranking,
        "preferred_state_features": [eid for eid in ranking if eid in preferred_eids],
        "candidates": [
            {"states": k, "silhouette": score, "minimum_share": share}
            for score, k, share in sorted(options, key=lambda item: item[1])
        ],
    }
    return ranking, {k: float(v) for k, v in medians.items()}, scaler, state_model, states, report


def select_peers(
    data: pd.DataFrame,
    target: str,
    train_end: int,
    context_eids: set[str],
    maximum: int = 12,
    correlation_matrix: pd.DataFrame | None = None,
) -> tuple[list[str], dict[str, float]]:
    scores: list[tuple[float, str]] = []
    correlations: dict[str, float] = {}
    if correlation_matrix is None:
        train = data.iloc[:train_end]
        positions = _sample_positions(train_end, 40_000)
        correlation_row = train.iloc[positions].corr(min_periods=500).loc[target]
    else:
        correlation_row = correlation_matrix.loc[target]
    for peer, correlation in correlation_row.items():
        if peer == target or pd.isna(correlation):
            continue
        correlations[peer] = float(correlation)
        bonus = 0.05 if peer in context_eids else 0.0
        scores.append((abs(float(correlation)) + bonus, peer))
    strong = [peer for score, peer in sorted(scores, reverse=True) if score >= 0.20]
    if len(strong) < 2:
        strong = [peer for _, peer in sorted(scores, reverse=True)[:maximum]]
    return strong[:maximum], correlations


def build_peer_correlation_matrix(data: pd.DataFrame, train_end: int, maximum_rows: int = 20_000) -> pd.DataFrame:
    """一次计算整组相关矩阵，避免为每个目标重复扫描全部同组点位。"""
    positions = _sample_positions(train_end, maximum_rows)
    sample = data.iloc[positions].replace([np.inf, -np.inf], np.nan)
    availability = sample.notna().mean()
    medians = sample.median().fillna(0.0)
    matrix = sample.fillna(medians).to_numpy(dtype=np.float32, copy=True)
    matrix -= matrix.mean(axis=0, keepdims=True)
    norms = np.sqrt(np.sum(matrix * matrix, axis=0, dtype=np.float64)).astype(np.float32)
    norms[norms <= 1e-12] = np.nan
    matrix /= norms
    correlations = matrix.T @ matrix
    correlations = np.clip(correlations, -1.0, 1.0)
    result = pd.DataFrame(correlations, index=data.columns, columns=data.columns)
    invalid = availability[availability < 0.40].index
    result.loc[invalid, :] = np.nan
    result.loc[:, invalid] = np.nan
    return result


def _fit_ridge(x: pd.DataFrame, y: pd.Series, seed: int = 42) -> tuple[Any, float]:
    del seed
    split = max(500, int(len(x) * 0.85))
    split = min(split, len(x) - 100)
    best_alpha = 1.0
    best_error = float("inf")
    for alpha in (0.01, 0.1, 1.0, 10.0):
        estimator = make_pipeline(StandardScaler(), Ridge(alpha=alpha))
        estimator.fit(x.iloc[:split], y.iloc[:split])
        error = mean_absolute_error(y.iloc[split:], estimator.predict(x.iloc[split:]))
        if error < best_error:
            best_alpha = alpha
            best_error = float(error)
    first = make_pipeline(StandardScaler(), Ridge(alpha=best_alpha)).fit(x, y)
    residual = y.to_numpy() - first.predict(x)
    cutoff = np.quantile(np.abs(residual), 0.98)
    keep = np.abs(residual) <= cutoff
    final = make_pipeline(StandardScaler(), Ridge(alpha=best_alpha)).fit(x.iloc[keep], y.iloc[keep]) if keep.sum() >= 500 else first
    return final, best_error


def _univariate_prediction(series: pd.Series, history_window: int) -> pd.Series:
    return series.shift(1).rolling(history_window, min_periods=max(3, history_window // 4)).median()


def train_target_model(
    data: pd.DataFrame,
    target: str,
    metadata: dict[str, str],
    states: np.ndarray,
    context_eids: set[str],
    correlation_matrix: pd.DataFrame | None = None,
    train_fraction: float = 0.65,
    validation_fraction: float = 0.15,
    residual_quantile: float = 0.997,
) -> tuple[TargetHealthModel | None, dict[str, Any]]:
    n = len(data)
    train_end = int(n * train_fraction)
    validation_end = int(n * (train_fraction + validation_fraction))
    series = data[target]
    availability = float(series.notna().mean())
    valid_train = series.iloc[:train_end].dropna()
    if len(valid_train) < 2_000 or valid_train.std() <= 1e-12:
        return None, {"status": "untrainable", "reason": "有效样本不足或历史恒值", "availability": availability}

    sensor_type = metadata.get("type", "")
    history_window = 180 if sensor_type == "温度" else 30
    persistence_window = 18 if sensor_type == "温度" else 6
    persistence_required = 12 if sensor_type == "温度" else 3
    is_cumulative = metadata.get("target_role") == "cumulative_target"
    peers, correlations = select_peers(
        data, target, train_end, context_eids, correlation_matrix=correlation_matrix
    )
    estimator = None
    peer_fill: dict[str, float] = {}
    validation_mae = float("nan")

    if is_cumulative:
        increments = series.diff()
        rolling_increment = increments.shift(1).rolling(history_window, min_periods=max(3, history_window // 4)).median()
        predicted = series.shift(1) + rolling_increment
        model_kind = "cumulative_counter"
        peers = []
    elif len(peers) >= 2:
        peer_fill = data[peers].iloc[:train_end].median().fillna(0.0).to_dict()
        features = _fill_frame(data[peers], peer_fill)
        valid = series.iloc[:train_end].notna()
        train_frame = pd.concat([features.iloc[:train_end].loc[valid], series.iloc[:train_end].loc[valid].rename("target")], axis=1)
        train_frame = train_frame.iloc[_sample_positions(len(train_frame), 80_000)]
        estimator, _ = _fit_ridge(train_frame[peers], train_frame["target"])
        predicted = pd.Series(estimator.predict(features), index=data.index)
        model_kind = "contextual_ridge"
    else:
        predicted = _univariate_prediction(series, history_window)
        model_kind = "univariate_rolling_median"

    stuck_peer_activity_threshold = None
    if peers:
        peer_activity = data[peers].diff().abs().median(axis=1).iloc[:train_end].dropna()
        if len(peer_activity) >= 500:
            candidate = float(peer_activity.quantile(0.75))
            if np.isfinite(candidate) and candidate > 0:
                stuck_peer_activity_threshold = candidate

    residual = series - predicted
    validation_residual = residual.iloc[train_end:validation_end].dropna()
    if len(validation_residual) < 200:
        return None, {"status": "untrainable", "reason": "验证期有效残差不足", "availability": availability}
    fallback_raw = np.quantile(validation_residual, [1 - residual_quantile, residual_quantile])
    target_iqr = float(np.subtract(*np.quantile(valid_train, [0.75, 0.25])))
    residual_floor = max(target_iqr * 0.001, float(valid_train.std()) * 0.001, 1e-6)
    fallback = (min(float(fallback_raw[0]), -residual_floor), max(float(fallback_raw[1]), residual_floor))
    limits: dict[int, tuple[float, float]] = {}
    for state in np.unique(states):
        mask = states[train_end:validation_end] == state
        values = residual.iloc[train_end:validation_end].to_numpy()[mask]
        values = values[np.isfinite(values)]
        if len(values) >= 100:
            raw = np.quantile(values, [1 - residual_quantile, residual_quantile])
            limits[int(state)] = (min(float(raw[0]), -residual_floor), max(float(raw[1]), residual_floor))
        else:
            limits[int(state)] = fallback

    q_low, q_high = np.quantile(valid_train, [0.001, 0.999])
    margin = max(float(q_high - q_low) * 0.10, residual_floor)
    diffs = valid_train.sort_index().diff().abs().dropna()
    derivative_limit = float(diffs.quantile(0.9995)) if len(diffs) else residual_floor
    sorted_unique = np.sort(valid_train.unique())
    gaps = np.diff(sorted_unique)
    positive_gaps = gaps[gaps > 0]
    resolution = float(np.quantile(positive_gaps, 0.10)) if len(positive_gaps) else residual_floor
    validation_mae = float(validation_residual.abs().mean())
    normalized_mae = validation_mae / max(target_iqr, valid_train.std(), 1e-6)
    if model_kind == "contextual_ridge" and len(peers) >= 3 and normalized_mae <= 0.10 and availability >= 0.90:
        grade = "A"
    elif model_kind == "contextual_ridge" and len(peers) >= 2 and normalized_mae <= 0.30:
        grade = "B"
    else:
        grade = "C"

    model = TargetHealthModel(
        eid=target,
        name=metadata.get("point", target),
        sensor_type=sensor_type,
        model_kind=model_kind,
        peer_eids=peers,
        peer_fill_values={key: float(value) for key, value in peer_fill.items()},
        estimator=estimator,
        residual_limits=limits,
        fallback_limits=fallback,
        history_bounds=(float(q_low - margin), float(q_high + margin)),
        derivative_limit=max(derivative_limit, residual_floor),
        resolution=max(resolution, 1e-9),
        persistence_window=persistence_window,
        persistence_required=persistence_required,
        history_window=history_window,
        confidence_grade=grade,
        availability=availability,
        validation_mae=validation_mae,
        stuck_peer_activity_threshold=stuck_peer_activity_threshold,
    )
    report = {
        "status": "trained",
        "model_kind": model_kind,
        "peer_count": len(peers),
        "peer_eids": peers,
        "peer_correlations": {peer: correlations.get(peer) for peer in peers},
        "confidence_grade": grade,
        "availability": availability,
        "validation_mae": validation_mae,
        "history_bounds": list(model.history_bounds),
        "derivative_limit": model.derivative_limit,
    }
    return model, report


def score_target(model: TargetHealthModel, data: pd.DataFrame, states: np.ndarray) -> pd.DataFrame:
    actual = data[model.eid]
    if model.model_kind == "contextual_ridge":
        features = _fill_frame(data.reindex(columns=model.peer_eids), model.peer_fill_values)
        predicted = pd.Series(model.estimator.predict(features), index=data.index)
    elif model.model_kind == "cumulative_counter":
        increments = actual.diff()
        predicted = actual.shift(1) + increments.shift(1).rolling(
            model.history_window, min_periods=max(3, model.history_window // 4)
        ).median()
    else:
        predicted = _univariate_prediction(actual, model.history_window)
    residual = actual - predicted
    lower = np.array([model.residual_limits.get(int(state), model.fallback_limits)[0] for state in states])
    upper = np.array([model.residual_limits.get(int(state), model.fallback_limits)[1] for state in states])
    raw_context = (residual.to_numpy() < lower) | (residual.to_numpy() > upper)
    missing = actual.isna().to_numpy()
    range_error = ((actual < model.history_bounds[0]) | (actual > model.history_bounds[1])).fillna(False).to_numpy()
    derivative = actual.diff().abs()
    spike = (derivative > model.derivative_limit).fillna(False).to_numpy()
    unchanged = actual.diff().abs().le(model.resolution * 0.10).fillna(False)
    stuck_window = 180 if model.sensor_type == "温度" else 30
    if model.model_kind == "cumulative_counter":
        stuck = np.zeros(len(data), dtype=bool)
    else:
        stuck = unchanged.rolling(stuck_window, min_periods=stuck_window).sum().ge(stuck_window).to_numpy()
        activity_threshold = getattr(model, "stuck_peer_activity_threshold", None)
        if activity_threshold is not None and model.peer_eids:
            peer_activity = data.reindex(columns=model.peer_eids).diff().abs().median(axis=1)
            active_line = peer_activity.rolling(stuck_window, min_periods=stuck_window).median().gt(activity_threshold)
            stuck = stuck & active_line.fillna(False).to_numpy()
    counter_backwards = (
        actual.diff().lt(-max(model.resolution * 2.0, 1e-9)).fillna(False).to_numpy()
        if model.model_kind == "cumulative_counter"
        else np.zeros(len(data), dtype=bool)
    )
    raw = raw_context | missing | range_error | spike | stuck | counter_backwards
    persistence = pd.Series(raw_context | range_error, index=data.index).rolling(
        model.persistence_window, min_periods=1
    ).sum().to_numpy()
    confirmed = (persistence >= model.persistence_required) | missing | stuck | counter_backwards
    width = np.maximum(np.maximum(np.abs(lower), np.abs(upper)), 1e-6)
    anomaly_score = np.clip(np.abs(residual.to_numpy()) / width, 0.0, 10.0)
    health = np.clip(100.0 - anomaly_score * 25.0, 0.0, 100.0)
    reason = np.full(len(data), "正常", dtype=object)
    reason[raw_context] = "与相关测点或自身历史不一致"
    reason[range_error] = "超出历史鲁棒范围"
    reason[spike] = "信号突变"
    reason[stuck] = "信号长时间不变"
    reason[counter_backwards] = "累计量倒退或清零"
    reason[missing] = "数据缺失"
    return pd.DataFrame(
        {
            "actual": actual,
            "predicted": predicted,
            "normal_lower": predicted + lower,
            "normal_upper": predicted + upper,
            "residual": residual,
            "state": states,
            "raw_anomaly": raw,
            "confirmed_anomaly": confirmed,
            "health_score": health,
            "confidence_grade": model.confidence_grade,
            "reason": reason,
        },
        index=data.index,
    )


def infer_group_states(bundle: GroupHealthBundle, data: pd.DataFrame) -> np.ndarray:
    if bundle.state_model is None or bundle.state_scaler is None or not bundle.state_eids:
        return np.zeros(len(data), dtype=int)
    features = _fill_frame(data.reindex(columns=bundle.state_eids), bundle.state_fill_values)
    scaled = np.clip(bundle.state_scaler.transform(features), -12.0, 12.0)
    return bundle.state_model.predict(scaled)
