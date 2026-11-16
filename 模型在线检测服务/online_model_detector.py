from __future__ import annotations

import argparse
import json
import logging
from logging.handlers import RotatingFileHandler
import math
import os
from pathlib import Path
import re
import signal
import sys
from threading import Event, Lock, Thread
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any


LOGGER = logging.getLogger("online_model_detector")
IDENTIFIER = re.compile(r"^[A-Za-z0-9_]+$")


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def resolve_path(config_dir: Path, value: str) -> Path:
    path = Path(os.path.expandvars(value))
    return path if path.is_absolute() else (config_dir / path).resolve()


def safe_identifier(value: str, label: str) -> str:
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"{label} 只能包含字母、数字和下划线: {value!r}")
    return f"`{value}`"


def setup_logging(config_dir: Path, config: dict[str, Any]) -> None:
    log_config = config.get("logging", {})
    level = getattr(logging, str(log_config.get("level", "INFO")).upper(), logging.INFO)
    log_dir = resolve_path(config_dir, str(log_config.get("directory", "logs")))
    log_dir.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(threadName)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    LOGGER.setLevel(level)
    LOGGER.handlers.clear()
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    LOGGER.addHandler(console)
    file_handler = RotatingFileHandler(
        log_dir / "online_model_detector.log",
        maxBytes=int(log_config.get("max_bytes", 20_000_000)),
        backupCount=int(log_config.get("backup_count", 10)),
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    LOGGER.addHandler(file_handler)


@dataclass
class BinValue:
    total: float = 0.0
    count: int = 0
    quality_good: bool = True
    last_timestamp: int = 0

    def add(self, value: float, quality_good: bool, timestamp: int, use_bad_quality: bool) -> None:
        if quality_good or use_bad_quality:
            self.total += value
            self.count += 1
        self.quality_good = quality_good
        self.last_timestamp = max(self.last_timestamp, timestamp)

    @property
    def mean(self) -> float | None:
        return self.total / self.count if self.count else None


class BinnedSensorCache:
    def __init__(
        self,
        required_eids: set[str],
        bin_seconds: int,
        cache_minutes: int,
        use_bad_quality_values: bool,
    ) -> None:
        self.required_eids = required_eids
        self.bin_seconds = bin_seconds
        self.cache_seconds = cache_minutes * 60
        self.maximum_bins = max(10, self.cache_seconds // self.bin_seconds + 12)
        self.use_bad_quality_values = use_bad_quality_values
        self._bins: dict[str, dict[int, BinValue]] = {eid: {} for eid in required_eids}
        self._latest_seen: dict[str, tuple[int, bool]] = {}
        self._first_received_monotonic: float | None = None
        self._lock = Lock()
        self.received_values = 0
        self.ignored_values = 0

    def add(self, eid: str, value: Any, quality: Any, timestamp: Any) -> bool:
        if eid not in self.required_eids:
            self.ignored_values += 1
            return False
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return False
        if not math.isfinite(numeric):
            return False
        ts = normalize_timestamp(timestamp)
        good = normalize_quality(quality)
        bin_timestamp = (ts // self.bin_seconds) * self.bin_seconds
        cutoff = bin_timestamp - self.cache_seconds - self.bin_seconds * 4
        with self._lock:
            if self._first_received_monotonic is None:
                self._first_received_monotonic = time.monotonic()
            eid_bins = self._bins[eid]
            aggregate = eid_bins.get(bin_timestamp)
            if aggregate is None:
                aggregate = BinValue()
                eid_bins[bin_timestamp] = aggregate
            aggregate.add(numeric, good, ts, self.use_bad_quality_values)
            self._latest_seen[eid] = (ts, good)
            if len(eid_bins) > self.maximum_bins:
                for old_bin in [key for key in eid_bins if key < cutoff]:
                    del eid_bins[old_bin]
            self.received_values += 1
        return True

    def snapshot(self, start_bin: int, end_bin: int, eids: set[str]) -> tuple[dict[str, dict[int, float]], dict[str, tuple[int, bool]]]:
        result: dict[str, dict[int, float]] = {}
        with self._lock:
            for eid in eids:
                values = {
                    ts: aggregate.mean
                    for ts, aggregate in self._bins.get(eid, {}).items()
                    if start_bin <= ts <= end_bin and aggregate.mean is not None
                }
                if values:
                    result[eid] = values
            latest = {eid: self._latest_seen[eid] for eid in eids if eid in self._latest_seen}
        return result, latest

    def reset_eid(self,eid):
        with self._lock:
            self._bins[eid]={}
            self._latest_seen.pop(eid,None)

    def warmup_age_seconds(self) -> int:
        with self._lock:
            first = self._first_received_monotonic
        return 0 if first is None else max(0, int(time.monotonic() - first))

    def coverage_count(self) -> int:
        with self._lock:
            return len(self._latest_seen)


def normalize_timestamp(value: Any) -> int:
    if value is None or value == "":
        return int(time.time())
    try:
        numeric = int(float(value))
    except (TypeError, ValueError):
        return int(time.time())
    if numeric > 10_000_000_000:
        numeric //= 1000
    return numeric


def normalize_quality(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "good", "ok", "yes", "y"}


def clean_payload(payload: str) -> str:
    return re.sub(r"[\x00-\x1F\x7F]", "", payload)


def parse_mqtt_payload(payload: bytes | str) -> list[tuple[str, Any, Any, Any]]:
    if isinstance(payload, bytes):
        text = payload.decode("utf-8", errors="replace")
    else:
        text = str(payload)
    data = json.loads(clean_payload(text))
    parsed: list[tuple[str, Any, Any, Any]] = []
    for item in data.get("values", []):
        item_id = str(item.get("id", "")).strip()
        if not item_id:
            continue
        eid = item_id.rsplit("_", 1)[-1].strip()
        if eid:
            parsed.append((eid, item.get("v"), item.get("q"), item.get("t")))
    return parsed


@dataclass
class ActiveGroup:
    group_id: str
    bundle: Any
    target_models: dict[str, Any]
    required_eids: set[str]


class ModelRegistry:
    def __init__(self, manifest_path: Path, summary_path: Path, project_root: Path) -> None:
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
        import joblib

        self.manifest_path = manifest_path
        self.summary_path = summary_path
        self.manifest = load_json(manifest_path)
        self.bin_seconds = int(self.manifest["bin_seconds"])
        import pandas as pd

        summary = pd.read_csv(summary_path, dtype=str).fillna("")
        ready_rows = summary[summary["deployment_status"].eq("ready")]
        self.ready_eids = set(ready_rows["eid"].astype(str))
        self.metadata = ready_rows.set_index("eid").to_dict("index")
        self.groups: list[ActiveGroup] = []
        self.required_eids: set[str] = set()
        loaded_ready: set[str] = set()
        for group_id, configured_path in self.manifest["group_models"].items():
            fallback = manifest_path.parent / "groups" / f"{group_id}.joblib"
            configured = Path(str(configured_path))
            model_path = configured if configured.exists() else fallback
            if not model_path.exists():
                raise FileNotFoundError(f"模型组文件不存在: {group_id} -> {model_path}")
            bundle = joblib.load(model_path)
            targets = {
                eid: model
                for eid, model in bundle.target_models.items()
                if eid in self.ready_eids
            }
            if not targets:
                continue
            required = set(bundle.state_eids)
            for model in targets.values():
                required.add(model.eid)
                required.update(model.peer_eids)
            self.groups.append(ActiveGroup(group_id, bundle, targets, required))
            self.required_eids.update(required)
            loaded_ready.update(targets)
        missing = self.ready_eids - loaded_ready
        if missing:
            raise RuntimeError(f"ready模型未在模型组中找到: {sorted(missing)}")


@dataclass
class DetectionResult:
    eid: str
    result: str
    detected_at: datetime
    message: str
    status_code: str
    health_score: float | None = None


class DatabaseWriter:
    MESSAGE_CANDIDATES = ("modelmessage", "modelmseeage", "modelmeseage")

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.schema = str(config.get("database", "gpwinccoa"))
        self.table = str(config.get("table", "autosensor"))
        self.schema_sql = safe_identifier(self.schema, "database")
        self.table_sql = safe_identifier(self.table, "table")
        self.message_column: str | None = None

    def connect(self):
        import pymysql

        password = os.getenv(str(self.config.get("password_env", "MODEL_DB_PASSWORD")))
        if password is None:
            password = str(self.config.get("password", ""))
        return pymysql.connect(
            host=str(self.config["host"]),
            port=int(self.config.get("port", 3306)),
            user=str(self.config["user"]),
            password=password,
            database=self.schema,
            charset="utf8mb4",
            connect_timeout=int(self.config.get("connect_timeout_seconds", 8)),
            read_timeout=int(self.config.get("read_timeout_seconds", 15)),
            write_timeout=int(self.config.get("write_timeout_seconds", 20)),
            autocommit=False,
        )

    def inspect_schema(self) -> dict[str, Any]:
        connection = self.connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
                    """,
                    (self.schema, self.table),
                )
                columns = {str(row[0]): {"type": row[1], "length": row[2]} for row in cursor.fetchall()}
        finally:
            connection.close()
        required = {"eid", "havemodel", "modeltestresult", "modeltesttime", "healthy"}
        missing = required - columns.keys()
        if missing:
            raise RuntimeError(f"数据库表缺少字段: {sorted(missing)}")
        configured = str(self.config.get("message_column", "auto"))
        if configured != "auto":
            if configured not in columns:
                raise RuntimeError(f"配置的模型消息字段不存在: {configured}")
            self.message_column = configured
        else:
            self.message_column = next((name for name in self.MESSAGE_CANDIDATES if name in columns), None)
        return {"columns": columns, "message_column": self.message_column}

    def synchronize_ready_flags(self, ready_eids: set[str], startup_message: str, clear_non_ready: bool) -> None:
        if not ready_eids:
            raise ValueError("ready EID列表为空，拒绝更新数据库")
        connection = self.connect()
        try:
            with connection.cursor() as cursor:
                if clear_non_ready:
                    cursor.execute(
                        f"UPDATE {self.schema_sql}.{self.table_sql} SET `havemodel`=NULL WHERE `havemodel`='Y'"
                    )
                message_sql = ""
                if self.message_column:
                    message_sql = f", {safe_identifier(self.message_column, 'message column')}=CASE WHEN `modeltesttime` IS NULL THEN %s ELSE {safe_identifier(self.message_column, 'message column')} END"
                sql = (
                    f"UPDATE {self.schema_sql}.{self.table_sql} SET `havemodel`='Y'"
                    f"{message_sql} WHERE `eid`=%s"
                )
                if self.message_column:
                    params = [(startup_message[:255], eid) for eid in sorted(ready_eids)]
                else:
                    params = [(eid,) for eid in sorted(ready_eids)]
                cursor.executemany(sql, params)
            connection.commit()
        except Exception:
            try:
                connection.rollback()
            except Exception:
                LOGGER.warning("数据库连接已断开，无法回滚ready标记事务", exc_info=True)
            raise
        finally:
            connection.close()

    def write_results(self, results: list[DetectionResult]) -> int:
        if not results:
            return 0
        connection = self.connect()
        try:
            with connection.cursor() as cursor:
                if self.message_column:
                    message_sql = safe_identifier(self.message_column, "message column")
                    sql = (
                        f"UPDATE {self.schema_sql}.{self.table_sql} "
                        f"SET `havemodel`='Y', `modeltestresult`=%s, `modeltesttime`=%s, "
                        f"`healthy`=%s, {message_sql}=%s "
                        "WHERE `eid`=%s"
                    )
                    params = [
                        (row.result, row.detected_at, row.health_score, row.message[:255], row.eid)
                        for row in results
                    ]
                else:
                    sql = (
                        f"UPDATE {self.schema_sql}.{self.table_sql} "
                        "SET `havemodel`='Y', `modeltestresult`=%s, `modeltesttime`=%s, `healthy`=%s "
                        "WHERE `eid`=%s"
                    )
                    params = [(row.result, row.detected_at, row.health_score, row.eid) for row in results]
                affected = cursor.executemany(sql, params)
            connection.commit()
            return int(affected)
        except Exception:
            try:
                connection.rollback()
            except Exception:
                LOGGER.warning("数据库连接已断开，无法回滚检测结果事务", exc_info=True)
            raise
        finally:
            connection.close()


def format_number(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "-"
    if not math.isfinite(number):
        return "-"
    return f"{number:.2f}"


def infer_group_states_compatible(bundle: Any, data) -> Any:
    """Infer operating states while preserving the dtype expected by saved KMeans models.

    Some production bundles were fitted from float64 state features while an imported
    scaler may emit float32 for an online frame. Recent scikit-learn versions require
    the prediction buffer dtype to match the saved cluster-center dtype exactly.
    """
    import numpy as np

    if bundle.state_model is None or bundle.state_scaler is None or not bundle.state_eids:
        return np.zeros(len(data), dtype=int)
    features = (
        data.reindex(columns=bundle.state_eids)
        .replace([np.inf, -np.inf], np.nan)
        .fillna(bundle.state_fill_values)
        .fillna(0.0)
    )
    scaled = np.clip(bundle.state_scaler.transform(features), -12.0, 12.0)
    centers = getattr(bundle.state_model, "cluster_centers_", None)
    expected_dtype = centers.dtype if centers is not None else np.float64
    scaled = np.ascontiguousarray(scaled, dtype=expected_dtype)
    return bundle.state_model.predict(scaled)


class OnlineDetector:
    def __init__(self, config_path: Path, config: dict[str, Any]) -> None:
        self.config_path = config_path
        self.config = config
        config_dir = config_path.parent
        path_config = config["paths"]
        self.project_root = resolve_path(config_dir, str(path_config.get("project_root", "..")))
        manifest_path = resolve_path(config_dir, str(path_config["manifest"] ))
        summary_path = resolve_path(config_dir, str(path_config["model_summary"]))
        self.registry = ModelRegistry(manifest_path, summary_path, self.project_root)
        detection = config["detection"]
        self.interval_seconds = int(detection.get("interval_seconds", 300))
        self.cache_minutes = int(detection.get("cache_minutes", 45))
        self.warmup_minutes = int(detection.get("warmup_minutes", 35))
        self.max_sample_age_seconds = int(detection.get("max_sample_age_seconds", 45))
        self.min_peer_coverage = float(detection.get("min_peer_coverage", 0.50))
        self.interpolate_limit_bins = int(detection.get("interpolate_limit_bins", 3))
        self.stop_event = Event()
        self.cache = BinnedSensorCache(
            self.registry.required_eids,
            self.registry.bin_seconds,
            self.cache_minutes,
            bool(detection.get("use_bad_quality_values", False)),
        )
        self.database = DatabaseWriter(config["database"])
        self.mqtt_client = None

    def check_environment(self) -> dict[str, Any]:
        schema = self.database.inspect_schema()
        return {
            "ready_models": len(self.registry.ready_eids),
            "active_groups": len(self.registry.groups),
            "required_cache_eids": len(self.registry.required_eids),
            "bin_seconds": self.registry.bin_seconds,
            "cache_minutes": self.cache_minutes,
            "warmup_minutes": self.warmup_minutes,
            "detection_interval_seconds": self.interval_seconds,
            "database_message_column": schema["message_column"],
        }

    def on_connect(self, client, userdata, flags, reason_code, properties=None) -> None:
        del userdata, flags, properties
        code = getattr(reason_code, "value", reason_code)
        try:
            code = int(code)
        except (TypeError, ValueError):
            code = 1
        if code == 0:
            topics = self.config["mqtt"]["topics"]
            for topic in topics:
                client.subscribe(str(topic), qos=int(self.config["mqtt"].get("qos", 0)))
            LOGGER.info("MQTT连接成功，已订阅%d个主题", len(topics))
        else:
            LOGGER.error("MQTT连接失败，reason_code=%s", reason_code)

    def on_disconnect(self, client, userdata, disconnect_flags, reason_code=None, properties=None) -> None:
        del client, userdata, properties
        code = reason_code if reason_code is not None else disconnect_flags
        code = getattr(code, "value", code)
        try:
            code = int(code)
        except (TypeError, ValueError):
            code = 1
        if code != 0:
            LOGGER.warning("MQTT意外断开，客户端将自动重连，reason_code=%s", code)

    def on_message(self, client, userdata, message) -> None:
        del client, userdata
        try:
            for eid, value, quality, timestamp in parse_mqtt_payload(message.payload):
                self.cache.add(eid, value, quality, timestamp)
        except Exception as exc:
            LOGGER.warning("MQTT消息解析失败 topic=%s error=%s", message.topic, exc)

    def create_mqtt_client(self):
        import paho.mqtt.client as mqtt

        mqtt_config = self.config["mqtt"]
        client_id = str(mqtt_config.get("client_id", "sensor-model-quality-detector"))
        try:
            client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
        except (AttributeError, TypeError):
            client = mqtt.Client(client_id=client_id)
        username = str(mqtt_config.get("username", ""))
        if username:
            password = os.getenv(str(mqtt_config.get("password_env", "MODEL_MQTT_PASSWORD")))
            if password is None:
                password = str(mqtt_config.get("password", ""))
            client.username_pw_set(username, password)
        client.on_connect = self.on_connect
        client.on_disconnect = self.on_disconnect
        client.on_message = self.on_message
        client.reconnect_delay_set(min_delay=1, max_delay=60)
        return client

    def score_group(self, group: ActiveGroup, now_epoch: int, detected_at: datetime) -> list[DetectionResult]:
        import numpy as np
        import pandas as pd
        from sensor_quality.full import score_target

        step = self.registry.bin_seconds
        end_bin = ((now_epoch // step) - 1) * step
        cache_bins = max(2, self.cache_minutes * 60 // step)
        start_bin = end_bin - (cache_bins - 1) * step
        snapshot, latest_seen = self.cache.snapshot(start_bin, end_bin, group.required_eids)
        index = np.arange(start_bin, end_bin + step, step, dtype="int64")
        data = pd.DataFrame(index=index, columns=sorted(group.required_eids), dtype=float)
        for eid, values in snapshot.items():
            common = data.index.intersection(values.keys())
            if len(common):
                data.loc[common, eid] = [values[int(ts)] for ts in common]
        data = data.interpolate(
            limit=self.interpolate_limit_bins,
            limit_direction="both",
        )
        data.index = pd.to_datetime(data.index, unit="s", utc=True).tz_convert("Asia/Shanghai")
        states = infer_group_states_compatible(group.bundle, data)
        rows: list[DetectionResult] = []
        for eid, model in group.target_models.items():
            if eid in getattr(self,'quality_disabled_eids',set()):
                rows.append(DetectionResult(eid,'检测已停用',detected_at,'qualityCheck非0，不进行质量判定和报警','QUALITY_CHECK_DISABLED'))
                continue
            try:
                scored = score_target(model, data, states)
                row = scored.iloc[-1]
                latest = latest_seen.get(eid)
                age = None if latest is None else now_epoch - latest[0]
                quality_good = False if latest is None else latest[1]
                if model.peer_eids:
                    peer_coverage = float(data[model.peer_eids].iloc[-1].notna().mean())
                else:
                    peer_coverage = float(pd.notna(row["actual"]))
                unavailable_reason = ""
                if latest is None:
                    unavailable_reason = "目标点尚未收到数据"
                elif age is not None and age > self.max_sample_age_seconds:
                    unavailable_reason = f"目标数据超时{age}秒"
                elif not quality_good:
                    unavailable_reason = "目标点MQTT质量位无效"
                elif len(snapshot.get(eid,{})) < self.warmup_minutes * 60 // step:
                    unavailable_reason = f"目标有效历史{len(snapshot.get(eid,{}))*step/60:.2f}/{self.warmup_minutes:.2f}分钟（{len(snapshot.get(eid,{}))}/{self.warmup_minutes*60//step}个{step}秒窗口）"
                elif peer_coverage < self.min_peer_coverage:
                    unavailable_reason = f"关联数据覆盖不足{peer_coverage:.0%}"
                elif pd.isna(row["actual"]) or pd.isna(row["predicted"]):
                    unavailable_reason = "当前窗口有效数据不足"

                if unavailable_reason:
                    result = "无法判断"
                    code = "UNAVAILABLE"
                    reason = unavailable_reason
                elif bool(row["confirmed_anomaly"]):
                    result = "异常"
                    code = "CONFIRMED_ANOMALY"
                    reason = str(row["reason"])
                elif bool(row["raw_anomaly"]):
                    result = "预警"
                    code = "PRELIMINARY_ANOMALY"
                    reason = str(row["reason"])
                else:
                    result = "正常"
                    code = "NORMAL"
                    reason = "模型范围内"
                message = (
                    (f"无法判断：{unavailable_reason};" if unavailable_reason else "")+
                    f"{model.confidence_grade}/{model.model_kind};健康={format_number(row['health_score'])};"
                    f"实测={format_number(row['actual'])};预测={format_number(row['predicted'])};"
                    f"范围=[{format_number(row['normal_lower'])},{format_number(row['normal_upper'])}];"
                    f"工况={int(row['state'])};关联={peer_coverage:.0%};{reason}"
                )[:255]
                health_score = float(row["health_score"])
                if not math.isfinite(health_score):
                    health_score = None
                rows.append(DetectionResult(eid, result, detected_at, message, code, health_score))
            except Exception as exc:
                LOGGER.exception("单点模型检测失败 eid=%s group=%s", eid, group.group_id)
                rows.append(
                    DetectionResult(
                        eid,
                        "无法判断",
                        detected_at,
                        f"模型执行失败:{type(exc).__name__}:{exc}"[:255],
                        "MODEL_ERROR",
                    )
                )
        return rows

    def run_detection_once(self, now_epoch: int | None = None, write_database: bool = True) -> list[DetectionResult]:
        now_epoch = int(time.time()) if now_epoch is None else int(now_epoch)
        detected_at = datetime.fromtimestamp(now_epoch)
        results: list[DetectionResult] = []
        started = time.perf_counter()
        for group in self.registry.groups:
            try:
                results.extend(self.score_group(group, now_epoch, detected_at))
            except Exception as exc:
                LOGGER.exception("模型组检测失败 group=%s", group.group_id)
                results.extend(
                    DetectionResult(
                        eid,
                        "无法判断",
                        detected_at,
                        f"模型组执行失败:{type(exc).__name__}:{exc}"[:255],
                        "GROUP_ERROR",
                    )
                    for eid in group.target_models
                )
        if len(results) != len(self.registry.ready_eids):
            LOGGER.error("检测结果数量异常 expected=%d actual=%d", len(self.registry.ready_eids), len(results))
        affected = 0
        if write_database:
            affected = self.database.write_results(results)
        counts: dict[str, int] = {}
        for row in results:
            counts[row.result] = counts.get(row.result, 0) + 1
        LOGGER.info(
            "模型检测完成 results=%d db_affected=%d counts=%s elapsed=%.2fs",
            len(results), affected, counts, time.perf_counter() - started,
        )
        return results

    def detection_loop(self) -> None:
        warmup_seconds = self.warmup_minutes * 60
        next_log = 0.0
        while not self.stop_event.is_set():
            age = self.cache.warmup_age_seconds()
            if age < warmup_seconds:
                now = time.monotonic()
                if now >= next_log:
                    LOGGER.info(
                        "缓存预热中 age=%ds/%ds observed_eids=%d/%d received=%d",
                        age, warmup_seconds, self.cache.coverage_count(),
                        len(self.registry.required_eids), self.cache.received_values,
                    )
                    next_log = now + 60
                self.stop_event.wait(min(10, max(1, warmup_seconds - age)))
                continue
            try:
                self.run_detection_once(write_database=True)
            except Exception:
                LOGGER.exception("本轮检测或数据库写入失败，将在下个周期重试")
            self.stop_event.wait(self.interval_seconds)

    def start(self) -> None:
        environment = self.check_environment()
        LOGGER.info("启动检查通过: %s", json.dumps(environment, ensure_ascii=False))
        startup_message = (
            f"ready生产模型已加载;缓存{self.cache_minutes}分钟;预热{self.warmup_minutes}分钟;"
            f"每{self.interval_seconds // 60 if self.interval_seconds % 60 == 0 else self.interval_seconds}"
            f"{'分钟' if self.interval_seconds % 60 == 0 else '秒'}检测"
        )
        self.database.synchronize_ready_flags(
            self.registry.ready_eids,
            startup_message,
            bool(self.config["database"].get("synchronize_havemodel", True)),
        )
        LOGGER.info("数据库havemodel已同步，ready模型=%d", len(self.registry.ready_eids))
        Thread(target=self.detection_loop, name="model-detector", daemon=True).start()
        self.mqtt_client = self.create_mqtt_client()
        mqtt_config = self.config["mqtt"]
        self.mqtt_client.connect(
            str(mqtt_config["broker"]),
            int(mqtt_config.get("port", 1883)),
            int(mqtt_config.get("keepalive", 60)),
        )
        self.mqtt_client.loop_start()
        LOGGER.info("服务已启动，等待MQTT数据")
        while not self.stop_event.wait(1):
            pass

    def stop(self) -> None:
        if self.stop_event.is_set():
            return
        self.stop_event.set()
        if self.mqtt_client is not None:
            try:
                self.mqtt_client.loop_stop()
                self.mqtt_client.disconnect()
            except Exception:
                LOGGER.exception("关闭MQTT客户端时出错")
        LOGGER.info("在线模型检测服务已停止")


def main() -> int:
    parser = argparse.ArgumentParser(description="MQTT在线传感器模型质量检测服务")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--check", action="store_true", help="只检查模型和数据库，不连接MQTT、不写库")
    args = parser.parse_args()
    config_path = Path(args.config).resolve()
    config = load_json(config_path)
    setup_logging(config_path.parent, config)
    detector = OnlineDetector(config_path, config)
    if args.check:
        print(json.dumps(detector.check_environment(), ensure_ascii=False, indent=2))
        return 0

    def request_stop(signum, frame) -> None:
        del signum, frame
        detector.stop()

    signal.signal(signal.SIGINT, request_stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, request_stop)
    try:
        detector.start()
    except KeyboardInterrupt:
        detector.stop()
    except Exception:
        LOGGER.exception("服务异常退出")
        detector.stop()
        return 1
    finally:
        detector.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
