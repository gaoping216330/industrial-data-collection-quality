"""Bounded MQTT intake, separate writer, and a conservative overload circuit breaker."""
from collections import deque
from dataclasses import dataclass
import base64
import json
import logging
import math
import sqlite3
import os
from pathlib import Path
import threading
import time
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen
from uuid import uuid4

import paho.mqtt.client as mqtt
import redis
from packet_header import packet_timestamp

LOG = logging.getLogger('data_cache')


@dataclass(slots=True)
class Packet:
    topic: str
    payload: bytes
    received_ms: int
    monotonic: float


class BoundedInbox:
    def __init__(self, max_packets=10000, max_bytes=200 * 1024 * 1024):
        self.max_packets = max(1, int(max_packets))
        self.max_bytes = max(1, int(max_bytes))
        self.items = deque()
        self.bytes = 0
        self.condition = threading.Condition()
        self.active_topics = set()

    def push(self, packet):
        dropped = []
        with self.condition:
            if len(packet.payload) > self.max_bytes:
                return [packet]
            while self.items and (len(self.items) >= self.max_packets or
                                  self.bytes + len(packet.payload) > self.max_bytes):
                old = self.items.popleft()
                self.bytes -= len(old.payload)
                dropped.append(old)
            self.items.append(packet)
            self.bytes += len(packet.payload)
            self.condition.notify_all()
        return dropped

    def pop(self, timeout=0.5, shard=0, shards=1):
        with self.condition:
            for attempt in range(2):
                for index, packet in enumerate(self.items):
                    if packet.topic not in self.active_topics:
                        del self.items[index]
                        self.active_topics.add(packet.topic)
                        self.bytes -= len(packet.payload)
                        return packet
                if attempt == 0:
                    self.condition.wait(timeout)
            return None

    def release(self, packet):
        with self.condition:
            self.active_topics.discard(packet.topic)
            self.condition.notify_all()

    def clear(self):
        with self.condition:
            packets = list(self.items)
            self.items.clear()
            self.bytes = 0
            return packets

    def snapshot(self):
        with self.condition:
            return {'pending_packets': len(self.items), 'pending_bytes': self.bytes,
                    'oldest_pending_ms': round((time.monotonic() - self.items[0].monotonic) * 1000, 1)
                    if self.items else 0}


def over_broker_limit(snapshot, settings):
    return (snapshot.get('ready_messages', 0) > settings.get('max_ready_messages', 10000) or
            snapshot.get('ready_bytes', 0) > settings.get('max_ready_bytes', 200 * 1024 * 1024))


class MqttCollector:
    def __init__(self, config, cache, decoder):
        self.config, self.cache, self.decoder = config, cache, decoder
        self.source = json.loads(Path(config['mqtt_config_path']).read_text(encoding='utf-8-sig'))['mqtt']
        self.settings = config.get('mqtt_intake', {})
        self.broker_settings = self.settings.get('broker_monitor', {})
        self.inbox = BoundedInbox(self.settings.get('max_packets', 10000),
                                  self.settings.get('max_bytes', 200 * 1024 * 1024))
        self.network_inbox = None
        if self.settings.get('disk_buffer_enabled', False):
            self.network_inbox = self.inbox
            from durable_inbox import DiskInbox
            self.inbox = DiskInbox(self.settings.get('disk_buffer_path',str(Path(__file__).parent/'spool'/'mqtt.sqlite3')),
                                   self.settings.get('disk_buffer_max_bytes',4*1024**3),max_age_seconds=self.settings.get('live_max_age_seconds',60))
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        self.gaps = deque(maxlen=1000)
        self.sequence = 0
        self.worker_count = max(1, min(6, int(self.settings.get('writer_threads', 2))))
        self.processing_started = {}
        self.last_reported_gap = 0
        self.metrics = {'enabled': True, 'client_id': 'readMqt_redis', 'run_id': uuid4().hex,
                        'connected': False, 'received_packets': 0, 'processed_packets': 0,
                        'dropped_packets': 0, 'failed_packets': 0, 'filtered_packets': 0, 'selected_packets': 0, 'circuit_breaks': 0,
                        'writer_threads': self.worker_count,
                        'timestamp_semantics': 'packet_send_time',
                        'processing_ms': 0, 'header_filter_ms': 0, 'disk_spool_ms': 0, 'packet_decode_ms': 0, 'packet_cache_ms': 0, 'disk_ack_ms': 0, 'last_packet_at': 0,
                        'last_processed_at': 0, 'last_gap_at': 0, 'broker_monitor': 'not_checked',
                        'broker_ready': None, 'broker_ready_bytes': None, 'state': 'starting'}
        self.threads = []
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                                  client_id='readMqt_redis', clean_session=True,
                                  protocol=mqtt.MQTTv311)
        if self.source.get('username'):
            password = os.environ.get(self.source.get('password_env', ''), self.source.get('password', ''))
            self.client.username_pw_set(self.source['username'], password)
        self.client.reconnect_delay_set(2, 30)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        self.client.on_subscribe = self.on_subscribe

    def gap(self, reason, packet=None):
        now = int(time.time() * 1000)
        with self.lock:
            self.sequence += 1
            self.metrics['last_gap_at'] = now
            self.gaps.append({'sequence': self.sequence, 'at': now, 'reason': reason,
                              'topic': packet.topic if packet else '*',
                              'received_ms': packet.received_ms if packet else now})

    def drop(self, packets, reason):
        if not packets:
            return
        with self.lock:
            self.metrics['dropped_packets'] += len(packets)
        for packet in packets:
            self.gap(reason, packet)

    def on_connect(self, client, userdata, flags, reason_code, properties):
        with self.lock:
            self.metrics['connected'] = reason_code == 0
            self.metrics['state'] = 'connected' if reason_code == 0 else 'connect_failed'
        LOG.info('mqtt_connect client_id=readMqt_redis code=%s', reason_code)
        if reason_code == 0:
            # One SUBSCRIBE packet; do not request persistent offline history.
            client.subscribe([(topic, 0) for topic in self.source['topics']])

    def on_subscribe(self, client, userdata, mid, reasons, properties):
        if any(getattr(code, 'is_failure', False) for code in reasons):
            LOG.error('mqtt_subscribe_failed codes=%s', reasons)
            self.gap('subscription_rejected')

    def on_disconnect(self, client, userdata, flags, reason_code, properties):
        with self.lock:
            self.metrics['connected'] = False
        if not self.stop_event.is_set():
            self.gap('connection_interrupted')
        LOG.info('mqtt_disconnect client_id=readMqt_redis code=%s', reason_code)

    def on_message(self, client, userdata, message):
        # No JSON decoding, SQL or Redis operations on the network callback thread.
        packet = Packet(message.topic, message.payload, int(time.time() * 1000), time.monotonic())
        try:
            dropped = (self.network_inbox if self.network_inbox is not None else self.inbox).push(packet)
        except (OSError,sqlite3.Error):
            LOG.exception('mqtt_disk_buffer_failed')
            dropped=[packet]
        with self.lock:
            self.metrics['received_packets'] += 1
            self.metrics['last_packet_at'] = packet.received_ms
        self.drop(dropped, 'local_buffer_overflow')

    def selected_timestamp(self,payload):
        value=payload.get('timestamp') if isinstance(payload,dict) else None
        if isinstance(value,bool) or value is None:raise ValueError('missing packet send timestamp')
        timestamp=float(value)
        if not math.isfinite(timestamp) or not 0<=timestamp<=4102444800000:raise ValueError('invalid packet send timestamp')
        return not self.settings.get('only_aligned_packets',False) or int(timestamp)//1000%5==0

    def spool_writer(self):
        # Disk fsync and JSON decoding never run on paho's network/heartbeat thread.
        while not self.stop_event.is_set():
            packet=self.network_inbox.pop()
            if packet is None:continue
            try:
                header_started=time.perf_counter()
                timestamp=packet_timestamp(packet.payload)
                with self.lock:self.metrics['header_filter_ms']=round((time.perf_counter()-header_started)*1000,3)
                if not self.selected_timestamp({'timestamp':timestamp}):
                    with self.lock:self.metrics['filtered_packets']+=1
                    continue
                # Keep the packet in this worker until its durable commit succeeds.
                while not self.stop_event.is_set():
                    try:
                        disk_started=time.perf_counter()
                        dropped=self.inbox.push(packet)
                        with self.lock:self.metrics['disk_spool_ms']=round((time.perf_counter()-disk_started)*1000,2)
                        self.drop(dropped,'disk_buffer_full')
                        if not dropped:
                            with self.lock:self.metrics['selected_packets']+=1
                        break
                    except (OSError,sqlite3.Error):
                        LOG.exception('mqtt_disk_spool_retry')
                        self.stop_event.wait(1)
            except (ValueError,TypeError,UnicodeError):
                self.gap('invalid_packet_timestamp_or_json',packet)
                with self.lock:self.metrics['failed_packets']+=1
                LOG.exception('mqtt_packet_filter_failed')
            finally:self.network_inbox.release(packet)

    def writer(self, shard=0):
        while not self.stop_event.is_set():
            packet = self.inbox.pop(shard=shard, shards=self.worker_count)
            if packet is None:
                continue
            try:
                if (self.settings.get('discard_expired_packets', False) and
                        time.monotonic() - packet.monotonic > self.settings.get('max_pending_seconds', 5)):
                    self.drop([packet], 'local_packet_too_old')
                    continue
                if not self.cache.ready():
                    if not hasattr(self.inbox,'acknowledge'):
                        self.drop([packet], 'directory_unavailable')
                    else:
                        self.stop_event.wait(1)
                    continue
                started = time.monotonic()
                with self.lock:
                    self.processing_started[shard] = started
                try:
                    decoded_started=time.perf_counter()
                    payload = self.decoder(packet.payload)
                    with self.lock:self.metrics['packet_decode_ms']=round((time.perf_counter()-decoded_started)*1000,2)
                    if not isinstance(payload, dict) or payload.get('timestamp') is None:
                        raise ValueError('MQTT packet requires outer timestamp (send time)')
                    if not self.selected_timestamp(payload):
                        if hasattr(self.inbox,'acknowledge'):self.inbox.acknowledge(packet)
                        with self.lock:self.metrics['filtered_packets']+=1
                        continue
                    packet_time = payload['timestamp']
                    items = payload.get('values')
                    if not isinstance(items, list):
                        raise ValueError('MQTT payload must contain an array')
                    cache_started=time.perf_counter()
                    point_batch=max(500,min(10000,int(self.settings.get('ingest_batch_points',10000))))
                    for offset in range(0, len(items), point_batch):
                        self.cache.ingest(items[offset:offset + point_batch], 'kepserver',
                                          packet.received_ms, packet_time=packet_time)
                    with self.lock:self.metrics['packet_cache_ms']=round((time.perf_counter()-cache_started)*1000,2)
                    ack_started=time.perf_counter()
                    if hasattr(self.inbox,'acknowledge'):self.inbox.acknowledge(packet)
                    with self.lock:self.metrics['disk_ack_ms']=round((time.perf_counter()-ack_started)*1000,2)
                    with self.lock:
                        self.metrics['processed_packets'] += 1
                        self.metrics['last_processed_at'] = int(time.time() * 1000)
                except Exception as exc:
                    with self.lock:
                        self.metrics['failed_packets'] += 1
                    if hasattr(self.inbox,'acknowledge'):
                        if isinstance(exc,(ValueError,TypeError,UnicodeError)):
                            self.gap('packet_decode_or_validation_failed', packet)
                            self.inbox.reject(packet,exc)
                            LOG.error('invalid_packet_quarantined topic=%s',packet.topic)
                        self.stop_event.wait(2)
                    else:
                        self.gap('packet_write_or_decode_failed', packet)
                    LOG.exception('mqtt_writer_failed topic=%s', packet.topic)
                finally:
                    with self.lock:
                        self.processing_started.pop(shard, None)
                        self.metrics['processing_ms'] = round((time.monotonic() - started) * 1000, 2)
            finally:
                self.inbox.release(packet)

    def snapshot(self):
        with self.lock:
            result = dict(self.metrics)
            started = min(self.processing_started.values(), default=0)
        result.update(self.inbox.snapshot())
        if self.network_inbox is not None:
            network=self.network_inbox.snapshot()
            result.update(network_pending_packets=network['pending_packets'],network_pending_bytes=network['pending_bytes'],network_oldest_pending_ms=network['oldest_pending_ms'])
            result['pending_packets']+=network['pending_packets']
            result['pending_bytes']+=network['pending_bytes']
            result['oldest_pending_ms']=max(result['oldest_pending_ms'],network['oldest_pending_ms'])
        result['ingest_timings']=self.cache.ingest_metrics.get('kepserver',{}) if hasattr(self.cache,'ingest_metrics') else {}
        result['processing_active_ms'] = round((time.monotonic() - started) * 1000, 1) if started else 0
        result['history_complete'] = (not result['last_gap_at'] or
                                      time.time() * 1000 - result['last_gap_at'] >= self.cache.seconds * 1000)
        return result

    def report(self):
        snapshot = self.snapshot()
        with self.lock:
            gaps = [dict(item) for item in self.gaps if item['sequence'] > self.last_reported_gap]
        with self.cache.client.pipeline(transaction=True) as pipe:
            pipe.hset(self.cache.prefix + 'mqtt_runtime', mapping={
                key: json.dumps(value) for key, value in snapshot.items()})
            for item in gaps:
                pipe.xadd(self.cache.prefix + 'history_gaps:kepserver', item, maxlen=10000)
            if gaps:
                pipe.expire(self.cache.prefix + 'history_gaps:kepserver', self.cache.seconds)
            pipe.execute()
        if gaps:
            self.last_reported_gap = gaps[-1]['sequence']

    def broker_snapshot(self):
        root = self.broker_settings.get('url', f"http://{self.source['broker']}:15672").rstrip('/')
        name = quote('mqtt-subscription-readMqt_redisqos0', safe='')
        vhost = quote(self.broker_settings.get('vhost', '/'), safe='')
        username = self.broker_settings.get('username', self.source.get('username', ''))
        password = os.environ.get(self.broker_settings.get('password_env', ''),
                                  self.broker_settings.get('password', self.source.get('password', '')))
        token = base64.b64encode(f'{username}:{password}'.encode()).decode()
        req = Request(f'{root}/api/queues/{vhost}/{name}', headers={'Authorization': 'Basic ' + token})
        try:
            with urlopen(req, timeout=2) as response:
                data = json.loads(response.read(1024 * 1024))
        except HTTPError as error:
            if error.code == 404:
                return {'ready_messages': 0, 'ready_bytes': 0, 'present': False}
            raise
        return {'ready_messages': data.get('messages_ready', 0),
                'ready_bytes': data.get('message_bytes_ready', 0), 'present': True}

    def connect(self):
        self.client.connect_async(self.source['broker'], int(self.source.get('port', 1883)), 30)
        self.client.loop_start()

    def controller(self):
        broker_next, report_next, local_next = 0, 0, 0
        broker_overload, local_overload = 0, 0
        while not self.stop_event.wait(1):
            try:
                snapshot = self.snapshot()
                if time.monotonic() >= local_next:
                    local_overload = local_overload + 1 if snapshot['processing_active_ms'] > self.settings.get('local_break_seconds', 20) * 1000 else 0
                    local_next = time.monotonic() + self.settings.get('local_check_seconds', 10)
                if self.broker_settings.get('enabled', True) and time.monotonic() >= broker_next:
                    try:
                        broker = self.broker_snapshot()
                        broker_overload = broker_overload + 1 if over_broker_limit(broker, self.broker_settings) else 0
                        with self.lock:
                            self.metrics.update(broker_monitor='ok', broker_ready=broker['ready_messages'],
                                                broker_ready_bytes=broker['ready_bytes'])
                        broker_next = time.monotonic() + self.broker_settings.get('poll_seconds', 60)
                    except Exception as exc:
                        with self.lock:
                            self.metrics['broker_monitor'] = type(exc).__name__
                        # Unknown is never reported as zero backlog. Reduce failed API polling.
                        broker_overload = 0
                        broker_next = time.monotonic() + 60
                reason = None
                if local_overload >= self.settings.get('local_consecutive_overload', 3):
                    reason = 'local_processing_overload'
                elif broker_overload >= self.broker_settings.get('consecutive_overload', 2):
                    reason = 'broker_queue_overload'
                if reason:
                    self.gap(reason)
                    with self.lock:
                        self.metrics['circuit_breaks'] += 1
                        self.metrics['state'] = 'cooldown'
                    LOG.warning('mqtt_circuit_open reason=%s metrics=%s', reason, self.snapshot())
                    self.client.disconnect()
                    self.client.loop_stop()
                    # Already received packets remain available to the writers during cooldown.
                    try:
                        self.report()
                    except redis.RedisError:
                        LOG.exception('mqtt_circuit_report_failed')
                    if self.stop_event.wait(self.settings.get('cooldown_seconds', 30)):
                        break
                    self.gap('circuit_reconnect_history_gap')
                    self.connect()
                    broker_overload = local_overload = 0
                    broker_next = time.monotonic() + self.broker_settings.get('poll_seconds', 60)
                    local_next = time.monotonic() + self.settings.get('local_check_seconds', 10)
                if time.monotonic() >= report_next:
                    self.report()
                    LOG.info('mqtt_runtime %s', self.snapshot())
                    report_next = time.monotonic() + 10
            except Exception:
                LOG.exception('mqtt_controller_failed')

    def start(self):
        # New clean session cannot recover packets sent while this subscriber was down.
        self.gap('subscriber_started_history_may_be_incomplete')
        targets = ([(self.spool_writer, ())] if self.network_inbox is not None else []) + [(self.writer, (index,)) for index in range(self.worker_count)] + [(self.controller, ())]
        for target, arguments in targets:
            thread = threading.Thread(target=target, args=arguments, name=target.__name__, daemon=True)
            thread.start()
            self.threads.append(thread)
        self.connect()

    def stop(self):
        self.stop_event.set()
        self.client.disconnect()
        self.client.loop_stop()
        self.drop(self.inbox.clear(), 'subscriber_stopped')
        if self.network_inbox is not None:self.drop(self.network_inbox.clear(),'subscriber_stopped_before_disk_commit')
        for thread in self.threads:
            thread.join(timeout=10)
        self.report()
