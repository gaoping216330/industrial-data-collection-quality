"""Independent OA/MQTT receiver. autosensor is read-only; values go to Redis."""
from __future__ import annotations

import json
import logging
from logging.handlers import RotatingFileHandler
import math
from pathlib import Path
import threading
import time
from uuid import uuid4

from flask import Flask, jsonify, request
import pymysql
import redis
import orjson
from waitress import serve

ROOT = Path(__file__).resolve().parent
LOG = logging.getLogger('data_cache')


def milliseconds(value, default=None):
    if value is None:
        return default
    if isinstance(value, bool):
        raise ValueError('timestamp must be milliseconds')
    number = float(value)
    if not math.isfinite(number) or not 0 <= number <= 4102444800000:
        raise ValueError('invalid timestamp')
    return int(number)


def normalize(item, source, received_time, packet_time=None):
    if not isinstance(item, dict):
        raise ValueError('item must be an object')
    eid = str(item.get('0', item.get('eid', item.get('id', '')))).rsplit('_', 1)[-1].strip().lower()
    if not eid or len(eid) > 128 or eid == 'none':
        raise ValueError('invalid eid')
    value = item.get('50', item.get('v', item.get('value')))
    if value is None or isinstance(value, (dict, list)):
        raise ValueError('invalid value')
    if not isinstance(value, bool):
        value = float(value)
        if not math.isfinite(value):
            raise ValueError('non-finite value')
    quality = str(item.get('q', ''))
    if len(quality) > 128:
        raise ValueError('quality too long')
    record = {
        'eid': eid, 'value': value, 'quality': quality, 'source': source,
        'source_time': milliseconds(item.get('1', item.get('t', item.get('timestamp'))), received_time),
        'last_change_time': milliseconds(item.get('99')),
        'received_time': received_time,
    }
    if source == 'kepserver' and packet_time is not None:
        record['source_time'] = milliseconds(packet_time)
        record['last_change_time'] = milliseconds(item.get('t'))
        record['time_schema'] = 2
    return record


def decode_mqtt(payload):
    # Existing gateway messages contain unescaped control characters in strings.
    # Keep those strings intact; normal JSON structure and point validation still apply.
    payload = payload.replace(b'\x00', b'')
    try:
        return orjson.loads(payload)
    except orjson.JSONDecodeError:
        return json.loads(payload.decode('utf-8'), strict=False)


HISTORY_LUA = r'''
local function sample_history(prefix, eid, received, sent, value, interval, retention, clocks, clock_updates)
    local timestamp = tonumber(sent)
    local step = tonumber(interval)
    -- Use the sender's target second, not a delayed packet's arrival window.
    local tick = math.floor(timestamp / step) * step
    if timestamp - tick >= 1000 then return end
    local clock = prefix .. 'history_sample_grid_at'
    local previous
    if clocks then previous=clocks[eid] else previous=redis.call('HGET',clock,eid) end
    if not previous or tick > tonumber(previous) then
        local history = prefix .. 'history:' .. eid
        redis.call('XADD', history, '*', 't', tick, 'v', value)
        local ttl = redis.call('TTL', history)
        if ttl < tonumber(retention) / 2 then redis.call('EXPIRE', history, retention) end
        if clocks then
            clocks[eid]=tick
            clock_updates[#clock_updates+1]=eid
            clock_updates[#clock_updates+1]=tick
        else redis.call('HSET',clock,eid,tick) end
    end
end
'''

STORE_LUA = HISTORY_LUA + r'''
local outcomes = {}
local receipts = {}
local latest = {}
local histories = {}
local pending = {}
local received = {}
for i, row in ipairs(cjson.decode(ARGV[5])) do
    local code = 1
    if redis.call('HGET', KEYS[1], row[1]) ~= ARGV[2] then
        code = -2
    else
        local receipt = received[row[1]] or redis.call('HGET', KEYS[2], row[1])
        if not receipt or tonumber(cjson.decode(receipt).received_time) <= tonumber(row[9]) then
            receipts[#receipts+1]=row[1]
            receipts[#receipts+1]=row[8]
            received[row[1]]=row[8]
        end
        local old = pending[row[1]] or redis.call('GET', ARGV[1] .. 'latest:' .. row[1])
        if old then
            local previous = cjson.decode(old)
            local incoming = cjson.decode(row[2])
            if previous.source == incoming.source and previous.time_schema == incoming.time_schema then
                if tonumber(previous.source_time) > tonumber(incoming.source_time) then
                    code = -1
                elseif previous.source_time == incoming.source_time and previous.value == incoming.value
                   and previous.quality == incoming.quality
                   and previous.last_change_time == incoming.last_change_time then
                    code = 0
                end
            end
        end
        if code == 1 then
            local history = ARGV[1] .. 'history:' .. row[1]
            if ARGV[2] ~= 'kepserver' or row[10] then
                histories[#histories+1]=row
            end
            latest[#latest+1]=ARGV[1] .. 'latest:' .. row[1]
            latest[#latest+1]=row[2]
            pending[row[1]]=row[2]
        end
    end
    outcomes[i] = code
end
if #receipts>0 then redis.call('HSET',KEYS[2],unpack(receipts)) end
if #latest>0 then redis.call('MSET',unpack(latest)) end
for _,row in ipairs(histories) do
    sample_history(ARGV[1],row[1],row[9],row[3],row[4],ARGV[6],ARGV[4])
end
return outcomes
'''

RECEIPT_LUA = r'''
for _, row in ipairs(cjson.decode(ARGV[2])) do
    if redis.call('HGET', KEYS[1], row[1]) == ARGV[1] then
        local old = redis.call('HGET', KEYS[2], row[1])
        if not old or tonumber(cjson.decode(old).received_time) <= tonumber(row[3]) then
            redis.call('HSET', KEYS[2], row[1], row[2])
        end
    end
end
return #cjson.decode(ARGV[2])
'''

FAST_STORE_LUA = HISTORY_LUA + r'''
local rows = cjson.decode(ARGV[2])
local eids = {}
local latest_keys = {}
for i,row in ipairs(rows) do eids[i]=row[1];latest_keys[i]=ARGV[1] .. 'latest:' .. row[1] end
if #eids==0 then return {0,0,0} end
local policies=redis.call('HMGET',KEYS[2],unpack(eids))
local old_receipts=redis.call('HMGET',KEYS[1],unpack(eids))
local old_latest=redis.call('MGET',unpack(latest_keys))
local old_clocks=redis.call('HMGET',ARGV[1] .. 'history_sample_grid_at',unpack(eids))
local clocks={}
for i,eid in ipairs(eids) do clocks[eid]=old_clocks[i] end
local clock_updates={}
local receipts = {}
local latest = {}
local written = 0
local ignored = 0
local stale = 0
local pending = {}
local received = {}
for i, row in ipairs(rows) do
    local old = pending[row[1]] or old_latest[i]
    local previous = old and cjson.decode(old) or nil
    if policies[i] ~= 'kepserver' then
        ignored = ignored + 1
    elseif not previous or previous.time_schema ~= row[8] or tonumber(previous.source_time) <= tonumber(row[4]) then
        local receipt = received[row[1]] or old_receipts[i]
        if not receipt or tonumber(cjson.decode(receipt).received_time) <= tonumber(row[9]) then
            receipts[#receipts + 1] = row[1]
            receipts[#receipts + 1] = row[3]
            received[row[1]]=row[3]
        end
        latest[#latest + 1] = ARGV[1] .. 'latest:' .. row[1]
        latest[#latest + 1] = row[2]
        pending[row[1]]=row[2]
        if row[10] then
            sample_history(ARGV[1], row[1], row[9], row[4], row[5], ARGV[3], ARGV[4],clocks,clock_updates)
        end
        written = written + 1
    else
        stale = stale + 1
    end
end
if #receipts > 0 then redis.call('HSET', KEYS[1], unpack(receipts)) end
if #latest > 0 then redis.call('MSET', unpack(latest)) end
if #clock_updates>0 then redis.call('HSET',ARGV[1] .. 'history_sample_grid_at',unpack(clock_updates)) end
return {written, ignored, stale}
'''


PUBLISH_CATALOG_LUA = r'''
for _, eid in ipairs(cjson.decode(ARGV[3])) do
    redis.call('DEL', ARGV[1] .. 'latest:' .. eid)
    redis.call('HDEL', KEYS[3], eid)
    redis.call('HDEL', ARGV[1] .. 'history_sample_grid_at', eid)
    redis.call('HDEL', ARGV[1] .. 'history_sample_at', eid)
    local history = ARGV[1] .. 'history:' .. eid
    if redis.call('EXISTS', history) == 1 then
        local archived = ARGV[1] .. 'history_archive:' .. ARGV[2] .. ':' .. eid
        redis.call('RENAME', history, archived)
        redis.call('EXPIRE', archived, ARGV[4])
    end
end
redis.call('RENAME', KEYS[2], KEYS[1])
return 1
'''


class Cache:
    def __init__(self, client, history_minutes=60, prefix='dq:cache:', history_interval_seconds=5, directory_refresh_seconds=600):
        self.client = client
        self.seconds = int(history_minutes) * 60
        self.history_interval_ms = max(1, int(history_interval_seconds * 1000))
        self.prefix = prefix
        self.policy_key = prefix + 'catalog'
        self.script = client.register_script(STORE_LUA)
        self.receipt_script = client.register_script(RECEIPT_LUA)
        self.fast_script = client.register_script(FAST_STORE_LUA)
        self.directory_revision = None
        self.directory_max_age = max(300, directory_refresh_seconds * 2 + 60)
        self.publish_catalog_script = client.register_script(PUBLISH_CATALOG_LUA)
        self.directory_loaded_at = 0
        self.directory_error = 'not_loaded'
        self.directory_counts = {}
        self.policy = None
        # Only skip committed duplicates; Redis still orders concurrent topic writers.
        self.mqtt_committed = {}
        self.mqtt_seeded = False
        self.mqtt_collector = None
        self.oa_spool = None
        self.ingest_metrics = {}

    def refresh_directory(self, database):
        try:
            revision = self.client.get(self.prefix+'catalog_revision')
            with pymysql.connect(**database, charset='utf8mb4', connect_timeout=8,
                                 read_timeout=15, cursorclass=pymysql.cursors.DictCursor) as connection:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT eid, channel, channeloa FROM autosensor "
                                   "WHERE tag=0 AND (channel='kepserver' OR channeloa='oa')")
                    rows = cursor.fetchall()
            policy = {}
            overlap = 0
            for row in rows:
                eid = str(row['eid']).strip().lower()
                kep = str(row.get('channel') or '').lower() == 'kepserver'
                oa = str(row.get('channeloa') or '').lower() == 'oa'
                overlap += int(kep and oa)
                # Duplicate directory rows also retain Kepserver ownership.
                if kep or policy.get(eid) != 'kepserver':
                    policy[eid] = 'kepserver' if kep else 'oa'
            self.publish_directory(policy)
            counts = {'kepserver': sum(v == 'kepserver' for v in policy.values()),
                      'oa_excluding_kepserver': sum(v == 'oa' for v in policy.values()),
                      'overlap_rows': overlap}
            self.client.hset(self.prefix + 'catalog_status', mapping={
                'loaded_at': time.time(), **counts})
            self.directory_counts = counts
            self.policy = policy
            self.directory_loaded_at = time.time()
            self.directory_error = None
            self.directory_revision = revision
            if not self.mqtt_seeded:
                self.seed_mqtt_committed(policy)
            LOG.info('catalog_loaded %s', counts)
            return True
        except Exception as exc:
            self.directory_error = type(exc).__name__ + ': ' + str(exc)
            LOG.error('catalog_load_failed %s', self.directory_error)
            return False

    def publish_directory(self, policy):
        previous = self.client.hgetall(self.policy_key)
        changed = [eid for eid,source in previous.items() if eid!='__catalog_loaded__' and policy.get(eid)!=source]
        temporary=self.prefix+'catalog-loading:'+uuid4().hex
        self.client.hset(temporary,mapping={'__catalog_loaded__':'yes',**policy})
        self.publish_catalog_script(keys=[self.policy_key,temporary,self.prefix+'receipt'],
                                    args=[self.prefix,uuid4().hex,orjson.dumps(changed),self.seconds+120])
        for eid in changed:
            self.mqtt_committed.pop(eid,None)
        LOG.info('catalog_ownership_changed count=%s',len(changed))

    def seed_mqtt_committed(self, policy):
        """Start the fast Kepserver path from Redis's last committed values."""
        eids = [eid for eid, source in policy.items() if source == 'kepserver']
        for offset in range(0, len(eids), 1000):
            chunk = eids[offset:offset + 1000]
            with self.client.pipeline(transaction=False) as pipe:
                for eid in chunk:
                    pipe.get(self.prefix + 'latest:' + eid)
                values = pipe.execute()
            for eid, value in zip(chunk, values):
                if not value:
                    continue
                try:
                    record = orjson.loads(value)
                    if record.get('source') == 'kepserver':
                        self.mqtt_committed[eid] = (record['source_time'], record['value'],
                                                     record['quality'], record['last_change_time'],
                                                     record.get('time_schema', 1))
                except (ValueError, KeyError, TypeError):
                    continue
        self.mqtt_seeded = True
        LOG.info('mqtt_fastpath_seeded count=%d', len(self.mqtt_committed))

    def ready(self):
        return time.time() - self.directory_loaded_at < self.directory_max_age

    def ingest(self, items, source, received_time=None, packet_time=None):
        started = time.perf_counter()
        now = int(time.time() * 1000)
        received_time = now if received_time is None else received_time
        result = {'received': len(items), 'cached': 0, 'ignored': 0,
                  'invalid': 0, 'stale': 0, 'duplicate': 0}
        records = []
        extra_receipts = {}
        policy = self.policy
        for item in items:
            receipt = None
            try:
                if not isinstance(item, dict):
                    raise ValueError('item must be an object')
                raw_eid = item.get('0', item.get('eid', item.get('id', '')))
                receipt_eid = str(raw_eid).rsplit('_', 1)[-1].strip().lower()
                if receipt_eid and len(receipt_eid) <= 128 and (policy is None or policy.get(receipt_eid) == source):
                    try:
                        sent_at = milliseconds(packet_time if source == 'kepserver' else item.get('1'))
                    except (ValueError, TypeError, OverflowError):
                        sent_at = None
                    try:
                        changed_at = milliseconds(item.get('t') if source == 'kepserver' else item.get('99'))
                    except (ValueError, TypeError, OverflowError):
                        changed_at = None
                    receipt = {'received_time': received_time, 'source_time': sent_at,
                               'last_change_time': changed_at,
                               'quality': str(item.get('q', ''))[:128], 'source': source}
                else:
                    receipt = None
                record = normalize(item, source, received_time, packet_time)
                eid = record['eid']
                if policy is not None and policy.get(eid) != source:
                    result['ignored'] += 1
                    continue
                fingerprint = (record['source_time'], record['value'], record['quality'], record['last_change_time'], record.get('time_schema', 1))
                if source == 'kepserver':
                    previous = self.mqtt_committed.get(eid)
                    if previous and previous[4] == fingerprint[4] and record['source_time'] < previous[0]:
                        result['stale'] += 1
                        if receipt:
                            extra_receipts[eid] = receipt
                        continue
                    if previous == fingerprint:
                        result['duplicate'] += 1
                        if receipt:
                            extra_receipts[eid] = receipt
                        continue
                record['receipt'] = receipt
                records.append(record)
            except (ValueError, TypeError, OverflowError):
                result['invalid'] += 1
                if receipt:
                    extra_receipts[receipt_eid] = receipt
        parsed_at = time.perf_counter()
        redis_exchanges=0
        script_batches=0
        redis_execute_ms=0
        if extra_receipts:
            for offset in range(0, len(extra_receipts), 500):
                chunk = list(extra_receipts.items())[offset:offset + 500]
                rows = [[eid, orjson.dumps(receipt).decode(), received_time] for eid, receipt in chunk]
                command_started=time.perf_counter()
                self.receipt_script(keys=[self.policy_key, self.prefix + 'receipt'],
                                    args=[source, orjson.dumps(rows)])
                redis_exchanges+=1;script_batches+=1;redis_execute_ms+=(time.perf_counter()-command_started)*1000
        labels = {1: 'cached', -2: 'ignored', -1: 'stale', 0: 'duplicate'}
        if source == 'kepserver' and self.mqtt_seeded:
            tasks=[]
            for offset in range(0, len(records), 500):
                batch = records[offset:offset + 500]
                rows=[]
                for record in batch:
                    payload={key:value for key,value in record.items() if key!='receipt'}
                    rows.append([record['eid'],orjson.dumps(payload).decode(),
                                 orjson.dumps(record['receipt']).decode(),record['source_time'],
                                 orjson.dumps(record['value']).decode(),record['quality'],
                                 '' if record['last_change_time'] is None else record['last_change_time'],
                                 record.get('time_schema',1), received_time,
                                 record['quality'].strip().lower() in {'1','true','good','ok','yes','y'}])
                tasks.append((batch,orjson.dumps(rows)))
            for window in range(0,len(tasks),4):
                selected=tasks[window:window+4]
                with self.client.pipeline(transaction=False) as pipe:
                    for batch,payload in selected:
                        self.fast_script(keys=[self.prefix+'receipt',self.policy_key],args=[self.prefix,payload,self.history_interval_ms,self.seconds+120],client=pipe)
                    command_started=time.perf_counter()
                    outcomes=pipe.execute()
                    redis_exchanges+=1;script_batches+=len(selected);redis_execute_ms+=(time.perf_counter()-command_started)*1000
                for (batch,_),(written,ignored,stale) in zip(selected,outcomes):
                    result['cached'] += written
                    result['ignored'] += ignored
                    result['stale'] += stale
                    for record in batch:
                        if self.policy is not None and self.policy.get(record['eid'])!='kepserver':
                            continue
                        previous=self.mqtt_committed.get(record['eid'])
                        fingerprint=(record['source_time'],record['value'],record['quality'],record['last_change_time'],record.get('time_schema',1))
                        if previous is None or previous[4]!=fingerprint[4] or fingerprint[0]>=previous[0]:
                            self.mqtt_committed[record['eid']] = fingerprint
            records = []
        for offset in range(0, len(records), 500):
            batch = records[offset:offset + 500]
            rows = [[r['eid'], orjson.dumps({k:v for k,v in r.items() if k!='receipt'}).decode(), r['source_time'],
                     orjson.dumps(r['value']).decode(), r['quality'],
                     '' if r['last_change_time'] is None else r['last_change_time'],
                     r.get('time_schema', 1), orjson.dumps(r['receipt']).decode(), received_time,
                     r['quality'].strip().lower() in {'1','true','good','ok','yes','y'}] for r in batch]
            command_started=time.perf_counter()
            outcomes = self.script(keys=[self.policy_key,self.prefix + 'receipt'], args=[self.prefix, source,
                                   f'{now - self.seconds * 1000}-0', self.seconds + 120,
                                   orjson.dumps(rows), self.history_interval_ms])
            redis_exchanges+=1;script_batches+=1;redis_execute_ms+=(time.perf_counter()-command_started)*1000
            for record, outcome in zip(batch, outcomes):
                result[labels[outcome]] += 1
                if source == 'kepserver' and outcome in (0, 1):
                    self.mqtt_committed[record['eid']] = (record['source_time'], record['value'],
                                                         record['quality'], record['last_change_time'], record.get('time_schema', 1))
        with self.client.pipeline(transaction=True) as pipe:
            key = self.prefix + 'stats:' + source
            for field, count in result.items():
                if count:pipe.hincrby(key, field, count)
            pipe.hset(key, mapping={'last_batch_at': now, 'last_batch': json.dumps(result)})
            command_started=time.perf_counter()
            pipe.execute()
            redis_exchanges+=1;redis_execute_ms+=(time.perf_counter()-command_started)*1000
        self.ingest_metrics[source] = dict(parse_ms=round((parsed_at-started)*1000,2),write_ms=round((time.perf_counter()-parsed_at)*1000,2),points=len(items),at=now,redis_exchanges=redis_exchanges,script_batches=script_batches,redis_execute_ms=round(redis_execute_ms,2))
        return result

    def trim_history(self):
        cutoff = f'{int(time.time() * 1000) - self.seconds * 1000}-0'
        keys = []
        for key in self.client.scan_iter(match=self.prefix + 'history:*', count=500):
            keys.append(key)
            if len(keys) == 500:
                with self.client.pipeline(transaction=False) as pipe:
                    for item in keys:
                        pipe.xtrim(item, minid=cutoff, approximate=True)
                    pipe.execute()
                time.sleep(0.01)
                keys.clear()
        if keys:
            with self.client.pipeline(transaction=False) as pipe:
                for item in keys:
                    pipe.xtrim(item, minid=cutoff, approximate=True)
                pipe.execute()


def create_app(config, cache):
    app = Flask(__name__)
    app.config['MAX_CONTENT_LENGTH'] = 4 * 1024 * 1024

    @app.after_request
    def ingest_status(response):
        if request.path == '/api/ingest/winccoa':
            payload = response.get_json(silent=True)
            if not isinstance(payload, dict):
                payload = {'error': 'http_error'}
            payload['success'] = 200 <= response.status_code < 300
            payload['status'] = 'success' if payload['success'] else 'error'
            response.set_data(app.json.dumps(payload))
            response.content_type = 'application/json'
        return response

    @app.before_request
    def authenticate():
        if request.path != '/health':
            LOG.info('http_request remote=%s method=%s path=%s bytes=%s',
                     request.remote_addr, request.method, request.path, request.content_length)
        if request.path == '/health':
            return None
        allowed = config['application']['allowed_oa_ips']
        if request.remote_addr not in {'127.0.0.1', '::1', *allowed}:
            return jsonify(error='source_ip_not_allowed'), 403

    @app.get('/health')
    def health():
        try:
            cache.client.ping()
        except redis.RedisError:
            return jsonify(status='redis_unavailable'), 503
        return jsonify(status='ready' if cache.ready() else 'directory_unavailable',
                       directory_ready=cache.ready()), 200 if cache.ready() else 503

    @app.post('/api/ingest/winccoa')
    def ingest_oa():
        payload = request.get_json(silent=True)
        items = payload.get('values') if isinstance(payload, dict) else payload
        if not isinstance(items, list):
            return jsonify(error='expected_json_array_or_values_array'), 400
        if len(items) > config['application']['max_batch']:
            return jsonify(error='batch_too_large', limit=config['application']['max_batch']), 413
        if not cache.ready():
            LOG.warning('oa_rejected_directory_unavailable remote=%s received=%d',
                        request.remote_addr, len(items))
            return jsonify(error='directory_unavailable', received=len(items), cached=0,
                           retryable=True), 503
        received_ms = int(time.time()*1000)
        try:
            result = cache.ingest(items, 'oa', received_ms)
        except redis.RedisError:
            LOG.exception('oa_cache_write_failed remote=%s', request.remote_addr)
            if cache.oa_spool is not None:
                from mqtt_collector import Packet
                packet=Packet('oa',orjson.dumps(items),received_ms,time.monotonic())
                try:
                    if not cache.oa_spool.push(packet):
                        return jsonify(received=len(items),cached=0,buffered=True,queued=len(items),message='已保存本地磁盘，等待写入Redis'),202
                except Exception:
                    LOG.exception('oa_disk_buffer_failed')
            return jsonify(error='redis_write_failed', retryable=True), 503
        LOG.info('oa_batch remote=%s %s', request.remote_addr, result)
        return jsonify(result)

    @app.get('/api/cache/status')
    def status():
        return jsonify(directory_ready=cache.ready(), directory_counts=cache.directory_counts,
                       directory_loaded_at=cache.directory_loaded_at,
                       history_interval_seconds=cache.history_interval_ms / 1000,
                       mqtt=cache.mqtt_collector.snapshot() if cache.mqtt_collector else {'enabled': False},
                       ingest_timings=cache.ingest_metrics,
                       oa_buffer=cache.oa_spool.snapshot() if cache.oa_spool else None,
                       stats={source: cache.client.hgetall(cache.prefix + 'stats:' + source)
                              for source in ('oa', 'kepserver')})

    return app


def start_mqtt(config, cache):
    from mqtt_collector import MqttCollector
    collector = MqttCollector(config, cache, decode_mqtt)
    cache.mqtt_collector = collector
    collector.start()
    return collector


def main():
    (ROOT / 'logs').mkdir(exist_ok=True)
    handler = RotatingFileHandler(ROOT / 'logs' / 'receiver.log', maxBytes=10_000_000,
                                  backupCount=3, encoding='utf-8')
    logging.basicConfig(level=logging.INFO, handlers=[handler, logging.StreamHandler()],
                        format='%(asctime)s %(levelname)s %(message)s')
    config = json.loads((ROOT / 'cache_config.json').read_text(encoding='utf-8-sig'))
    cache = Cache(redis.Redis(**dict(config['redis'],socket_connect_timeout=2,socket_timeout=3,health_check_interval=15,retry=redis.retry.Retry(redis.backoff.NoBackoff(),0))), config['history_minutes'],
                  history_interval_seconds=config.get('history_interval_seconds', 5),
                  directory_refresh_seconds=config.get('catalog_refresh_seconds',600))
    for attempt in range(30):
        try:
            cache.client.ping()
            break
        except redis.RedisError:
            if attempt == 29:
                raise
            time.sleep(1)
    cache.refresh_directory(config['database'])
    if config.get('disk_buffer_enabled',False):
        from durable_inbox import DiskInbox
        cache.oa_spool=DiskInbox(str(ROOT/'spool'/'oa.sqlite3'),config.get('oa_disk_buffer_max_bytes',1024**3),max_age_seconds=60)
        def replay_oa():
            while True:
                packet=cache.oa_spool.pop()
                if packet is None:continue
                try:
                    if not cache.ready():
                        time.sleep(2);continue
                    cache.ingest(orjson.loads(packet.payload),'oa',packet.received_ms)
                    cache.oa_spool.acknowledge(packet)
                except Exception:
                    LOG.exception('oa_disk_replay_retry');time.sleep(2)
                finally:cache.oa_spool.release(packet)
        threading.Thread(target=replay_oa,name='oa_disk_replay',daemon=True).start()

    def maintenance():
        trimmed_at=0
        while True:
            time.sleep(5)
            try:
                if cache.oa_spool:cache.oa_spool.quarantine_expired()
                if cache.mqtt_collector and hasattr(cache.mqtt_collector.inbox,'quarantine_expired'):cache.mqtt_collector.inbox.quarantine_expired()
                revision=cache.client.get(cache.prefix+'catalog_revision')
                if time.time()-cache.directory_loaded_at>=config['catalog_refresh_seconds'] or revision!=cache.directory_revision:
                    cache.refresh_directory(config['database'])
                if time.time()-trimmed_at>=60:
                    cache.trim_history()
                    trimmed_at=time.time()
            except redis.RedisError:
                LOG.exception('catalog_or_history_maintenance_failed')

    threading.Thread(target=maintenance, daemon=True).start()
    mqtt_client = start_mqtt(config, cache) if config.get('mqtt_enabled') else None
    LOG.info('receiver_start host=%s port=%s directory_ready=%s',
             config['application']['host'], config['application']['port'], cache.ready())
    try:
        serve(create_app(config, cache), host=config['application']['host'],
              port=config['application']['port'], threads=8, max_request_body_size=4 * 1024 * 1024)
    finally:
        if mqtt_client:
            mqtt_client.stop()


if __name__ == '__main__':
    import sys as _diag_sys
    from pathlib import Path as _DiagPath
    _diag_sys.path.insert(0,str(_DiagPath(__file__).resolve().parent.parent/'运行诊断'))
    from runtime_logging import install as _install_diagnostics
    _install_diagnostics('cache_service')

    main()
