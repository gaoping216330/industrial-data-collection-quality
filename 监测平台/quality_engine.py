"""OA/Kepserver receipt, clock, quality and configurable flatline monitor."""
from __future__ import annotations

import json
import hashlib
import logging
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import pymysql
import redis

from confirmation_window import ConfirmedObservations
from collection_health import CollectionHealth, receiver_ready
from directory_sync import reconcile, DIRECTORY_PENDING
from display_group import display_group_token
from availability import AvailabilityTracker
from device_incidents import record_devices
from publication_state import write_publication

ROOT = Path(__file__).resolve().parent
SOURCE = ROOT.parent / '实时数据缓存服务'
PREFIX = 'dq:cache:'
QUALITY = 'dq:quality:'
LOG = logging.getLogger('quality_engine')
CLOCK_REASONS = {'发送时间超前','发送时间滞后','发送时间无效'}
PENDING_REASON = '异常待确认（连续5分钟）'
ALARM_MUTED = '报警已屏蔽'
COLLECTION_UNKNOWN='采集不可用，现场状态未知'
PENDING_REASONS = {COLLECTION_UNKNOWN, '本机积压待判定', PENDING_REASON, DIRECTORY_PENDING, ALARM_MUTED}
PUBLISH_INTERVAL_MS = 300_000


def config():
    return json.loads((SOURCE / 'cache_config.json').read_text(encoding='utf-8-sig'))


def db_connect():
    return pymysql.connect(**config()['database'], charset='utf8mb4', autocommit=False,
                           connect_timeout=8, read_timeout=20, write_timeout=20,
                           cursorclass=pymysql.cursors.DictCursor)


def redis_connect(read_timeout=3):
    return redis.Redis(**dict(config()['redis'],socket_connect_timeout=2,socket_timeout=read_timeout,health_check_interval=15,retry=redis.retry.Retry(redis.backoff.NoBackoff(),0)))


SCHEMA = [
    """CREATE TABLE IF NOT EXISTS dq_point_quality_current (
      eid varchar(128) NOT NULL PRIMARY KEY, source varchar(16) NOT NULL,
      department varchar(255), process varchar(255), equipment varchar(255),
      `group` varchar(255), point varchar(255), mqttname varchar(255),
      channel varchar(255), dpname varchar(255), device_name varchar(255),
      tag_type varchar(64), device_key varchar(600),
      status varchar(32) NOT NULL, reasons_json text NOT NULL,
      receipt_time_ms bigint, source_time_ms bigint, value_change_time_ms bigint,
      value_text varchar(255), quality varchar(128),
      updated_at datetime(3) NOT NULL,
      KEY ix_source_status(source,status), KEY ix_device(source,device_key(190))
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci""",
    """CREATE TABLE IF NOT EXISTS dq_device_quality_current (
      device_key varchar(600) NOT NULL PRIMARY KEY, source varchar(16) NOT NULL,
      device_name varchar(255), department varchar(255), process varchar(255),
      equipment varchar(255), `group` varchar(255), point_count int NOT NULL,
      abnormal_count int NOT NULL, flatline_status varchar(32) NOT NULL,
      updated_at datetime(3) NOT NULL,
      KEY ix_source(source)
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci""",
    """CREATE TABLE IF NOT EXISTS dq_quality_event (
      id bigint NOT NULL AUTO_INCREMENT PRIMARY KEY, eid varchar(128) NOT NULL,
      source varchar(16) NOT NULL, device_key varchar(600), reason varchar(64) NOT NULL,
      opened_at datetime(3) NOT NULL, closed_at datetime(3),
      KEY ix_eid_open(eid,closed_at), KEY ix_source_open(source,closed_at),
      KEY ix_opened(opened_at)
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci""",
    """CREATE TABLE IF NOT EXISTS dq_quality_rule (
      id bigint NOT NULL AUTO_INCREMENT PRIMARY KEY, scope varchar(16) NOT NULL,
      source varchar(16) NOT NULL, target_key varchar(600) NOT NULL DEFAULT '',
      flatline_enabled tinyint(1) NOT NULL, flatline_seconds int NOT NULL DEFAULT 600,
      tolerance double NOT NULL DEFAULT 0, excluded tinyint(1) NOT NULL DEFAULT 0,
      updated_at datetime(3) NOT NULL,
      UNIQUE KEY uq_rule(scope,source,target_key(180))
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci""",
    """CREATE TABLE IF NOT EXISTS dq_model_quality_current (
      eid varchar(128) NOT NULL PRIMARY KEY, model_result varchar(32) NOT NULL,
      status_code varchar(64) NOT NULL, message varchar(255), health_score double,
      detected_at datetime(3) NOT NULL, KEY ix_result(model_result)
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci""",
]


def init_schema():
    from flatline_settings import ensure_schema
    ensure_schema(db_connect)
    with db_connect() as db:
        with db.cursor() as cur:
            for statement in SCHEMA:
                cur.execute(statement)
            cur.execute("SHOW INDEX FROM dq_quality_event WHERE Key_name='ix_open_points'")
            if not cur.fetchone():
                cur.execute('ALTER TABLE dq_quality_event ADD INDEX ix_open_points '
                            '(closed_at,eid,source,opened_at), ALGORITHM=INPLACE, LOCK=NONE')
            cur.execute("SHOW COLUMNS FROM dq_quality_event LIKE 'resolution_note'")
            if not cur.fetchone():
                cur.execute('ALTER TABLE dq_quality_event ADD COLUMN resolution_note varchar(255) NULL')
            cur.execute("SHOW COLUMNS FROM dq_point_quality_current LIKE 'display_group_token'")
            if not cur.fetchone():
                cur.execute("ALTER TABLE dq_point_quality_current ADD COLUMN display_group_token char(64) NOT NULL DEFAULT ''")
            cur.execute("SHOW COLUMNS FROM dq_point_quality_current LIKE 'data_status'")
            if not cur.fetchone():
                cur.execute("ALTER TABLE dq_point_quality_current ADD COLUMN data_status varchar(32) NOT NULL DEFAULT '正常'")
            cur.execute("SHOW COLUMNS FROM dq_point_quality_current LIKE 'data_missing_since'")
            if not cur.fetchone():
                cur.execute('ALTER TABLE dq_point_quality_current ADD COLUMN data_missing_since bigint NULL')
            cur.execute("SHOW INDEX FROM dq_point_quality_current WHERE Key_name='ix_business_token'")
            if not cur.fetchone():
                cur.execute('ALTER TABLE dq_point_quality_current ADD INDEX ix_business_token(display_group_token)')
            cur.execute("SHOW INDEX FROM dq_point_quality_current WHERE Key_name='ix_display_group'")
            if not cur.fetchone():
                cur.execute('ALTER TABLE dq_point_quality_current ADD INDEX ix_display_group(source,display_group_token)')
            cur.execute("SHOW COLUMNS FROM dq_point_quality_current LIKE 'flatline_disabled'")
            if not cur.fetchone():
                cur.execute('ALTER TABLE dq_point_quality_current ADD COLUMN flatline_disabled tinyint NOT NULL DEFAULT 0')
            cur.execute("SHOW COLUMNS FROM dq_point_quality_current LIKE 'quality_check'")
            if not cur.fetchone():
                cur.execute('ALTER TABLE dq_point_quality_current ADD COLUMN quality_check tinyint NOT NULL DEFAULT 0')
            cur.execute("SHOW COLUMNS FROM dq_point_quality_current LIKE 'tag'")
            if not cur.fetchone():
                cur.execute('ALTER TABLE dq_point_quality_current ADD COLUMN tag varchar(255) NULL')
            cur.execute("SHOW COLUMNS FROM dq_point_quality_current LIKE 'owner_code'")
            if not cur.fetchone():
                cur.execute('ALTER TABLE dq_point_quality_current ADD COLUMN owner_code varchar(255) NULL')
            cur.execute("""CREATE TABLE IF NOT EXISTS dq_person_directory (
                person_code varchar(255) PRIMARY KEY,person_name varchar(255) NOT NULL
                ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci""")
            people=json.loads((ROOT/'personnel.json').read_text(encoding='utf-8'))
            cur.executemany('INSERT IGNORE INTO dq_person_directory (person_code,person_name) VALUES (%s,%s)',
                            list(people.items()))
            for source, enabled in [('kepserver', 1), ('oa', 1)]:
                cur.execute("""INSERT IGNORE INTO dq_quality_rule
                    (scope,source,target_key,flatline_enabled,flatline_seconds,tolerance,excluded,updated_at)
                    VALUES ('global',%s,'',%s,600,0,0,NOW(3))""", (source, enabled))
            cur.execute("""CREATE TABLE IF NOT EXISTS dq_equipment_owner (
                ownership_key char(64) PRIMARY KEY, source varchar(16) NOT NULL,
                department varchar(255), process varchar(255), equipment varchar(255),
                `group` varchar(255), owner_name varchar(255), updated_at datetime(3) NOT NULL
                ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci""")
            cur.execute("""CREATE OR REPLACE VIEW dq_point_quality_display AS
                SELECT p.*, COALESCE(u.person_name,NULLIF(p.owner_code,'')) owner_name
                FROM dq_point_quality_current p
                LEFT JOIN dq_person_directory u ON u.person_code=p.owner_code""")
            cur.execute("""CREATE OR REPLACE VIEW dq_device_quality_display AS
                SELECT p.display_group_token device_token,GROUP_CONCAT(DISTINCT p.source ORDER BY p.source SEPARATOR ',') source,
                       MAX(p.device_key) device_key,MAX(p.department) department,MAX(p.process) process,
                       MAX(p.equipment) equipment,MAX(p.`group`) `group`,MAX(p.device_name) device_name,
                       COUNT(DISTINCT p.device_key) collector_count,
                       GROUP_CONCAT(DISTINCT p.owner_name ORDER BY p.owner_name SEPARATOR '、') owner_name,
                       COUNT(*) point_count,SUM(p.status='异常' AND COALESCE(p.quality_check,0)=0) abnormal_count,SUM(p.quality_check=1) alarm_muted_count,
                       SUM(p.data_status IN ('从未收到','停止发送') AND COALESCE(p.quality_check,0)=0) missing_count,
                       SUM(p.data_status='从未收到' AND COALESCE(p.quality_check,0)=0) never_count,SUM(p.data_status='停止发送' AND COALESCE(p.quality_check,0)=0) stopped_count,
                       CASE WHEN SUM(COALESCE(p.quality_check,0)=0)=0 THEN '检测已停用' WHEN SUM(p.data_status IN ('从未收到','停止发送') AND COALESCE(p.quality_check,0)=0)=SUM(COALESCE(p.quality_check,0)=0) THEN '设备掉线'
                            WHEN SUM(p.data_status IN ('从未收到','停止发送') AND COALESCE(p.quality_check,0)=0)>0 THEN '部分点位断线'
                            WHEN SUM(p.data_status='断线待确认' AND COALESCE(p.quality_check,0)=0)>0 THEN '断线待确认'
                            WHEN SUM(p.data_status='待判定' AND COALESCE(p.quality_check,0)=0)>0 THEN '待判定' ELSE '正常' END data_status,
                       MAX(d.flatline_status) flatline_status,MAX(d.updated_at) updated_at
                FROM dq_point_quality_display p
                LEFT JOIN dq_device_quality_current d ON d.device_key=p.display_group_token
                GROUP BY p.display_group_token""")
            cur.execute("""CREATE OR REPLACE VIEW dq_quality_event_display AS
                SELECT e.*,p.department,p.process,p.equipment,p.`group`,p.point,
                       p.mqttname,p.dpname,p.device_name,p.owner_name,p.tag
                FROM dq_quality_event e LEFT JOIN dq_point_quality_display p ON p.eid=e.eid""")
        db.commit()


def load_directory(cur):
    cur.execute("""SELECT eid,department,process,equipment,`group`,point,mqttname,
                        channel,channeloa,dpname,device_name,tag_type,createdBy,tag,qualityCheck,flatlineCheck
                   FROM autosensor WHERE tag=0 AND (channel='kepserver' OR channeloa='oa')""")
    result = {}
    for row in cur.fetchall():
        eid = str(row['eid'] or '').strip().lower()
        if not eid:
            continue
        source = 'kepserver' if (row['channel'] or '').lower() == 'kepserver' else 'oa'
        if source == 'oa' and eid in result and result[eid]['source'] == 'kepserver':
            continue
        row['eid'], row['source'] = eid, source
        row['flatline_disabled']=int(int(row.get('flatlineCheck') or 0)!=0)
        row['quality_check']=int(int(row.get('qualityCheck') or 0)!=0)
        if source == 'kepserver':
            # A missing name must not merge unrelated devices.
            row['device_key'] = 'kepserver:' + (str(row['device_name']).strip() if row['device_name'] else 'eid:' + eid)
        else:
            parts = [str(row[x] or '').strip() for x in ('department','process','equipment','group')]
            identity = json.dumps(parts, ensure_ascii=False, separators=(',', ':'))
            row['device_key'] = 'oa:' + hashlib.sha256(identity.encode()).hexdigest() if any(parts) else 'oa:eid:' + eid
        if source == 'kepserver' or eid not in result:
            result[eid] = row
    return result


def load_rules(cur):
    cur.execute('SELECT * FROM dq_quality_rule')
    return {(x['scope'], x['source'], x['target_key']): x for x in cur.fetchall()}


def effective_rule(row, rules):
    source = row['source']
    rule = {'flatline_enabled': source in ('kepserver','oa'), 'flatline_seconds': 600,
            'tolerance': 0, 'excluded': 0}
    for key in [('global',source,''), ('device',source,row['device_key']), ('device',source,'business:'+display_group_token(row)), ('point',source,row['eid'])]:
        if key in rules:
            rule.update({name: rules[key][name] for name in rule})
    rule['flatline_seconds'] = max(60, min(86400, int(rule['flatline_seconds'])))
    return rule


def is_numeric(row):
    return str(row.get('tag_type') or '').strip().lower() in {
        'float', 'double', 'short', 'word', 'long', 'dword', 'int', 'integer', 'real'}


def quality_bad(value):
    return str(value).strip().lower() in {'false', '0', 'bad', 'invalid', 'error', 'no'}


def clock_state(receipt, now_ms):
    if receipt is None:
        return None
    sent = receipt.get('source_time')
    if sent is None:
        return '发送时间无效'
    # Compare at network receipt, not when a queued packet is eventually parsed.
    delta = int(sent) - int(receipt['received_time'])
    if delta > 10000:
        return '发送时间超前'
    if delta < -10000:
        return '发送时间滞后'
    return None


def flat_state(row, receipt, latest, state, rule, now_ms, gap_at):
    if int(row.get('flatline_disabled') or 0)!=0 or int(row.get('quality_check') or 0)!=0 or not rule['flatline_enabled'] or rule['excluded'] or not is_numeric(row):
        return 'excluded'
    if receipt is None or latest is None:
        return 'unknown'
    received = int(receipt['received_time'])
    if now_ms - received > 10000 or gap_at and now_ms - gap_at < rule['flatline_seconds'] * 1000:
        return 'unknown'
    if state['continuous_since'] is None or now_ms - state['continuous_since'] < rule['flatline_seconds'] * 1000:
        return 'unknown'
    changed = latest.get('last_change_time')
    if changed is None:
        return 'unknown'
    return 'flat' if int(changed) <= now_ms - rule['flatline_seconds'] * 1000 else 'moving'


def _decode(value):
    if value is None:
        return None
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return None


class QualityEngine:
    def __init__(self):
        self.cache = redis_connect(read_timeout=6)
        self.roster = {}
        self.rules = {}
        self.previous = {}
        self.offline_reset_ms=int(self.cache.get(QUALITY+'offline_reset_at') or 0)
        self.availability=AvailabilityTracker(self.offline_reset_ms)
        self.event_active = defaultdict(set)
        self.confirmation = ConfirmedObservations(self.event_active,recovery_ms=30000)
        self.next_publish_ms = 0
        self.last_summary = None
        self.startup_published=False
        self.continuity = {}
        self.started_ms = int(time.time() * 1000)
        self.collection_health=CollectionHealth(db_connect,self.started_ms)
        self.loaded_at = 0
        self.rule_loaded_at = 0
        self.directory_revision = None

    def load(self, revision=None):
        old_roster=self.roster
        initial=not self.loaded_at
        with db_connect() as db:
            with db.cursor() as cur:
                roster=load_directory(cur)
                rules=load_rules(cur)
                reset=reconcile(cur,roster,datetime.now())
                cur.execute('SELECT eid,reasons_json,data_status,data_missing_since FROM dq_point_quality_current')
                stored=cur.fetchall()
                previous={x['eid']:set(_decode(x['reasons_json']) or []) for x in stored}
                self.availability.known={x['eid']:next((reason for reason in ('停止发送','从未收到') if reason in previous.get(x['eid'],set())),x.get('data_status','正常')) for x in stored}
                self.availability.since={x['eid']:x['data_missing_since'] for x in stored if x.get('data_missing_since') is not None}
                if initial:
                    cur.execute('SELECT eid,reason,opened_at FROM dq_quality_event WHERE closed_at IS NULL')
                    for event in cur.fetchall():
                        self.event_active[event['eid']].add(event['reason'])
                        if event['reason'] in ('停止发送','从未收到') and event.get('opened_at') and event['eid'] not in self.availability.since:
                            self.availability.since[event['eid']]=int(event['opened_at'].timestamp()*1000)
            db.commit()
        for eid,row in roster.items():
            if row.get('flatline_disabled'):
                reason='设备恒值异常'
                self.event_active.get(eid,set()).discard(reason)
                self.confirmation.window.pending.get(eid,{}).pop(reason,None)
                self.confirmation.recovering.pop((eid,reason),None)
        self.confirmation.events[:]=[e for e in self.confirmation.events if not (roster.get(e['eid'],{}).get('flatline_disabled') and e['reason']=='设备恒值异常')]
        self.confirmation.open_buffer={k:v for k,v in self.confirmation.open_buffer.items() if not (roster.get(k[0],{}).get('flatline_disabled') and k[1]=='设备恒值异常')}
        removed=set(old_roster)-set(roster)
        invalid=reset|removed
        for eid in invalid:
            self.availability.known.pop(eid,None)
            self.availability.since.pop(eid,None)
            self.availability.window.pending.pop(eid,None)
            self.event_active.pop(eid,None)
            self.confirmation.window.pending.pop(eid,None)
            self.continuity.pop(eid,None)
        self.confirmation.events[:]=[e for e in self.confirmation.events if e['eid'] not in invalid]
        self.confirmation.closes[:]=[e for e in self.confirmation.closes if e[0] not in invalid]
        self.confirmation.open_buffer={k:v for k,v in self.confirmation.open_buffer.items() if k[0] not in invalid}
        if initial or reset or set(old_roster)!=set(roster):
            self.next_publish_ms=0
        self.roster=roster;self.rules=rules;self.previous=previous
        self.directory_revision=revision
        self.loaded_at=time.time();self.rule_loaded_at=self.loaded_at
        if self.last_summary is not None and self.next_publish_ms:
            write_publication(self.last_summary,kind='directory')
        LOG.info('directory_loaded count=%s identity_reset=%s removed=%s',len(roster),len(reset),len(removed))

    def refresh_rules(self):
        with db_connect() as db:
            with db.cursor() as cur:
                self.rules=load_rules(cur)
        self.rule_loaded_at=time.time()

    def run_once(self):
        revision=self.cache.get(PREFIX+'catalog_revision')
        if time.time()-self.loaded_at>=600 or revision!=self.directory_revision:
            self.load(revision)
        elif time.time()-self.rule_loaded_at>=60:
            self.refresh_rules()
        now_ms = int(time.time() * 1000)
        rows = list(self.roster.values())
        mqtt = {k: _decode(v) for k,v in self.cache.hgetall(PREFIX+'mqtt_runtime').items()}
        gap_at = int(mqtt.get('last_gap_at') or 0)
        backlog_unknown = int(mqtt.get('oldest_pending_ms') or 0) > 10000
        from observation_reader import read_observation
        def include_latest(row):
            rule=effective_rule(row,self.rules)
            return (not row.get('flatline_disabled') and rule['flatline_enabled']
                    and is_numeric(row) and (not backlog_unknown or row['source']=='oa')
                    and not (row['source']=='kepserver' and gap_at and
                             now_ms-gap_at<rule['flatline_seconds']*1000))
        receipts,latest=read_observation(self.cache,rows,include_latest,PREFIX,batch_size=500)
        # Receipt ages must use the time after the batch reads, not a stale scan-start clock.
        now_ms=int(time.time()*1000)
        health=self.collection_health.observe(now_ms,rows,receipts,mqtt,receiver_ready())
        if not self.startup_published and all(not item['blocked'] for item in health['items'].values()):
            self.next_publish_ms=0
            self.startup_published=True
        LOG.info('scan_loaded_cache points=%s',len(rows))
        states, groups = {}, defaultdict(list)
        for row in rows:
            eid = row['eid']
            receipt = receipts[eid]
            current = latest[eid]
            if receipt and receipt.get('source',row['source'])!=row['source']:
                receipt=None
            if current and current.get('source',row['source'])!=row['source']:
                current=None
            track = self.continuity.setdefault(eid, {'last_received': None, 'continuous_since': None})
            received = int(receipt['received_time']) if receipt else None
            if received and received != track['last_received']:
                if track['last_received'] is None or received - track['last_received'] > 20000:
                    track['continuous_since'] = received
                track['last_received'] = received
            if received and now_ms - received > 20000:
                track['continuous_since'] = None
            rule = effective_rule(row, self.rules)
            flat = flat_state(row, receipt, current, track, rule, now_ms,
                              gap_at if row['source']=='kepserver' else 0)
            base = set()
            if receipt is None and now_ms - self.started_ms > 10000:
                base.add('本机积压待判定' if row['source']=='kepserver' and backlog_unknown else '从未收到')
            elif receipt and now_ms - received > 10000:
                base.add('本机积压待判定' if row['source']=='kepserver' and backlog_unknown else '停止发送')
            if receipt:
                clock = clock_state(receipt, now_ms)
                if clock:
                    base.add(clock)
                if row['source']=='kepserver' and quality_bad(receipt.get('quality','')):
                    base.add('质量位BAD')
            states[eid] = {'reasons': base, 'flat': flat, 'receipt': receipt, 'latest': current,
                           'rule': rule, 'row': row}
            states[eid]['data_status']=self.availability.observe(eid,receipt,now_ms,self.started_ms,unknown=row['source']=='kepserver' and backlog_unknown)
            states[eid]['data_missing_since']=self.availability.since.get(eid)
            groups[display_group_token(row)].append(eid)
        devices = {}
        for key, eids in groups.items():
            participating = [states[eid] for eid in eids if states[eid]['flat'] != 'excluded']
            flat_count = sum(x['flat']=='flat' for x in participating)
            moving_count = sum(x['flat']=='moving' for x in participating)
            all_flat = bool(participating) and flat_count == len(participating)
            device_status = '设备恒值异常' if all_flat else '无法判断' if any(x['flat']=='unknown' for x in participating) else '正常'
            if all_flat:
                for eid in eids:
                    if states[eid]['flat']=='flat':
                        states[eid]['reasons'].add('设备恒值异常')
            if any(health['items'][states[eid]['row']['source']]['blocked'] for eid in eids):device_status='采集未知'
            devices[key] = (eids, device_status)
        for eid, state in states.items():
            if state['row'].get('quality_check')==1:
                self.confirmation.observe(eid,set(),now_ms,force=True)
                state['reasons']={ALARM_MUTED}
                continue
            source_health=health['items'][state['row']['source']]
            if source_health['blocked']:
                kept=self.confirmation.observe(eid,set(),now_ms,suspended=True)
                state['reasons']=kept | {COLLECTION_UNKNOWN}
                state['data_status']=COLLECTION_UNKNOWN
                self.availability.window.pending.pop(eid,None)
                self.continuity.pop(eid,None)
                continue
            raw = state['reasons']
            candidates = raw - PENDING_REASONS
            receipt = state['receipt'] or {}
            received = receipt.get('received_time')
            starts = {'停止发送': max(int(received),self.offline_reset_ms,self.collection_health.gates[state['row']['source']].resume_ms)} if received is not None else {}
            ends = {'停止发送': min(now_ms,int(received))} if received is not None and now_ms-int(received)<=10000 else {}
            if '本机积压待判定' in raw:
                # Unknown collector state does not prove an existing outage has recovered.
                candidates |= self.event_active.get(eid,set()) & {'停止发送','从未收到'}
            confirmed = self.confirmation.observe(eid, candidates, now_ms, starts=starts, ends=ends)
            if state['data_status']=='正常' and confirmed & {'停止发送','从未收到'}:state['data_status']='恢复观察（连续正常30秒）'
            state['reasons'] = confirmed | (raw & {'本机积压待判定'})
            if candidates - confirmed:
                state['reasons'].add(PENDING_REASON)
        # Observe every 10 seconds, publish official results only every 5 minutes.
        # Keep confirmed episodes in the event buffer even if they recover before publication.
        if self.last_summary is not None and now_ms < self.next_publish_ms:
            heartbeat = dict(self.last_summary, run_at=now_ms)
            self.cache.set(QUALITY+'summary', json.dumps(heartbeat, ensure_ascii=False))
            LOG.info('observation_done points=%s next_publish_ms=%s', len(rows), self.next_publish_ms)
            return heartbeat
        changes = []
        opens = list(self.confirmation.events)
        closes = list(self.confirmation.closes)
        summary = {'expected': Counter(), 'received': Counter(), 'reasons': defaultdict(Counter),
                   'abnormal': Counter(), 'run_at': now_ms, 'engine': 'running'}
        for eid, state in states.items():
            row = state['row']; source = row['source']; reasons = state['reasons']
            summary['expected'][source] += 1
            if state['receipt']:
                summary['received'][source] += 1
            real_reasons = reasons - PENDING_REASONS
            if real_reasons:
                summary['abnormal'][source] += 1
            for reason in reasons:
                summary['reasons'][source][reason] += 1
            old = self.previous.get(eid)
            if old is None or old != reasons or self.availability.known.get(eid)!=state['data_status']:
                changes.append(state)
        with db_connect() as db:
            with db.cursor() as cur:
                new_changes=[item for item in changes if item['row']['eid'] not in self.previous]
                existing_changes=[item for item in changes if item['row']['eid'] in self.previous]
                LOG.info('scan_changes new=%s existing=%s open=%s close=%s',
                         len(new_changes),len(existing_changes),len(opens),len(closes))
                for offset in range(0,len(new_changes),500):
                    values=[]
                    for item in new_changes[offset:offset+500]:
                        x=item['row']; r=item['receipt'] or {}; v=item['latest'] or {}
                        values.append((x['eid'],x['source'],x['department'],x['process'],x['equipment'],
                                       x['group'],x['point'],x['mqttname'],x['channel'],x['dpname'],
                                       x['device_name'],x['tag_type'],x['device_key'],
                                       ALARM_MUTED if ALARM_MUTED in item['reasons'] else '异常' if item['reasons']-PENDING_REASONS else '待判定' if item['reasons'] else '正常',
                                       json.dumps(sorted(item['reasons']),ensure_ascii=False),
                                       r.get('received_time'),r.get('source_time'),v.get('last_change_time'),
                                       str(v.get('value'))[:255] if v else None,r.get('quality',''),
                                       str(x.get('createdBy') or '').strip().lower(),x.get('tag'),datetime.fromtimestamp(now_ms/1000),item['data_status'],item['data_missing_since']))
                    cur.executemany("""INSERT INTO dq_point_quality_current
                      (eid,source,department,process,equipment,`group`,point,mqttname,channel,dpname,
                       device_name,tag_type,device_key,status,reasons_json,receipt_time_ms,
                       source_time_ms,value_change_time_ms,value_text,quality,owner_code,tag,updated_at,data_status,data_missing_since)
                      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                      ON DUPLICATE KEY UPDATE status=VALUES(status),reasons_json=VALUES(reasons_json),
                        receipt_time_ms=VALUES(receipt_time_ms),source_time_ms=VALUES(source_time_ms),
                        value_change_time_ms=COALESCE(VALUES(value_change_time_ms),value_change_time_ms),
                        value_text=COALESCE(VALUES(value_text),value_text),
                        quality=VALUES(quality),data_status=VALUES(data_status),data_missing_since=VALUES(data_missing_since),updated_at=NOW(3)""",values)
                for offset in range(0,len(existing_changes),500):
                    values=[]
                    for item in existing_changes[offset:offset+500]:
                        x=item['row']; r=item['receipt'] or {}; v=item['latest'] or {}
                        values.append((x['eid'],x['source'],ALARM_MUTED if ALARM_MUTED in item['reasons'] else '异常' if item['reasons']-PENDING_REASONS else '待判定' if item['reasons'] else '正常',
                                       json.dumps(sorted(item['reasons']),ensure_ascii=False),
                                       r.get('received_time'),r.get('source_time'),v.get('last_change_time'),
                                       str(v.get('value'))[:255] if v else None,r.get('quality',''),datetime.fromtimestamp(now_ms/1000),item['data_status'],item['data_missing_since']))
                    cur.executemany("""INSERT INTO dq_point_quality_current
                        (eid,source,status,reasons_json,receipt_time_ms,source_time_ms,
                         value_change_time_ms,value_text,quality,updated_at,data_status,data_missing_since)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE status=VALUES(status),reasons_json=VALUES(reasons_json),
                        receipt_time_ms=VALUES(receipt_time_ms),source_time_ms=VALUES(source_time_ms),
                        value_change_time_ms=COALESCE(VALUES(value_change_time_ms),value_change_time_ms),
                        value_text=COALESCE(VALUES(value_text),value_text),
                        quality=VALUES(quality),data_status=VALUES(data_status),data_missing_since=VALUES(data_missing_since),updated_at=NOW(3)""",values)
                LOG.info('scan_current_written count=%s',len(changes))
                # Close previously committed events before inserting any new episodes.
                if closes:
                    cur.executemany("UPDATE dq_quality_event SET closed_at=%s "
                                    "WHERE eid=%s AND reason=%s AND closed_at IS NULL",
                                    [(datetime.fromtimestamp(stamp/1000),eid,reason)
                                     for eid,reason,stamp in closes])
                if opens:
                    cur.executemany("""INSERT INTO dq_quality_event
                         (eid,source,device_key,reason,opened_at,closed_at)
                         VALUES (%s,%s,%s,%s,%s,%s)""",
                        [(event['eid'],states[event['eid']]['row']['source'],
                          states[event['eid']]['row']['device_key'],event['reason'],
                          datetime.fromtimestamp(event['opened_ms']/1000),
                          datetime.fromtimestamp(event['closed_ms']/1000) if event['closed_ms'] else None)
                         for event in opens])
                LOG.info('scan_events_written open=%s close=%s',len(opens),len(closes))
                device_values=[]
                for key,(eids,flat_status) in devices.items():
                    r=states[eids[0]]['row']
                    if flat_status == '设备恒值异常' and not all('设备恒值异常' in states[eid]['reasons'] for eid in eids if states[eid]['flat'] != 'excluded' and states[eid]['row'].get('quality_check')!=1):
                        flat_status = '恒值待确认'
                    if all(states[eid]['row'].get('quality_check')==1 for eid in eids):
                        flat_status=ALARM_MUTED
                    device_values.append((key,r['source'],r['device_name'],r['department'],r['process'],
                                          r['equipment'],r['group'],len(eids),
                                          sum(bool(states[eid]['reasons']-PENDING_REASONS) for eid in eids),flat_status,datetime.fromtimestamp(now_ms/1000)))
                for offset in range(0,len(device_values),500):
                    cur.executemany("""INSERT INTO dq_device_quality_current
                      (device_key,source,device_name,department,process,equipment,`group`,point_count,
                       abnormal_count,flatline_status,updated_at)
                      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                      ON DUPLICATE KEY UPDATE abnormal_count=VALUES(abnormal_count),
                       flatline_status=VALUES(flatline_status),point_count=VALUES(point_count),updated_at=NOW(3)""",
                        device_values[offset:offset+500])
                record_devices(cur,devices,states,health,PENDING_REASONS,now_ms)
            db.commit()
        self.previous = {eid: item['reasons'] for eid,item in states.items()}
        self.availability.known={eid:item['data_status'] for eid,item in states.items()}
        published_ms=int(time.time()*1000)
        self.confirmation.committed()
        self.next_publish_ms = published_ms + PUBLISH_INTERVAL_MS
        summary = {'expected':dict(summary['expected']),'received':dict(summary['received']),
                   'abnormal':dict(summary['abnormal']),
                   'reasons':{s:dict(v) for s,v in summary['reasons'].items()},
                   'run_at':now_ms,'engine':'running','changed':len(changes),
                   'devices':len(devices),'mqtt_gap_at':gap_at,
                   'published_at':published_ms,'publish_interval_seconds':300,
                   'confirmation_seconds':300,'observation_interval_seconds':10}
        self.last_summary = summary
        write_publication(summary)
        self.cache.set(QUALITY+'summary',json.dumps(summary,ensure_ascii=False))
        LOG.info('scan_done expected=%s changed=%s events_open=%s events_closed=%s',
                 len(rows),len(changes),len(opens),len(closes))
        return summary


def main():
    (ROOT/'logs').mkdir(exist_ok=True)
    logging.basicConfig(level=logging.INFO, handlers=[logging.FileHandler(ROOT/'logs'/'engine.log',encoding='utf-8'),logging.StreamHandler()],
                        format='%(asctime)s %(levelname)s %(message)s')
    init_schema()
    engine = QualityEngine()
    while True:
        started=time.monotonic()
        try:
            engine.run_once()
        except Exception:
            LOG.exception('scan_failed')
        time.sleep(max(0,10-(time.monotonic()-started)))


if __name__=='__main__':
    import sys as _diag_sys
    from pathlib import Path as _DiagPath
    _diag_sys.path.insert(0,str(_DiagPath(__file__).resolve().parent.parent/'运行诊断'))
    from runtime_logging import install as _install_diagnostics
    _install_diagnostics('quality_engine')

    main()
