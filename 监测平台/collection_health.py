"""Source health gates and compact incident storage, independent of point alarm muting."""
import json,time,uuid,urllib.request
from datetime import datetime

SCHEMA=["""CREATE TABLE IF NOT EXISTS dq_collection_health (
 name varchar(32) PRIMARY KEY, observed_ms bigint NOT NULL,payload_json mediumtext NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci""",
"""CREATE TABLE IF NOT EXISTS dq_collection_event (
 id char(32) PRIMARY KEY,source varchar(20) NOT NULL,reason varchar(128) NOT NULL,
 opened_at datetime(3) NOT NULL,closed_at datetime(3),affected_count int NOT NULL DEFAULT 0,
 active_key varchar(32) UNIQUE,KEY ix_health_time(opened_at),KEY ix_health_open(source,closed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci""",
"""CREATE TABLE IF NOT EXISTS dq_collection_event_point (
 event_id char(32) NOT NULL,eid varchar(128) NOT NULL,PRIMARY KEY(event_id,eid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"""]

def stamp(ms):return datetime.fromtimestamp(ms/1000)

def receiver_ready():
    try:
        with urllib.request.urlopen('http://127.0.0.1:8788/health',timeout=1) as response:
            return json.load(response).get('status')=='ready'
    except Exception:return False

class SourceGate:
    def __init__(self,started):
        self.started=started;self.failed_since=None;self.good_since=None
        self.resume_ms=started;self.last_observed=None;self.healthy=False
    def observe(self,now,bad,reason,last_received=None):
        gap=self.last_observed is not None and not 0<=now-self.last_observed<=40000
        self.last_observed=now
        if gap:self.failed_since=None;self.good_since=None;self.resume_ms=now;self.healthy=False
        if now-self.started<60000:
            return dict(state='启动观察',blocked=True,reason='启动后观察60秒',since=self.started)
        if bad:
            self.healthy=False
            self.good_since=None
            if self.failed_since is None:self.failed_since=now
            return dict(state='未恢复' if now-self.failed_since>=300000 else '待确认',blocked=True,
                        reason=reason,since=self.failed_since)
        if self.good_since is None:self.good_since=now
        if now-self.good_since<30000:
            return dict(state='恢复观察',blocked=True,reason='连续正常30秒后恢复判定',since=self.failed_since or self.good_since)
        if not self.healthy:self.resume_ms=now
        self.healthy=True
        self.failed_since=None
        return dict(state='正常',blocked=False,reason='',since=None)

class CollectionHealth:
    def __init__(self,connect,started):
        self.connect=connect;self.started=started
        self.gates={s:SourceGate(started) for s in ('oa','kepserver')}
        self.last_persisted=None
        with connect() as db:
            with db.cursor() as c:
                from device_incidents import SCHEMA as DEVICE_SCHEMA
                for sql in SCHEMA+[DEVICE_SCHEMA]:c.execute(sql)
                c.execute("SELECT observed_ms FROM dq_collection_health WHERE name='current'")
                old=c.fetchone()
                if old:self.last_persisted=int(old['observed_ms'])
            db.commit()

    def observe(self,now,rows,receipts,mqtt,ready):
        latest={s:0 for s in self.gates};counts={s:0 for s in self.gates}
        for row in rows:
            s=row['source'];counts[s]+=1;r=receipts.get(row['eid']) or {}
            if r.get('source',s)==s:latest[s]=max(latest[s],int(r.get('received_time') or 0))
        items={}
        for s,gate in self.gates.items():
            reason='OA 数据流中断' if s=='oa' else 'MQTT 数据流中断'
            bad=not latest[s] or now-latest[s]>10000
            if not ready:bad=True;reason='本机接收服务不可用'
            elif s=='kepserver' and not mqtt.get('connected'):bad=True;reason='MQTT 连接中断'
            elif s=='kepserver' and int(mqtt.get('oldest_pending_ms') or 0)>10000:bad=True;reason='本机采集积压'
            item=gate.observe(now,bad,reason,latest[s])
            if not counts[s]:item.update(state='无应收点位',blocked=False,reason='')
            item.update(source=s,affected_count=counts[s],last_received_ms=latest[s] or None)
            items[s]=item
        payload=dict(observed_ms=now,receiver_ready=ready,items=items,
                     startup_seconds=60,recovery_seconds=30,confirmation_seconds=300)
        with self.connect() as db:
            with db.cursor() as c:
                if self.last_persisted and now-self.last_persisted>40000:
                    identifier=uuid.uuid4().hex
                    c.execute('INSERT INTO dq_collection_event(id,source,reason,opened_at,closed_at,affected_count) VALUES(%s,%s,%s,%s,%s,%s)',
                              (identifier,'platform','平台监测中断（现场状态未知）',stamp(self.last_persisted),stamp(now),len(rows)))
                    c.executemany('INSERT IGNORE INTO dq_collection_event_point(event_id,eid) VALUES(%s,%s)',[(identifier,r['eid']) for r in rows])
                for s,item in items.items():
                    c.execute('SELECT id FROM dq_collection_event WHERE active_key=%s',(s,));active=c.fetchone()
                    if item['state']=='未恢复' and not active:
                        identifier=uuid.uuid4().hex
                        c.execute('INSERT INTO dq_collection_event(id,source,reason,opened_at,affected_count,active_key) VALUES(%s,%s,%s,%s,%s,%s)',
                                  (identifier,s,item['reason'],stamp(item['since']),counts[s],s))
                        c.executemany('INSERT INTO dq_collection_event_point(event_id,eid) VALUES(%s,%s)',[(identifier,r['eid']) for r in rows if r['source']==s])
                    elif not item['blocked'] and active:
                        ended=self.gates[s].good_since or now
                        c.execute('UPDATE dq_collection_event SET closed_at=%s,active_key=NULL WHERE id=%s',(stamp(ended),active['id']))
                c.execute("INSERT INTO dq_collection_health(name,observed_ms,payload_json) VALUES('current',%s,%s) ON DUPLICATE KEY UPDATE observed_ms=VALUES(observed_ms),payload_json=VALUES(payload_json)",
                          (now,json.dumps(payload,ensure_ascii=False)))
            db.commit()
        self.last_persisted=now
        return payload
