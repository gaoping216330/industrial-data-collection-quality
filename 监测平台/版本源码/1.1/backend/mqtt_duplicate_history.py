"""One durable row per MQTT address-pair episode, updated rather than appended per packet."""
import json
from datetime import datetime
SCHEMA="""CREATE TABLE IF NOT EXISTS dq_mqtt_duplicate_event (
 id char(32) PRIMARY KEY,event_key char(64) NOT NULL,active_key char(64) UNIQUE,
 eid varchar(128) NOT NULL,address_a varchar(512),address_b varchar(512),topic_a varchar(255),topic_b varchar(255),
 started_at datetime(3) NOT NULL,last_seen_at datetime(3) NOT NULL,ended_at datetime(3),recovery_confirmed_at datetime(3),
 source_time_ms bigint,duplicate_count bigint NOT NULL,conflict_count bigint NOT NULL,
 types_json text,evidence_json mediumtext,event_json mediumtext NOT NULL,
 KEY ix_duplicate_eid(eid,started_at),KEY ix_duplicate_time(started_at),KEY ix_duplicate_open(ended_at,last_seen_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"""
def stamp(ms):return datetime.fromtimestamp(ms/1000) if ms is not None else None
class DuplicateHistory:
 def __init__(self,connect):
  self.connect=connect
  with connect() as db:
   with db.cursor() as c:c.execute(SCHEMA)
   db.commit()
 def load_open(self):
  with self.connect() as db:
   with db.cursor() as c:
    c.execute('SELECT event_json FROM dq_mqtt_duplicate_event WHERE ended_at IS NULL')
    return [json.loads(r['event_json']) for r in c.fetchall()]
 def save(self,rows):
  values=[]
  for r in sorted(rows,key=lambda x:(0 if x.get('ended_at') else 1,x['first_received_at'])):
   values.append((r['episode_id'],r['event_key'],None if r.get('ended_at') else r['event_key'],r['eid'],r.get('address_a'),r.get('address_b'),r.get('topic_a'),r.get('topic_b'),stamp(r['first_received_at']),stamp(r['last_received_at']),stamp(r.get('ended_at')),stamp(r.get('confirmed_at')),r.get('source_time'),r['duplicate_count'],r['conflict_count'],json.dumps(r['types'],ensure_ascii=False),json.dumps(r.get('evidence',{}),ensure_ascii=False),json.dumps(r,ensure_ascii=False)))
  with self.connect() as db:
   with db.cursor() as c:
    for offset in range(0,len(values),100):c.executemany('INSERT INTO dq_mqtt_duplicate_event(id,event_key,active_key,eid,address_a,address_b,topic_a,topic_b,started_at,last_seen_at,ended_at,recovery_confirmed_at,source_time_ms,duplicate_count,conflict_count,types_json,evidence_json,event_json) VALUES('+','.join(['%s']*18)+') ON DUPLICATE KEY UPDATE active_key=VALUES(active_key),last_seen_at=VALUES(last_seen_at),ended_at=VALUES(ended_at),recovery_confirmed_at=VALUES(recovery_confirmed_at),source_time_ms=VALUES(source_time_ms),duplicate_count=VALUES(duplicate_count),conflict_count=VALUES(conflict_count),types_json=VALUES(types_json),evidence_json=VALUES(evidence_json),event_json=VALUES(event_json)',values[offset:offset+100])
   db.commit()
