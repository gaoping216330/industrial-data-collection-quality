"""Bounded durable MQTT inbox. A job is deleted only after successful processing."""
import sqlite3
import threading
import time
from pathlib import Path


class DiskInbox:
    def __init__(self, path, max_bytes=4*1024**3, max_age_seconds=0):
        Path(path).parent.mkdir(parents=True,exist_ok=True)
        self.db=sqlite3.connect(path,check_same_thread=False,timeout=5)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('CREATE TABLE IF NOT EXISTS inbox(id INTEGER PRIMARY KEY,topic TEXT,payload BLOB,received_ms INTEGER,size INTEGER)')
        self.db.execute('CREATE TABLE IF NOT EXISTS rejected(id INTEGER PRIMARY KEY,topic TEXT,payload BLOB,received_ms INTEGER,size INTEGER,reason TEXT)')
        self.db.commit()
        columns={r[1] for r in self.db.execute('PRAGMA table_info(inbox)')}
        if 'quarantined_at' not in columns:self.db.execute('ALTER TABLE inbox ADD COLUMN quarantined_at INTEGER NOT NULL DEFAULT 0')
        self.db.execute('CREATE INDEX IF NOT EXISTS ix_quarantine_time ON inbox(quarantined_at)')
        self.db.execute('CREATE INDEX IF NOT EXISTS ix_live_time ON inbox(received_ms) WHERE quarantined_at=0')
        self.db.commit()
        self.max_age_seconds=max(0,float(max_age_seconds))
        self.limit=int(max_bytes)
        self.condition=threading.Condition()
        self.active_topics=set()
        self.leases={}
        self.bytes,self.count=self.db.execute('SELECT COALESCE(SUM(length(payload)),0),COUNT(*) FROM inbox WHERE quarantined_at=0').fetchone()
        self.quarantine_count,self.quarantine_bytes=self.db.execute('SELECT COUNT(*),COALESCE(SUM(length(payload)),0) FROM inbox WHERE quarantined_at>0').fetchone()
        self.quarantine_expired()
        self.bytes+=self.db.execute('SELECT COALESCE(SUM(length(payload)),0) FROM rejected').fetchone()[0]

    def quarantine_expired(self):
        if not self.max_age_seconds:return 0
        with self.condition:
            now=int(time.time()*1000);cutoff=now-int(self.max_age_seconds*1000)
            leased=list(self.leases.values())
            where='quarantined_at=0 AND received_ms<?'
            args=[cutoff]
            if leased:where+=' AND id NOT IN ('+','.join('?' for _ in leased)+')';args+=leased
            count,size=self.db.execute('SELECT COUNT(*),COALESCE(SUM(length(payload)),0) FROM inbox WHERE '+where,args).fetchone()
            if count:
                self.db.execute('UPDATE inbox SET quarantined_at=? WHERE '+where,[now]+args)
                self.db.commit();self.count-=count;self.bytes-=size
                self.quarantine_count+=count;self.quarantine_bytes+=size
            # Archive is retained separately from the live queue; no automatic replay.
            # At most 24 hours and 8GiB of archived payloads are retained.
            old=list(self.db.execute('SELECT id,length(payload) FROM inbox WHERE quarantined_at>0 AND quarantined_at<?',(now-86400000,)))
            if self.quarantine_bytes-sum(r[1] for r in old)>8*1024**3:
                excluded={r[0] for r in old};remaining=self.quarantine_bytes-sum(r[1] for r in old)
                for row in self.db.execute('SELECT id,length(payload) FROM inbox WHERE quarantined_at>0 ORDER BY quarantined_at,id'):
                    if remaining<=8*1024**3:break
                    if row[0] not in excluded:old.append(row);remaining-=row[1]
            if old:
                self.db.executemany('DELETE FROM inbox WHERE id=?',[(r[0],) for r in old]);self.db.commit()
                self.quarantine_count-=len(old);self.quarantine_bytes-=sum(r[1] for r in old)
            if count:
                import logging
                logging.getLogger('data_cache').warning('expired_packets_isolated count=%s bytes=%s cutoff_ms=%s',count,size,cutoff)
            return count

    def push(self,packet):
        with self.condition:
            if self.bytes+len(packet.payload)>self.limit:return [packet]
            self.db.execute('INSERT INTO inbox(topic,payload,received_ms,size) VALUES(?,?,?,?)',(packet.topic,packet.payload,packet.received_ms,len(packet.payload)))
            self.db.commit()
            self.bytes+=len(packet.payload);self.count+=1
            self.condition.notify_all()
            return []

    def pop(self,timeout=.5,shard=0,shards=1):
        from mqtt_collector import Packet
        with self.condition:
            for attempt in range(2):
                excluded=list(self.active_topics)
                where=' WHERE quarantined_at=0'+(' AND topic NOT IN ('+','.join('?' for _ in excluded)+')' if excluded else '')
                row=self.db.execute('SELECT id,topic,payload,received_ms FROM inbox'+where+' ORDER BY id LIMIT 1',excluded).fetchone()
                if row:
                    job,topic,payload,received=row
                    packet=Packet(topic,payload,received,time.monotonic()-max(0,(time.time()*1000-received)/1000))
                    self.active_topics.add(topic);self.leases[id(packet)]=job
                    return packet
                if not attempt:self.condition.wait(timeout)
            return None

    def acknowledge(self,packet):
        with self.condition:
            job=self.leases.pop(id(packet),None)
            if job is not None:
                self.db.execute('DELETE FROM inbox WHERE id=?',(job,));self.db.commit()
                self.bytes-=len(packet.payload);self.count-=1

    def reject(self,packet,reason):
        with self.condition:
            job=self.leases.get(id(packet))
            self.db.execute('INSERT INTO rejected(topic,payload,received_ms,size,reason) VALUES(?,?,?,?,?)',(packet.topic,packet.payload,packet.received_ms,len(packet.payload),str(reason)[:500]))
            self.db.execute('DELETE FROM inbox WHERE id=?',(job,));self.db.commit()
            self.leases.pop(id(packet),None);self.count-=1
            # Rejected payloads still consume the disk budget.

    def release(self,packet):
        with self.condition:
            self.leases.pop(id(packet),None)
            self.active_topics.discard(packet.topic)
            self.condition.notify_all()

    def clear(self):
        # Keep unprocessed packets across disconnects, circuit breaks and restarts.
        return []

    def snapshot(self):
        with self.condition:
            oldest=self.db.execute('SELECT received_ms FROM inbox WHERE quarantined_at=0 ORDER BY id LIMIT 1').fetchone()
            return dict(pending_packets=self.count,pending_bytes=self.bytes,
                        oldest_pending_ms=max(0,time.time()*1000-oldest[0]) if oldest else 0,
                        buffer_backend='disk',disk_buffer_limit_bytes=self.limit,quarantined_packets=self.quarantine_count,quarantined_bytes=self.quarantine_bytes)
