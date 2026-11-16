"""Explicit offline-only reset with a verified prior backup and bounded deletes."""
import argparse
import gzip
import json
import re
from datetime import datetime
from pathlib import Path
import pymysql
from quality_engine import config,redis_connect,QUALITY

MISSING={'从未收到','停止发送'}

def connect():
    db=pymysql.connect(**config()['database'],charset='utf8mb4',autocommit=False,connect_timeout=8,read_timeout=360,write_timeout=120,cursorclass=pymysql.cursors.DictCursor)
    with db.cursor() as cur:cur.execute('SET SESSION innodb_lock_wait_timeout=300')
    return db

def backup_events(path):
    decoder=json.JSONDecoder()
    with gzip.open(path,'rt',encoding='utf-8') as stream:
        buffer=stream.read(65536)
        marker=re.search(r'"events"\s*:\s*\[',buffer)
        if not marker:raise ValueError('Backup events array missing')
        buffer=buffer[marker.end():]
        while True:
            buffer=buffer.lstrip(' \n\r\t,')
            if buffer.startswith(']'):return
            try:
                row,end=decoder.raw_decode(buffer)
            except json.JSONDecodeError:
                chunk=stream.read(65536)
                if not chunk:raise
                buffer+=chunk;continue
            yield row;buffer=buffer[end:]


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--existing-backup');args=parser.parse_args()
    root=Path(__file__).resolve().parent;now=datetime.now();folder=root/'backups';folder.mkdir(exist_ok=True)
    previous=None;cutoff=None
    if args.existing_backup:
        previous=Path(args.existing_backup).resolve()
        if previous.parent!=folder.resolve() or not previous.is_file():raise ValueError('Backup must exist in platform backups directory')
        with gzip.open(previous,'rt',encoding='utf-8') as f:
            header=f.read(1024)
        match=re.search(r'"reset_at": "([^"]+)"',header)
        if not match:raise ValueError('Prior backup metadata invalid')
        cutoff=datetime.fromisoformat(match[1])
    backup=folder/('offline_reset_'+now.strftime('%Y%m%d_%H%M%S')+'.json.gz')
    with connect() as db:
        with db.cursor() as cur:
            sql="SELECT * FROM dq_quality_event WHERE reason IN ('从未收到','停止发送')"
            params=()
            if cutoff:sql+=' AND (opened_at>=%s OR closed_at>=%s)';params=(cutoff,cutoff)
            cur.execute(sql,params);events=cur.fetchall()
            cur.execute("SELECT * FROM dq_point_quality_current WHERE data_status IN ('从未收到','停止发送','断线待确认') OR data_missing_since IS NOT NULL OR JSON_CONTAINS(reasons_json,JSON_QUOTE('从未收到')) OR JSON_CONTAINS(reasons_json,JSON_QUOTE('停止发送'))")
            points=cur.fetchall()
            with gzip.open(backup,'wt',encoding='utf-8') as f:json.dump(dict(reset_at=now.isoformat(),previous_backup=str(previous) if previous else None,events=events,points=points),f,ensure_ascii=False,default=str)
            print('Backup complete: '+str(backup),flush=True)
            pending={'本机积压待判定','异常待确认（连续5分钟）','目录已变更，等待新来源数据','报警已屏蔽'}
            updates=[]
            for row in points:
                reasons=json.loads(row['reasons_json']) if isinstance(row['reasons_json'],str) else row['reasons_json']
                reasons=[r for r in reasons if r not in MISSING]
                if not reasons:reasons=['异常待确认（连续5分钟）']
                status='异常' if set(reasons)-pending else '报警已屏蔽' if '报警已屏蔽' in reasons else '待判定'
                updates.append((json.dumps(reasons,ensure_ascii=False),status,row['eid']))
            if updates:
                cur.execute('CREATE TEMPORARY TABLE dq_offline_reset_stage (eid varchar(128) PRIMARY KEY,reasons_json text,status varchar(32)) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci')
                cur.executemany('INSERT INTO dq_offline_reset_stage (reasons_json,status,eid) VALUES (%s,%s,%s)',updates)
                cur.execute("UPDATE dq_point_quality_current p JOIN dq_offline_reset_stage s ON s.eid=p.eid SET p.reasons_json=s.reasons_json,p.status=s.status,p.data_status='待判定',p.data_missing_since=NULL")
        db.commit()
        removed=0;batches=0;ids=[]
        def delete_batch(values):
            with db.cursor() as cur:
                cur.execute("DELETE FROM dq_quality_event WHERE id IN ("+','.join(['%s']*len(values))+") AND reason IN ('从未收到','停止发送')",values)
                count=cur.rowcount
            db.commit();return count
        for path in ([previous] if previous else [])+[backup]:
            for event in backup_events(path):
                if event.get('reason') not in MISSING:raise ValueError('Non-offline event in backup')
                ids.append(int(event['id']))
                if len(ids)>=2000:
                    removed+=delete_batch(ids);ids=[];batches+=1
                    if batches%25==0:print('Deleted backed-up offline event rows: '+str(removed),flush=True)
        if ids:removed+=delete_batch(ids)
        print('Deleted backed-up offline event rows: '+str(removed),flush=True)
    finished=datetime.now();cache=redis_connect();cache.set(QUALITY+'offline_reset_at',int(finished.timestamp()*1000));cache.delete(QUALITY+'summary')
    print(json.dumps(dict(backup=str(backup),previous_backup=str(previous) if previous else None,reset_at=finished.isoformat(),deleted_offline_events=removed,reset_points=len(points)),ensure_ascii=False),flush=True)

if __name__=='__main__':main()
