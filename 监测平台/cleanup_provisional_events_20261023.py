"""One-time audited cleanup of events emitted by the superseded startup rules."""
import gzip
import json
from pathlib import Path

from quality_engine import ROOT, db_connect


def main():
    cutoff='2026-10-23 16:07:21'
    backup=ROOT/'logs'/'provisional_events_before_20261023_160721.jsonl.gz'
    if backup.exists():
        raise SystemExit(f'Backup already exists; refusing a second cleanup: {backup}')
    conditions=("(reason=%s OR reason=%s) AND closed_at IS NOT NULL AND opened_at<%s")
    reasons=('发送时间滞后','停止发送',cutoff)
    ids=[]
    with db_connect() as db:
        with db.cursor() as cur, gzip.open(backup,'wt',encoding='utf-8') as out:
            cur.execute('SELECT id,eid,source,device_key,reason,opened_at,closed_at '
                        'FROM dq_quality_event WHERE '+conditions+' ORDER BY id',reasons)
            while True:
                batch=cur.fetchmany(2000)
                if not batch:
                    break
                for row in batch:
                    ids.append(row['id'])
                    out.write(json.dumps({key:value.isoformat(sep=' ') if hasattr(value,'isoformat') else value
                                          for key,value in row.items()},ensure_ascii=False)+'\n')
    if not ids:
        print('No provisional closed events found; backup:',backup)
        return
    deleted=0
    with db_connect() as db:
        with db.cursor() as cur:
            for offset in range(0,len(ids),2000):
                chunk=ids[offset:offset+2000]
                cur.execute('DELETE FROM dq_quality_event WHERE id IN ('+','.join(['%s']*len(chunk))+')',chunk)
                deleted+=cur.rowcount
                db.commit()
                if deleted%20000<2000:
                    print('deleted',deleted,'of',len(ids),flush=True)
    print('backup',backup,'selected',len(ids),'deleted',deleted,flush=True)


if __name__=='__main__':
    main()
