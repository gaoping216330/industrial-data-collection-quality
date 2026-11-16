"""One-way autosensor replication. Source is read-only; target-only rows survive."""
from __future__ import annotations

import argparse
import gzip
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import time
from datetime import datetime
from uuid import uuid4

import pymysql
import redis

ROOT = Path(__file__).resolve().parent
PROJECT = ROOT.parent
LOG = logging.getLogger('catalog_sync')
INTERVAL = 600


def load_settings(path):
    return json.loads(path.read_text(encoding='utf-8-sig'))


def connect_config(path):
    original=load_settings(path)['database']
    result={k:v for k,v in original.items() if k in ('host','port','user','password','database')}
    env=original.get('password_env')
    if env and os.environ.get(env):
        result['password']=os.environ[env]
    return result


def connect(settings):
    return pymysql.connect(**settings,charset='utf8mb4',autocommit=False,
                           connect_timeout=8,read_timeout=90,write_timeout=90,
                           cursorclass=pymysql.cursors.DictCursor)


def quote(name):
    return '`'+name.replace('`','``')+'`'


def upsert_sql(columns):
    return 'INSERT INTO autosensor ('+','.join(map(quote,columns))+') VALUES ('+','.join(['%s']*len(columns))+') ON DUPLICATE KEY UPDATE '+','.join(quote(c)+'=VALUES('+quote(c)+')' for c in columns if c!='eid')


def difference(source, target, columns):
    target_by_eid={r['eid']:r for r in target}
    source_keys={r['eid'] for r in source}
    additions=[];updates=[]
    for row in source:
        old=target_by_eid.get(row['eid'])
        if old is None:
            additions.append(row)
        elif any(row[c]!=old[c] for c in columns):
            updates.append(row)
    return additions,updates,len(set(target_by_eid)-source_keys)


def status_file(result):
    temporary=ROOT/'status.tmp'
    temporary.write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    temporary.replace(ROOT/'status.json')


def backup(cursor, rows, columns, additions):
    folder=ROOT/'backups';folder.mkdir(exist_ok=True)
    path=folder/(datetime.now().strftime('%Y%m%d-%H%M%S')+'-'+uuid4().hex[:8]+'.sql.gz')
    cursor.execute('SHOW CREATE TABLE autosensor')
    create=next(v for k,v in cursor.fetchone().items() if k!='Table')
    with gzip.open(path,'wt',encoding='utf-8') as out:
        out.write('-- Before-image of target 192.0.2.10/gpwinccoa.autosensor. Manual recovery only.\n')
        out.write('-- Original schema:\n'+'\n'.join('-- '+s for s in create.splitlines())+'\nSTART TRANSACTION;\n')
        for row in additions:
            out.write(cursor.mogrify('DELETE FROM autosensor WHERE eid=%s',(row['eid'],))+';\n')
        sql=upsert_sql(columns)
        for row in rows:
            out.write(cursor.mogrify(sql,tuple(row[c] for c in columns))+';\n')
        out.write('COMMIT;\n')
    return str(path)


def notify_revision():
    options=dict(load_settings(PROJECT/'实时数据缓存服务/cache_config.json')['redis'])
    options.update(socket_timeout=3,socket_connect_timeout=2)
    client=redis.Redis(**options)
    try:
        revision=str(time.time_ns())
        client.set('dq:cache:catalog_revision',revision)
        return revision
    finally:
        client.close()


def run(apply=False):
    started=time.time()
    source_settings=connect_config(PROJECT/'模型在线检测服务/config.json')
    target_settings=connect_config(PROJECT/'实时数据缓存服务/cache_config.json')
    allowed_target=(target_settings.get('host')==__import__('os').environ.get('DQ_APPROVED_REMOTE_TARGET_HOST') and int(target_settings.get('port',3306))==3306) or (target_settings.get('host') in ('127.0.0.1','localhost') and int(target_settings.get('port',3306))==3307)
    if source_settings['host']!=__import__('os').environ.get('DQ_APPROVED_SOURCE_HOST') or not allowed_target or target_settings.get('database')!='gpwinccoa':
        raise ValueError('Unexpected source/target host; refusing to replicate')
    with connect(source_settings) as source,connect(target_settings) as target:
        with source.cursor() as src,target.cursor() as dst:
            dst.execute("SELECT GET_LOCK('dq_autosensor_sync',0) acquired")
            if dst.fetchone()['acquired']!=1:
                return {'status':'skipped','reason':'another_sync_running'}
            try:
                source.begin()
                src.execute('SHOW FULL COLUMNS FROM autosensor');source_schema=src.fetchall()
                dst.execute('SHOW FULL COLUMNS FROM autosensor');target_schema=dst.fetchall()
                columns=[r['Field'] for r in source_schema if r['Field']!='flatlineCheck']
                if set(columns)!={r['Field'] for r in target_schema if r['Field']!='flatlineCheck'}:
                    raise ValueError('Column sets differ; refusing partial replication')
                if [r['Field'] for r in source_schema if r['Key']=='PRI']!=['eid']:
                    raise ValueError('Source primary key must be eid')
                if [r['Field'] for r in target_schema if r['Key']=='PRI']!=['eid']:
                    raise ValueError('Target primary key must be eid')
                src.execute('SELECT '+','.join(map(quote,columns))+' FROM autosensor ORDER BY eid')
                source_rows=src.fetchall()
                if not source_rows:
                    raise ValueError('Empty source catalog; refusing replication')
                dst.execute('SELECT '+','.join(map(quote,columns))+' FROM autosensor ORDER BY eid')
                target_rows=dst.fetchall()
                additions,updates,extra=difference(source_rows,target_rows,columns)
                result=dict(status='checked',source_host=source_settings['host'],target_host=target_settings['host'],target_port=target_settings.get('port',3306),
                            source_rows=len(source_rows),target_rows_before=len(target_rows),
                            inserted=len(additions),updated=len(updates),target_only_preserved=extra,
                            interval_seconds=INTERVAL,started_at=datetime.fromtimestamp(started).isoformat(sep=' '))
                mismatches=[(a,b) for a,b in zip(sorted((x for x in source_schema if x['Field'] in columns),key=lambda x:x['Field']),sorted((x for x in target_schema if x['Field'] in columns),key=lambda x:x['Field'])) if a['Type']!=b['Type']]
                if any(a['Field']!='dpname' or a['Type']!='varchar(100)' or b['Type']!='varchar(45)' for a,b in mismatches):
                    raise ValueError('Unexpected column type differences')
                result['widen_dpname']=bool(mismatches)
                if not apply:
                    return result
                if additions or updates or mismatches:
                    result['backup']=backup(dst,target_rows,columns,additions)
                if mismatches:
                    dst.execute('ALTER TABLE autosensor MODIFY COLUMN dpname varchar(100) NULL')
                target.begin()
                sql=upsert_sql(columns)
                changed=additions+updates
                for offset in range(0,len(changed),500):
                    dst.executemany(sql,[tuple(row[c] for c in columns) for row in changed[offset:offset+500]])
                target.commit()
                dst.execute('SELECT '+','.join(map(quote,columns))+' FROM autosensor ORDER BY eid')
                remaining=difference(source_rows,dst.fetchall(),columns)
                if remaining[0] or remaining[1]:
                    raise RuntimeError('Committed target differs from source snapshot; inspect backup and retry')
                target.commit()
                result.update(status='success',finished_at=datetime.now().isoformat(sep=' '),elapsed_seconds=round(time.time()-started,2))
                # Notify even on no-op runs: a previous commit may have outlived Redis availability.
                try:
                    result['catalog_revision']=notify_revision()
                    result['notification']='sent'
                except redis.RedisError:
                    result['notification']='retry_next_cycle'
                    LOG.warning('Catalog committed; Redis notification unavailable')
                status_file(result)
                return result
            finally:
                dst.execute("SELECT RELEASE_LOCK('dq_autosensor_sync')")


def main():
    ROOT.mkdir(exist_ok=True)
    handler=RotatingFileHandler(ROOT/'sync.log',maxBytes=5_000_000,backupCount=3,encoding='utf-8')
    logging.basicConfig(level=logging.INFO,handlers=[handler],format='%(asctime)s %(levelname)s %(message)s')
    parser=argparse.ArgumentParser()
    parser.add_argument('--apply',action='store_true')
    parser.add_argument('--loop',action='store_true')
    args=parser.parse_args()
    if args.loop and not args.apply:
        parser.error('--loop requires --apply')
    while True:
        started=time.monotonic()
        try:
            result=run(args.apply)
            LOG.info('%s',json.dumps(result,ensure_ascii=False))
            print(json.dumps(result,ensure_ascii=True),flush=True)
        except Exception as exc:
            LOG.exception('sync_failed')
            status_file(dict(status='error',error_type=type(exc).__name__,failed_at=datetime.now().isoformat(sep=' '),interval_seconds=INTERVAL))
            if not args.loop:
                raise
        if not args.loop:
            break
        time.sleep(max(1,INTERVAL-(time.monotonic()-started)))


if __name__=='__main__':
    import sys as _diag_sys
    from pathlib import Path as _DiagPath
    _diag_sys.path.insert(0,str(_DiagPath(__file__).resolve().parent.parent/'运行诊断'))
    from runtime_logging import install as _install_diagnostics
    _install_diagnostics('sync_autosensor')

    main()
