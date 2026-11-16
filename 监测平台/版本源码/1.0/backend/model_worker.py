"""Run validated models using incremental Redis history reads, without another MQTT client."""
from __future__ import annotations

import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
MODEL_ROOT = ROOT.parent / '模型在线检测服务'
# The copied model wheelhouse is compiled for Python 3.12. Redis/PyMySQL are
# pure Python packages already installed on D: for the 3.13 receiver.
if sys.version_info[:2] == (3, 12):
    sys.path.insert(0, str(MODEL_ROOT / '.deps'))
    sys.path.append('D:/python313/Lib/site-packages')
else:
    sys.path.insert(0, str(ROOT / '.deps'))
sys.path.insert(0, str(MODEL_ROOT))

from quality_engine import PREFIX, QUALITY, db_connect, redis_connect

LOG = logging.getLogger('model_worker')


def init_table():
    with db_connect() as db:
        with db.cursor() as cur:
            cur.execute("""CREATE TABLE IF NOT EXISTS dq_model_quality_current (
              eid varchar(128) NOT NULL PRIMARY KEY, model_result varchar(32) NOT NULL,
              status_code varchar(64) NOT NULL, message varchar(255), health_score double,
              detected_at datetime(3) NOT NULL, KEY ix_result(model_result)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci""")
            cur.execute('SHOW COLUMNS FROM dq_model_quality_current')
            columns={row['Field'] for row in cur.fetchall()}
            if 'is_active' not in columns:
                cur.execute('ALTER TABLE dq_model_quality_current ADD COLUMN is_active tinyint NOT NULL DEFAULT 1')
            for column in ('deployment_status','model_origin'):
                if column not in columns:
                    cur.execute('ALTER TABLE dq_model_quality_current ADD COLUMN '+column+' varchar(128) NULL')
        db.commit()


def set_status(cache, **fields):
    inventory=ROOT/'model_inventory.json'
    audit=json.loads(inventory.read_text(encoding='utf-8')) if inventory.exists() else {}
    cache.set(QUALITY+'model_status',json.dumps({'at':int(time.time()*1000),**audit,**fields},ensure_ascii=False))


def load_detector():
    from online_model_detector import OnlineDetector
    path=MODEL_ROOT/'config.json'
    settings=json.loads(path.read_text(encoding='utf-8-sig'))
    detector=OnlineDetector(path,settings)
    from all_model_registry import AllModelRegistry
    from online_model_detector import BinnedSensorCache
    detector.registry=AllModelRegistry(ROOT.parent)
    detector.cache=BinnedSensorCache(detector.registry.required_eids,detector.registry.bin_seconds,detector.cache_minutes,False)
    return detector


def refill(cache, detector, now_ms):
    start_ms=now_ms-detector.cache_minutes*60000
    count=0
    detector.history_refill_paused=False
    eids=list(detector.registry.required_eids)
    owners=dict(zip(eids,cache.hmget(PREFIX+'catalog',eids)))
    if not isinstance(getattr(detector,'history_cursors',None),dict):detector.history_cursors={}
    if not isinstance(getattr(detector,'history_owners',None),dict):detector.history_owners={}
    cursors=detector.history_cursors
    for eid in eids:
        if eid in detector.history_owners and detector.history_owners[eid]!=owners.get(eid):
            detector.cache.reset_eid(eid);cursors.pop(eid,None)
    detector.history_owners=owners
    pause=0.025
    for offset in range(0,len(eids),8):
        batch=[eid for eid in eids[offset:offset+8] if owners.get(eid) in ('oa','kepserver')]
        if not batch:continue
        if offset%200==0:
            try:
                if float(cache.hget(PREFIX+'mqtt_runtime','oldest_pending_ms') or 0)>10000:
                    detector.history_refill_paused=True
                    return count
            except (TypeError,ValueError):pass
        with cache.pipeline(transaction=False) as pipe:
            for eid in batch:
                previous=cursors.get(eid)
                minimum='('+previous if previous and int(previous.split('-')[0])>=start_ms else f'{start_ms}-0'
                pipe.xrange(PREFIX+'history:'+eid,min=minimum,max=f'{now_ms+10000}-18446744073709551615')
            histories=pipe.execute()
        for eid,history in zip(batch,histories):
            for stream_id,fields in history:
                cursors[eid]=stream_id
                if set(fields)!={'t','v'} and (fields.get('schema')!='2' or fields.get('s')!=owners[eid]):continue
                try:ts=int(fields['t']);value=json.loads(fields['v'])
                except (KeyError,TypeError,ValueError):continue
                if start_ms<=ts<=now_ms+10000:
                    detector.cache.add(eid,value,True if set(fields)=={'t','v'} else fields.get('q'),ts);count+=1
        time.sleep(pause)
    return count


def save_results(detector,results):
    with db_connect() as db:
        with db.cursor() as cur:
            cur.execute('UPDATE dq_model_quality_current SET is_active=0 WHERE is_active=1')
            cur.executemany("""INSERT INTO dq_model_quality_current
                (eid,model_result,status_code,message,health_score,detected_at,deployment_status,model_origin,is_active)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,1)
                ON DUPLICATE KEY UPDATE model_result=VALUES(model_result),status_code=VALUES(status_code),
                message=VALUES(message),health_score=VALUES(health_score),detected_at=VALUES(detected_at),
                deployment_status=VALUES(deployment_status),model_origin=VALUES(model_origin),is_active=1""",
                [(x.eid,x.result,x.status_code,x.message,x.health_score,x.detected_at,
                  detector.registry.metadata[x.eid].get('deployment_status','unknown'),
                  detector.registry.metadata[x.eid].get('model_origin','')) for x in results])
        db.commit()


def unavailable_results(detector,message,code):
    from online_model_detector import DetectionResult
    return [DetectionResult(eid,'无法判断',datetime.now(),message,code) for eid in sorted(detector.registry.ready_eids)]


def load_disabled_targets():
    with db_connect() as db:
        with db.cursor() as cur:
            cur.execute('SELECT eid FROM autosensor WHERE COALESCE(qualityCheck,0)<>0')
            return {row['eid'] for row in cur.fetchall()}

def run_once(cache, detector):
    detector.quality_disabled_eids=load_disabled_targets()
    now_ms=int(time.time()*1000)
    mqtt={k:json.loads(v) for k,v in cache.hgetall(PREFIX+'mqtt_runtime').items()}
    gap=int(mqtt.get('last_gap_at') or 0)
    set_status(cache,state='正在读取模型历史',ready_models=len(detector.registry.ready_eids),evaluated=0)
    samples=refill(cache,detector,now_ms)
    if getattr(detector,'history_refill_paused',False):
        set_status(cache,state='采集积压，模型读取暂缓',ready_models=len(detector.registry.ready_eids),evaluated=0,samples=samples)
        return
    if samples==0 and detector.cache.coverage_count()==0:
        save_results(detector,unavailable_results(detector,'模型所需点位没有可用历史数据','NO_HISTORY'))
        set_status(cache,state='没有可用的整包时间历史',ready_models=len(detector.registry.ready_eids),evaluated=0)
        return
    set_status(cache,state='正在推理',ready_models=len(detector.registry.ready_eids),evaluated=0,samples=samples)
    results=detector.run_detection_once(now_epoch=now_ms//1000,write_database=False)
    from online_model_detector import DetectionResult
    for eid in sorted(detector.registry.unloaded_eids-detector.quality_disabled_eids):
        results.append(DetectionResult(eid,'无法判断',datetime.now(),'模型文件加载失败，详见模型清单','MODEL_LOAD_ERROR'))
    save_results(detector,results)
    counts={}
    for item in results:counts[item.result]=counts.get(item.result,0)+1
    set_status(cache,state='已推理',ready_models=len(detector.registry.ready_eids),
               evaluated=len(results),results=counts,samples=samples,history_warming=False,remaining_seconds=0)
    LOG.info('model_evaluated results=%s samples=%s counts=%s',len(results),samples,counts)


def main():
    (ROOT/'logs').mkdir(exist_ok=True)
    logging.basicConfig(level=logging.INFO,handlers=[logging.FileHandler(ROOT/'logs'/'model.log',encoding='utf-8'),logging.StreamHandler()],
                        format='%(asctime)s %(levelname)s %(message)s')
    from threadpoolctl import threadpool_limits
    threadpool_limits(limits=1)
    cache=redis_connect()
    init_table()
    try:
        detector=load_detector()
    except Exception as exc:
        LOG.exception('model_load_failed')
        set_status(cache,state='模型加载失败',error=f'{type(exc).__name__}: {exc}',ready_models=0,evaluated=0)
        raise
    LOG.info('models_loaded selected=%s groups=%s required=%s',len(detector.registry.ready_eids),
             len(detector.registry.groups),len(detector.registry.required_eids))
    while True:
        started=time.monotonic()
        try:
            run_once(cache,detector)
        except Exception as exc:
            LOG.exception('model_run_failed')
            set_status(cache,state='模型推理失败',error=f'{type(exc).__name__}: {exc}',
                       ready_models=len(detector.registry.ready_eids),evaluated=0)
        time.sleep(10 if getattr(detector,'history_refill_paused',False) else max(1,300-(time.monotonic()-started)))


if __name__=='__main__':
    main()
