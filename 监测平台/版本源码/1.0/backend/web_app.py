"""Local dashboard and scoped rule API for OA/Kepserver quality monitoring."""
from __future__ import annotations

import json
import math
import re
import pymysql
import time
import threading
import copy
from datetime import datetime
from pathlib import Path

from flask import Flask, jsonify, request, session, send_from_directory
from waitress import serve
import redis
from redis.retry import Retry
from redis.backoff import NoBackoff

from quality_engine import ROOT, PREFIX, QUALITY, db_connect, config
from publication_state import read_publication

app = Flask(__name__, static_folder=str(ROOT/'static'), static_url_path='/static')

from admin_access import install_admin
from excel_export import install_export
install_admin(app,ROOT)
install_export(app)

@app.errorhandler(pymysql.MySQLError)
def database_api_error(exc):
    app.logger.error('Database API failure: %s',type(exc).__name__,exc_info=True)
    return jsonify(error='数据库暂不可用：无法读取数据库数据，请检查数据库电脑的MySQL服务及磁盘空间后重试。',code='database_unavailable'),503


@app.errorhandler(500)
def unexpected_api_error(exc):
    return jsonify(error='服务处理失败，请稍后重试或检查服务日志。',code='internal_error'),500


_redis_settings=dict(config()['redis'])
_redis_settings.update(socket_connect_timeout=0.5,socket_timeout=0.8,
                       retry=Retry(NoBackoff(),0),max_connections=12)
_read_cache=redis.Redis(**_redis_settings)


def redis_connect():
    # Web reads must fail promptly; the collector has its own write connection.
    return _read_cache


_snapshot_lock=threading.Lock()
_poll_started=False
_overview_snapshot={'summary':None,'mqtt':{},'model':None,
                    'redis':{'used_bytes':None,'max_bytes':None,'history_minutes':config()['history_minutes']},
                    'snapshot_status':'loading','snapshot_at':None}


def database_summary():
    reason_names=['从未收到','停止发送','发送时间超前','发送时间滞后','发送时间无效',
                  '质量位BAD','设备恒值异常','本机积压待判定','异常待确认（连续5分钟）','目录已变更，等待新来源数据']
    sums=','.join('SUM(JSON_CONTAINS(reasons_json,JSON_QUOTE(%s))) r'+str(i) for i in range(len(reason_names)))
    rows=query("SELECT source,COUNT(*) expected,SUM(receipt_time_ms IS NOT NULL) received,"
               "SUM(status='异常') abnormal,MAX(updated_at) updated_at,"+sums+
               ' FROM dq_point_quality_current GROUP BY source',reason_names)
    summary={'expected':{},'received':{},'abnormal':{},'reasons':{},'run_at':None}
    for row in rows:
        source=row['source']
        for field in ('expected','received','abnormal'):
            summary[field][source]=int(row[field] or 0)
        summary['reasons'][source]={name:int(row['r'+str(i)] or 0) for i,name in enumerate(reason_names)}
        if row['updated_at']:
            stamp=int(row['updated_at'].timestamp()*1000)
            summary['run_at']=max(summary['run_at'] or 0,stamp)
    return summary


def _poll_overview():
    fallback_at=0
    while True:
        try:
            with _read_cache.pipeline(transaction=False) as pipe:
                pipe.get(QUALITY+'summary');pipe.hgetall(PREFIX+'mqtt_runtime')
                pipe.info('memory');pipe.get(QUALITY+'model_status')
                summary,runtime,memory,model=pipe.execute()
            fresh={'summary':json.loads(summary) if summary else None,
                   'mqtt':{k:json.loads(v) for k,v in runtime.items()},
                   'redis':{'used_bytes':memory['used_memory'],'max_bytes':memory['maxmemory'],'history_minutes':config()['history_minutes']},
                   'model':json.loads(model) if model else None,
                   'snapshot_status':'live','snapshot_at':int(time.time()*1000)}
            with _snapshot_lock:
                _overview_snapshot.update(fresh)
        except (redis.RedisError,ValueError,KeyError):
            with _snapshot_lock:
                _overview_snapshot['snapshot_status']='stale'
            if time.monotonic()-fallback_at>=15:
                fallback_at=time.monotonic()
                try:
                    summary=database_summary()
                    with _snapshot_lock:
                        _overview_snapshot.update(summary=summary,snapshot_status='database',
                                                  snapshot_at=int(time.time()*1000))
                except Exception:
                    pass
        time.sleep(5)


def start_overview_poll():
    global _poll_started
    with _snapshot_lock:
        if _poll_started:
            return
        _poll_started=True
    threading.Thread(target=_poll_overview,name='dashboard_snapshot',daemon=True).start()

_result_cache={}
_result_cache_guard=threading.Lock()
_result_cache_locks={}

def published_cache(key,loader):
    publication=read_publication() or {}
    version=publication.get('version') or str(int(time.time()//30))
    with _result_cache_guard:
        lock=_result_cache_locks.setdefault(key,threading.Lock())
    with lock:
        hit=_result_cache.get(key)
        if hit and hit[0]==version:return copy.deepcopy(hit[1])
        result=loader()
        _result_cache[key]=(version,result)
        return copy.deepcopy(result)


def business_devices():
    return published_cache('business-devices',lambda:query('SELECT * FROM dq_device_quality_display'))


DEVICE_TOKEN_SQL = "display_group_token"
ANOMALY_FILTERS = {
    **{name: "JSON_CONTAINS(reasons_json, JSON_QUOTE('"+name+"'))" for name in ['质量位BAD','设备恒值异常','发送时间超前','发送时间滞后','发送时间无效']},
    'never': "JSON_CONTAINS(reasons_json, JSON_QUOTE('从未收到'))",
    'stopped': "JSON_CONTAINS(reasons_json, JSON_QUOTE('停止发送'))",
    'abnormal': "status='异常'",
}


ANOMALY_FILTERS={key:'COALESCE(quality_check,0)=0 AND ('+value+')' for key,value in ANOMALY_FILTERS.items()}

def anomaly_selection():
    source=request.args.get('source','')
    category=request.args.get('category','abnormal')
    if source not in ('oa','kepserver') or category not in ANOMALY_FILTERS:
        return None
    return source,category,ANOMALY_FILTERS[category]


@app.get('/api/anomaly-devices')
def anomaly_devices():
    selection=anomaly_selection()
    if selection is None:
        return jsonify(error='invalid_anomaly_filter'),400
    source,category,predicate=selection
    page=max(1,int(request.args.get('page',1)))
    grouped=f"""SELECT display_group_token device_token,%s source,
        MAX(device_key) device_key,MAX(department) department,MAX(process) process,
        MAX(equipment) equipment,MAX(`group`) `group`,MAX(device_name) device_name,
        COUNT(DISTINCT device_key) collector_count,
        GROUP_CONCAT(DISTINCT owner_name ORDER BY owner_name SEPARATOR '、') owner_name,
        MIN(CASE WHEN COALESCE(quality_check,0)=0 AND data_status IN ('从未收到','停止发送') THEN data_missing_since END) first_offline_ms,
        COUNT(*) point_count,SUM(source=%s AND ({predicate})) affected_count,
        SUM(JSON_CONTAINS(reasons_json,JSON_QUOTE('从未收到')) OR
            JSON_CONTAINS(reasons_json,JSON_QUOTE('停止发送'))) missing_count
        FROM dq_point_quality_display
        GROUP BY display_group_token HAVING affected_count>0"""
    groups=published_cache(('anomaly-devices',source,category),lambda:query(grouped,(source,source)))
    groups.sort(key=lambda x:(str(x['department'] or ''),str(x['process'] or ''),str(x['equipment'] or ''),str(x['group'] or '')))
    totals={'n':len(groups),'affected_count':sum(int(x['affected_count'] or 0) for x in groups)}
    rows=groups[min((page-1)*50,1000000):min(page*50,1000050)]
    for row in rows:
        for field in ('point_count','affected_count','missing_count'):
            row[field]=int(row[field] or 0)
    return jsonify(source=source,category=category,page=page,total=totals['n'],
                   affected_count=int(totals['affected_count']),items=public_rows(rows))


@app.get('/api/anomaly-devices/<token>')
def anomaly_device_detail(token):
    selection=anomaly_selection()
    if selection is None or len(token)!=64 or any(c not in '0123456789abcdef' for c in token):
        return jsonify(error='invalid_anomaly_filter'),400
    source,category,predicate=selection
    page=max(1,int(request.args.get('page',1)))
    device_where=f'{DEVICE_TOKEN_SQL}=%s'
    params=[token]
    rows=query("SELECT MAX(department) department,MAX(process) process,MAX(equipment) equipment,MAX(`group`) `group`,MAX(device_key) device_key,GROUP_CONCAT(DISTINCT device_name ORDER BY device_name SEPARATOR '、') device_name,GROUP_CONCAT(DISTINCT source ORDER BY source SEPARATOR ',') source,MAX(tag) tag,COUNT(*) point_count FROM dq_point_quality_display WHERE "+device_where,params)
    if not rows or not rows[0].get('point_count'):
        return jsonify(error='device_not_found'),404
    device=rows[0]
    counts=query('SELECT COUNT(*) point_count,SUM(source=%s AND ('+predicate+')) affected_count '
                 'FROM dq_point_quality_display WHERE '+device_where,[source]+params)[0]
    found=query('SELECT * FROM dq_point_quality_display WHERE '+device_where+' AND ('+predicate+') '
                'AND source=%s ORDER BY eid LIMIT 50 OFFSET %s',params+[source,min((page-1)*50,1000000)])
    items=live_values(public_rows(found))
    if items:
        eids=[x['eid'] for x in items]
        events=query('SELECT eid,reason,MIN(opened_at) opened_at FROM dq_quality_event '
                     'WHERE closed_at IS NULL AND TIMESTAMPDIFF(MICROSECOND,opened_at,NOW(3))>300000000 AND eid IN ('+','.join(['%s']*len(eids))+') GROUP BY eid,reason',eids)
        by_eid={}
        for event in events:
            by_eid.setdefault(event['eid'],{})[event['reason']]=event['opened_at'].isoformat(sep=' ')
        for item in items:
            item['active_events']=by_eid.get(item['eid'],{})
            if '从未收到' in item['reasons']:
                item['missing_since']=item['active_events'].get('从未收到')
                item['missing_since_basis']='已收到数据，等待下轮判定' if item.get('receipt_time_ms') else '首次记录未收到；此前时间未知'
            elif '停止发送' in item['reasons']:
                last=item.get('receipt_time_ms')
                fresh=last is not None and time.time()*1000-last<=10000
                item['missing_since']=item['active_events'].get('停止发送')
                if last and not fresh:
                    item['missing_since']=item['active_events'].get('停止发送') or item.get('data_missing_since') or last
                item['missing_since_basis']='已收到新数据，恢复时间等待下一轮正式判定' if fresh else '正式记录的断线开始时间；清理后从重新观察起点计时'
            else:
                item['missing_since']=None
                item['missing_since_basis']='当前异常不属于缺失数据'
            item['missing_ended_at']=None
            duration=event_duration(item.get('missing_since'))
            item['missing_duration_seconds']=duration['duration_seconds']
            item['missing_duration_hours']=duration['duration_hours']
    return jsonify(device=public_rows([device])[0],point_count=counts['point_count'],
                   total=int(counts['affected_count'] or 0),page=page,items=items,category=category)


@app.get('/api/open-device-events')
def open_device_events():
    source=request.args.get('source','')
    if source not in ('','oa','kepserver'):
        return jsonify(error='invalid_source'),400
    page=max(1,int(request.args.get('page',1)))
    params=[source] if source else []
    source_filter=' AND p.source=%s' if source else ''
    token="p.display_group_token"
    grouped=f"""SELECT {token} device_token,GROUP_CONCAT(DISTINCT p.source ORDER BY p.source SEPARATOR ',') source,MAX(p.device_key) device_key,
        MAX(p.department) department,MAX(p.process) process,MAX(p.equipment) equipment,MAX(p.`group`) `group`,MAX(p.device_name) device_name,
        GROUP_CONCAT(DISTINCT p.owner_name ORDER BY p.owner_name SEPARATOR '、') owner_name,
        COUNT(DISTINCT e.eid) affected_count,COUNT(*) event_count,
        GROUP_CONCAT(DISTINCT e.reason ORDER BY e.reason SEPARATOR '、') reasons_text,
        MIN(e.opened_at) started_at,MAX(e.opened_at) last_started_at
        FROM dq_quality_event e JOIN dq_point_quality_display p ON p.eid=e.eid AND p.source=e.source
        WHERE e.closed_at IS NULL AND COALESCE(p.quality_check,0)=0 AND TIMESTAMPDIFF(MICROSECOND,e.opened_at,NOW(3))>300000000 {source_filter}
        GROUP BY p.display_group_token"""
    with db_connect() as db:
        with db.cursor() as cur:
            cur.execute('SELECT COUNT(*) n,COALESCE(SUM(affected_count),0) affected_count FROM ('+grouped+') x',params)
            totals=cur.fetchone()
            cur.execute(grouped+' ORDER BY last_started_at DESC,device_token LIMIT 50 OFFSET %s',
                        params+[min((page-1)*50,1000000)])
            rows=cur.fetchall()
    for row in rows:
        row['affected_count']=int(row['affected_count'])
        row['ended_at']=None
    return jsonify(total=totals['n'],affected_count=int(totals['affected_count']),
                   page=page,items=public_rows(rows))


@app.get('/api/open-device-events/<token>')
def open_device_event_detail(token):
    source=request.args.get('source','')
    if source not in ('oa','kepserver','kepserver,oa','oa,kepserver') or len(token)!=64 or any(c not in '0123456789abcdef' for c in token):
        return jsonify(error='invalid_device'),400
    page=max(1,int(request.args.get('page',1)))
    token_sql="p.display_group_token"
    clause=f'{token_sql}=%s'
    params=[token]
    device=query('SELECT p.department,p.process,p.equipment,p.`group`,p.source,p.device_name,p.tag '
                 'FROM dq_point_quality_display p WHERE '+clause+' LIMIT 1',params)
    if not device:
        return jsonify(error='device_not_found'),404
    point_count=query('SELECT COUNT(*) n FROM dq_point_quality_display p WHERE '+clause,params)[0]['n']
    # Aggregate each point's open reasons before counting it, so a point with
    # multiple concurrent reasons is listed once.
    event_points="""SELECT eid,source,MIN(opened_at) started_at,MAX(opened_at) last_started_at,
        GROUP_CONCAT(DISTINCT reason ORDER BY reason SEPARATOR '、') reasons_text
        FROM dq_quality_event WHERE closed_at IS NULL AND TIMESTAMPDIFF(MICROSECOND,opened_at,NOW(3))>300000000 GROUP BY eid,source"""
    joined=' FROM dq_point_quality_display p JOIN ('+event_points+') a ON a.eid=p.eid AND a.source=p.source WHERE '+clause+' AND COALESCE(p.quality_check,0)=0'
    total=query('SELECT COUNT(*) n'+joined,params)[0]['n']
    rows=query('SELECT p.*,a.started_at,a.last_started_at,a.reasons_text'+joined+
               ' ORDER BY a.last_started_at DESC,p.eid LIMIT 50 OFFSET %s',params+[min((page-1)*50,1000000)])
    for row in rows:
        row['ended_at']=None
    return jsonify(device=public_rows(device)[0],point_count=point_count,total=total,page=page,
                   items=public_rows(rows))


def query(sql, params=()):
    with db_connect() as db:
        with db.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def event_duration(start, end=None, now_ms=None):
    """Milliseconds or local MySQL datetimes; ongoing durations use server time."""
    def stamp(value):
        if isinstance(value, (int,float)):
            return value
        if isinstance(value,str):
            value=datetime.fromisoformat(value)
        return value.timestamp()*1000 if value is not None else None
    start_ms=stamp(start)
    end_ms=stamp(end)
    if start_ms is None:
        return {'duration_seconds':None,'duration_hours':None,'ongoing':end is None}
    seconds=max(0,((end_ms if end_ms is not None else (now_ms if now_ms is not None else time.time()*1000))-start_ms)/1000)
    return {'duration_seconds':int(seconds),'duration_hours':round(seconds/3600,4),'ongoing':end is None}


def public_rows(rows):
    result=[]
    for row in rows:
        item={k:(v.isoformat(sep=' ') if hasattr(v,'isoformat') else v) for k,v in row.items()}
        if 'opened_at' in row:
            item.update(event_duration(row.get('opened_at'),row.get('closed_at')))
        elif 'started_at' in row:
            item.update(event_duration(row.get('started_at'),row.get('ended_at')))
        if 'reasons_json' in item:
            item['reasons']=json.loads(item.pop('reasons_json'))
        result.append(item)
    from device_manual_reason import enrich
    return enrich(result,query)


def live_values(rows):
    if not rows:
        return rows
    cache=redis_connect()
    eids=[x['eid'] for x in rows]
    try:
        with cache.pipeline(transaction=False) as pipe:
            pipe.hmget(PREFIX+'receipt',eids)
            pipe.mget([PREFIX+'latest:'+eid for eid in eids])
            receipts,latest=pipe.execute()
    except redis.RedisError:
        for row in rows:
            row['live_unavailable']=True
        return rows
    for row, raw_receipt, raw_latest in zip(rows,receipts,latest):
        receipt=json.loads(raw_receipt) if raw_receipt else {}
        value=json.loads(raw_latest) if raw_latest else {}
        if receipt.get('source',row.get('source'))!=row.get('source'):
            receipt={}
        if value.get('source',row.get('source'))!=row.get('source'):
            value={}
        row['receipt_time_ms']=receipt.get('received_time')
        row['source_time_ms']=receipt.get('source_time')
        row['value_change_time_ms']=value.get('last_change_time')
        row['value_text']=str(value.get('value',''))
        row['quality']=receipt.get('quality','')
    return rows


@app.get('/')
def index():
    return send_from_directory(ROOT/'static','index.html')


@app.get('/api/database-location')
def database_location():
    database=config()['database']
    manifest=ROOT.parent/'本地数据库迁移/migration_status.json'
    migration=json.loads(manifest.read_text(encoding='utf-8')) if manifest.exists() else {}
    return jsonify(local=database['host'] in ('127.0.0.1','localhost'),host=database['host'],port=database.get('port',3306),database=database['database'],history_pending=migration.get('old_platform_history')=='not_migrated_source_unavailable')


@app.get('/api/overview')
def overview():
    start_overview_poll()
    with _snapshot_lock:
        snapshot=copy.deepcopy(_overview_snapshot)
    publication=read_publication()
    if publication and publication.get('summary'):
        latest=publication['summary']
        current=snapshot.get('summary') or {}
        if latest.get('published_at',0)>current.get('published_at',0):
            snapshot['summary']=latest
    return jsonify(snapshot)


def model_catalog_snapshot():
    def read_catalog():
        models={r['eid'] for r in query("SELECT eid FROM dq_model_quality_current WHERE is_active=1 AND deployment_status='ready'")}
        people={r['person_code']:r['person_name'] for r in query('SELECT person_code,person_name FROM dq_person_directory')}
        rows=query('SELECT eid,department,process,equipment,owner_code,quality_check FROM dq_point_quality_current')
        for row in rows:
            row['owner_name']=people.get(row['owner_code'],row['owner_code'] or '')
            row['has_model']=row['eid'] in models
        return rows
    return published_cache('model-catalog-snapshot',read_catalog)

@app.get('/api/model-filter-options')
def model_filter_options():
    fields=('department','process','equipment','owner_name')
    values={tuple(r.get(f) or '' for f in fields) for r in model_catalog_snapshot() if r['has_model']}
    return jsonify(items=[dict(zip(fields,v)) for v in sorted(values)])

@app.get('/api/model-coverage')
def model_coverage():
    groups={}
    for point in model_catalog_snapshot():
        department=point['department'] or ''
        row=groups.setdefault(department,dict(department=department,point_count=0,model_count=0,enabled_count=0))
        row['point_count']+=1
        row['model_count']+=int(point['has_model'])
        row['enabled_count']+=int(point['has_model'] and not point['quality_check'])
    total=sum(x['enabled_count'] for x in groups.values())
    for row in groups.values():
        row['coverage_percent']=round(row['enabled_count']*100/max(row['point_count'],1),2)
        row['share_percent']=round(row['enabled_count']*100/max(total,1),2)
    return jsonify(items=sorted(groups.values(),key=lambda r:(-r['enabled_count'],r['department'])),total=total,point_count=sum(x['point_count'] for x in groups.values()))


@app.get('/api/models')
def models():
    from model_training_api import model_selection
    page=max(1,int(request.args.get('page',1)))
    try:where,params,order=model_selection(request.args)
    except ValueError as exc:return jsonify(error=str(exc)),400
    base=' FROM dq_model_quality_current m LEFT JOIN autosensor a ON a.eid=CONVERT(m.eid USING utf8) COLLATE utf8_general_ci LEFT JOIN dq_point_quality_display p ON p.eid=m.eid WHERE '+where
    with _snapshot_lock:status=copy.deepcopy(_overview_snapshot.get('model'))
    total=query('SELECT COUNT(*) n'+base,params)[0]['n']
    rows=query('SELECT m.*,a.department,a.process,a.equipment,a.`group`,a.point,a.qualityCheck,p.owner_name,p.display_group_token'+base+' ORDER BY '+order+' LIMIT 50 OFFSET %s',params+[min((page-1)*50,1000000)])
    for row in rows:
        row['quality_check']=int(int(row.get('qualityCheck') or 0)!=0)
        row['alarm_muted']=row['quality_check']
        if row['quality_check']:
            row.update(model_result='检测已停用',health_score=None,status_code='QUALITY_CHECK_DISABLED',message='qualityCheck非0，已停止该点位质量判定和报警；采集与趋势保留')
    return jsonify(total=total,page=page,items=public_rows(rows),status=status)


@app.get('/api/point-live-values')
def point_live_values():
    eids=list(dict.fromkeys(request.args.getlist('eid')))
    if len(eids)>51 or any(not eid or len(eid)>128 for eid in eids):
        return jsonify(error='invalid_eids'),400
    if not eids:
        return jsonify(items=[])
    try:
        with redis_connect().pipeline(transaction=False) as pipe:
            pipe.hmget(PREFIX+'catalog',eids)
            pipe.hmget(PREFIX+'receipt',eids)
            pipe.mget([PREFIX+'latest:'+eid for eid in eids])
            owners,receipts,latest=pipe.execute()
    except redis.RedisError:
        response=jsonify(items=[],unavailable=True,message='实时缓存繁忙，保留上次显示；请查看采集链路状态')
        response.headers['Cache-Control']='no-store'
        return response
    rows=[]
    for eid,owner,raw_receipt,raw_latest in zip(eids,owners,receipts,latest):
        if owner not in ('oa','kepserver'):continue
        receipt=json.loads(raw_receipt) if raw_receipt else {}
        value=json.loads(raw_latest) if raw_latest else {}
        if receipt.get('source',owner)!=owner:receipt={}
        if value.get('source',owner)!=owner:value={}
        rows.append(dict(eid=eid,source=owner,receipt_time_ms=receipt.get('received_time'),
                         source_time_ms=receipt.get('source_time'),value_text=str(value.get('value','')),
                         quality=receipt.get('quality','')))
    response=jsonify(items=rows,refreshed_at=int(time.time()*1000))
    response.headers['Cache-Control']='no-store'
    return response


@app.get('/api/point-filter-options')
def point_filter_options():
    fields=('department','process','equipment','group','eid','point')
    field=request.args.get('field','department')
    if field not in fields:return jsonify(error='筛选字段无效'),400
    where=["`"+field+"` IS NOT NULL", "`"+field+"`<>''"];params=[]
    for other in fields:
        value=request.args.get(other,'')
        if other!=field and value:
            where.append('`'+other+'`=%s');params.append(value)
    search=request.args.get('q','').strip()[:100]
    if search:
        where.append('`'+field+'` LIKE %s');params.append('%'+search.replace('\\','\\\\').replace('%','\\%').replace('_','\\_')+'%')
    offset=max(0,min(int(request.args.get('offset',0)),1000000))
    rows=query('SELECT DISTINCT `'+field+'` value FROM dq_point_quality_current WHERE '+' AND '.join(where)+' ORDER BY `'+field+'` LIMIT 101 OFFSET %s',params+[offset])
    return jsonify(field=field,items=[row['value'] for row in rows[:100]],has_more=len(rows)>100,next_offset=offset+100)


import alarm_audit


def audit_alarm_rejection(result,note):
    eid=request.path.split('/')[-2]
    payload=request.get_json(silent=True) or {}
    enabled=payload.get('enabled') if type(payload.get('enabled')) is bool else None
    audit_id=alarm_audit.begin(db_connect,eid,enabled,result=result,note=note)
    alarm_audit.finish(db_connect,audit_id,result,note=note)


app.config['AUDIT_ALARM_REJECTION']=audit_alarm_rejection


@app.get('/api/alarm-audit')
def alarm_audit_records():
    if not session.get('admin'):return jsonify(error='仅管理员可查看操作记录'),401
    page=max(1,int(request.args.get('page',1)))
    where=[];params=[]
    for field in ('eid','actor','result'):
        if request.args.get(field):where.append('`'+field+'`=%s');params.append(request.args[field])
    clause=' WHERE '+' AND '.join(where) if where else ''
    total=query('SELECT COUNT(*) n FROM dq_alarm_setting_audit'+clause,params)[0]['n']
    rows=query('SELECT * FROM dq_alarm_setting_audit'+clause+' ORDER BY requested_at DESC,id LIMIT 50 OFFSET %s',params+[(page-1)*50])
    return jsonify(total=total,page=page,items=public_rows(rows))


@app.put('/api/points/<eid>/alarm')
def point_alarm(eid):
    payload=request.get_json(silent=True) or {}
    enabled=payload.get('enabled')
    if type(enabled) is not bool:
        audit_alarm_rejection('INVALID','enabled 必须是布尔值')
        return jsonify(error='enabled 必须是布尔值'),400
    found=query('SELECT * FROM dq_point_quality_display WHERE eid=%s',(eid,))
    if not found:
        audit_alarm_rejection('NOT_FOUND','当前目录没有该EID')
        return jsonify(error='点位不存在'),404
    row=found[0];flag=0 if enabled else 1
    source_settings=json.loads((ROOT.parent/'模型在线检测服务/config.json').read_text(encoding='utf-8-sig'))['database']
    settings={k:v for k,v in source_settings.items() if k in ('host','port','user','password','database')}
    import os
    if source_settings.get('password_env') and os.environ.get(source_settings['password_env']):settings['password']=os.environ[source_settings['password_env']]
    audit_id=None;source_saved=False;target_saved=False
    try:
        with pymysql.connect(**settings,charset='utf8mb4',autocommit=False,connect_timeout=8,read_timeout=15,write_timeout=15) as db:
            with db.cursor() as cur:
                cur.execute('SELECT qualityCheck FROM autosensor WHERE eid=%s FOR UPDATE',(eid,))
                original=cur.fetchone()
                if not original:
                    audit_alarm_rejection('NOT_FOUND','原始目录没有该EID，未修改')
                    return jsonify(error='原始目录没有该点位，未修改'),409
                old_enabled=int(original[0] or 0)==0
                # A durable audit row is a precondition for modifying the source.
                audit_id=alarm_audit.begin(db_connect,eid,enabled,row,old_enabled,settings.get('host'),config()['database'].get('host'),note=str(payload.get('note') or '')[:1000])
                cur.execute('UPDATE autosensor SET qualityCheck=%s WHERE eid=%s',(flag,eid))
            db.commit();source_saved=True
        alarm_audit.source_committed(db_connect,audit_id)
        with db_connect() as db:
            with db.cursor() as cur:
                cur.execute('UPDATE autosensor SET qualityCheck=%s WHERE eid=%s',(flag,eid))
                cur.execute('UPDATE dq_point_quality_current SET quality_check=%s WHERE eid=%s',(flag,eid))
                closed_events=0
                if not enabled:
                    cur.execute("UPDATE dq_quality_event SET closed_at=NOW(3),resolution_note='报警屏蔽关闭（不代表数据恢复）' WHERE eid=%s AND closed_at IS NULL",(eid,))
                    closed_events=cur.rowcount
                result='SUCCESS' if old_enabled!=enabled or row.get('quality_check')!=flag else 'NO_CHANGE'
                alarm_audit.finish_cursor(cur,audit_id,result,source_saved=True,target_saved=True,closed_events=closed_events,note=str(payload.get('note') or ('管理员启用报警' if enabled else '管理员停用报警，采集和趋势保持正常')))
            db.commit();target_saved=True
    except Exception as exc:
        app.logger.exception('Alarm setting failed, audit_id=%s',audit_id)
        try:
            if audit_id:alarm_audit.finish(db_connect,audit_id,'PARTIAL' if source_saved else 'FAILED',source_saved=source_saved,target_saved=target_saved,note='原始目录已保存，目标同步失败' if source_saved else '报警设置保存失败',error_type=type(exc).__name__)
            else:
                audit_id=alarm_audit.begin(db_connect,eid,enabled,row,source_host=settings.get('host'),target_host=config()['database'].get('host'),result='FAILED',note='尚未修改源数据，连接源库或写入审计记录失败')
                alarm_audit.finish(db_connect,audit_id,'FAILED',error_type=type(exc).__name__)
        except Exception:app.logger.exception('Audit storage unavailable; source_saved=%s',source_saved)
        return jsonify(error='原始目录已保存，本机同步未完成；请重试，或等待目录同步' if source_saved else '保存失败（含审计检查），未确认修改；请稍后重试',audit_id=audit_id),503
    try:redis_connect().set(PREFIX+'catalog_revision',str(time.time_ns()))
    except redis.RedisError:
        # Both SQL changes and the success audit are durable; only reload is deferred.
        app.logger.exception('Directory reload notification delayed, audit_id=%s',audit_id)
        return jsonify(success=True,enabled=enabled,audit_id=audit_id,message='已保存；缓存通知暂不可用，等待目录自动重载')
    return jsonify(success=True,enabled=enabled,audit_id=audit_id,message='已启用报警' if enabled else '已停用报警；采集和趋势保持正常')


@app.get('/api/points')
def points():
    source=request.args.get('source','')
    reason=request.args.get('reason','')
    search=request.args.get('search','').strip()[:100]
    status=request.args.get('status','')
    page=max(1,int(request.args.get('page',1)))
    where=[]; params=[]
    if source in ('oa','kepserver'):
        where.append('source=%s');params.append(source)
    if status in ('正常','异常','待判定','报警已屏蔽'):
        where.append('status=%s');params.append(status)
    if reason:
        where.append('JSON_CONTAINS(reasons_json,%s)');params.append(json.dumps(reason,ensure_ascii=False))
    if search:
        like='%'+search.replace('%','\\%').replace('_','\\_')+'%'
        where.append("(eid LIKE %s OR department LIKE %s OR process LIKE %s OR equipment LIKE %s OR device_name LIKE %s OR mqttname LIKE %s)")
        params.extend([like]*6)
    for field in ('eid','department','process','equipment','group','point','mqttname'):
        value=request.args.get(field,'').strip()
        if value:
            where.append('`'+field+'`=%s');params.append(value)
    eids=list(dict.fromkeys(x.lower() for x in re.split(r'[\s,，、;；]+',request.args.get('eids','').strip()) if x))
    if len(eids)>1000:return jsonify(error='每次最多查询1000个EID'),400
    if eids:
        where.append('eid IN ('+','.join(['%s']*len(eids))+')');params.extend(eids)
    clause=(' WHERE '+' AND '.join(where)) if where else ''
    total=query('SELECT COUNT(*) n FROM dq_point_quality_display'+clause,params)[0]['n']
    rows=query('SELECT * FROM dq_point_quality_display'+clause+
               ' ORDER BY status DESC, updated_at DESC, eid LIMIT 50 OFFSET %s',params+[min((page-1)*50,1000000)])
    return jsonify(total=total,page=page,items=live_values(public_rows(rows)))


def read_live_history(eid):
    cache=redis_connect()
    key=PREFIX+'history:'+eid
    # Keep raw samples for cursor inspection and exact chart exports.
    samples=[]
    history_available=True
    try:
        history=cache.xrange(key,min=str(int(time.time()*1000)-config()['history_minutes']*60000)+'-0')
    except redis.RedisError:
        history=[]
        history_available=False
    for _,data in history:
        try:
            t=int(data['t']);v=float(json.loads(data['v']))
            if math.isfinite(v):
                samples.append((t,v))
        except (KeyError,TypeError,ValueError):
            continue
    return samples,history_available


@app.get('/api/points/<eid>/realtime')
def realtime_trend(eid):
    eid=eid.strip().lower()
    try:
        if redis_connect().hget(PREFIX+'catalog',eid) not in ('oa','kepserver'):
            return jsonify(error='point_not_found'),404
    except redis.RedisError:return jsonify(history=[],history_available=False)
    samples,available=read_live_history(eid)
    return jsonify(history=samples,history_available=available)


@app.get('/api/points/<eid>/history')
def aggregate_history(eid):
    from history_service import fetch_history,HistoryError
    eid=eid.strip().lower()
    if not query('SELECT eid FROM dq_point_quality_current WHERE eid=%s',(eid,)):
        return jsonify(error='点位不在当前监测目录'),404
    try:
        data=fetch_history(eid,request.args.get('granularity','minute'),request.args.get('begin',''),request.args.get('end',''))
        response=jsonify(data);response.headers['Cache-Control']='no-store'
        return response
    except ValueError as exc:return jsonify(error=str(exc)),400
    except HistoryError as exc:return jsonify(error=str(exc)),502


@app.get('/api/points/<eid>')
def point_detail(eid):
    eid=eid.strip().lower()
    found=query('SELECT * FROM dq_point_quality_display WHERE eid=%s',(eid,))
    if not found:
        return jsonify(error='point_not_found'),404
    samples,history_available=read_live_history(eid)
    events=query('SELECT id,reason,opened_at,closed_at,resolution_note FROM dq_quality_event WHERE eid=%s AND TIMESTAMPDIFF(MICROSECOND,opened_at,COALESCE(closed_at,NOW(3)))>300000000 ORDER BY opened_at DESC LIMIT 30',(eid,))
    with _snapshot_lock:
        status=copy.deepcopy(_overview_snapshot.get('model'))
        runtime=copy.deepcopy(_overview_snapshot.get('mqtt',{}))
    model=query('SELECT * FROM dq_model_quality_current WHERE is_active=1 AND eid=%s',(eid,)) if (status and status.get('state')=='已推理' and int(runtime.get('last_gap_at') or 0)<=status.get('at',0)) else []
    point=live_values(public_rows(found))[0]
    if point.get('quality_check') and model:
        for row in model:row.update(model_result='检测已停用',health_score=None,message='qualityCheck非0，不判定、不报警；采集与趋势保留')
    receipt={'received_time':point.get('receipt_time_ms'),'source_time':point.get('source_time_ms')}
    return jsonify(point=point,history=samples,history_available=history_available,events=public_rows(events),
                   model=public_rows(model)[0] if model else None,
                   receipt=receipt if point.get('receipt_time_ms') else None)


DEVICE_FIELDS = ('department','process','equipment','group','owner_name')

def device_selection():
    where=[];params=[]
    source=request.args.get('source','')
    if source in ('oa','kepserver'):
        where.append('source=%s');params.append(source)
    for field in DEVICE_FIELDS:
        value=request.args.get(field,'')
        if value:
            where.append('`'+field+'`=%s');params.append(value)
    return (' WHERE '+' AND '.join(where)) if where else '',params

@app.get('/api/device-filter-options')
def device_filter_options():
    fields=('source',*DEVICE_FIELDS)
    values={tuple((part if field=='source' else row.get(field)) for field in fields) for row in business_devices() for part in str(row.get('source') or '').split(',')}
    rows=[dict(zip(fields,item)) for item in sorted(values,key=lambda item:tuple(str(x or '') for x in item))]
    return jsonify(items=public_rows(rows))

@app.get('/api/business-devices/<token>')
def business_device_detail(token):
    if len(token)!=64 or any(c not in '0123456789abcdef' for c in token):return jsonify(error='invalid_device'),400
    device=next((row for row in business_devices() if row['device_token']==token),None)
    if not device:return jsonify(error='device_not_found'),404
    selected_filter=request.args.get('filter','all')
    if selected_filter not in ('all','missing','abnormal'):return jsonify(error='invalid_filter'),400
    missing=selected_filter=='missing'
    page=max(1,int(request.args.get('page',1)))
    clause='display_group_token=%s'
    if missing:clause+=" AND COALESCE(quality_check,0)=0 AND data_status IN ('从未收到','停止发送')"
    if selected_filter=='abnormal':clause+=" AND COALESCE(quality_check,0)=0 AND status='异常'"
    rows=query('SELECT * FROM dq_point_quality_display WHERE '+clause+' ORDER BY source,eid LIMIT 50 OFFSET %s',[token,min((page-1)*50,1000000)])
    items=live_values(public_rows(rows))
    return jsonify(device=public_rows([device])[0],total=int(device['missing_count'] if missing else device['abnormal_count'] if selected_filter=='abnormal' else device['point_count']),page=page,items=items)


@app.get('/api/devices')
def devices():
    page=max(1,int(request.args.get('page',1)))
    rows=business_devices()
    for field in ('source',*DEVICE_FIELDS):
        value=request.args.get(field,'')
        if value:rows=[row for row in rows if (value in str(row.get('source') or '').split(',') if field=='source' else str(row.get(field) or '')==value)]
    data_status=request.args.get('data_status','')
    if data_status:rows=[row for row in rows if row.get('data_status')==data_status]
    rows.sort(key=lambda x:(str(x.get('department') or ''),str(x.get('process') or ''),str(x.get('equipment') or ''),str(x.get('group') or '')))
    return jsonify(total=len(rows),page=page,items=public_rows(rows[min((page-1)*50,1000000):min(page*50,1000050)]))


@app.get('/api/events')
def events():
    source=request.args.get('source','')
    status=request.args.get('status','open')
    page=max(1,int(request.args.get('page',1)))
    where=['TIMESTAMPDIFF(MICROSECOND,opened_at,COALESCE(closed_at,NOW(3)))>300000000'];params=[]
    if source in ('oa','kepserver'):
        where.append('source=%s');params.append(source)
    if status=='open':
        where.append('closed_at IS NULL');where.append('eid IN (SELECT eid FROM dq_point_quality_current WHERE COALESCE(quality_check,0)=0)')
    elif status=='closed':
        where.append('closed_at IS NOT NULL')
    eid=request.args.get('eid','').strip()
    if eid:
        where.append('eid=%s');params.append(eid)
    clause=(' WHERE '+' AND '.join(where)) if where else ''
    total=query('SELECT COUNT(*) n FROM dq_quality_event'+clause,params)[0]['n']
    rows=query('SELECT * FROM dq_quality_event_display'+clause+
               ' ORDER BY opened_at DESC LIMIT 50 OFFSET %s',params+[min((page-1)*50,1000000)])
    return jsonify(total=total,page=page,items=public_rows(rows))


@app.get('/api/rules')
def rules():
    return jsonify(items=public_rows(query('SELECT * FROM dq_quality_rule ORDER BY scope,source,id')))


@app.put('/api/rules')
def save_rule():
    body=request.get_json(silent=True) or {}
    scope=body.get('scope');source=body.get('source');target=str(body.get('target_key','')).strip()
    if scope not in ('global','device','point') or source not in ('oa','kepserver') or (scope=='global' and target) or (scope!='global' and not target):
        return jsonify(error='invalid_rule_scope'),400
    try:
        seconds=int(body['flatline_seconds']);tolerance=float(body.get('tolerance',0))
    except (KeyError,TypeError,ValueError):
        return jsonify(error='invalid_rule_values'),400
    if seconds<60 or seconds>86400 or not math.isfinite(tolerance) or tolerance<0:
        return jsonify(error='invalid_rule_range'),400
    enabled=int(bool(body.get('flatline_enabled',False)))
    excluded=int(bool(body.get('excluded',False)))
    with db_connect() as db:
        with db.cursor() as cur:
            cur.execute("""INSERT INTO dq_quality_rule
                (scope,source,target_key,flatline_enabled,flatline_seconds,tolerance,excluded,updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,NOW(3)) ON DUPLICATE KEY UPDATE
                flatline_enabled=VALUES(flatline_enabled),flatline_seconds=VALUES(flatline_seconds),
                tolerance=VALUES(tolerance),excluded=VALUES(excluded),updated_at=NOW(3)""",
                (scope,source,target,enabled,seconds,tolerance,excluded))
        db.commit()
    return jsonify(saved=True)


@app.get('/api/publication')
def publication_version():
    data=read_publication()
    response=jsonify(version=data['version'] if data else None,
                     kind=data.get('kind') if data else None,
                     published_at=(data.get('summary') or {}).get('published_at') if data else None)
    response.headers['Cache-Control']='no-store'
    return response


@app.get('/api/catalog-sync')
def catalog_sync_status():
    path=ROOT.parent/'点位目录同步'/'status.json'
    if not path.exists():
        return jsonify(status='not_started',interval_seconds=600)
    try:
        data=json.loads(path.read_text(encoding='utf-8'))
        fields=('status','source_host','target_host','target_port','source_rows','inserted','updated',
                'target_only_preserved','interval_seconds','finished_at','failed_at','notification')
        return jsonify({k:data.get(k) for k in fields})
    except (OSError,ValueError):
        return jsonify(status='unavailable',interval_seconds=600)


@app.get('/api/health')
def health():
    try:
        cache=redis_connect();cache.ping()
        with db_connect() as db:
            db.ping()
        summary=json.loads(cache.get(QUALITY+'summary') or 'null')
        fresh=bool(summary and __import__('time').time()*1000-summary['run_at']<30000)
        return jsonify(status='ready' if fresh else 'engine_stale',engine_fresh=fresh),200 if fresh else 503
    except Exception as exc:
        return jsonify(status='unavailable',error=type(exc).__name__),503


from statistics_api import install_statistics
install_statistics(app,query,public_rows,published_cache)


from mqtt_duplicate_api import install_duplicate_api
install_duplicate_api(app,query,public_rows,redis_connect,PREFIX)


import device_manual_reason
device_manual_reason.install(app,db_connect,query)


import account_audit
account_audit.install(app,db_connect)


from collection_health_api import install as install_collection_health
install_collection_health(app,query,public_rows,redis_connect)


from important_logs_api import install as install_important_logs
install_important_logs(app,query,public_rows)

from model_training_api import install as install_model_training
install_model_training(app,query,public_rows)

from trend_export_api import install as install_trend_export
install_trend_export(app,query)

if __name__=='__main__':
    try:
        account_audit.ensure_schema(db_connect)
        alarm_audit.ensure_schema(db_connect)
        device_manual_reason.ensure_schema(db_connect)
    except pymysql.MySQLError:
        app.logger.exception('Database unavailable at web startup; keeping read-only cache and status endpoints available')
    start_overview_poll()
    serve(app,host='0.0.0.0',port=8789,threads=8)
