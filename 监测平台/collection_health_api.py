"""Read-only collection health and paginated incident drill-down APIs."""
import json,time
from flask import jsonify,request
from collection_health import receiver_ready

def install(app,query,public_rows,redis_connect):
    @app.get('/api/collection-health')
    def collection_health():
        now=int(time.time()*1000);dependencies={};snapshot={}
        try:
            rows=query("SELECT observed_ms,payload_json FROM dq_collection_health WHERE name='current'")
            dependencies['database']=True
            if rows:snapshot=json.loads(rows[0]['payload_json'])
        except Exception:dependencies['database']=False
        try:dependencies['redis']=bool(redis_connect().ping())
        except Exception:dependencies['redis']=False
        dependencies['receiver']=receiver_ready()
        age=now-int(snapshot.get('observed_ms') or 0)
        snapshot.update(dependencies=dependencies,monitor_stale=age>40000,now_ms=now)
        if age>40000 or not all(dependencies.values()):
            for item in snapshot.get('items',{}).values():
                item.update(state='采集未知',blocked=True,reason='监测或采集服务不可用，现场状态未知')
        return jsonify(snapshot)

    @app.get('/api/collection-events')
    def collection_events():
        source=request.args.get('source','');scope=request.args.get('scope','');status=request.args.get('status','open')
        page=max(1,int(request.args.get('page',1)));where=[];params=[]
        if source in ('oa','kepserver','platform'):where.append('source=%s');params.append(source)
        if scope=='platform':where.append("source='platform'")
        if scope=='link':where.append("source<>'platform'")
        if status=='open':where.append('closed_at IS NULL')
        elif status=='closed':where.append('closed_at IS NOT NULL')
        clause=' WHERE '+' AND '.join(where) if where else ''
        total=query('SELECT COUNT(*) n FROM dq_collection_event'+clause,params)[0]['n']
        rows=query('SELECT * FROM dq_collection_event'+clause+' ORDER BY opened_at DESC LIMIT 50 OFFSET %s',params+[min((page-1)*50,1000000)])
        return jsonify(items=public_rows(rows),total=total,page=page)

    @app.get('/api/collection-events/<event_id>/points')
    def collection_points(event_id):
        page=max(1,int(request.args.get('page',1)))
        if len(event_id)!=32 or any(c not in '0123456789abcdef' for c in event_id):return jsonify(error='事件标识无效'),400
        token=request.args.get('device','')
        clause='a.event_id=%s';params=[event_id]
        if token:
            if len(token)!=64 or any(c not in '0123456789abcdef' for c in token):return jsonify(error='设备标识无效'),400
            clause+=' AND p.display_group_token=%s';params.append(token)
        total=query('SELECT COUNT(*) n FROM dq_collection_event_point a LEFT JOIN dq_point_quality_display p ON p.eid=a.eid WHERE '+clause,params)[0]['n']
        rows=query('SELECT a.eid,p.department,p.process,p.equipment,p.`group`,p.point,p.owner_name,p.source,p.mqttname,p.dpname,p.display_group_token FROM dq_collection_event_point a LEFT JOIN dq_point_quality_display p ON p.eid=a.eid WHERE '+clause+' ORDER BY p.department,p.process,p.equipment,a.eid LIMIT 50 OFFSET %s',params+[min((page-1)*50,1000000)])
        return jsonify(items=public_rows(rows),total=total,page=page)

    @app.get('/api/collection-events/<event_id>/devices')
    def collection_devices(event_id):
        page=max(1,int(request.args.get('page',1)))
        if len(event_id)!=32 or any(c not in '0123456789abcdef' for c in event_id):return jsonify(error='事件标识无效'),400
        base=' FROM dq_collection_event_point a LEFT JOIN dq_point_quality_display p ON p.eid=a.eid WHERE a.event_id=%s'
        total=query('SELECT COUNT(*) n FROM (SELECT p.display_group_token'+base+' GROUP BY p.display_group_token) g',[event_id])[0]['n']
        rows=query('SELECT p.display_group_token,MAX(p.department) department,MAX(p.process) process,MAX(p.equipment) equipment,MAX(p.`group`) `group`,COUNT(*) affected_count,GROUP_CONCAT(DISTINCT p.owner_name) owner_name'+base+' GROUP BY p.display_group_token ORDER BY department,process,equipment LIMIT 50 OFFSET %s',[event_id,min((page-1)*50,1000000)])
        return jsonify(items=public_rows(rows),total=total,page=page)
