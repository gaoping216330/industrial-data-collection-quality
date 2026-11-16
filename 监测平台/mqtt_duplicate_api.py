"""Read MQTT duplicate evidence without reading sensor history."""
import json
import time
from flask import jsonify,request
import redis

def install_duplicate_api(app,query,public_rows,connect,prefix):
    @app.get('/api/mqtt-duplicates')
    def mqtt_duplicates():
        if request.args.get('history')=='1':
            page=max(1,int(request.args.get('page',1)));where=[];params=[]
            status=request.args.get('status','')
            if status=='open':where.append('ended_at IS NULL')
            elif status=='closed':where.append('ended_at IS NOT NULL')
            eid=request.args.get('eid','').strip()
            if eid:where.append('eid=%s');params.append(eid)
            clause=' WHERE '+' AND '.join(where) if where else ''
            total=query('SELECT COUNT(*) n FROM dq_mqtt_duplicate_event'+clause,params)[0]['n']
            rows=query('SELECT * FROM dq_mqtt_duplicate_event'+clause+' ORDER BY last_seen_at DESC LIMIT 50 OFFSET %s',params+[min((page-1)*50,1000000)])
            for row in rows:
                row['types']=json.loads(row.pop('types_json') or '[]');row['evidence']=json.loads(row.pop('evidence_json') or '{}');row.pop('event_json',None)
            found=query('SELECT * FROM dq_point_quality_display WHERE eid IN ('+','.join(['%s']*len(rows))+')',[r['eid'] for r in rows]) if rows else []
            metadata={r['eid']:r for r in public_rows(found)}
            rows=[dict(metadata.get(r['eid'],{}),**r) for r in public_rows(rows)]
            return jsonify(available=True,history=True,total=total,page=page,items=rows)
        try:raw,heartbeat=connect().mget([prefix+'mqtt_duplicates',prefix+'mqtt_duplicate_heartbeat'])
        except redis.RedisError:return jsonify(available=False,items=[],total=0,page=1,message='重复检测缓存暂不可用')
        if not raw:return jsonify(available=False,items=[],total=0,page=1,message='重复发送检测正在启动，尚无观测结果')
        snapshot=json.loads(raw);checked=snapshot.get('checked_at',0)
        runtime=json.loads(heartbeat) if heartbeat else {}
        stale=not heartbeat or int(time.time()*1000)-checked>(int(snapshot.get('interval_seconds',600))+30)*1000
        status=dict(snapshot);status.update(runtime)
        rows=sorted(snapshot.get('items',[]),key=lambda x:x['last_received_at'],reverse=True)
        total=len(rows);point_count=len({r['eid'] for r in rows});page=max(1,int(request.args.get('page',1)))
        selected=rows[(page-1)*50:page*50] if request.args.get('summary')!='1' else []
        if selected:
            found=query('SELECT * FROM dq_point_quality_display WHERE eid IN ('+','.join(['%s']*len(selected))+')',[r['eid'] for r in selected])
            by_eid={r['eid']:r for r in public_rows(found)}
            selected=[dict(by_eid.get(r['eid'],{}),**r) for r in selected]
        return jsonify(available=True,stale=stale,total=total,point_count=point_count,page=page,items=selected,started_at=snapshot.get('started_at'),checked_at=checked,window_minutes=10,connected=status.get('connected'),skipped_packets=status.get('skipped_packets',0),pending_packets=status.get('pending_packets',0),observed_points=snapshot.get('observed_points',0),received_packets=status.get('received_packets',0),last_packet_at=status.get('last_packet_at'),interval_seconds=snapshot.get('interval_seconds',600),next_publish_at=snapshot.get('next_publish_at'),collecting=snapshot.get('collecting',False))
