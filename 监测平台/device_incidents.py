"""One incident per business device; count changes never generate extra log entries."""
import json,uuid
from datetime import datetime,timedelta

SCHEMA="""CREATE TABLE IF NOT EXISTS dq_device_incident (
 id char(32) PRIMARY KEY,device_token char(64) NOT NULL,active_key char(64) UNIQUE,
 source varchar(20) NOT NULL,department varchar(255),process varchar(255),equipment varchar(255),`group` varchar(255),
 reason varchar(512) NOT NULL,affected_count int NOT NULL,opened_at datetime(3) NOT NULL,closed_at datetime(3),
 resolution_note varchar(128),KEY ix_device_incident_time(opened_at),KEY ix_device_incident_token(device_token,closed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"""

def summarize(eids,states,pending):
    enabled=[states[eid] for eid in eids if states[eid]['row'].get('quality_check')!=1]
    affected=[x for x in enabled if x['reasons']-pending]
    reasons=sorted(set().union(*(x['reasons']-pending for x in affected))) if affected else []
    missing=sum(bool((x['reasons']-pending)&{'停止发送','从未收到'}) for x in enabled)
    label='设备掉线' if enabled and missing==len(enabled) else '设备异常'
    return affected,label+'：'+'、'.join(reasons),not enabled

def record_devices(cur,devices,states,health,pending,now_ms):
    cur.execute('SELECT * FROM dq_device_incident WHERE closed_at IS NULL')
    active={r['device_token']:r for r in cur.fetchall()}
    cur.execute('SELECT p.display_group_token token,MIN(CASE WHEN e.closed_at IS NULL THEN e.opened_at END) opened_at,MAX(e.closed_at) closed_at FROM dq_quality_event e JOIN dq_point_quality_display p ON p.eid=e.eid WHERE e.closed_at IS NULL OR e.closed_at>=%s GROUP BY p.display_group_token',(datetime.fromtimestamp(now_ms/1000)-timedelta(minutes=10),))
    times={r['token']:r for r in cur.fetchall()}
    now=datetime.fromtimestamp(now_ms/1000)
    for token,(eids,_) in devices.items():
        if any(health['items'][states[eid]['row']['source']]['blocked'] for eid in eids):continue
        affected,reason,muted=summarize(eids,states,pending)
        previous=active.get(token)
        if affected and not previous:
            row=states[eids[0]]['row'];sources={states[eid]['row']['source'] for eid in eids}
            opened=times.get(token,{}).get('opened_at') or now
            # On first deployment this is a summary of existing confirmed incidents, not a new physical outage.
            cur.execute('INSERT INTO dq_device_incident (id,device_token,active_key,source,department,process,equipment,`group`,reason,affected_count,opened_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                        (uuid.uuid4().hex,token,token,next(iter(sources)) if len(sources)==1 else 'mixed',row.get('department'),row.get('process'),row.get('equipment'),row.get('group'),reason[:512],len(affected),opened))
        elif affected and previous:
            peak=max(previous['affected_count'],len(affected))
            if previous['reason']!=reason[:512] or peak!=previous['affected_count']:
                cur.execute('UPDATE dq_device_incident SET reason=%s,affected_count=%s WHERE id=%s',(reason[:512],peak,previous['id']))
        elif previous:
            closed=now if muted else max(previous['opened_at'],times.get(token,{}).get('closed_at') or now)
            note='检测已停用，未证明现场恢复' if muted else '连续正常30秒，确认恢复'
            cur.execute('UPDATE dq_device_incident SET closed_at=%s,active_key=NULL,resolution_note=%s WHERE id=%s',(closed,note,previous['id']))
    for token,previous in active.items():
        if token not in devices:
            cur.execute('UPDATE dq_device_incident SET closed_at=%s,active_key=NULL,resolution_note=%s WHERE id=%s',(now,'目录已变更，结束跟踪；未证明现场恢复',previous['id']))
