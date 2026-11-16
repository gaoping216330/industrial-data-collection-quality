"""Reconcile display metadata and retire obsolete monitoring rows, not source rows."""
import json
from display_group import display_group_token

DIRECTORY_PENDING = '目录已变更，等待新来源数据'


def reconcile(cur, roster, now, allow_empty=False):
    if not roster and not allow_empty:
        raise ValueError('Empty monitoring directory; retaining previous state')
    cur.execute('SELECT * FROM dq_point_quality_current')
    previous_rows={r['eid']:r for r in cur.fetchall()}
    previous={eid:(r['source'],r['device_key'],int(r.get('quality_check') or 0)) for eid,r in previous_rows.items()}
    identity={eid:(r['source'],r['device_key'],int(r.get('quality_check') or 0)) for eid,r in roster.items()}
    reset={eid for eid,old in previous.items() if identity.get(eid)!=old}
    cur.execute('CREATE TEMPORARY TABLE dq_catalog_stage (eid varchar(128) PRIMARY KEY,source varchar(16),device_key varchar(600),alarm_muted tinyint) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci')
    for offset in range(0,len(roster),500):
        batch=list(roster.values())[offset:offset+500]
        cur.executemany('INSERT INTO dq_catalog_stage (eid,source,device_key,alarm_muted) VALUES (%s,%s,%s,%s)',
                        [(r['eid'],r['source'],r['device_key'],int(r.get('quality_check') or 0)) for r in batch])
    cur.execute("""UPDATE dq_quality_event e LEFT JOIN dq_catalog_stage c ON c.eid=e.eid
        SET e.closed_at=%s,e.resolution_note=IF(c.alarm_muted=1,'报警屏蔽关闭（不代表数据恢复）','目录调整关闭：点位退出监测、来源或设备归属变更')
        WHERE e.closed_at IS NULL AND (c.alarm_muted=1 OR c.eid IS NULL OR e.source<>c.source OR NOT(e.device_key <=> c.device_key))""",(now,))
    cur.execute('DELETE p FROM dq_point_quality_current p LEFT JOIN dq_catalog_stage c ON c.eid=p.eid WHERE c.eid IS NULL')
    metadata=['eid','source','department','process','equipment','group','point','mqttname','channel','dpname','device_name','tag_type','device_key','owner_code','tag','quality_check','display_group_token']
    columns=metadata+['status','reasons_json','updated_at']
    changed='NOT(source <=> VALUES(source) AND device_key <=> VALUES(device_key))'
    assignments=[f'{name}=IF({changed},VALUES({name}),{name})' for name in ['status','reasons_json']]
    assignments += [f'{name}=IF({changed},NULL,{name})' for name in ['receipt_time_ms','source_time_ms','value_change_time_ms','value_text','quality']]
    assignments += [f'`{name}`=VALUES(`{name}`)' for name in metadata if name!='eid']
    assignments += ['updated_at=VALUES(updated_at)']
    sql='INSERT INTO dq_point_quality_current ('+','.join('`'+c+'`' for c in columns)+') VALUES ('+','.join(['%s']*len(columns))+') ON DUPLICATE KEY UPDATE '+','.join(assignments)
    rows=[]
    for r in roster.values():
        item=dict(r,display_group_token=display_group_token(r),quality_check=int(r.get('quality_check') or 0),owner_code=str(r.get('createdBy') or '').strip().lower())
        old=previous_rows.get(r['eid'])
        if old and all(str(old.get(c) if old.get(c) is not None else '')==str(item.get(c) if item.get(c) is not None else '') for c in metadata):
            continue
        rows.append(tuple(item.get(c) for c in metadata)+('待判定',json.dumps([DIRECTORY_PENDING],ensure_ascii=False),now))
    for offset in range(0,len(rows),500):
        cur.executemany(sql,rows[offset:offset+500])
    cur.execute('DELETE d FROM dq_device_quality_current d LEFT JOIN (SELECT DISTINCT display_group_token device_key FROM dq_point_quality_current) c ON c.device_key=d.device_key WHERE c.device_key IS NULL')
    cur.execute("SHOW TABLES LIKE 'dq_model_quality_current'")
    if cur.fetchone():
        cur.execute("UPDATE dq_model_quality_current m LEFT JOIN dq_catalog_stage c ON c.eid=m.eid SET m.model_result='无法判断',m.status_code='OUTSIDE_CATALOG',m.health_score=NULL,m.message='点位不在当前监测目录，模型保留等待数据' WHERE c.eid IS NULL")
        if reset:
            ids=sorted(reset)
            cur.execute("UPDATE dq_model_quality_current SET model_result='无法判断',status_code='DIRECTORY_CHANGED',health_score=NULL,message='目录来源或设备归属已变更，等待新一轮模型推理' WHERE eid IN ("+','.join(['%s']*len(ids))+')',ids)
    return reset
