"""Single device note inherited by all associated EIDs, with edit history."""
from flask import jsonify,request,session

SCHEMA=["""CREATE TABLE IF NOT EXISTS dq_device_manual_reason (
 device_token char(64) PRIMARY KEY,reason varchar(1000) NOT NULL,
 department varchar(255),process varchar(255),equipment varchar(255),`group` varchar(255),
 updated_by varchar(128),updated_at datetime(3) NOT NULL,client_ip varchar(64)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
"""CREATE TABLE IF NOT EXISTS dq_device_manual_reason_audit (
 id bigint AUTO_INCREMENT PRIMARY KEY,device_token char(64) NOT NULL,
 old_reason varchar(1000),new_reason varchar(1000),actor varchar(128),client_ip varchar(64),
 changed_at datetime(3) NOT NULL,KEY ix_device_time(device_token,changed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""]

def ensure_schema(connect):
    with connect() as db:
        with db.cursor() as cur:
            for sql in SCHEMA:cur.execute(sql)
        db.commit()

def enrich(rows,query):
    missing=list({r['eid'] for r in rows if r.get('eid') and not (r.get('display_group_token') or r.get('device_token'))})
    by_eid={}
    if missing:
        found=query('SELECT eid,display_group_token FROM dq_point_quality_current WHERE eid IN ('+','.join(['%s']*len(missing))+')',missing)
        by_eid={r['eid']:r['display_group_token'] for r in found}
    tokens=list({r.get('display_group_token') or r.get('device_token') or by_eid.get(r.get('eid')) for r in rows}-{None,''})
    if not tokens:return rows
    notes=query('SELECT device_token,reason,updated_by,updated_at FROM dq_device_manual_reason WHERE device_token IN ('+','.join(['%s']*len(tokens))+')',tokens)
    lookup={r['device_token']:r for r in notes}
    for row in rows:
        token=row.get('display_group_token') or row.get('device_token') or by_eid.get(row.get('eid'));note=lookup.get(token,{})
        row['manual_reason']=note.get('reason','');row['manual_reason_by']=note.get('updated_by')
        stamp=note.get('updated_at');row['manual_reason_at']=stamp.isoformat(sep=' ') if stamp else None
    return rows

def install(app,connect,query):
    @app.put('/api/devices/<token>/manual-reason')
    def save_manual_reason(token):
        if not session.get('admin'):return jsonify(error='仅管理员可以填写'),401
        if len(token)!=64 or any(c not in '0123456789abcdef' for c in token):return jsonify(error='设备标识无效'),400
        data=request.get_json(silent=True) or {};reason=data.get('reason')
        if not isinstance(reason,str) or len(reason)>1000:return jsonify(error='原因必须是1000字以内的文本'),400
        reason=reason.strip();actor=session.get('username','admin');ip=request.remote_addr
        rows=query('SELECT department,process,equipment,`group` FROM dq_point_quality_current WHERE display_group_token=%s LIMIT 1',(token,))
        if not rows:return jsonify(error='设备不存在'),404
        row=rows[0]
        with connect() as db:
            with db.cursor() as cur:
                cur.execute("INSERT IGNORE INTO dq_device_manual_reason (device_token,reason,updated_at) VALUES (%s,'',NOW(3))",(token,))
                cur.execute('SELECT reason FROM dq_device_manual_reason WHERE device_token=%s FOR UPDATE',(token,));old=cur.fetchone()['reason']
                cur.execute('UPDATE dq_device_manual_reason SET reason=%s,department=%s,process=%s,equipment=%s,`group`=%s,updated_by=%s,updated_at=NOW(3),client_ip=%s WHERE device_token=%s',(reason,row['department'],row['process'],row['equipment'],row['group'],actor,ip,token))
                cur.execute('INSERT INTO dq_device_manual_reason_audit (device_token,old_reason,new_reason,actor,client_ip,changed_at) VALUES (%s,%s,%s,%s,%s,NOW(3))',(token,old,reason,actor,ip))
            db.commit()
        return jsonify(success=True,reason=reason,updated_by=actor)
