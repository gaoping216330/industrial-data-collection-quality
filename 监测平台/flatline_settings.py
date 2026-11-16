"""Independent local flatline opt-out; upstream catalog replication preserves it."""
import json
import time
from flask import jsonify, request, session


def ensure_schema(connect):
    with connect() as db:
        with db.cursor() as cur:
            for table,column,definition in [('autosensor','flatlineCheck','INT NOT NULL DEFAULT 0'),('dq_point_quality_current','flatline_disabled','tinyint NOT NULL DEFAULT 0')]:
                cur.execute('SHOW TABLES LIKE %s',(table,))
                if not cur.fetchone():continue
                cur.execute('SHOW COLUMNS FROM `'+table+'` LIKE %s',(column,))
                if not cur.fetchone():cur.execute('ALTER TABLE `'+table+'` ADD COLUMN `'+column+'` '+definition)
            cur.execute('''CREATE TABLE IF NOT EXISTS dq_flatline_setting_audit (
                id bigint AUTO_INCREMENT PRIMARY KEY,eid varchar(128) NOT NULL,
                old_value int NOT NULL,new_value int NOT NULL,actor varchar(128),
                client_ip varchar(64),changed_at datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                INDEX(eid,changed_at)) CHARACTER SET utf8mb4''')
        db.commit()


def install(app,connect,redis_connect,prefix):
    @app.get('/api/flatline-settings')
    def flags():
        ids=list(dict.fromkeys(request.args.get('eids','').split(',')))
        if not ids or len(ids)>500:return jsonify(items=[])
        with connect() as db:
            with db.cursor() as cur:
                cur.execute('SELECT eid,flatlineCheck FROM autosensor WHERE eid IN ('+','.join(['%s']*len(ids))+')',ids)
                return jsonify(items=[dict(eid=r['eid'],disabled=int(r['flatlineCheck'] or 0)!=0) for r in cur.fetchall()])

    @app.put('/api/points/<eid>/flatline')
    def update(eid):
        if not session.get('admin'):return jsonify(error='仅管理员可修改'),401
        payload=request.get_json(silent=True) or {}
        if type(payload.get('enabled')) is not bool:return jsonify(error='enabled必须是布尔值'),400
        flag=0 if payload['enabled'] else 1
        with connect() as db:
            with db.cursor() as cur:
                cur.execute('SELECT flatlineCheck FROM autosensor WHERE eid=%s FOR UPDATE',(eid,))
                row=cur.fetchone()
                if not row:return jsonify(error='点位不存在'),404
                cur.execute('UPDATE autosensor SET flatlineCheck=%s WHERE eid=%s',(flag,eid))
                cur.execute('UPDATE dq_point_quality_current SET flatline_disabled=%s WHERE eid=%s',(flag,eid))
                cur.execute('INSERT INTO dq_flatline_setting_audit (eid,old_value,new_value,actor,client_ip) VALUES (%s,%s,%s,%s,%s)',(eid,int(row['flatlineCheck'] or 0),flag,session.get('username'),request.remote_addr))
                audit_id=cur.lastrowid
                if flag:
                    cur.execute("UPDATE dq_quality_event SET closed_at=NOW(3),resolution_note='恒值检测停用，结束跟踪（不代表数据恢复）' WHERE eid=%s AND reason='设备恒值异常' AND closed_at IS NULL",(eid,))
            db.commit()
        try:redis_connect().set(prefix+'catalog_revision',str(time.time_ns()))
        except Exception:app.logger.exception('Flatline flag saved; catalog notification deferred')
        return jsonify(success=True,enabled=not flag,audit_id=audit_id)
