import json
from uuid import uuid4
from flask import request,session
SCHEMA="""CREATE TABLE IF NOT EXISTS dq_admin_account_audit (
 id char(36) PRIMARY KEY,action varchar(64),target_username varchar(128),actor varchar(128),
 client_ip varchar(64),details_json text,result varchar(32),requested_at datetime(3),finished_at datetime(3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""
def ensure_schema(connect):
    with connect() as db:
        with db.cursor() as cur:cur.execute(SCHEMA)
        db.commit()
def install(app,connect):
    def record(action,target,details,result,audit_id=None):
        with connect() as db:
            with db.cursor() as cur:
                if audit_id:cur.execute('UPDATE dq_admin_account_audit SET result=%s,finished_at=NOW(3) WHERE id=%s',(result,audit_id))
                else:
                    audit_id=str(uuid4());cur.execute('INSERT INTO dq_admin_account_audit (id,action,target_username,actor,client_ip,details_json,result,requested_at) VALUES (%s,%s,%s,%s,%s,%s,%s,NOW(3))',(audit_id,action,target,session.get('username'),request.remote_addr,json.dumps(details,ensure_ascii=False),result))
            db.commit()
        return audit_id
    app.config['AUDIT_ACCOUNT_CHANGE']=record
