"""Durable alarm-operation audit, committed before changing the source catalog."""
from uuid import uuid4
from flask import request,session

SCHEMA="""CREATE TABLE IF NOT EXISTS dq_alarm_setting_audit (
 id char(36) NOT NULL PRIMARY KEY,
 requested_at datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
 source_committed_at datetime(3) NULL,finished_at datetime(3) NULL,
 eid varchar(128) NOT NULL,point varchar(512) NULL,
 department varchar(255) NULL,process varchar(255) NULL,equipment varchar(255) NULL,`group` varchar(255) NULL,
 source varchar(32) NULL,owner_name varchar(255) NULL,
 actor varchar(128) NOT NULL,actor_role varchar(32) NOT NULL,login_at varchar(64) NULL,
 client_ip varchar(64) NULL,user_agent varchar(512) NULL,
 old_enabled tinyint NULL,local_old_enabled tinyint NULL,new_enabled tinyint NULL,
 action varchar(32) NOT NULL,result varchar(32) NOT NULL,
 source_saved tinyint NOT NULL DEFAULT 0,target_saved tinyint NOT NULL DEFAULT 0,
 source_host varchar(64) NULL,target_host varchar(64) NULL,
 closed_events int NOT NULL DEFAULT 0,note varchar(1000) NULL,error_type varchar(128) NULL,
 KEY ix_eid_time(eid,requested_at),KEY ix_actor_time(actor,requested_at),KEY ix_result_time(result,requested_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""


def ensure_schema(connect):
    with connect() as db:
        with db.cursor() as cur:cur.execute(SCHEMA)
        db.commit()


def begin(connect,eid,new_enabled,row=None,old_enabled=None,source_host=None,target_host=None,result='REQUESTED',note=''):
    row=row or {};audit_id=str(uuid4())
    fields=['id','eid','point','department','process','equipment','group','source','owner_name','actor','actor_role','login_at','client_ip','user_agent','old_enabled','local_old_enabled','new_enabled','action','result','source_host','target_host','note']
    values=[audit_id,eid,row.get('point'),row.get('department'),row.get('process'),row.get('equipment'),row.get('group'),row.get('source'),row.get('owner_name'),session.get('username','admin' if session.get('admin') else 'anonymous'),session.get('role','administrator') if session.get('admin') else 'anonymous',session.get('login_at'),request.remote_addr,request.headers.get('User-Agent','')[:512],None if old_enabled is None else int(old_enabled),None if 'quality_check' not in row else int(row['quality_check']!=1),None if new_enabled is None else int(new_enabled),'ENABLE' if new_enabled is True else 'DISABLE' if new_enabled is False else 'INVALID',result,source_host,target_host,note[:1000]]
    with connect() as db:
        with db.cursor() as cur:cur.execute('INSERT INTO dq_alarm_setting_audit ('+','.join('`'+f+'`' for f in fields)+') VALUES ('+','.join(['%s']*len(fields))+')',values)
        db.commit()
    return audit_id


def finish_cursor(cur,audit_id,result,source_saved=False,target_saved=False,closed_events=0,note='',error_type=None):
    cur.execute('UPDATE dq_alarm_setting_audit SET result=%s,source_saved=%s,target_saved=%s,closed_events=%s,note=%s,error_type=%s,finished_at=NOW(3) WHERE id=%s',(result,int(source_saved),int(target_saved),closed_events,note[:1000],error_type,audit_id))


def finish(connect,audit_id,result,**kwargs):
    with connect() as db:
        with db.cursor() as cur:finish_cursor(cur,audit_id,result,**kwargs)
        db.commit()


def source_committed(connect,audit_id):
    with connect() as db:
        with db.cursor() as cur:cur.execute("UPDATE dq_alarm_setting_audit SET source_saved=1,source_committed_at=NOW(3),result='SOURCE_SAVED' WHERE id=%s",(audit_id,))
        db.commit()
