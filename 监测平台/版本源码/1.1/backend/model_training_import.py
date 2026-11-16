"""Import immutable training evidence; live sensor metadata stays joined by EID."""
import ast,csv,json,math,hashlib
from pathlib import Path
from datetime import datetime
from quality_engine import db_connect
ROOT=Path(__file__).resolve().parent
ARTIFACT=ROOT.parent/'outputs/all_sensor_typeai_ptf/models_53d'
SCHEMA="""CREATE TABLE IF NOT EXISTS dq_model_training_info (
 eid varchar(128) PRIMARY KEY,training_status varchar(64),model_generated tinyint NOT NULL DEFAULT 0,
 model_kind varchar(64),deployment_status varchar(64),confidence_grade varchar(16),target_role varchar(64),
 group_id varchar(128),group_name varchar(512),sensor_type varchar(128),peer_count int,
 peer_eids_json text,peer_correlations_json text,state_eids_json text,availability double,validation_mae double,test_mae double,
 test_raw_anomaly_rate double,test_confirmed_anomaly_rate double,history_bounds_json text,
 derivative_limit double,history_confirmed_points bigint,training_reason text,
 training_created_at varchar(64),bin_seconds int,artifact_path varchar(512),artifact_sha256 char(64),
 raw_training_json mediumtext,imported_at datetime(3) NOT NULL,
 KEY ix_training_deployment(deployment_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"""

def literal(raw,default):
    if not raw:return default
    try:return ast.literal_eval(raw)
    except (SyntaxError,ValueError):return default

def number(raw):
    try:
        v=float(raw);return v if math.isfinite(v) else None
    except (ValueError,TypeError):return None

def load_rows():
    manifest=json.loads((ARTIFACT/'manifest.json').read_text(encoding='utf-8-sig'))
    digest=hashlib.sha256((ARTIFACT/'model_summary.csv').read_bytes()).hexdigest()
    rows=[];peers=[]
    with (ARTIFACT/'model_summary.csv').open(encoding='utf-8-sig',newline='') as f:
        for r in csv.DictReader(f):
            peer_eids=literal(r.get('peer_eids'),[]);correlations=literal(r.get('peer_correlations'),{})
            for i,eid in enumerate(peer_eids):peers.append((r['eid'],str(eid),i+1,number(correlations.get(eid))))
            rows.append((r['eid'],r.get('status'),int(bool(r.get('model_kind'))),r.get('model_kind'),r.get('deployment_status'),r.get('confidence_grade'),r.get('target_role'),r.get('group_id'),r.get('group_name'),r.get('type'),len(peer_eids),json.dumps(peer_eids),json.dumps(correlations),number(r.get('availability')),number(r.get('validation_mae')),number(r.get('test_mae')),number(r.get('test_raw_anomaly_rate')),number(r.get('test_confirmed_anomaly_rate')),json.dumps(literal(r.get('history_bounds'),[])),number(r.get('derivative_limit')),int(number(r.get('history_confirmed_points')) or 0),r.get('reason'),manifest.get('created_at'),manifest.get('bin_seconds'),str(ARTIFACT),digest,json.dumps(r,ensure_ascii=False),datetime.now()))
    return rows,peers

def main():
    rows,peers=load_rows()
    fields=['eid','training_status','model_generated','model_kind','deployment_status','confidence_grade','target_role','group_id','group_name','sensor_type','peer_count','peer_eids_json','peer_correlations_json','availability','validation_mae','test_mae','test_raw_anomaly_rate','test_confirmed_anomaly_rate','history_bounds_json','derivative_limit','history_confirmed_points','training_reason','training_created_at','bin_seconds','artifact_path','artifact_sha256','raw_training_json','imported_at']
    with db_connect() as db:
        with db.cursor() as c:
            c.execute(SCHEMA)
            c.execute("CREATE TABLE IF NOT EXISTS dq_model_training_peer (eid varchar(128) NOT NULL,peer_eid varchar(128) NOT NULL,rank_no int NOT NULL,correlation double,PRIMARY KEY(eid,peer_eid),KEY ix_peer(peer_eid)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci")
            sql='INSERT INTO dq_model_training_info ('+','.join(fields)+') VALUES ('+','.join(['%s']*len(fields))+') ON DUPLICATE KEY UPDATE '+','.join(k+'=VALUES('+k+')' for k in fields if k!='eid')
            for offset in range(0,len(rows),500):c.executemany(sql,rows[offset:offset+500])
            c.execute('DELETE FROM dq_model_training_peer')
            for offset in range(0,len(peers),500):c.executemany('INSERT INTO dq_model_training_peer(eid,peer_eid,rank_no,correlation) VALUES(%s,%s,%s,%s)',peers[offset:offset+500])
            c.execute("""CREATE OR REPLACE VIEW autosensorAI_training AS SELECT a.eid,a.company,a.department,a.process,a.equipment,a.`group`,a.point,a.model,a.tag,a.did,a.channel,a.qualityCheck,
 t.training_status,t.model_generated,t.model_kind,t.deployment_status,t.confidence_grade,t.target_role,t.group_id,t.group_name,t.sensor_type,t.peer_count,t.state_eids_json,t.peer_eids_json,t.peer_correlations_json,t.availability,t.validation_mae,t.test_mae,t.test_raw_anomaly_rate,t.test_confirmed_anomaly_rate,t.history_bounds_json,t.derivative_limit,t.history_confirmed_points,t.training_reason,t.training_created_at,t.bin_seconds,t.artifact_path,t.artifact_sha256,t.imported_at,
 CASE WHEN t.eid IS NULL THEN '未纳入本次训练清单' WHEN t.deployment_status='ready' THEN '可上线' WHEN t.deployment_status='observation_only' THEN '观察模式' WHEN t.deployment_status='needs_recalibration' THEN '需要校准' ELSE '样本或关联不足' END model_status_ai
 FROM autosensor a LEFT JOIN dq_model_training_info t ON t.eid=CONVERT(a.eid USING utf8mb4) COLLATE utf8mb4_unicode_ci""")
        db.commit()
    print(json.dumps(dict(training_rows=len(rows),generated_models=sum(r[2] for r in rows),validated_models=sum(r[4]=='ready' for r in rows),peer_links=len(peers)),ensure_ascii=False))

if __name__=='__main__':main()
