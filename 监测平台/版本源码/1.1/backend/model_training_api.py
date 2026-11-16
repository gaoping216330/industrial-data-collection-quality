"""Model listing selection and normalized training evidence APIs."""
import json,math
from flask import request,jsonify

def model_selection(args):
    clauses=["m.is_active=1","m.deployment_status='ready'"];params=[]
    for field in ('department','process','equipment','owner_name'):
        value=args.get(field,'').strip()
        if value:
            clauses.append(('p.owner_name' if field=='owner_name' else 'a.`'+field+'`')+'=%s');params.append(value)
    result=args.get('result','')
    if result in ('正常','异常','无法判断'):
        clauses+=['COALESCE(a.qualityCheck,0)=0','m.model_result=%s'];params.append(result)
    elif result=='检测已停用':clauses.append('COALESCE(a.qualityCheck,0)<>0')
    eid=args.get('eid','').strip()
    if eid:clauses.append('m.eid=%s');params.append(eid)
    for field,op in [('health_min','>='),('health_max','<=')]:
        value=args.get(field,'')
        if value:
            try:n=float(value)
            except ValueError:raise ValueError('健康度请输入0到100的数字')
            if not math.isfinite(n) or not 0<=n<=100:raise ValueError('健康度范围为0到100')
            clauses+=['COALESCE(a.qualityCheck,0)=0','m.health_score'+op+'%s'];params.append(n)
    direction='DESC' if args.get('direction','asc')=='desc' else 'ASC'
    health='CASE WHEN COALESCE(a.qualityCheck,0)=0 THEN m.health_score ELSE NULL END'
    order='('+health+' IS NULL) ASC,'+health+' '+direction+',m.eid' if args.get('sort')=='health' else 'm.detected_at DESC,m.eid'
    return ' AND '.join(clauses),params,order

def install(app,query,public_rows):
    @app.get('/api/model-training/<eid>')
    def model_training(eid):
        rows=query('SELECT * FROM autosensorAI_training WHERE eid=%s',[eid])
        if not rows:return jsonify(error='点位不存在'),404
        row=rows[0]
        for key in ('peer_eids_json','peer_correlations_json','history_bounds_json','state_eids_json'):
            row[key[:-5]]=json.loads(row.get(key) or ('{}' if 'correlations' in key else '[]'))
        peers=query('SELECT r.peer_eid eid,r.rank_no,r.correlation,r.relation_role,a.department,a.process,a.equipment,a.`group`,a.point,a.channel,a.tag,a.qualityCheck FROM dq_model_training_peer r LEFT JOIN autosensor a ON a.eid=CONVERT(r.peer_eid USING utf8) COLLATE utf8_general_ci WHERE r.eid=%s ORDER BY r.rank_no',[eid])
        return jsonify(training=public_rows([row])[0],peers=public_rows(peers))
