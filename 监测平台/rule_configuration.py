"""Selectable business-device/point flatline configuration and change history."""
import json
from flask import jsonify,request,session
from quality_engine import effective_rule,is_numeric

AUDIT_SCHEMA="""CREATE TABLE IF NOT EXISTS dq_quality_rule_audit (
 id bigint AUTO_INCREMENT PRIMARY KEY,action varchar(32),rule_id bigint,
 actor varchar(128),client_ip varchar(64),old_json mediumtext,new_json mediumtext,
 changed_at datetime(3) NOT NULL,KEY ix_rule_time(rule_id,changed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"""
FIELDS=('department','process','equipment','group')

def filter_rows(rows,args):
    return [r for r in rows if all(not args.get(f) or str(r.get(f) or '')==args[f] for f in ('source',*FIELDS))]

def target_rows(rows,scope,source,target):
    rows=[r for r in rows if r['source']==source]
    if scope=='global':return rows if not target else []
    if scope=='point':return [r for r in rows if r['eid']==target]
    if scope=='device':
        if target.startswith('business:'):return [r for r in rows if r['display_group_token']==target[9:]]
        return [r for r in rows if r['device_key']==target]
    return []

def target_label(row,point=False):
    parts=[str(row.get(f) or '—') for f in FIELDS]
    if point:parts.extend((row['eid'],str(row.get('point') or '—')))
    return ' / '.join(parts)

def describe_rules(rules,rows):
    targets={}
    for row in rows:
        for scope,key in [('point',row['eid']),('device',row['device_key']),('device','business:'+row['display_group_token'])]:
            targets.setdefault((scope,row['source'],key),row)
    for rule in rules:
        row=targets.get((rule['scope'],rule['source'],rule['target_key']))
        rule['target_label']=('全部 '+('OA' if rule['source']=='oa' else 'KEP')+' 点位') if rule['scope']=='global' else target_label(row,rule['scope']=='point') if row else '目录未匹配：'+rule['target_key']
    return rules

def audit(cur,action,old,new):
    row=new or old
    cur.execute('INSERT INTO dq_quality_rule_audit(action,rule_id,actor,client_ip,old_json,new_json,changed_at) VALUES(%s,%s,%s,%s,%s,%s,NOW(3))',
        (action,row.get('id'),session.get('username','unknown'),request.remote_addr,
         json.dumps(old,ensure_ascii=False,default=str) if old else None,json.dumps(new,ensure_ascii=False,default=str) if new else None))

def ensure_schema(connect):
    with connect() as db:
        with db.cursor() as cur:cur.execute(AUDIT_SCHEMA)
        db.commit()

def install(app,query,public_rows,published_cache,connect):
    def catalog():
        return published_cache('rule-target-catalog',lambda:query('SELECT eid,source,department,process,equipment,`group`,point,device_key,display_group_token,quality_check,flatline_disabled,tag_type FROM dq_point_quality_current'))
    app.config['RULE_TARGET_CATALOG']=catalog
    @app.get('/api/rule-target-options')
    def options():
        scope=request.args.get('scope','device');source=request.args.get('source','kepserver')
        if scope not in ('point','device','global') or source not in ('oa','kepserver'):return jsonify(error='invalid_scope_or_source'),400
        rows=[r for r in catalog() if r['source']==source]
        choices={}
        for f in FIELDS:
            choices[f]=sorted({str(r.get(f) or '') for r in rows if r.get(f)})
            if request.args.get(f):rows=[r for r in rows if str(r.get(f) or '')==request.args[f]]
        groups={}
        for row in rows:
            key=row['eid'] if scope=='point' else 'business:'+row['display_group_token']
            item=groups.setdefault(key,dict(value=key,label=target_label(row,scope=='point'),point_count=0,eid=row['eid'] if scope=='point' else None))
            item['point_count']+=1
        term=request.args.get('q','').strip().lower();items=sorted((x for x in groups.values() if not term or term in x['label'].lower()),key=lambda x:x['label'])
        try:offset=max(0,int(request.args.get('offset',0)))
        except ValueError:return jsonify(error='invalid_offset'),400
        return jsonify(fields=choices,items=items[offset:offset+100],total=len(items),next_offset=offset+100,has_more=offset+100<len(items))
    @app.get('/api/rule-effective')
    def preview():
        scope=request.args.get('scope','global');source=request.args.get('source','kepserver');target=request.args.get('target_key','')
        selected=target_rows(catalog(),scope,source,target)
        if not selected:return jsonify(error='当前目录没有匹配的点位或设备'),404
        rules=query('SELECT * FROM dq_quality_rule');lookup={(r['scope'],r['source'],r['target_key']):r for r in rules}
        variants={};overrides=0;muted=0;participating=0;numeric=0
        for row in selected:
            rule=effective_rule(row,lookup);key=(int(rule['flatline_enabled']),rule['flatline_seconds'],int(rule['excluded']))
            variants[key]=variants.get(key,0)+1
            overrides+=int(('point',source,row['eid']) in lookup)
            muted+=int(bool(row['quality_check']))
            numeric+=int(is_numeric(row))
            participating+=int(is_numeric(row) and not row['quality_check'] and not row.get('flatline_disabled') and rule['flatline_enabled'] and not rule['excluded'])
        saved=next((r for r in rules if (r['scope'],r['source'],r['target_key'])==(scope,source,target)),None)
        form_lookup=lookup if scope=='point' else {k:v for k,v in lookup.items() if k[0]!='point'}
        editable=effective_rule(selected[0],form_lookup)
        return jsonify(point_count=len(selected),alarm_muted_count=muted,point_override_count=overrides,numeric_count=numeric,participating_count=participating,editable_rule=editable,target=public_rows([selected[0]])[0],
            saved_rule=public_rows([saved])[0] if saved else None,
            variants=[dict(flatline_enabled=e,flatline_seconds=t,excluded=x,point_count=n) for (e,t,x),n in sorted(variants.items())])
    @app.delete('/api/rules/<int:rule_id>')
    def inherit(rule_id):
        with connect() as db:
            with db.cursor() as cur:
                cur.execute('SELECT * FROM dq_quality_rule WHERE id=%s FOR UPDATE',(rule_id,));old=cur.fetchone()
                if not old:return jsonify(error='规则不存在'),404
                if old['scope']=='global':return jsonify(error='来源默认规则请直接修改'),400
                cur.execute('DELETE FROM dq_quality_rule WHERE id=%s',(rule_id,));audit(cur,'RESTORE_INHERITANCE',old,None)
            db.commit()
        return jsonify(saved=True)
