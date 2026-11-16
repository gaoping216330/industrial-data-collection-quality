"""Concise operations log: platform, links and device episodes; no per-point rows."""
from flask import jsonify,request
from datetime import datetime

UNION="""SELECT id,CASE WHEN source='platform' THEN 'platform' ELSE 'link' END level,
 source,NULL device_token,NULL department,NULL process,NULL equipment,NULL `group`,
 reason,affected_count,opened_at,closed_at,NULL resolution_note FROM dq_collection_event
 UNION ALL SELECT id,'device' level,source,device_token,department,process,equipment,`group`,
 reason,affected_count,opened_at,closed_at,resolution_note FROM dq_device_incident"""

def install(app,query,public_rows):
    @app.get('/api/important-logs')
    def important_logs():
        page=max(1,int(request.args.get('page',1)));where=[];params=[]
        for field,allowed in [('level',{'platform','link','device'}),('source',{'oa','kepserver','platform','mixed'})]:
            value=request.args.get(field,'')
            if value in allowed:where.append(field+'=%s');params.append(value)
        status=request.args.get('status','')
        if status=='open':where.append('closed_at IS NULL')
        elif status=='closed':where.append('closed_at IS NOT NULL')
        for key,op in [('begin','>='),('end','<=')]:
            value=request.args.get(key,'')
            if value:
                try:parsed=datetime.fromisoformat(value)
                except ValueError:return jsonify(error='日期格式不正确'),400
                where.append('opened_at'+op+'%s');params.append(parsed)
        base=' FROM ('+UNION+') log'+(' WHERE '+' AND '.join(where) if where else '')
        total=query('SELECT COUNT(*) n'+base,params)[0]['n']
        rows=query('SELECT *'+base+' ORDER BY COALESCE(closed_at,opened_at) DESC,opened_at DESC LIMIT 50 OFFSET %s',params+[min((page-1)*50,1000000)])
        return jsonify(items=public_rows(rows),total=total,page=page)
