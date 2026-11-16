"""Published department/person stopped-data statistics and paged drill-down."""
from flask import jsonify,request,send_file
from datetime import datetime
from excel_export import workbook

STOPPED="(data_status IN ('从未收到','停止发送') AND COALESCE(quality_check,0)=0)"
AFFECTED="(COALESCE(quality_check,0)=0 AND (status='异常' OR data_status IN ('从未收到','停止发送')))"
FIELDS=('department','process','owner_code','owner_name')

def aggregate(rows):
    # One EID has one catalog owner; sums remain consistent at every tree level.
    groups={}
    for row in rows:
        key=tuple(str(row.get(f) or '').strip() for f in FIELDS)
        target=groups.setdefault(key,dict(zip(FIELDS,key),point_count=0,stopped_count=0,abnormal_count=0,affected_count=0))
        for field in ('point_count','stopped_count','abnormal_count','affected_count'):target[field]+=int(row.get(field) or 0)
    items=sorted(groups.values(),key=lambda r:tuple(r[f] for f in FIELDS))
    return {'items':items,'point_count':sum(r['point_count'] for r in items),'stopped_count':sum(r['stopped_count'] for r in items),'abnormal_count':sum(r['abnormal_count'] for r in items),'affected_count':sum(r['affected_count'] for r in items)}


def install_statistics(app,query,public_rows,published_cache):
    @app.get('/api/stopped-statistics')
    def stopped_statistics():
        def load():
            rows=query("SELECT department,process,owner_code,owner_name,COUNT(*) point_count,SUM("+STOPPED+") stopped_count,SUM(status='异常' AND COALESCE(quality_check,0)=0) abnormal_count,SUM("+AFFECTED+") affected_count FROM dq_point_quality_display GROUP BY department,process,owner_code,owner_name")
            return aggregate(rows)
        return jsonify(published_cache('stopped-statistics',load))

    @app.get('/api/stopped-points')
    def stopped_points():
        where=["COALESCE(quality_check,0)=0", AFFECTED if request.args.get('metric')=='affected' else "status='异常'" if request.args.get('metric')=='abnormal' else STOPPED];params=[]
        for field in ('department','process','owner_code'):
            if field in request.args:
                where.append("TRIM(COALESCE(`"+field+"`,''))=%s");params.append(request.args[field])
        clause=' WHERE '+' AND '.join(where)
        page=max(1,int(request.args.get('page',1)))
        total=query('SELECT COUNT(*) n FROM dq_point_quality_display'+clause,params)[0]['n']
        rows=query('SELECT * FROM dq_point_quality_display'+clause+' ORDER BY department,process,equipment,`group`,eid LIMIT 50 OFFSET %s',params+[(page-1)*50])
        return jsonify(total=total,page=page,items=public_rows(rows))

    @app.get('/api/stopped-statistics/export')
    def statistics_export():
        data=stopped_statistics().get_json()
        rows=[dict(row,point_share=round(row['point_count']*100/max(data['point_count'],1),2),stopped_share=round(row['stopped_count']*100/max(row['point_count'],1),2)) for row in data['items']]
        return send_file(workbook(rows),mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',as_attachment=True,download_name='部门人员停更统计_'+datetime.now().strftime('%Y%m%d_%H%M%S')+'.xlsx')
