"""Export the raw samples currently loaded in a point-detail chart; no database writes."""
import math
from datetime import datetime
from flask import request,jsonify,send_file
from excel_export import workbook

def install(app,query):
 @app.post('/api/trend-export')
 def trend_export():
  if request.content_length and request.content_length>12*1024*1024:return jsonify(error='数据过多，请缩小时间窗口后导出'),413
  payload=request.get_json(silent=True) or {}
  if not isinstance(payload,dict):return jsonify(error='数据格式无效'),400
  eid=str(payload.get('eid') or '')
  allowed={eid}|{r['peer_eid'] for r in query('SELECT peer_eid FROM dq_model_training_peer WHERE eid=%s',[eid])}
  series=payload.get('series',[])
  if not isinstance(series,list) or len(series)>64:return jsonify(error='一次最多导出64条勾选曲线，请分批导出'),400
  mode=payload.get('mode','live');granularity=payload.get('granularity','')
  kind='实时5秒样本' if mode=='live' else {'second':'秒级原始','minute':'分钟平均','hour':'小时平均','day':'天平均'}.get(granularity,'历史')
  begin=payload.get('begin');end=payload.get('end')
  if begin is not None and (not isinstance(begin,(int,float)) or not math.isfinite(begin)):return jsonify(error='时间窗口无效'),400
  if end is not None and (not isinstance(end,(int,float)) or not math.isfinite(end)):return jsonify(error='时间窗口无效'),400
  ids=[s['eid'] for s in series if isinstance(s,dict) and isinstance(s.get('eid'),str)]
  if len(ids)!=len(series) or any(x not in allowed for x in ids):return jsonify(error='只允许导出本点及其训练关联点'),400
  metadata={r['eid']:r for r in query('SELECT eid,department,process,equipment,`group`,point,channel FROM autosensor WHERE eid IN ('+','.join(['%s']*len(ids))+')',ids)} if ids else {}
  rows=[];count=0
  for s in series:
   points=s.get('points')
   if not isinstance(points,list):return jsonify(error='采样数据格式无效'),400
   count+=len(points)
   if count>150000:return jsonify(error='超过15万条，请缩小窗口或减少曲线'),400
   meta=metadata.get(s['eid'],{})
   for point in points:
    if not isinstance(point,list) or len(point)!=2:return jsonify(error='采样格式无效'),400
    t,v=point
    if not isinstance(t,(int,float)) or not isinstance(v,(int,float)) or not math.isfinite(t) or not math.isfinite(v):return jsonify(error='采样包含无效数值'),400
    if begin is not None and t<begin or end is not None and t>end:continue
    try:when=datetime.fromtimestamp(t/1000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    except (ValueError,OSError,OverflowError):return jsonify(error='采样时间无效'),400
    rows.append({'部门':meta.get('department'),'工序':meta.get('process'),'设备':meta.get('equipment'),'组':meta.get('group'),'EID':s['eid'],'点位':meta.get('point'),'关系':'目标点' if s['eid']==eid else '关联点','数据类型':kind,'采样时间（北京时间）':when,'原始数值':float(v)})
  if not rows:return jsonify(error='所选窗口没有可导出的有效样本'),400
  rows.sort(key=lambda r:(r['采样时间（北京时间）'],r['EID']))
  response=send_file(workbook(rows,sheet_name='趋势数据'),mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',as_attachment=True,download_name='趋势数据_'+eid+'_'+datetime.now().strftime('%Y%m%d_%H%M%S')+'.xlsx')
  response.headers['Cache-Control']='no-store';return response
