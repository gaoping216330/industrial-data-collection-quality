"""Small dependency-free XLSX writer and full-result API exports."""
from io import BytesIO
import json
import math
from datetime import datetime
from zipfile import ZipFile, ZIP_DEFLATED
from xml.sax.saxutils import escape
from flask import jsonify, request, send_file

LABELS={'level':'日志层级','manual_reason':'异常原因（人工填写）','manual_reason_by':'人工填写人','manual_reason_at':'人工填写时间','first_offline_ms':'最早离线时间','duplicate_count':'同时间戳重复次数','conflict_count':'内容冲突次数','cross_topic_count':'跨Topic重复观测次数','first_received_at':'首次发现毫秒时间','last_received_at':'最近发现毫秒时间','topics':'Topic列表','types':'重复类型','stopped_count':'停更点位数','point_share':'点位占比(%)','stopped_share':'停更占比(%)','department':'部门','process':'工序','equipment':'设备','group':'组','eid':'EID','point':'点位','tag':'tag','source':'来源','owner_name':'负责人','quality_check':'报警屏蔽标识(1停用0启用)','status':'状态','data_status':'数据状态','value_text':'当前值','receipt_time_ms':'最后收到','reasons':'判定原因','reason':'异常原因','reasons_text':'异常原因汇总','opened_at':'开始时间','closed_at':'恢复时间','started_at':'最早开始时间','ended_at':'结束时间','duration_seconds':'持续秒数','duration_hours':'持续小时数','point_count':'点位总数','abnormal_count':'异常点位数','affected_count':'异常点位数','missing_count':'缺数据点位数','model_result':'模型判定','message':'判定说明','detected_at':'推理时间','mqttname':'mqttname','dpname':'dpname','resolution_note':'结束说明','quality':'质量位','health_score':'健康分'}


def column(n):
    name=''
    while n:n,r=divmod(n-1,26);name=chr(65+r)+name
    return name


def workbook(rows,columns=None,sheet_name="异常数据"):
    keys=columns or list(dict.fromkeys([k for k in LABELS if any(k in r for r in rows)]+[k for r in rows for k in r if k not in LABELS])) or ['eid']
    data=BytesIO()
    with ZipFile(data,'w',ZIP_DEFLATED) as z:
        z.writestr('[Content_Types].xml','<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/><Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/></Types>')
        z.writestr('_rels/.rels','<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>')
        z.writestr('xl/workbook.xml','<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="'+escape(sheet_name)+'" sheetId="1" r:id="rId1"/></sheets></workbook>')
        z.writestr('xl/_rels/workbook.xml.rels','<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/></Relationships>')
        z.writestr('xl/styles.xml','<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><numFmts count="1"><numFmt numFmtId="164" formatCode="0.00"/></numFmts><fonts count="2"><font><sz val="11"/><name val="Microsoft YaHei"/></font><font><b/><sz val="11"/><name val="Microsoft YaHei"/></font></fonts><fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills><borders count="1"><border/></borders><cellStyleXfs count="1"><xf/></cellStyleXfs><cellXfs count="3"><xf xfId="0"/><xf fontId="1" xfId="0" applyFont="1"/><xf numFmtId="164" xfId="0" applyNumberFormat="1"/></cellXfs></styleSheet>')
        with z.open('xl/worksheets/sheet1.xml','w') as sheet:
            def write(text):sheet.write(text.encode('utf-8'))
            write('<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetViews><sheetView workbookViewId="0"><pane ySplit="1" topLeftCell="A2" state="frozen"/></sheetView></sheetViews><cols>'+''.join(f'<col min="{i}" max="{i}" width="24" customWidth="1"/>' for i in range(1,len(keys)+1))+'</cols><sheetData>')
            for index,row in enumerate([dict(zip(keys,[LABELS.get(k,k) for k in keys]))]+rows,1):
                write(f'<row r="{index}">')
                for i,k in enumerate(keys,1):
                    v=row.get(k);ref=f'{column(i)}{index}'
                    if k.endswith('_ms') and isinstance(v,(int,float)):v=datetime.fromtimestamp(v/1000).strftime('%Y-%m-%d %H:%M:%S')
                    if k=='value_text' and v not in (None,''):
                        try:v=float(v)
                        except (ValueError,TypeError):pass
                    if isinstance(v,(int,float)) and not isinstance(v,bool) and math.isfinite(v):write(f'<c r="{ref}" s="{2 if isinstance(v,float) else 0}"><v>{v}</v></c>')
                    else:
                        if isinstance(v,(list,dict)):v=json.dumps(v,ensure_ascii=False)
                        text=''.join(c for c in str(v if v is not None else '') if ord(c)>=32 or c in '\n\t\r')[:32767]
                        write(f'<c r="{ref}" t="inlineStr" s="{1 if index==1 else 0}"><is><t xml:space="preserve">{escape(text)}</t></is></c>')
                write('</row>')
            write(f'</sheetData><autoFilter ref="A1:{column(len(keys))}{len(rows)+1}"/></worksheet>')
    data.seek(0);return data


def install_export(app):
    @app.get('/api/export')
    def export():
        endpoints={'important-logs':'/api/important-logs','collection-events':'/api/collection-events','collection-points':'/api/collection-events','mqtt-duplicates':'/api/mqtt-duplicates','statistics':'/api/stopped-points','points':'/api/points','devices':'/api/devices','models':'/api/models','events':'/api/events','overview':'/api/points','rules':'/api/points','system':'/api/points','anomaly':'/api/anomaly-devices','open-events':'/api/open-device-events','business-device':'/api/business-devices'}
        kind=request.args.get('view','overview');endpoint=endpoints.get(kind)
        if not endpoint:return jsonify(error='未知导出类型'),400
        args=request.args.to_dict();args.pop('view',None);token=args.pop('token','')
        if token:
            if len(token)!=64 or any(c not in '0123456789abcdef' for c in token):return jsonify(error='设备标识无效'),400
            endpoint+='/'+token
        if kind in ('overview','points','rules','system'):args['status']='异常'
        if kind=='business-device':args['filter']='abnormal'
        if kind=='collection-points':
            event_id=args.pop('event_id','')
            if len(event_id)!=32 or any(c not in '0123456789abcdef' for c in event_id):return jsonify(error='事件标识无效'),400
            endpoint+='/'+event_id+'/points'
        rows=[];page=1
        while True:
            args['page']=page
            with app.test_request_context(endpoint,query_string=args):
                response=app.make_response(app.view_functions[app.url_map.bind('').match(endpoint)[0]](**app.url_map.bind('').match(endpoint)[1]))
                if response.status_code!=200:return jsonify(error='导出读取失败，请稍后重试'),503
                result=response.get_json()
            batch=result.get('items',[])
            rows.extend(batch)
            if page*50>=result.get('total',0) or not batch:break
            page+=1
        if kind=='devices':rows=[r for r in rows if r.get('abnormal_count',0)>0]
        if kind=='models':rows=[r for r in rows if r.get('model_result') in ('异常','预警')]
        columns=None
        if kind=='important-logs':
            columns=['层级','来源','部门','工序','设备','组','重要事项','影响点位','异常开始','恢复或结束','持续小时','结束说明','人工原因']
            rows=[dict(zip(columns,[{'platform':'平台','link':'采集链路','device':'设备'}.get(r.get('level'),r.get('level')),{'kepserver':'kep','platform':'本机','mixed':'OA / kep'}.get(r.get('source'),r.get('source')),r.get('department'),r.get('process'),r.get('equipment'),r.get('group'),r.get('reason'),r.get('affected_count'),r.get('opened_at'),r.get('closed_at'),r.get('duration_hours'),r.get('resolution_note'),r.get('manual_reason')])) for r in rows]
        if kind=='mqtt-duplicates':
            columns=['部门 / 工序','设备 / 组','EID / 点位','负责人','说明','MQTT点地址1','MQTT点地址2','重复开始时间','最近重复发送时间','结束时间']
            def date_label(value):
                if isinstance(value,(int,float)):return datetime.fromtimestamp(value/1000).strftime('%Y-%m-%d %H:%M:%S')
                return value or ''
            rows=[dict(zip(columns,[str(r.get('department') or '')+' / '+str(r.get('process') or ''),str(r.get('equipment') or '')+' / '+str(r.get('group') or ''),str(r.get('eid') or '')+' / '+str(r.get('point') or ''),r.get('owner_name') or '未配置','、'.join(r.get('types',[])),r.get('address_a') or '报文未提供完整地址',r.get('address_b') or '报文未提供完整地址',date_label(r.get('started_at') or r.get('first_received_at')),date_label(r.get('source_time') or r.get('source_time_ms')),date_label(r.get('ended_at'))])) for r in rows]
        response=send_file(workbook(rows,columns=columns),mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',as_attachment=True,download_name='异常数据_'+kind+'_'+datetime.now().strftime('%Y%m%d_%H%M%S')+'.xlsx')
        response.headers['Cache-Control']='no-store'
        return response
