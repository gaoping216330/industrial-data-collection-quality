"""Server-side aggregate history with bounded windows and Shanghai timestamps."""
import json
import math
import time
from datetime import datetime,timedelta,timezone
from urllib.request import Request,urlopen

import os

BASE_URL=os.getenv('DQ_HISTORY_BASE_URL', 'http://127.0.0.1:9002/data-governance/sensor/batch')
SHANGHAI=timezone(timedelta(hours=8))
STEPS={'second':1,'minute':60,'hour':3600,'day':86400}

class HistoryError(Exception):pass

def parse_time(value):
    try:
        parsed=datetime.fromisoformat(value)
        return parsed.replace(tzinfo=SHANGHAI) if parsed.tzinfo is None else parsed.astimezone(SHANGHAI)
    except (ValueError,TypeError):raise ValueError('时间格式无效，请选择起止时间')

def fetch_history(eid,granularity,begin,end,post=None):
    if granularity not in STEPS:raise ValueError('历史趋势支持秒级原始值，以及分钟、小时和天平均值')
    begin=parse_time(begin);end=parse_time(end)
    if begin>=end:raise ValueError('结束时间必须晚于开始时间')
    seconds=STEPS[granularity]
    if (end-begin).total_seconds()>seconds*3599*64:raise ValueError('时间范围过长，请缩短范围或改用小时／天平均值')
    cutoff=time.monotonic()+60
    points={};cursor=begin;requests=0
    path='sv/seconds/all' if granularity=='second' else granularity+'/avg'
    while cursor<end:
        if requests>=64:raise HistoryError('查询批次超过64次，请缩短时间范围或选择更粗粒度')
        if time.monotonic()>cutoff:raise HistoryError('历史接口查询超时，请缩短时间范围后重试')
        boundary=min(end,cursor+timedelta(seconds=seconds*3599))
        payload={'id':[eid],'beginTime':cursor.strftime('%Y-%m-%d %H:%M:%S'),'endTime':boundary.strftime('%Y-%m-%d %H:%M:%S'),'limit':3600,'orderBy':'asc','precision':2,'cur':'N'}
        if granularity=='second':
            payload.pop('precision');payload.pop('cur')
        last_stamp=None;page_count=0
        try:
            if post is not None:data=post(payload)
            else:
                request=Request(BASE_URL+'/'+path,data=json.dumps(payload).encode(),headers={'Content-Type':'application/json'},method='POST')
                with urlopen(request,timeout=min(8,max(0.1,cutoff-time.monotonic()))) as response:data=json.load(response)
        except Exception as exc:raise HistoryError('历史数据接口连接或响应失败，请稍后重试') from exc
        if not isinstance(data,dict):raise HistoryError('历史数据接口返回格式无效')
        if str(data.get('code'))!='200':raise HistoryError('历史数据接口返回失败状态：'+str(data.get('code')))
        for item in data.get('data') or []:
            if str(item.get('eid'))!=eid:continue
            page_count+=len(item.get('datas') or [])
            for sample in item.get('datas') or []:
                try:
                    stamp=parse_time(sample['ts']);last_stamp=max(last_stamp,stamp) if last_stamp else stamp;value=float(sample['v'])
                    if begin<=stamp<=end and math.isfinite(value):points[int(stamp.timestamp()*1000)]=value
                except (KeyError,ValueError,TypeError):continue
        if page_count>=3600 and last_stamp and last_stamp<boundary:
            if last_stamp<=cursor:raise HistoryError('同一时间戳的数据超过接口分页能力，无法完整读取，请改用分钟平均值')
            cursor=last_stamp
        else:cursor=boundary
        requests+=1
    rows=sorted(points.items());count=len(rows)
    return {'history':rows,'total_samples':count,'display_samples':len(rows),'requests':requests,'granularity':granularity,'timezone':'Asia/Shanghai','begin':begin.isoformat(),'end':end.isoformat()}
