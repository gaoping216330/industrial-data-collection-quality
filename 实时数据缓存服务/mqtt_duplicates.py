"""Observe duplicate packet evidence without treating normal new timestamps as duplicates."""
from collections import deque
import threading,time,json,math,hashlib,uuid

class DuplicateTracker:
    def __init__(self):
        self.lock=threading.Lock();self.seen={};self.events={};self.completed=[];self.started_at=int(time.time()*1000);self.sequence=0;self.last_advance=None
    def restore(self,rows):
        with self.lock:
            for r in rows:
                if r['event_key'] not in self.events:self.events[r['event_key']]=dict(r,recovery_since=None)
    def observe(self,items,sent,topic,received,policy):
        try:
            if isinstance(sent,bool):return
            sent=int(float(sent))
            if not 0<=sent<=4102444800000:return
        except (ValueError,TypeError,OverflowError):return
        with self.lock:
            self.sequence+=1;packet=self.sequence;touched=set();detected=set()
            for item in items:
                if not isinstance(item,dict):continue
                raw=str(item.get('0',item.get('eid',item.get('id','')))).strip()
                eid=raw.rsplit('_',1)[-1].strip().lower()
                if not eid or not policy or policy.get(eid)!='kepserver':continue
                value=item.get('50',item.get('v',item.get('value')))
                try:
                    if value is None or isinstance(value,(dict,list)):continue
                    value=float(value)
                    if not math.isfinite(value):continue
                except (TypeError,ValueError):continue
                address=raw if raw.lower()!=eid else None
                fingerprint=(value,str(item.get('q','')),str(item.get('t','')))
                state=self.seen.setdefault(eid,{'recent':deque(maxlen=16),'addresses':{},'last_received':0})
                state['last_received']=received;touched.add(eid)
                state['addresses']={k:v for k,v in state['addresses'].items() if 0<=received-v['received']<=10000}
                previous=next((x for x in reversed(state['recent']) if x['sent']==sent),None)
                others=[x for x in state['addresses'].values() if address and x['address'] and x['address']!=address]
                candidates=others[:8] if others else [previous] if previous else []
                current=dict(address=address,topic=topic,sent=sent,received=received,fingerprint=fingerprint,packet=packet)
                for prior in candidates:
                    if prior is None:continue
                    different=bool(address and prior['address'] and address!=prior['address'])
                    kind='不同MQTT地址共用EID' if different else '同时间戳内容冲突' if prior['fingerprint']!=fingerprint else '同地址同时间戳重发'
                    endpoints=sorted([(prior['address'] or '',prior['topic']),(address or '',topic)])
                    key=hashlib.sha256(json.dumps([eid,endpoints],ensure_ascii=False).encode()).hexdigest()
                    event=self.events.get(key)
                    if event is None or event.get('ended_at'):
                        if event is not None:self.completed.append(event)
                        event=self.events[key]=dict(episode_id=uuid.uuid4().hex,event_key=key,persisted=False,eid=eid,address_a=endpoints[0][0] or None,address_b=endpoints[1][0] or None,topic_a=endpoints[0][1],topic_b=endpoints[1][1],first_received_at=received,last_received_at=received,source_time=sent,duplicate_count=0,conflict_count=0,cross_topic_count=0,types=[],ended_at=None,confirmed_at=None,recovery_since=None)
                    event.update(last_received_at=received,source_time=sent,recovery_since=None,persisted=False)
                    event['duplicate_count']+=1
                    if kind=='同时间戳内容冲突':event['conflict_count']+=1
                    if prior['topic']!=topic:event['cross_topic_count']+=1
                    if kind not in event['types']:event['types'].append(kind)
                    event['evidence']={'kind':kind,'same_packet':prior['packet']==packet,'first':{'address':prior['address'],'topic':prior['topic'],'sent_ms':prior['sent'],'received_ms':prior['received'],'value':prior['fingerprint'][0],'quality':prior['fingerprint'][1],'change_time':prior['fingerprint'][2]},'second':{'address':address,'topic':topic,'sent_ms':sent,'received_ms':received,'value':value,'quality':fingerprint[1],'change_time':fingerprint[2]}}
                    event['topics']=sorted({prior['topic'],topic});detected.add(key)
                state['recent'].append(current)
                if address:
                    state['addresses'][address]=current
                    if len(state['addresses'])>16:state['addresses'].pop(next(iter(state['addresses'])))
            for key,event in self.events.items():
                if not event.get('ended_at') and event['eid'] in touched and key not in detected and event.get('recovery_since') is None:event['recovery_since']=received
    def advance(self,policy,now,reliable=True):
        with self.lock:
            gap=self.last_advance is not None and not 0<=now-self.last_advance<=40000
            self.last_advance=now
            for event in self.events.values():
                if event.get('ended_at'):continue
                state=self.seen.get(event['eid'],{})
                if not reliable or gap or now-state.get('last_received',0)>30000:
                    event['recovery_since']=None;continue
                since=event.get('recovery_since')
                if since is not None and now-since>=600000:
                    event.update(ended_at=since,confirmed_at=now,persisted=False)
    def snapshot(self,policy,now=None,reset_events=False,history=False):
        now=int(time.time()*1000) if now is None else now
        with self.lock:
            self.seen={k:v for k,v in self.seen.items() if policy and policy.get(k)=='kepserver'}
            self.events={k:v for k,v in self.events.items() if not v.get('ended_at') or not v.get('persisted') or now-v['ended_at']<86400000}
            self.completed=[v for v in self.completed if not v.get('persisted') or now-v['ended_at']<86400000]
            rows=[json.loads(json.dumps(v)) for v in list(self.events.values())+(self.completed if history else []) if history or (not v.get('ended_at') and policy and policy.get(v['eid'])=='kepserver' and now-v['last_received_at']<=600000)]
        return dict(schema_version=2,started_at=self.started_at,checked_at=now,window_minutes=10,items=rows,point_count=len({r['eid'] for r in rows}),pair_count=len(rows),observed_points=len(self.seen))
    def acknowledge(self,rows):
        with self.lock:
            for row in rows:
                event=next((x for x in [self.events.get(row['event_key'])]+self.completed if x and x['episode_id']==row['episode_id']),None)
                if event and event['episode_id']==row['episode_id'] and event['last_received_at']==row['last_received_at'] and event.get('ended_at')==row.get('ended_at'):event['persisted']=True
    def publish(self,client,prefix,policy):
        client.set(prefix+'mqtt_duplicates',json.dumps(self.snapshot(policy),ensure_ascii=False),ex=1800)
