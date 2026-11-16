"""Independent, bounded read-only MQTT observer; never alters ingestion/history."""
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import queue
import sys
import threading
import time
import paho.mqtt.client as mqtt
import redis
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT.parent/'实时数据缓存服务'))
from mqtt_duplicates import DuplicateTracker
from cache_service import decode_mqtt
from quality_engine import db_connect
from mqtt_duplicate_history import DuplicateHistory


def main():
    logging.basicConfig(level=logging.INFO,handlers=[RotatingFileHandler(ROOT/'logs/duplicate_monitor.log',maxBytes=2000000,backupCount=2,encoding='utf-8')],format='%(asctime)s %(message)s')
    settings=json.loads((ROOT.parent/'实时数据缓存服务/cache_config.json').read_text(encoding='utf-8-sig'))
    source=json.loads(Path(settings['mqtt_config_path']).read_text(encoding='utf-8-sig'))['mqtt']
    cache=redis.Redis(**settings['redis']);tracker=DuplicateTracker();prefix='dq:cache:';inbox=queue.Queue(maxsize=1000)
    history=DuplicateHistory(db_connect);tracker.restore(history.load_open())
    policy={};skipped=0;connected=False;processed_received_at=0;pending_bytes=0;received_packets=0;processed_packets=0;last_packet_at=0;guard=threading.Lock()
    client=mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,client_id='readMqt_redis_duplicate_monitor',clean_session=True,protocol=mqtt.MQTTv311)
    if source.get('username'):client.username_pw_set(source['username'],os.environ.get(source.get('password_env',''),source.get('password','')))
    client.reconnect_delay_set(2,30)
    def connect(client,userdata,flags,reason,properties):
        nonlocal connected
        connected=reason==0
        if connected:client.subscribe([(topic,0) for topic in source['topics']])
        logging.info('duplicate_monitor connected=%s',connected)
    def disconnect(client,userdata,flags,reason,properties):
        nonlocal connected
        connected=False
    def message(client,userdata,msg):
        nonlocal pending_bytes,skipped,received_packets,last_packet_at
        size=len(msg.payload)
        with guard:
            received_packets+=1;last_packet_at=int(time.time()*1000)
            if pending_bytes+size>200*1024*1024:skipped+=1;return
            try:inbox.put_nowait((msg.topic,msg.payload,int(time.time()*1000)));pending_bytes+=size
            except queue.Full:skipped+=1
    client.on_connect=connect;client.on_disconnect=disconnect;client.on_message=message
    def work():
        nonlocal pending_bytes,skipped,processed_packets,processed_received_at
        while True:
            topic,raw,received=inbox.get()
            try:
                payload=decode_mqtt(raw)
                if isinstance(payload,dict) and isinstance(payload.get('values'),list):tracker.observe(payload['values'],payload.get('timestamp'),topic,received,policy)
            except Exception:
                with guard:skipped+=1
                logging.exception('duplicate_monitor decode/observe failed')
            finally:
                with guard:pending_bytes-=len(raw);processed_packets+=1;processed_received_at=received
                inbox.task_done()
    threading.Thread(target=work,name='duplicate-observer',daemon=True).start()
    client.connect_async(source['broker'],int(source.get('port',1883)),keepalive=30);client.loop_start()
    interval=600
    next_policy=0;next_flush=0;last_skipped=0;next_publish=time.monotonic()+interval
    initial_raw=cache.get(prefix+'mqtt_duplicates')
    initial=tracker.snapshot(policy)
    if initial_raw and json.loads(initial_raw).get('schema_version')!=2:
        (ROOT/'logs/mqtt_duplicates_legacy_snapshot.json').write_text(initial_raw,encoding='utf-8')
    initial.update(interval_seconds=interval,next_publish_at=int(time.time()*1000)+interval*1000,collecting=True)
    cache.set(prefix+'mqtt_duplicates',json.dumps(initial,ensure_ascii=False),ex=1800)
    while True:
        try:
            if time.monotonic()>=next_policy:
                policy=cache.hgetall(prefix+'catalog');next_policy=time.monotonic()+60
            with guard:runtime=dict(connected=connected,skipped_packets=skipped,pending_packets=inbox.qsize(),pending_bytes=pending_bytes,client_id='readMqt_redis_duplicate_monitor',received_packets=received_packets,processed_packets=processed_packets,last_packet_at=last_packet_at,heartbeat_at=int(time.time()*1000))
            now=int(time.time()*1000)
            reliable=connected and skipped==last_skipped and now-processed_received_at<=30000
            tracker.advance(policy,now,reliable=reliable);last_skipped=skipped
            if time.monotonic()>=next_flush:
                pending=[r for r in tracker.snapshot(policy,now,history=True)['items'] if not r.get('persisted')]
                if pending:history.save(pending);tracker.acknowledge(pending)
                next_flush=time.monotonic()+60
            cache.set(prefix+'mqtt_duplicate_heartbeat',json.dumps(runtime),ex=35)
            if time.monotonic()>=next_publish:
                snapshot=tracker.snapshot(policy,reset_events=True)
                next_publish=time.monotonic()+interval
                snapshot.update(runtime,interval_seconds=interval,next_publish_at=int(time.time()*1000)+interval*1000,collecting=False)
                cache.set(prefix+'mqtt_duplicates',json.dumps(snapshot,ensure_ascii=False),ex=1800)
                logging.info('duplicate_monitor published points=%s next_publish_at=%s',snapshot['point_count'],snapshot['next_publish_at'])
            logging.info('duplicate_monitor heartbeat received=%s pending=%s skipped=%s connected=%s',runtime['received_packets'],runtime['pending_packets'],skipped,connected)
        except Exception:logging.exception('duplicate_monitor report failed')
        time.sleep(10)

if __name__=='__main__':main()
