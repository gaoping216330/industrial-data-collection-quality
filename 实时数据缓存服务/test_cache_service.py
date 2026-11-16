import json
import os
import time
import unittest
import threading
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

import redis

from cache_service import Cache, create_app, decode_mqtt, normalize
from mqtt_collector import BoundedInbox, MqttCollector, Packet, over_broker_limit


class ReceiverTests(unittest.TestCase):
    def setUp(self):
        self.client = redis.Redis(host='127.0.0.1',port=int(os.environ.get('REDIS_TEST_PORT','6379')),socket_timeout=5,decode_responses=True)
        self.prefix = 'dq:test:' + uuid4().hex + ':'
        self.cache = Cache(self.client, prefix=self.prefix)
        self.client.hset(self.cache.policy_key, mapping={'kep': 'kepserver', 'oa': 'oa'})
        self.cache.directory_loaded_at = time.time()
        self.config = {'application': {'allowed_oa_ips': ['192.0.2.10'], 'max_batch': 2000}}
        self.http = create_app(self.config, self.cache).test_client()

    def tearDown(self):
        keys = list(self.client.scan_iter(match=self.prefix + '*'))
        if keys:
            self.client.delete(*keys)
        self.client.close()

    def sample(self, eid='oa', timestamp=None, value=6300):
        return {'0': eid, '1': timestamp or int(time.time() * 1000)//5000*5000, '50': value,
                '99': 1769629196913, 'q': True}

    def test_same_batch_cannot_regress_latest_or_receipt(self):
        timestamp=int(time.time()*1000)//5000*5000
        self.cache.ingest([self.sample(timestamp=timestamp,value=9),self.sample(timestamp=timestamp-5000,value=1)],'oa',timestamp)
        self.assertEqual(json.loads(self.client.get(self.prefix+'latest:oa'))['value'],9)
        self.cache.ingest([self.sample(timestamp=timestamp-10000)],'oa',timestamp-10000)
        self.assertEqual(json.loads(self.client.hget(self.prefix+'receipt','oa'))['received_time'],timestamp)

    def test_oa_timeout_is_durably_accepted_not_cached(self):
        import tempfile
        from pathlib import Path
        from durable_inbox import DiskInbox
        with tempfile.TemporaryDirectory() as root:
            self.cache.oa_spool=DiskInbox(str(Path(root)/'oa.db'))
            with patch.object(self.cache,'ingest',side_effect=redis.TimeoutError('test')):
                response=self.http.post('/api/ingest/winccoa',json=[self.sample()])
            self.assertEqual(response.status_code,202)
            self.assertTrue(response.json['success'])
            self.assertTrue(response.json['buffered'])
            self.assertEqual(response.json['cached'],0)
            self.assertEqual(self.cache.oa_spool.snapshot()['pending_packets'],1)
            self.cache.oa_spool.db.close()
            self.cache.oa_spool=None

    def test_mqtt_disk_packet_retries_after_redis_timeout(self):
        import tempfile
        from pathlib import Path
        from durable_inbox import DiskInbox
        with tempfile.TemporaryDirectory() as root:
            collector=self.collector();collector.inbox=DiskInbox(str(Path(root)/'mqtt.db'))
            packet=Packet('test',json.dumps({'timestamp':int(time.time()*1000),'values':[self.sample('kep')]}).encode(),int(time.time()*1000),time.monotonic())
            collector.inbox.push(packet);done=threading.Event();original=self.cache.ingest;attempts=[]
            def flaky(*args,**kwargs):
                attempts.append(1)
                if len(attempts)==1:raise redis.TimeoutError('temporary failure')
                result=original(*args,**kwargs);done.set();return result
            with patch.object(self.cache,'ingest',side_effect=flaky):
                thread=threading.Thread(target=collector.writer);thread.start()
                try:self.assertTrue(done.wait(8))
                finally:collector.stop_event.set();thread.join(timeout=3)
            self.assertEqual(collector.inbox.snapshot()['pending_packets'],0)
            self.assertEqual(collector.snapshot()['dropped_packets'],0)
            self.assertEqual(len(attempts),2)
            collector.inbox.db.close()

    def test_packet_time_filter_uses_outer_send_second_only(self):
        collector=self.collector();collector.settings['only_aligned_packets']=True
        self.assertTrue(collector.selected_timestamp({'timestamp':15620,'values':[{'t':12345}]}))
        self.assertFalse(collector.selected_timestamp({'timestamp':16620,'values':[{'t':15000}]}))
        for invalid in (None,True,float('nan')):
            with self.assertRaises(ValueError):collector.selected_timestamp({'timestamp':invalid})

    def test_network_callback_never_touches_disk_or_decoder(self):
        collector=self.collector();collector.network_inbox=BoundedInbox()
        with patch.object(collector.inbox,'push') as disk,patch.object(collector,'decoder') as decoder:
            collector.on_message(None,None,SimpleNamespace(topic='test',payload=b'{}'))
            disk.assert_not_called();decoder.assert_not_called()
        self.assertEqual(collector.network_inbox.snapshot()['pending_packets'],1)

    def test_spool_thread_filters_before_disk(self):
        collector=self.collector();collector.settings['only_aligned_packets']=True
        collector.network_inbox=BoundedInbox();saved=threading.Event()
        for stamp in (16620,15620):
            collector.on_message(None,None,SimpleNamespace(topic='test',payload=json.dumps({'timestamp':stamp,'values':[]}).encode()))
        with patch.object(collector.inbox,'push',side_effect=lambda p:(saved.set() or [])) as disk:
            thread=threading.Thread(target=collector.spool_writer);thread.start()
            try:self.assertTrue(saved.wait(3))
            finally:collector.stop_event.set();thread.join(timeout=2)
            self.assertEqual(disk.call_count,1)
        self.assertEqual(collector.metrics['filtered_packets'],1)
        self.assertEqual(collector.metrics['dropped_packets'],0)

    def test_fast_batches_share_network_exchange(self):
        stamp=int(time.time()*1000)//5000*5000
        self.cache.mqtt_seeded=True
        eids=['batch'+str(i) for i in range(1005)]
        self.client.hset(self.cache.policy_key,mapping={eid:'kepserver' for eid in eids})
        result=self.cache.ingest([self.sample(eid,stamp) for eid in eids],'kepserver',stamp,packet_time=stamp)
        self.assertEqual(result['cached'],1005)
        self.assertEqual(self.cache.ingest_metrics['kepserver']['script_batches'],3)
        self.assertEqual(self.cache.ingest_metrics['kepserver']['redis_exchanges'],2)
        self.assertEqual(self.client.xlen(self.prefix+'history:'+eids[0]),1)
        self.assertEqual(self.client.xlen(self.prefix+'history:'+eids[-1]),1)

    def test_fast_repeated_eid_keeps_latest_and_one_sample_across_batches(self):
        stamp=int(time.time()*1000)//5000*5000;self.cache.mqtt_seeded=True
        self.cache.ingest([self.sample('kep',stamp,i) for i in range(1005)],'kepserver',stamp,packet_time=stamp)
        self.assertEqual(json.loads(self.client.get(self.prefix+'latest:kep'))['value'],1004)
        self.assertEqual(self.client.xlen(self.prefix+'history:kep'),1)

    def test_fast_pipeline_partial_error_is_safe_to_retry(self):
        stamp=int(time.time()*1000)//5000*5000;self.cache.mqtt_seeded=True
        eids=['retry'+str(i) for i in range(501)]
        self.client.hset(self.cache.policy_key,mapping={eid:'kepserver' for eid in eids})
        bad=self.prefix+'history:'+eids[-1];self.client.set(bad,'wrong-type')
        items=[self.sample(eid,stamp) for eid in eids]
        with self.assertRaises(redis.ResponseError):self.cache.ingest(items,'kepserver',stamp,packet_time=stamp)
        self.client.delete(bad)
        self.cache.ingest(items,'kepserver',stamp,packet_time=stamp)
        self.assertEqual(self.client.xlen(self.prefix+'history:'+eids[0]),1)
        self.assertEqual(self.client.xlen(bad),1)

    def test_oa_numeric_field_mapping(self):
        record = normalize(self.sample(), 'oa', 1792652232000)
        self.assertEqual(record['value'], 6300)
        self.assertEqual(record['last_change_time'], 1769629196913)
        self.assertEqual(record['source'], 'oa')
        self.assertEqual(record['received_time'], 1792652232000)

    def test_existing_mqtt_control_characters(self):
        payload = b'{"values":[{"id":"path\tname_oa","v":1,"t":1792652231045}]}'
        decoded = decode_mqtt(payload)
        self.assertEqual(normalize(decoded['values'][0], 'kepserver', 1)['eid'], 'oa')

    def test_source_priority_and_catalog_filter(self):
        result = self.cache.ingest([self.sample('kep'), self.sample('oa'), self.sample('unknown')], 'oa')
        self.assertEqual((result['cached'], result['ignored']), (1, 2))
        self.assertIsNone(self.client.get(self.prefix + 'latest:kep'))
        result = self.cache.ingest([self.sample('kep'), self.sample('oa')], 'kepserver')
        self.assertEqual((result['cached'], result['ignored']), (1, 1))
        result = self.cache.ingest([self.sample('kep', value=999)], 'oa')
        self.assertEqual(result['ignored'], 1)
        self.assertEqual(json.loads(self.client.get(self.prefix + 'latest:kep'))['value'], 6300)

    def test_old_samples_do_not_overwrite_latest(self):
        timestamp = int(time.time() * 1000)//5000*5000
        self.cache.ingest([self.sample(timestamp=timestamp)], 'oa')
        duplicate = self.cache.ingest([self.sample(timestamp=timestamp)], 'oa')
        old = self.cache.ingest([self.sample(timestamp=timestamp - 1000, value=1)], 'oa')
        self.assertEqual(duplicate['duplicate'], 1)
        self.assertEqual(old['stale'], 1)
        self.assertEqual(self.client.xlen(self.prefix + 'history:oa'), 1)

    def test_mqtt_fastpath_keeps_monotonic_time_after_restart(self):
        self.cache.policy = {'kep':'kepserver','oa':'oa'}
        self.cache.mqtt_seeded = True
        t = int(time.time()*1000)
        first = {'id':'gateway_kep','v':10,'q':True,'t':t-900000}
        result = self.cache.ingest([first], 'kepserver', t, packet_time=t)
        self.assertEqual(result['cached'], 1)
        self.assertEqual(json.loads(self.client.hget(self.prefix+'receipt','kep'))['source_time'],t)
        restarted = Cache(self.client,prefix=self.prefix)
        restarted.policy = self.cache.policy
        restarted.seed_mqtt_committed(restarted.policy)
        older = restarted.ingest([{'id':'gateway_kep','v':5,'q':True,'t':t-900000}],
                                  'kepserver',t+1000,packet_time=t-1000)
        self.assertEqual(older['stale'],1)
        self.assertEqual(json.loads(self.client.get(self.prefix+'latest:kep'))['value'],10)

    def test_http_auth_validation_and_database_unavailable(self):
        path = '/api/ingest/winccoa'
        denied = self.http.post(path, json=[self.sample()],
                                environ_base={'REMOTE_ADDR': '192.0.2.10'})
        self.assertEqual(denied.status_code, 403)
        response = self.http.post(path, json=[self.sample()],
                                  environ_base={'REMOTE_ADDR': '192.0.2.10'})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json['cached'], 1)
        response = self.http.post(path, json=[{'0': 'oa', '50': 'nan'}])
        self.assertEqual(response.json['invalid'], 1)
        self.cache.directory_loaded_at = 0
        response = self.http.post(path, json=[self.sample()])
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json['cached'], 0)

    def test_history_retention_and_current_value(self):
        history = self.prefix + 'history:oa'
        old_id = str(int(time.time() * 1000) - 3601000) + '-0'
        self.client.xadd(history, {'v': 'old'}, id=old_id)
        self.cache.ingest([self.sample()], 'oa')
        self.cache.trim_history()
        self.assertGreaterEqual(self.client.xlen(history), 1)
        self.assertGreater(self.client.ttl(history), 3500)
        self.assertEqual(self.client.ttl(self.prefix + 'latest:oa'), -1)

    def test_batch_duplicates_priority_and_timestamp(self):
        timestamp = int(time.time() * 1000)//5000*5000
        self.cache.policy = {'kep': 'kepserver', 'oa': 'oa'}
        received = timestamp - 1000
        result = self.cache.ingest([self.sample('kep', timestamp), self.sample('oa'),
                                    self.sample('unknown')], 'kepserver', received)
        self.assertEqual((result['cached'], result['ignored']), (1, 2))
        actual = json.loads(self.client.get(self.prefix + 'latest:kep'))
        self.assertEqual(actual['received_time'], received)
        # Duplicates filtered before Redis still have correct counters.
        result = self.cache.ingest([self.sample('kep', timestamp)], 'kepserver')
        self.assertEqual(result['duplicate'], 1)
        self.assertEqual(self.client.xlen(self.prefix + 'history:kep'), 1)

    def collector(self):
        config = {'mqtt_config_path': 'unused', 'mqtt_intake': {
            'max_packets': 2, 'max_bytes': 1000, 'writer_threads': 1, 'broker_monitor': {'enabled': False}}}
        with patch('pathlib.Path.read_text', return_value=json.dumps({'mqtt': {'broker': 'localhost', 'topics': ['test']}})):
            return MqttCollector(config, self.cache, decode_mqtt)

    def test_network_callback_only_buffers_and_records_overflow(self):
        collector = self.collector()
        with patch.object(self.cache, 'ingest') as ingest:
            message = SimpleNamespace(topic='test', payload=b'{}')
            for _ in range(3):
                collector.on_message(None, None, message)
            ingest.assert_not_called()
        snapshot = collector.snapshot()
        self.assertEqual(snapshot['pending_packets'], 2)
        self.assertEqual(snapshot['dropped_packets'], 1)
        self.assertFalse(snapshot['history_complete'])
        collector.report()
        gaps = self.client.xrange(self.prefix + 'history_gaps:kepserver')
        self.assertEqual(gaps[0][1]['reason'], 'local_buffer_overflow')

    def test_writer_discards_stale_packet_and_writes_fresh_packet(self):
        collector = self.collector()
        collector.settings['discard_expired_packets'] = True
        collector.inbox.push(Packet('test', b'{}', 1, time.monotonic() - 10))
        payload = json.dumps({'timestamp': int(time.time() * 1000), 'values': [self.sample('kep')]}).encode()
        collector.inbox.push(Packet('test', payload, 12345, time.monotonic()))
        written = threading.Event()
        original = self.cache.ingest
        def ingest(*args, **kwargs):
            result = original(*args, **kwargs)
            written.set()
            return result
        with patch.object(self.cache, 'ingest', side_effect=ingest):
            thread = threading.Thread(target=collector.writer)
            thread.start()
            try:
                self.assertTrue(written.wait(5))
            finally:
                collector.stop_event.set()
                thread.join(timeout=2)
        record = json.loads(self.client.get(self.prefix + 'latest:kep'))
        self.assertEqual(record['received_time'], 12345)
        self.assertEqual(collector.snapshot()['dropped_packets'], 1)

    def test_default_writer_preserves_delayed_received_packets(self):
        collector = self.collector()
        timestamp = int(time.time() * 1000)//5000*5000
        payload = json.dumps({'timestamp': timestamp, 'values': [self.sample('kep', timestamp)]}).encode()
        collector.inbox.push(Packet('test', payload, timestamp - 10000, time.monotonic() - 10))
        written = threading.Event()
        original = self.cache.ingest
        def ingest(*args, **kwargs):
            result = original(*args, **kwargs)
            written.set()
            return result
        with patch.object(self.cache, 'ingest', side_effect=ingest):
            thread = threading.Thread(target=collector.writer)
            thread.start()
            try:
                self.assertTrue(written.wait(5))
            finally:
                collector.stop_event.set()
                thread.join(timeout=2)
        self.assertEqual(collector.snapshot()['dropped_packets'], 0)
        self.assertEqual(json.loads(self.client.get(self.prefix + 'latest:kep'))['received_time'], timestamp - 10000)

    def test_workers_share_topics_without_reordering(self):
        inbox = BoundedInbox()
        first = Packet('topic-a', b'first', 1, time.monotonic())
        second = Packet('topic-a', b'second', 2, time.monotonic())
        other = Packet('topic-b', b'other', 3, time.monotonic())
        for packet in (first, second, other):
            inbox.push(packet)
        self.assertIs(inbox.pop(timeout=0, shard=0, shards=4), first)
        self.assertIs(inbox.pop(timeout=0, shard=1, shards=4), other)
        self.assertIsNone(inbox.pop(timeout=0, shard=2, shards=4))
        inbox.release(first)
        self.assertIs(inbox.pop(timeout=0, shard=3, shards=4), second)
        inbox.release(second)
        inbox.release(other)

    def test_compact_history_samples_but_latest_updates_each_second(self):
        sent = int(time.time()*1000)//5000*5000
        for i in range(12):
            self.cache.ingest([self.sample('oa',sent+i*1000,value=i)], 'oa',sent+i*1000)
        history = self.client.xrange(self.prefix+'history:oa')
        self.assertEqual(len(history),3)
        self.assertEqual(set(history[0][1]),{'t','v'})
        self.assertEqual(json.loads(history[1][1]['v']),5)
        self.assertEqual(json.loads(self.client.get(self.prefix+'latest:oa'))['value'],11)
        restarted=Cache(self.client,prefix=self.prefix)
        restarted.ingest([self.sample('oa',sent+12000,value=12)],'oa',sent+12000)
        self.assertEqual(self.client.xlen(self.prefix+'history:oa'),3)

    def test_sources_share_send_time_grid_despite_arrival_delays(self):
        base=int(time.time()*1000)//5000*5000
        self.cache.policy={'oa':'oa','kep':'kepserver'}
        self.cache.mqtt_seeded=True
        for i in range(12):
            self.cache.ingest([self.sample('oa',base+i*1000+750,value=i)],'oa',base+i*1000+1500)
            self.cache.ingest([{'id':'path_kep','v':i,'q':True,'t':123}],
                              'kepserver',base+i*1000+82000,packet_time=base+i*1000+120)
        for eid in ('oa','kep'):
            history=self.client.xrange(self.prefix+'history:'+eid)
            self.assertEqual([int(row['t']) for _,row in history],[base,base+5000,base+10000])
            self.assertEqual([json.loads(row['v']) for _,row in history],[0,5,10])
        self.assertEqual(json.loads(self.client.get(self.prefix+'latest:oa'))['source_time'],base+11750)

    def test_missing_target_second_is_not_filled_from_other_seconds(self):
        base=int(time.time()*1000)//5000*5000
        for i in (1,2,3,4,6):
            self.cache.ingest([self.sample('oa',base+i*1000)],'oa',base+i*1000)
        self.assertEqual(self.client.xlen(self.prefix+'history:oa'),0)
        self.assertIsNotNone(self.client.get(self.prefix+'latest:oa'))

    def test_bad_quality_latest_is_kept_but_not_used_as_model_history(self):
        item={'id':'device_kep','v':12,'q':False,'t':1}
        self.cache.ingest([item],'kepserver',packet_time=int(time.time()*1000))
        self.assertEqual(self.client.xlen(self.prefix+'history:kep'),0)
        self.assertEqual(json.loads(self.client.get(self.prefix+'latest:kep'))['value'],12)

    def test_owner_switch_archives_history_and_clears_old_receipts(self):
        self.cache.ingest([self.sample('oa')],'oa')
        self.cache.publish_directory({'oa':'kepserver','kep':'kepserver'})
        self.cache.policy={'oa':'kepserver','kep':'kepserver'}
        self.assertIsNone(self.client.get(self.prefix+'latest:oa'))
        self.assertIsNone(self.client.hget(self.prefix+'receipt','oa'))
        self.assertEqual(self.client.xlen(self.prefix+'history:oa'),0)
        self.assertEqual(len(list(self.client.scan_iter(match=self.prefix+'history_archive:*:oa'))),1)
        self.assertEqual(self.cache.ingest([self.sample('oa')],'oa')['ignored'],1)
        self.assertEqual(self.cache.ingest([self.sample('oa')],'kepserver')['cached'],1)

    def test_fast_writer_cannot_use_old_ownership_after_catalog_switch(self):
        self.cache.policy={'kep':'kepserver'}
        self.cache.mqtt_seeded=True
        self.client.hset(self.cache.policy_key,'kep','oa')
        self.assertEqual(self.cache.ingest([self.sample('kep')],'kepserver')['cached'],0)
        self.assertIsNone(self.client.get(self.prefix+'latest:kep'))

    def test_directory_stays_ready_between_ten_minute_refreshes(self):
        self.cache.directory_loaded_at=time.time()-601
        self.assertTrue(self.cache.ready())
        self.cache.directory_loaded_at=time.time()-1261
        self.assertFalse(self.cache.ready())

    def test_kep_packet_time_and_last_change_are_separate(self):
        item = {'id': 'device_kep', 'v': 3.5, 'q': True, 't': 1764040121617}
        sent = int(time.time() * 1000)//5000*5000
        received = sent + 200
        self.cache.ingest([item], 'kepserver', received, packet_time=sent)
        latest = json.loads(self.client.get(self.prefix + 'latest:kep'))
        self.assertEqual(latest['source_time'], sent)
        self.assertEqual(latest['last_change_time'], item['t'])
        self.assertEqual(latest['received_time'], received)
        self.assertEqual(latest['time_schema'], 2)
        # Same unchanged value in a later packet is a new sample, not a duplicate.
        result = self.cache.ingest([item], 'kepserver', received + 1000, packet_time=sent + 1000)
        self.assertEqual(result['cached'], 1)
        self.assertEqual(self.client.xlen(self.prefix + 'history:kep'), 1)
        history = self.client.xrevrange(self.prefix + 'history:kep', count=1)[0][1]
        self.assertEqual(history['t'], str(sent))
        self.assertEqual(set(history), {'t','v'})

    def test_kep_new_time_semantics_replaces_legacy_even_if_older(self):
        now = int(time.time() * 1000)
        self.cache.ingest([{'id': 'device_kep', 'v': 1, 't': now + 10000}], 'kepserver')
        self.cache.ingest([{'id': 'device_kep', 'v': 1, 't': now + 10000}],
                          'kepserver', packet_time=now)
        self.assertEqual(json.loads(self.client.get(self.prefix + 'latest:kep'))['source_time'], now)

    def test_byte_limit_and_broker_thresholds(self):
        inbox = BoundedInbox(max_packets=5, max_bytes=6)
        packet = lambda data: Packet('test', data, 1, time.monotonic())
        first = packet(b'1234')
        self.assertEqual(inbox.push(first), [])
        self.assertEqual(inbox.push(packet(b'5678')), [first])
        self.assertEqual(inbox.snapshot()['pending_bytes'], 4)
        self.assertEqual(len(inbox.push(packet(b'oversize'))), 1)
        self.assertEqual(inbox.snapshot()['pending_bytes'], 4)
        self.assertFalse(over_broker_limit({'ready_messages': 10000}, {}))
        self.assertTrue(over_broker_limit({'ready_messages': 10001}, {}))
        self.assertFalse(over_broker_limit({'ready_bytes': 200 * 1024 * 1024}, {}))
        self.assertTrue(over_broker_limit({'ready_bytes': 200 * 1024 * 1024 + 1}, {}))
        self.assertFalse(over_broker_limit({'ready_messages': 1, 'ready_bytes': 10}, {}))

    def test_sustained_broker_backlog_disconnects_and_reconnects(self):
        collector = self.collector()
        collector.broker_settings = {'enabled': True, 'poll_seconds': 0, 'consecutive_overload': 2}
        collector.settings['cooldown_seconds'] = 0
        # Two polls trip the breaker, one cooldown wait, then terminate the controller.
        with patch.object(collector.stop_event, 'wait', side_effect=[False, False, False, True]), \
             patch.object(collector, 'broker_snapshot', return_value={'ready_messages': 10001,
                                                                     'ready_bytes': 1}), \
             patch.object(collector.client, 'disconnect') as disconnect, \
             patch.object(collector.client, 'loop_stop') as loop_stop, \
             patch.object(collector, 'connect') as reconnect:
            collector.controller()
        disconnect.assert_called_once()
        loop_stop.assert_called_once()
        reconnect.assert_called_once()
        self.assertEqual(collector.snapshot()['circuit_breaks'], 1)
        reasons = [item['reason'] for item in collector.gaps]
        self.assertIn('broker_queue_overload', reasons)
        self.assertIn('circuit_reconnect_history_gap', reasons)

    def test_local_checks_are_ten_seconds_apart_and_ignore_queue_age(self):
        for active_ms, expected_breaks in ((21000, 1), (20000, 0), (0, 0)):
            collector = self.collector()
            collector.settings.update(local_break_seconds=20, local_check_seconds=10,
                                      local_consecutive_overload=3)
            clock = {'value': 0}
            def wait(seconds):
                if seconds != 1:
                    return True  # End at cooldown; do not open a network connection.
                clock['value'] += 1
                return clock['value'] > 25
            with patch.object(collector.stop_event, 'wait', side_effect=wait), \
                 patch('mqtt_collector.time.monotonic', side_effect=lambda: clock['value']), \
                 patch.object(collector, 'snapshot', return_value={
                     'processing_active_ms': active_ms, 'oldest_pending_ms': 100000}), \
                 patch.object(collector, 'report'), \
                 patch.object(collector.client, 'disconnect'), \
                 patch.object(collector.client, 'loop_stop'):
                collector.controller()
            self.assertEqual(collector.metrics['circuit_breaks'], expected_breaks)
            if expected_breaks:
                self.assertEqual(clock['value'], 21)  # Checks at t=1,11,21.


if __name__ == '__main__':
    unittest.main()
