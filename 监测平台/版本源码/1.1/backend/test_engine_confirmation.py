import json
import unittest
from unittest.mock import MagicMock, patch

from quality_engine import QualityEngine, PENDING_REASON, effective_rule


class EngineConfirmationTests(unittest.TestCase):
    def setUp(self):
        self.clock = 1_800_000_000_000
        self.cache = MagicMock()
        self.cache.hgetall.return_value = {}
        self.cache.pipeline.return_value.__enter__.return_value.execute.return_value = []
        self.db = MagicMock()
        self.cursor = self.db.__enter__.return_value.cursor.return_value.__enter__.return_value
        self.cache_patch = patch('quality_engine.redis_connect', return_value=self.cache)
        self.cache_patch.start()
        self.addCleanup(self.cache_patch.stop)
        self.db_patch = patch('quality_engine.db_connect', return_value=self.db)
        self.db_patch.start()
        self.addCleanup(self.db_patch.stop)
        self.publication_patch=patch('quality_engine.write_publication')
        self.publication_mock=self.publication_patch.start()
        self.addCleanup(self.publication_patch.stop)
        from types import SimpleNamespace
        self.health=MagicMock()
        self.health.gates={s:SimpleNamespace(resume_ms=0) for s in ('oa','kepserver')}
        self.health.observe.return_value={'items':{s:{'blocked':False,'state':'正常'} for s in ('oa','kepserver')}}
        hp=patch('quality_engine.CollectionHealth',return_value=self.health);hp.start();self.addCleanup(hp.stop)
        rp=patch('quality_engine.receiver_ready',return_value=True);rp.start();self.addCleanup(rp.stop)
        self.engine = QualityEngine()
        self.engine.started_ms = self.clock - 100_000
        self.engine.loaded_at = self.clock / 1000
        self.engine.load = MagicMock()
        self.engine.refresh_rules = MagicMock()
        self.quality = 'False'
        self.set_rows(['a'])
        self.cache.pipeline.return_value.__enter__.return_value.execute.side_effect = lambda: [[json.dumps({
            'received_time': self.clock, 'source_time': self.clock,
            'quality': self.quality}) for eid in self.engine.roster]]

    def set_rows(self, eids):
        self.engine.roster = {eid: dict(eid=eid,source='kepserver',device_key='device',
            tag_type='Float',device_name='PLC',department='dept',process='proc',
            equipment='equipment',group='group') for eid in eids}
        self.engine.previous = {eid: set() for eid in eids}
        self.engine.rules = {('global','kepserver',''): dict(
            flatline_enabled=False,flatline_seconds=600,tolerance=0,excluded=0)}

    def scan(self, offset):
        self.clock = 1_800_000_000_000 + offset
        with patch('quality_engine.time.time', return_value=self.clock / 1000):
            return self.engine.run_once()

    def test_alarm_muted_point_stays_in_roster_with_receipts_and_no_events(self):
        self.engine.roster['a']['quality_check']=1
        result=self.scan(0)
        self.assertEqual(result['expected'],{'kepserver':1})
        self.assertEqual(result['received'],{'kepserver':1})
        self.assertEqual(result['abnormal'],{})
        self.assertEqual(self.engine.previous['a'],{'报警已屏蔽'})
        for offset in range(10_000,310_000,10_000):self.scan(offset)
        inserts=[call for call in self.cursor.executemany.call_args_list if 'INSERT INTO dq_quality_event' in call.args[0]]
        self.assertEqual(inserts,[])
        self.assertEqual(self.engine.roster['a']['eid'],'a')
        writes=[call for call in self.cursor.executemany.call_args_list if 'INSERT INTO dq_point_quality_current' in call.args[0]]
        self.assertTrue(any(row[2]=='报警已屏蔽' for call in writes for row in call.args[1]))

    def test_observation_does_not_publish_early_and_event_uses_first_seen_time(self):
        first = self.scan(0)
        self.assertEqual(first['abnormal'], {})
        self.assertEqual(self.engine.previous['a'], {PENDING_REASON})
        self.cursor.reset_mock()
        for offset in range(10_000,300_000,10_000):
            self.scan(offset)
        self.cursor.executemany.assert_not_called()
        self.assertEqual(self.publication_mock.call_count,1)
        result = self.scan(300_000)
        self.assertEqual(self.publication_mock.call_count,2)
        self.assertEqual(result['abnormal'], {'kepserver':1})
        inserts = [call for call in self.cursor.executemany.call_args_list
                   if 'INSERT INTO dq_quality_event' in call.args[0]]
        self.assertEqual(len(inserts),1)
        self.assertEqual(inserts[0].args[1][0][4].timestamp()*1000,1_800_000_000_000)

    def test_current_and_device_writes_use_real_bulk_insert(self):
        from pymysql.cursors import RE_INSERT_VALUES
        self.scan(0)
        writes=[call for call in self.cursor.executemany.call_args_list if 'INSERT INTO dq_' in call.args[0]]
        self.assertTrue(writes)
        for call in writes:
            sql,rows=call.args
            self.assertIsNotNone(RE_INSERT_VALUES.match(sql))
            for row in rows:
                self.assertEqual(sql.count('%s'),len(row))

    def test_partial_flatline_never_creates_sensor_or_device_flatline_alarm(self):
        self.set_rows(['a','b'])
        self.quality = 'True'
        with patch('quality_engine.flat_state', side_effect=lambda row,*args: 'flat' if row['eid']=='a' else 'moving'):
            result = self.scan(0)
        self.assertEqual(result['abnormal'], {})
        self.assertEqual(self.engine.previous, {'a':set(),'b':set()})
        devices = [call for call in self.cursor.executemany.call_args_list
                   if 'INSERT INTO dq_device_quality_current' in call.args[0]]
        self.assertEqual(devices[0].args[1][0][-2], '正常')

    def test_distinct_collectors_on_same_business_device_share_flatline_decision(self):
        self.set_rows(['a','b']);self.quality='True'
        self.engine.roster['a']['device_key']='plc1'
        self.engine.roster['b']['device_key']='plc2'
        with patch('quality_engine.flat_state',side_effect=lambda row,*args:'flat' if row['eid']=='a' else 'moving'):
            for offset in range(0,310000,10000):result=self.scan(offset)
        self.assertEqual(result['abnormal'],{})
        self.assertNotIn('设备恒值异常',self.engine.previous['a'])

    def test_point_state_is_batched_in_single_insert_not_per_point_update(self):
        from pymysql.cursors import RE_INSERT_VALUES
        self.scan(0)
        writes=[call for call in self.cursor.executemany.call_args_list if 'dq_point_quality_current' in call.args[0]]
        for call in writes:
            sql,rows=call.args
            self.assertIsNotNone(RE_INSERT_VALUES.match(sql))
            self.assertEqual(sql.count('%s'),len(rows[0]))
            self.assertIn('data_status',sql)

    def test_chain_outage_suspends_point_confirmation(self):
        self.health.observe.return_value={'items':{s:{'blocked':True,'state':'未恢复'} for s in ('oa','kepserver')}}
        for offset in range(0,610000,10000):self.scan(offset)
        inserts=[call for call in self.cursor.executemany.call_args_list if 'INSERT INTO dq_quality_event' in call.args[0]]
        self.assertFalse(inserts)
        self.assertIn('采集不可用，现场状态未知',self.engine.previous['a'])

    def test_default_flatline_is_ten_minutes(self):
        rule = effective_rule(self.engine.roster['a'], {})
        self.assertEqual(rule['flatline_seconds'],600)

    def test_oa_false_quality_does_not_become_kepserver_bad_alarm(self):
        self.engine.roster['a']['source']='oa'
        # Isolate quality-bit behavior from the independently enabled OA flatline rule.
        self.engine.rules[('global','oa','')]=dict(flatline_enabled=False,flatline_seconds=600,tolerance=0,excluded=0)
        result=self.scan(0)
        self.assertEqual(result['abnormal'],{})
        self.assertNotIn('质量位BAD',self.engine.previous['a'])

    def test_all_flat_device_is_pending_until_confirmation_window_finishes(self):
        self.set_rows(['a','b'])
        self.quality = 'True'
        with patch('quality_engine.flat_state', return_value='flat'):
            first = self.scan(0)
            self.assertEqual(first['abnormal'], {})
            for offset in range(10_000,310_000,10_000):
                result = self.scan(offset)
        self.assertEqual(result['abnormal'], {'kepserver':2})
        self.assertEqual(self.engine.previous['a'], {'设备恒值异常'})
        self.assertEqual(self.engine.previous['b'], {'设备恒值异常'})


if __name__ == '__main__':
    unittest.main()
