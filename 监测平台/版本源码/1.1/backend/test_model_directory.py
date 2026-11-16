import unittest
from unittest.mock import MagicMock
from model_worker import refill


class ModelDirectoryTests(unittest.TestCase):
    def test_model_history_accepts_both_current_sources_and_rejects_removed_points(self):
        cache=MagicMock()
        cache.hmget.side_effect=lambda key,eids:[{'kep':'kepserver','oa':'oa','removed':None}[eid] for eid in eids]
        cache.pipeline.return_value.__enter__.return_value.execute.return_value=[[('1-0',{'t':'10000','v':'2'})],[('1-0',{'t':'10000','v':'3'})]]
        detector=MagicMock()
        detector.cache_minutes=60
        detector.registry.required_eids=['kep','oa','removed']
        self.assertEqual(refill(cache,detector,20000),2)
        self.assertEqual(detector.cache.add.call_count,2)
        detector.cache.add.assert_any_call('kep',2,True,10000)
        detector.cache.add.assert_any_call('oa',3,True,10000)
        self.assertEqual(cache.pipeline.return_value.__enter__.return_value.xrange.call_count,2)

    def test_second_read_uses_exclusive_cursor_and_source_change_resets_cache(self):
        cache=MagicMock();cache.hmget.return_value=['kepserver'];cache.hget.return_value='0'
        pipe=cache.pipeline.return_value.__enter__.return_value
        pipe.execute.return_value=[[('10000-0',{'t':'10000','v':'2'})]]
        detector=MagicMock();detector.cache_minutes=60;detector.registry.required_eids=['a']
        self.assertEqual(refill(cache,detector,20000),1)
        pipe.execute.return_value=[[('15000-0',{'t':'15000','v':'3'})]]
        self.assertEqual(refill(cache,detector,25000),1)
        self.assertEqual(pipe.xrange.call_args.kwargs['min'],'(10000-0')
        cache.hmget.return_value=['oa']
        refill(cache,detector,30000)
        detector.cache.reset_eid.assert_called_once_with('a')
        self.assertNotEqual(pipe.xrange.call_args.kwargs['min'],'(15000-0')

    def test_recent_global_gap_does_not_override_point_model_with_valid_cached_history(self):
        from types import SimpleNamespace
        from unittest.mock import patch
        from model_worker import run_once
        cache=MagicMock();cache.hgetall.return_value={'last_gap_at':'1800000000000','connected':'true'}
        detector=MagicMock();detector.registry.ready_eids={'a'};detector.registry.unloaded_eids=set()
        detector.cache.coverage_count.return_value=1;detector.history_refill_paused=False
        result=SimpleNamespace(eid='a',result='正常');detector.run_detection_once.return_value=[result]
        with patch('model_worker.load_disabled_targets',return_value=set()),patch('model_worker.refill',return_value=0),patch('model_worker.save_results') as save,patch('model_worker.set_status'):
            run_once(cache,detector)
        detector.run_detection_once.assert_called_once()
        self.assertEqual(result.result,'正常')
        save.assert_called_once_with(detector,[result])

    def test_live_ingestion_backlog_pauses_reads_without_clearing_cached_history(self):
        cache=MagicMock();cache.hmget.return_value=['kepserver'];cache.hget.return_value='20000'
        detector=MagicMock();detector.registry.required_eids=['a'];detector.cache_minutes=45
        detector.history_cursors={'a':'10000-0'};detector.history_owners={'a':'kepserver'}
        self.assertEqual(refill(cache,detector,20000),0)
        self.assertTrue(detector.history_refill_paused)
        self.assertEqual(detector.history_cursors,{'a':'10000-0'})
        detector.cache.reset_eid.assert_not_called()
        detector.cache.add.assert_not_called()
        cache.pipeline.assert_not_called()


if __name__=='__main__':
    unittest.main()
