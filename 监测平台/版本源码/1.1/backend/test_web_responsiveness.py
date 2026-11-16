import time
import unittest
from unittest.mock import patch

import redis
import web_app


class WebResponsivenessTests(unittest.TestCase):
    def test_previous_source_cache_is_not_displayed_under_new_owner(self):
        import json
        rows=[{'eid':'same','source':'kepserver'}]
        with patch.object(web_app,'redis_connect') as connect:
            connect.return_value.pipeline.return_value.__enter__.return_value.execute.return_value=(
                [json.dumps({'source':'oa','received_time':123})],
                [json.dumps({'source':'oa','value':999})])
            result=web_app.live_values(rows)
        self.assertIsNone(result[0]['receipt_time_ms'])
        self.assertNotEqual(result[0]['value_text'],'999')

    def test_overview_serves_snapshot_without_waiting_for_redis(self):
        with patch.object(web_app,'start_overview_poll'), patch.object(web_app,'redis_connect') as connect:
            started=time.monotonic()
            response=web_app.app.test_client().get('/api/overview')
            self.assertEqual(response.status_code,200)
            self.assertLess(time.monotonic()-started,0.5)
            connect.assert_not_called()
            self.assertIn('snapshot_status',response.get_json())

    def test_redis_timeout_preserves_database_values_and_marks_fallback(self):
        rows=[{'eid':'example','value_text':'12.5','receipt_time_ms':123000}]
        with patch.object(web_app,'redis_connect') as connect:
            connect.return_value.pipeline.side_effect=redis.TimeoutError('busy')
            result=web_app.live_values(rows)
        self.assertEqual(result[0]['value_text'],'12.5')
        self.assertEqual(result[0]['receipt_time_ms'],123000)
        self.assertTrue(result[0]['live_unavailable'])


if __name__=='__main__':
    unittest.main()


class PublishedCacheTests(unittest.TestCase):
    def test_reuses_snapshot_and_invalidates_on_publication(self):
        key=('test','revision-cache')
        web_app._result_cache.pop(key,None)
        with patch.object(web_app,'read_publication',return_value={'version':'first'}) as revision:
            loader=__import__('unittest.mock',fromlist=['MagicMock']).MagicMock(return_value=[{'n':1}])
            first=web_app.published_cache(key,loader);first[0]['n']=9
            self.assertEqual(web_app.published_cache(key,loader),[{'n':1}])
            self.assertEqual(loader.call_count,1)
            revision.return_value={'version':'next'}
            web_app.published_cache(key,loader)
            self.assertEqual(loader.call_count,2)
