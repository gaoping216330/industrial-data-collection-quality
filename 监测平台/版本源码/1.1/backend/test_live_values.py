import unittest
from unittest.mock import patch
import web_app

class LiveValuesTests(unittest.TestCase):
    def test_scope_uses_current_catalog_and_no_database(self):
        import json
        with patch.object(web_app,'redis_connect') as connect,patch.object(web_app,'query') as query:
            pipe=connect.return_value.pipeline.return_value.__enter__.return_value
            pipe.execute.return_value=[['kepserver',None,'oa'],[json.dumps({'source':'kepserver','received_time':123}),None,json.dumps({'source':'oa','received_time':456})],[json.dumps({'source':'kepserver','value':12.34}),None,json.dumps({'source':'oa','value':56.78})]]
            response=web_app.app.test_client().get('/api/point-live-values?eid=a&eid=removed&eid=b&eid=a')
            self.assertEqual(response.status_code,200)
            rows=response.get_json()['items']
            self.assertEqual([r['eid'] for r in rows],['a','b'])
            self.assertEqual([r['receipt_time_ms'] for r in rows],[123,456])
            self.assertEqual([r['value_text'] for r in rows],['12.34','56.78'])
            pipe.execute.assert_called_once()
            query.assert_not_called()
            self.assertEqual(response.headers['Cache-Control'],'no-store')
    def test_busy_cache_is_explicit_and_does_not_return_fake_freshness(self):
        from redis.exceptions import TimeoutError
        with patch.object(web_app,'redis_connect') as connect:
            connect.return_value.pipeline.return_value.__enter__.return_value.execute.side_effect=TimeoutError()
            response=web_app.app.test_client().get('/api/point-live-values?eid=a')
            self.assertTrue(response.get_json()['unavailable'])
            self.assertNotIn('refreshed_at',response.get_json())
            self.assertEqual(response.headers['Cache-Control'],'no-store')
    def test_limit_rejects_bulk_reads(self):
        response=web_app.app.test_client().get('/api/point-live-values?'+'&'.join('eid='+str(i) for i in range(52)))
        self.assertEqual(response.status_code,400)
