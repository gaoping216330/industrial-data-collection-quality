import unittest
from datetime import datetime,timedelta
from history_service import fetch_history,HistoryError

class HistoryTests(unittest.TestCase):
    def test_pages_at_boundary_without_skipping_or_duplicating_samples(self):
        begin=datetime(2026,1,1);end=begin+timedelta(minutes=4000)
        def post(payload):
            start=datetime.fromisoformat(payload['beginTime']);stop=datetime.fromisoformat(payload['endTime'])
            rows=[]
            while start<=stop and len(rows)<payload['limit']:
                rows.append({'ts':start.isoformat(sep=' '),'v':99 if start==begin+timedelta(minutes=100) else 1})
                start+=timedelta(minutes=1)
            return {'code':'200','data':[{'eid':'a','datas':rows}]}
        result=fetch_history('a','minute',begin.isoformat(),end.isoformat(),post)
        self.assertEqual(result['requests'],2)
        self.assertEqual(result['total_samples'],4001)
        self.assertEqual(len(result['history']),4001)
        self.assertIn(99,[value for _,value in result['history']])
        self.assertEqual(result['history'][0][0],1767196800000)
    def test_null_history_is_successful_empty_result(self):
        result=fetch_history('a','day','2026-01-01','2026-01-02',lambda payload:{'code':200,'data':[{'eid':'a','datas':None}]})
        self.assertEqual(result['history'],[])
    def test_rejects_bad_range_or_granularity(self):
        for kind,begin,end in [('millisecond','2026-01-01','2026-01-02'),('hour','2026-02-01','2026-01-01')]:
            with self.assertRaises(ValueError):fetch_history('a',kind,begin,end)
    def test_upstream_failure_is_not_reported_as_empty_success(self):
        with self.assertRaises(HistoryError):fetch_history('a','hour','2026-01-01','2026-01-02',lambda payload:{'code':'500'})

    def test_second_page_saturation_continues_from_last_timestamp(self):
        begin=datetime(2026,1,1);end=begin+timedelta(seconds=4000);payloads=[]
        def post(payload):
            payloads.append(payload)
            start=datetime.fromisoformat(payload['beginTime']);stop=datetime.fromisoformat(payload['endTime'])
            rows=[]
            while start<=stop and len(rows)<3600:
                rows.append({'ts':start.isoformat(sep=' '),'v':1.23456});start+=timedelta(milliseconds=500)
            return {'code':'200','data':[{'eid':'a','datas':rows}]}
        result=fetch_history('a','second',begin.isoformat(),end.isoformat(),post)
        self.assertEqual(result['total_samples'],8001)
        self.assertGreaterEqual(result['requests'],3)
        self.assertTrue(all(p['limit']==3600 and p['orderBy']=='asc' for p in payloads))
        self.assertNotIn('precision',payloads[0])
        self.assertEqual(result['history'][0][1],1.23456)
