import unittest
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parent.parent/'实时数据缓存服务'))
from mqtt_duplicates import DuplicateTracker

class DuplicateTests(unittest.TestCase):
    def setUp(self):self.t=DuplicateTracker();self.p={'abc':'kepserver','oa':'oa'}
    def point(self,value=1):return {'id':'device_abc','v':value,'t':50,'q':True}
    def test_constant_is_not_duplicate(self):
        for ts in range(10):self.t.observe([self.point()],ts,'one',1000+ts,self.p)
        self.assertEqual(self.t.snapshot(self.p,1100)['point_count'],0)
    def test_repeated_same_timestamp_and_conflict(self):
        self.t.observe([self.point()],1,'one',1000,self.p)
        self.t.observe([self.point()],1,'one',1001,self.p)
        self.t.observe([self.point(2)],1,'two',1002,self.p)
        rows=self.t.snapshot(self.p,1100)['items']
        self.assertEqual(sum(r['duplicate_count'] for r in rows),2)
        self.assertEqual(sum(r['conflict_count'] for r in rows),1)
        self.assertTrue(any(r['topics']==['one','two'] for r in rows))
    def test_duplicate_inside_packet(self):
        self.t.observe([self.point(),self.point()],1,'one',1000,self.p)
        self.assertEqual(self.t.snapshot(self.p,1100)['items'][0]['duplicate_count'],1)
    def test_overlapping_topics_without_matching_timestamp(self):
        self.t.observe([self.point()],1,'one',1000,self.p)
        self.t.observe([self.point()],2,'two',1001,self.p)
        self.assertEqual(self.t.snapshot(self.p,1100)['point_count'],0)
    def test_expiration_and_ownership(self):
        self.t.observe([self.point(),self.point()],1,'one',1000,self.p)
        self.assertEqual(self.t.snapshot(self.p,602000)['point_count'],0)
        self.t.observe([{'id':'oa','v':1}]*2,2,'two',603000,self.p)
        self.assertEqual(self.t.snapshot(self.p,603000)['point_count'],0)

    def test_two_raw_addresses_are_retained_even_with_different_send_times(self):
        self.t.observe([self.point()],1,'one',1000,self.p)
        other=dict(self.point(),id='other_abc')
        self.t.observe([other],2,'two',2000,self.p)
        event=self.t.snapshot(self.p,2100)['items'][0]
        self.assertEqual({event['address_a'],event['address_b']},{'device_abc','other_abc'})
        self.assertIn('不同MQTT地址共用EID',event['types'])
    def test_missing_full_address_does_not_fabricate_second_tag(self):
        self.t.observe([{'id':'abc','v':1}]*2,1,'one',1000,self.p)
        event=self.t.snapshot(self.p,1001)['items'][0]
        self.assertIsNone(event['address_a']);self.assertIsNone(event['address_b'])
    def test_recovery_requires_continuously_observed_clean_data(self):
        self.t.observe([self.point(),self.point()],1,'one',1000,self.p)
        for now in range(10000,620001,10000):
            self.t.observe([self.point()],now,'one',now,self.p)
            self.t.advance(self.p,now,True)
        event=self.t.snapshot(self.p,620001,history=True)['items'][0]
        self.assertEqual(event['ended_at'],10000)
        self.assertEqual(event['confirmed_at'],610000)
    def test_disconnected_monitor_does_not_close_episode(self):
        self.t.observe([self.point(),self.point()],1,'one',1000,self.p)
        self.t.advance(self.p,700000,False)
        event=self.t.snapshot(self.p,700000,history=True)['items'][0]
        self.assertIsNone(event['ended_at'])
    def test_completed_episode_survives_new_duplicate_before_database_flush(self):
        self.t.observe([self.point(),self.point()],1,'one',1000,self.p)
        for now in range(10000,620001,10000):
            self.t.observe([self.point()],now,'one',now,self.p);self.t.advance(self.p,now,True)
        self.t.observe([self.point(),self.point()],630000,'one',630000,self.p)
        rows=self.t.snapshot(self.p,630001,history=True)['items']
        self.assertEqual(len(rows),2)
        self.assertEqual(len({r['episode_id'] for r in rows}),2)
        self.assertEqual(sum(r['ended_at'] is not None for r in rows),1)
