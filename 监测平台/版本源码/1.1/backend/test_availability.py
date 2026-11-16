import unittest
from availability import AvailabilityTracker

class AvailabilityTests(unittest.TestCase):
    def test_never_received_requires_five_minutes_and_remembers_start(self):
        tracker=AvailabilityTracker();now=1800000000000
        self.assertEqual(tracker.observe('a',None,now,now-20000),'断线待确认')
        for step in range(10000,300000,10000):
            self.assertEqual(tracker.observe('a',None,now+step,now-20000),'断线待确认')
        self.assertEqual(tracker.observe('a',None,now+300000,now-20000),'从未收到')
        self.assertEqual(tracker.since['a'],now-20000)
    def test_confirmed_stop_uses_actual_last_received_time(self):
        tracker=AvailabilityTracker();now=1800000000000
        self.assertEqual(tracker.observe('a',{'received_time':now-360000},now,now-10000),'停止发送')
        self.assertEqual(tracker.since['a'],now-360000)
    def test_recovery_and_brief_outage_are_not_offline(self):
        tracker=AvailabilityTracker();now=1800000000000
        self.assertEqual(tracker.observe('a',{'received_time':now-11000},now,now-20000),'断线待确认')
        self.assertEqual(tracker.observe('a',{'received_time':now},now,now-20000),'正常')
        self.assertNotIn('a',tracker.since)
    def test_collector_backlog_does_not_prove_a_new_outage(self):
        tracker=AvailabilityTracker();now=1800000000000
        self.assertEqual(tracker.observe('a',None,now,now-20000,unknown=True),'待判定')

    def test_explicit_reset_prevents_old_receipt_instant_confirmation(self):
        now=1800000000000;tracker=AvailabilityTracker(reset_ms=now)
        receipt={'received_time':now-86400000}
        self.assertEqual(tracker.observe('a',receipt,now,now),'断线待确认')
        self.assertEqual(tracker.since['a'],now)
        for step in range(10000,300000,10000):
            self.assertEqual(tracker.observe('a',receipt,now+step,now),'断线待确认')
        self.assertEqual(tracker.observe('a',receipt,now+300000,now),'停止发送')
