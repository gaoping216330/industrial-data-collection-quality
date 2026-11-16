import unittest
from collections import defaultdict
from collection_health import SourceGate
from confirmation_window import ConfirmedObservations

class SourceHealthTests(unittest.TestCase):
    def test_startup_recovery_and_continuous_outage(self):
        gate=SourceGate(0)
        self.assertEqual(gate.observe(50000,False,'')['state'],'启动观察')
        for now in range(60000,90000,10000):self.assertTrue(gate.observe(now,False,'')['blocked'])
        self.assertFalse(gate.observe(90000,False,'')['blocked'])
        for now in range(100000,400000,10000):self.assertEqual(gate.observe(now,True,'MQTT断开')['state'],'待确认')
        self.assertEqual(gate.observe(400000,True,'MQTT断开')['state'],'未恢复')
        for now in range(410000,440000,10000):self.assertTrue(gate.observe(now,False,'')['blocked'])
        self.assertFalse(gate.observe(440000,False,'')['blocked'])
        self.assertEqual(gate.resume_ms,440000)
    def test_observation_gap_does_not_confirm_unobserved_outage(self):
        gate=SourceGate(0)
        gate.observe(60000,True,'无数据')
        self.assertEqual(gate.observe(500000,True,'无数据')['state'],'待确认')
    def test_single_healthy_packet_does_not_end_outage(self):
        gate=SourceGate(0)
        for now in range(60000,370000,10000):gate.observe(now,True,'无数据')
        gate.observe(370000,False,'')
        self.assertTrue(gate.observe(380000,True,'无数据')['blocked'])

class RecoveryAndSuspensionTests(unittest.TestCase):
    def test_chain_failure_keeps_existing_alarm_and_cancels_new_pending(self):
        p=ConfirmedObservations(defaultdict(set,{'old':{'停止发送'}}),recovery_ms=30000)
        p.observe('new',{'停止发送'},0)
        self.assertEqual(p.observe('old',set(),10000,suspended=True),{'停止发送'})
        p.observe('new',set(),10000,suspended=True)
        self.assertFalse(p.observe('new',{'停止发送'},320000))
        self.assertFalse(p.closes)
    def test_recovery_is_continuous_and_uses_first_normal_time(self):
        p=ConfirmedObservations(defaultdict(set,{'p':{'BAD'}}),recovery_ms=30000)
        for now in (0,10000,20000):self.assertEqual(p.observe('p',set(),now),{'BAD'})
        self.assertEqual(p.observe('p',set(),30000),set())
        self.assertEqual(p.closes,[('p','BAD',0)])
    def test_recovery_flapping_and_monitor_gap_reset_timer(self):
        p=ConfirmedObservations(defaultdict(set,{'p':{'BAD'}}),recovery_ms=30000)
        p.observe('p',set(),0);p.observe('p',{'BAD'},10000)
        p.observe('p',set(),20000)
        self.assertEqual(p.observe('p',set(),80000),{'BAD'})
        self.assertFalse(p.closes)

if __name__=='__main__':unittest.main()
