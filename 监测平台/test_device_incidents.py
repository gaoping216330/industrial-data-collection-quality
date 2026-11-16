import unittest
from unittest.mock import MagicMock
from datetime import datetime
from device_incidents import record_devices,summarize

class DeviceIncidentTests(unittest.TestCase):
    def inputs(self):
        r={'source':'kepserver','department':'d','process':'p','equipment':'e','group':'g'}
        states={'a':{'row':dict(r),'reasons':{'质量位BAD'}},'b':{'row':dict(r),'reasons':set()}}
        return {'token':(['a','b'],'正常')},states,{'items':{'kepserver':{'blocked':False}}}
    def test_partial_failure_is_device_abnormal_not_entire_device_offline(self):
        devices,states,health=self.inputs()
        affected,reason,muted=summarize(['a','b'],states,set())
        self.assertEqual(len(affected),1);self.assertTrue(reason.startswith('设备异常'))
        for s in states.values():s['reasons']={'停止发送'}
        self.assertTrue(summarize(['a','b'],states,set())[1].startswith('设备掉线'))
    def test_same_device_does_not_insert_repeated_incidents(self):
        devices,states,health=self.inputs();c=MagicMock()
        c.fetchall.side_effect=[[{'device_token':'token','id':'id','affected_count':1,'reason':'设备异常：质量位BAD'}],[]]
        record_devices(c,devices,states,health,set(),1800000000000)
        self.assertFalse(any('INSERT' in call.args[0] or 'UPDATE' in call.args[0] for call in c.execute.call_args_list))
    def test_chain_outage_never_recovers_existing_device_incident(self):
        devices,states,health=self.inputs();health['items']['kepserver']['blocked']=True;c=MagicMock()
        c.fetchall.side_effect=[[{'device_token':'token','id':'id'}],[]]
        record_devices(c,devices,states,health,set(),1800000000000)
        self.assertFalse(any('UPDATE' in call.args[0] for call in c.execute.call_args_list))
    def test_muting_ends_tracking_without_claiming_recovery(self):
        devices,states,health=self.inputs()
        for s in states.values():s['row']['quality_check']=1
        c=MagicMock();c.fetchall.side_effect=[[{'device_token':'token','id':'id','opened_at':datetime(2026,1,1)}],[]]
        record_devices(c,devices,states,health,{'报警已屏蔽'},1800000000000)
        updates=[call for call in c.execute.call_args_list if 'UPDATE' in call.args[0]]
        self.assertEqual(len(updates),1);self.assertIn('未证明现场恢复',updates[0].args[1][1])

if __name__=='__main__':unittest.main()
