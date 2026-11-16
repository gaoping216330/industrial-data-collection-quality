import unittest
from unittest.mock import Mock,MagicMock
from datetime import datetime
from flask import Flask
from device_manual_reason import enrich,install

class ManualReasonTests(unittest.TestCase):
    def test_device_note_inherited_by_all_matching_eids(self):
        token='a'*64;other='b'*64
        rows=[{'eid':'one','display_group_token':token},{'eid':'two','display_group_token':token},{'device_token':token},{'eid':'third','display_group_token':other}]
        query=Mock(return_value=[dict(device_token=token,reason='PLC检修',updated_by='admin',updated_at=datetime(2026,10,27,12,0))])
        result=enrich(rows,query)
        self.assertTrue(all(r['manual_reason']=='PLC检修' for r in result[:3]))
        self.assertEqual(result[3]['manual_reason'],'')
        query.assert_called_once()
    def test_admin_update_stores_note_and_audit(self):
        app=Flask(__name__);app.secret_key='test';connect=MagicMock();query=Mock(return_value=[dict(department='厂',process='炉',equipment='设备',group='组')])
        cursor=connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value;cursor.fetchone.return_value={'reason':'旧原因'}
        install(app,connect,query);client=app.test_client();url='/api/devices/'+'a'*64+'/manual-reason'
        self.assertEqual(client.put(url,json={'reason':'新原因'}).status_code,401)
        with client.session_transaction() as s:s.update(admin=True,username='tester')
        self.assertEqual(client.put(url,json={'reason':'新原因'}).status_code,200)
        calls=cursor.execute.call_args_list
        self.assertTrue(any('UPDATE dq_device_manual_reason SET' in c.args[0] and c.args[1][0]=='新原因' for c in calls))
        audit=next(c for c in calls if 'INSERT INTO dq_device_manual_reason_audit' in c.args[0]);self.assertEqual(audit.args[1][1:4],('旧原因','新原因','tester'))
        connect.return_value.__enter__.return_value.commit.assert_called_once()
        self.assertEqual(client.put(url,json={'reason':'x'*1001}).status_code,400)

    def test_event_rows_without_device_token_inherit_by_eid(self):
        token='a'*64
        rows=[{'eid':'one','reason':'质量位BAD'}]
        query=Mock(side_effect=[[{'eid':'one','display_group_token':token}],[dict(device_token=token,reason='检修中',updated_by='216330',updated_at=datetime(2026,10,28,9,0))]])
        result=enrich(rows,query)
        self.assertEqual(result[0]['manual_reason'],'检修中')
        self.assertEqual(result[0]['manual_reason_by'],'216330')
        self.assertEqual(result[0]['reason'],'质量位BAD')
