import unittest
from flask import Flask
from unittest.mock import MagicMock
from flatline_settings import install
from quality_engine import flat_state, quality_bad

class FlatlineSettingsTests(unittest.TestCase):
    def test_nonzero_only_excludes_flatline(self):
        rule=dict(flatline_enabled=True,flatline_seconds=300,excluded=False)
        for value in (1,2,-1):
            self.assertEqual(flat_state(dict(tag_type='Float',flatline_disabled=value),{}, {},{},rule,1000000,0),'excluded')
        self.assertTrue(quality_bad('BAD'))
    def test_admin_required_and_validates_payload(self):
        app=Flask(__name__);app.secret_key='test';connect=MagicMock()
        install(app,connect,MagicMock(),'test:');client=app.test_client()
        self.assertEqual(client.put('/api/points/x/flatline',json={'enabled':False}).status_code,401)
        with client.session_transaction() as s:s['admin']=True;s['username']='test'
        self.assertEqual(client.put('/api/points/x/flatline',json={'enabled':1}).status_code,400)
        connect.assert_not_called()
    def test_local_update_and_audit_atomic(self):
        app=Flask(__name__);app.secret_key='test';connect=MagicMock();cur=connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value={'flatlineCheck':0};cur.lastrowid=7
        install(app,connect,MagicMock(),'test:');client=app.test_client()
        with client.session_transaction() as s:s['admin']=True;s['username']='test'
        self.assertEqual(client.put('/api/points/x/flatline',json={'enabled':False}).status_code,200)
        sql=[c.args[0] for c in cur.execute.call_args_list]
        self.assertTrue(any('dq_flatline_setting_audit' in x for x in sql))
        self.assertTrue(any("reason='设备恒值异常'" in x for x in sql))
        connect.return_value.__enter__.return_value.commit.assert_called_once()
