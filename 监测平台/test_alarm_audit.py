import unittest
from unittest.mock import patch,MagicMock
import web_app as w

class AlarmAuditTests(unittest.TestCase):
    def setUp(self):
        self.client=w.app.test_client()
        with self.client.session_transaction() as s:s.update(admin=True,username='admin',login_at='2026-10-27T16:00:00')
        self.row={'eid':'test','quality_check':0,'point':'压力','source':'kepserver','department':'测试部门'}
        self.source=MagicMock();self.target=MagicMock()
        self.source.cursor.return_value.__enter__.return_value.fetchone.return_value=(0,)
        self.patchers=[patch.object(w,'query',return_value=[self.row]),patch.object(w.pymysql,'connect'),patch.object(w,'db_connect'),patch.object(w,'redis_connect'),patch.object(w.alarm_audit,'begin',return_value='audit-test-id'),patch.object(w.alarm_audit,'source_committed'),patch.object(w.alarm_audit,'finish_cursor'),patch.object(w.alarm_audit,'finish')]
        self.mocks=[p.start() for p in self.patchers]
        self.mocks[1].return_value.__enter__.return_value=self.source
        self.mocks[2].return_value.__enter__.return_value=self.target
    def tearDown(self):
        for p in reversed(self.patchers):p.stop()
    def test_success_audit_in_same_target_transaction(self):
        response=self.client.put('/api/points/test/alarm',json={'enabled':False})
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.json['audit_id'],'audit-test-id')
        self.mocks[4].assert_called_once()
        args=self.mocks[4].call_args.args
        self.assertEqual(args[1:5],('test',False,self.row,True))
        self.assertEqual(self.mocks[6].call_args.args[2],'SUCCESS')
        self.assertTrue(self.mocks[6].call_args.kwargs['source_saved'])
        self.assertTrue(self.mocks[6].call_args.kwargs['target_saved'])
        self.target.commit.assert_called_once()
    def test_source_failed(self):
        self.source.cursor.return_value.__enter__.return_value.execute.side_effect=[None,RuntimeError('source-write-failed')]
        response=self.client.put('/api/points/test/alarm',json={'enabled':False})
        self.assertEqual(response.status_code,503)
        self.assertEqual(self.mocks[7].call_args.args[2],'FAILED')
        self.target.commit.assert_not_called()
    def test_target_failed_keeps_source_audit(self):
        self.target.cursor.return_value.__enter__.return_value.execute.side_effect=RuntimeError('target-write-failed')
        response=self.client.put('/api/points/test/alarm',json={'enabled':False})
        self.assertEqual(response.status_code,503)
        self.assertEqual(self.mocks[7].call_args.args[2],'PARTIAL')
        self.assertTrue(self.mocks[7].call_args.kwargs['source_saved'])
    def test_audit_failure_blocks_source_update(self):
        self.mocks[4].side_effect=RuntimeError('audit-unavailable')
        response=self.client.put('/api/points/test/alarm',json={'enabled':False})
        self.assertEqual(response.status_code,503)
        sql=[c.args[0] for c in self.source.cursor.return_value.__enter__.return_value.execute.call_args_list]
        self.assertFalse(any('UPDATE autosensor' in q for q in sql))
        self.source.commit.assert_not_called()
    def test_denied_operation_audited(self):
        with self.client.session_transaction() as s:s.clear()
        response=self.client.put('/api/points/test/alarm',json={'enabled':False})
        self.assertEqual(response.status_code,401)
        self.assertEqual(self.mocks[4].call_args.kwargs['result'],'DENIED')
        self.mocks[1].assert_not_called()
    def test_read_audit_admin_only(self):
        with self.client.session_transaction() as s:s.clear()
        self.assertEqual(self.client.get('/api/alarm-audit').status_code,401)
