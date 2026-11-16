import unittest
import tempfile
import json
from pathlib import Path
from flask import Flask,jsonify
from werkzeug.security import generate_password_hash
from admin_access import install_admin

class AdminUsersTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        (self.root/'admin_credentials.json').write_text(json.dumps(dict(username='admin',password_hash=generate_password_hash('root-password-123'),secret_key='test-secret')))
        self.app=Flask(__name__);install_admin(self.app,self.root)
        @self.app.put('/api/business-test')
        def business():return jsonify(success=True)
        self.super=self.app.test_client();self.other=self.app.test_client()
        self.super.post('/api/admin/login',json=dict(username='admin',password='root-password-123'))
    def tearDown(self):self.temp.cleanup()
    def create(self):return self.super.post('/api/admin/users',json=dict(username='216330',name='测试管理员',password='member-password-123'))
    def test_roles_and_business_authority(self):
        self.assertEqual(self.create().status_code,200)
        self.assertEqual(self.other.post('/api/admin/login',json=dict(username='216330',password='member-password-123')).status_code,200)
        self.assertEqual(self.other.get('/api/admin/status').json['role'],'admin')
        self.assertEqual(self.other.put('/api/business-test',json={}).status_code,200)
        self.assertEqual(self.other.get('/api/admin/users').status_code,403)
        self.assertEqual(self.other.post('/api/admin/users',json=dict(username='123',name='x',password='1234567890')).status_code,403)
        self.assertEqual(self.other.put('/api/admin/users/216330',json={'enabled':False}).status_code,403)
        self.assertTrue(self.super.get('/api/admin/status').json['superadmin'])
    def test_disable_restore_reset_revokes_sessions(self):
        self.create();self.other.post('/api/admin/login',json=dict(username='216330',password='member-password-123'))
        self.assertEqual(self.super.put('/api/admin/users/216330',json={'enabled':False}).status_code,200)
        self.assertFalse(self.other.get('/api/admin/status').json['admin'])
        self.assertEqual(self.other.post('/api/admin/login',json=dict(username='216330',password='member-password-123')).status_code,401)
        self.super.put('/api/admin/users/216330',json={'enabled':True})
        self.other.post('/api/admin/login',json=dict(username='216330',password='member-password-123'))
        self.super.put('/api/admin/users/216330',json={'password':'changed-member-123'})
        self.assertFalse(self.other.get('/api/admin/status').json['admin'])
        self.assertEqual(self.other.post('/api/admin/login',json=dict(username='216330',password='changed-member-123')).status_code,200)
    def test_no_superuser_escalation_or_disable(self):
        self.assertEqual(self.super.post('/api/admin/users',json=dict(username='123',name='x',password='1234567890',role='superadmin')).status_code,400)
        self.assertEqual(self.super.put('/api/admin/users/admin',json={'enabled':False}).status_code,400)
        self.create();self.assertEqual(self.create().status_code,409)
        payload=self.super.get('/api/admin/users').get_data(as_text=True)
        self.assertNotIn('password_hash',payload)
    def test_individual_password_change_and_restart_persistence(self):
        self.create();self.other.post('/api/admin/login',json=dict(username='216330',password='member-password-123'))
        self.assertEqual(self.other.post('/api/admin/password',json=dict(old_password='member-password-123',password='my-new-password-123')).status_code,200)
        self.assertTrue(self.super.get('/api/admin/status').json['admin'])
        app=Flask('new');install_admin(app,self.root)
        client=app.test_client();self.assertEqual(client.post('/api/admin/login',json=dict(username='216330',password='my-new-password-123')).status_code,200)

    def test_short_passwords_supported_for_create_change_and_reset(self):
        r=self.super.post('/api/admin/users',json=dict(username='123',name='短密码测试',password='1'))
        self.assertEqual(r.status_code,200)
        self.assertEqual(self.other.post('/api/admin/login',json=dict(username='123',password='1')).status_code,200)
        self.assertEqual(self.other.post('/api/admin/password',json=dict(old_password='1',password='2')).status_code,200)
        self.assertEqual(self.super.put('/api/admin/users/123',json={'password':'3'}).status_code,200)
        self.assertEqual(self.other.post('/api/admin/login',json=dict(username='123',password='3')).status_code,200)
