import json
import tempfile
import unittest
from pathlib import Path
from zipfile import ZipFile
from io import BytesIO
from xml.etree import ElementTree as ET
from flask import Flask,jsonify,request
from werkzeug.security import generate_password_hash
from admin_access import install_admin
from excel_export import install_export,workbook

class AdminExportTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();root=Path(self.tmp.name)
        (root/'admin_credentials.json').write_text(json.dumps({'username':'admin','password_hash':generate_password_hash('test-password-123'),'secret_key':'test-secret'}))
        self.app=Flask(__name__);install_admin(self.app,root);install_export(self.app)
        @self.app.put('/api/points/test/alarm')
        def alarm():return jsonify(success=True)
        @self.app.get('/api/points')
        def points():
            page=int(request.args['page']);self.seen.append(dict(request.args))
            return jsonify(total=101,items=[{'eid':str(i),'point':'=1+1','value_text':'1.234'} for i in range((page-1)*50,min(page*50,101))])
        self.seen=[];self.client=self.app.test_client()
    def tearDown(self):self.tmp.cleanup()
    def test_permissions(self):
        self.assertEqual(self.client.put('/api/points/test/alarm',json={}).status_code,401)
        self.assertEqual(self.client.post('/api/admin/login',json={'username':'admin','password':'wrong'}).status_code,401)
        self.assertEqual(self.client.post('/api/admin/login',json={'username':'admin','password':'test-password-123'}).status_code,200)
        self.assertEqual(self.client.put('/api/points/test/alarm',json={}).status_code,200)
        self.assertEqual(self.client.put('/api/points/test/alarm',json={},headers={'Origin':'http://evil.example'}).status_code,403)
        self.client.post('/api/admin/logout',json={})
        self.assertEqual(self.client.put('/api/points/test/alarm',json={}).status_code,401)
    def test_password_change(self):
        self.client.post('/api/admin/login',json={'username':'admin','password':'test-password-123'})
        self.assertEqual(self.client.post('/api/admin/password',json={'old_password':'test-password-123','password':'new-password-456'}).status_code,200)
        self.assertFalse(self.client.get('/api/admin/status').json['admin'])
        self.assertEqual(self.client.post('/api/admin/login',json={'username':'admin','password':'new-password-456'}).status_code,200)
    def test_full_filtered_export(self):
        r=self.client.get('/api/export?view=points&department=炼铁厂&eids=abc,def')
        self.assertEqual(r.status_code,200)
        self.assertEqual(len(self.seen),3)
        self.assertTrue(all(x['status']=='异常' and x['department']=='炼铁厂' and x['eids']=='abc,def' for x in self.seen))
        with ZipFile(BytesIO(r.data)) as z:
            for name in z.namelist():ET.fromstring(z.read(name))
            sheet=ET.fromstring(z.read('xl/worksheets/sheet1.xml'))
            ns={'s':'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
            self.assertEqual(len(sheet.findall('s:sheetData/s:row',ns)),102)
            self.assertEqual(len(sheet.findall('.//s:f',ns)),0)
    def test_empty_export(self):
        with ZipFile(workbook([])) as z:ET.fromstring(z.read('xl/worksheets/sheet1.xml'))
