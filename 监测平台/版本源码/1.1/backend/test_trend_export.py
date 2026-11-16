import unittest,io,zipfile
from unittest.mock import patch
from flask import Flask
from trend_export_api import install
class TrendExportTests(unittest.TestCase):
 def test_readonly_export_matches_window_and_preserves_raw_values(self):
  def query(sql,params):
   if 'training_peer' in sql:return [{'peer_eid':'b'}]
   return [{'eid':'a','point':'温度'},{'eid':'b','point':'压力'}]
  app=Flask('test-export');install(app,query)
  response=app.test_client().post('/api/trend-export',json={'eid':'a','mode':'live','begin':2000,'end':3000,'series':[{'eid':'a','points':[[1000,999.0],[2000,1.234567]]},{'eid':'b','points':[[2000,56.789]]}]})
  self.assertEqual(response.status_code,200)
  z=zipfile.ZipFile(io.BytesIO(response.data));sheet=z.read('xl/worksheets/sheet1.xml').decode()
  self.assertIn('1.234567',sheet);self.assertIn('56.789',sheet);self.assertNotIn('999.0',sheet)
  self.assertIn('采样时间（北京时间）',sheet);self.assertIn('趋势数据',z.read('xl/workbook.xml').decode())
 def test_unrelated_curve_rejected(self):
  app=Flask('test-reject');install(app,lambda *args:[])
  r=app.test_client().post('/api/trend-export',json={'eid':'a','series':[{'eid':'x','points':[]}]})
  self.assertEqual(r.status_code,400)
if __name__=='__main__':unittest.main()
