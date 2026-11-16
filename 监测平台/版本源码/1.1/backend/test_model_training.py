import unittest
from model_training_api import model_selection
from model_training_import import literal,number
from unittest.mock import patch,MagicMock

class TrainingSelectionTests(unittest.TestCase):
 def test_health_order_nulls_last_and_filter_enabled_only(self):
  where,args,order=model_selection({'result':'异常','sort':'health','direction':'desc','health_min':'20','health_max':'90'})
  self.assertIn("deployment_status='ready'",where);self.assertIn('qualityCheck,0)=0',where)
  self.assertEqual(args,['异常',20,90]);self.assertIn('IS NULL) ASC',order);self.assertIn(' DESC',order)
 def test_disabled_selection_does_not_use_stale_health(self):
  where,args,order=model_selection({'result':'检测已停用'})
  self.assertIn('qualityCheck,0)<>0',where)
 def test_invalid_health_is_rejected_and_sort_is_whitelisted(self):
  for x in ('nan','-1','101','xx'):
   with self.assertRaises(ValueError):model_selection({'health_min':x})
  _,_,order=model_selection({'sort':'health; DROP TABLE autosensor','direction':'oops'})
  self.assertEqual(order,'m.detected_at DESC,m.eid')
 def test_directory_and_owner_filters_are_parameterized(self):
  where,args,_=model_selection({'department':'炼铁厂','process':'一号高炉','equipment':'矿槽除尘','owner_name':"x' OR 1=1 --"})
  self.assertEqual(args,['炼铁厂','一号高炉','矿槽除尘',"x' OR 1=1 --"])
  self.assertIn('a.`department`=%s',where)
  self.assertIn('p.owner_name=%s',where)
  self.assertNotIn('OR 1=1',where)
 def test_training_literals_are_not_executable(self):
  self.assertEqual(literal("['a','b']",[]),['a','b'])
  self.assertEqual(literal("__import__('os').system('no')",[]),[])
  self.assertIsNone(number('nan'))
 def test_nonzero_qualitycheck_is_disabled_when_directory_loaded(self):
  from quality_engine import load_directory
  c=MagicMock();c.fetchall.return_value=[{'eid':'p','qualityCheck':2,'channel':'kepserver','channeloa':'','device_name':'x','department':'d','process':'p','equipment':'e','group':'g'}]
  rows=load_directory(c)
  self.assertEqual(rows['p']['quality_check'],1)

if __name__=='__main__':unittest.main()
