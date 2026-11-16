import unittest
from flask import Flask
from unittest.mock import MagicMock
from rule_configuration import install,target_rows,describe_rules,audit
from quality_engine import effective_rule
from display_group import display_group_token

class RuleConfigurationTests(unittest.TestCase):
 def setUp(self):
  self.row=dict(eid='a',source='oa',department='d',process='p',equipment='e',group='g',point='v',device_key='oa:old',quality_check=0)
  self.row['display_group_token']=display_group_token(self.row)
 def rule(self,minutes):return dict(flatline_enabled=1,flatline_seconds=minutes*60,tolerance=0,excluded=0)
 def test_business_device_rule_and_point_priority(self):
  row=self.row;k='business:'+display_group_token(row)
  rules={('global','oa',''):self.rule(10),('device','oa',row['device_key']):self.rule(20),('device','oa',k):self.rule(30),('point','oa','a'):self.rule(40)}
  self.assertEqual(effective_rule(row,rules)['flatline_seconds'],2400)
  del rules[('point','oa','a')]
  self.assertEqual(effective_rule(row,rules)['flatline_seconds'],1800)
  other=dict(row,eid='b',device_key='different-collector')
  self.assertEqual(effective_rule(other,rules)['flatline_seconds'],1800)
 def test_target_validation_honors_source_and_full_business_key(self):
  target='business:'+self.row['display_group_token']
  self.assertEqual(target_rows([self.row],'device','oa',target),[self.row])
  self.assertEqual(target_rows([self.row],'device','kepserver',target),[])
  self.assertEqual(target_rows([self.row],'global','oa','unexpected'),[])
 def test_labels_are_human_readable(self):
  r=dict(scope='device',source='oa',target_key='business:'+self.row['display_group_token'])
  self.assertEqual(describe_rules([r],[self.row])[0]['target_label'],'d / p / e / g')
 def test_options_and_preview_are_read_only(self):
  app=Flask(__name__);query=MagicMock(side_effect=lambda sql,params=():[] if 'dq_quality_rule' in sql else [self.row])
  connect=MagicMock();install(app,query,lambda rows:rows,lambda key,load:load(),connect)
  c=app.test_client();options=c.get('/api/rule-target-options?source=oa&scope=device').json
  self.assertEqual(options['items'][0]['point_count'],1)
  response=c.get('/api/rule-effective?scope=point&source=oa&target_key=a')
  self.assertEqual(response.status_code,200);self.assertIsNone(response.json['saved_rule'])
  self.assertEqual(response.json['editable_rule']['flatline_seconds'],600)
  connect.assert_not_called()

 def test_rule_audit_keeps_actor_and_before_after(self):
  from flask import session
  import json
  app=Flask(__name__);app.secret_key='test';cur=MagicMock()
  with app.test_request_context('/api/rules',method='PUT',environ_base={'REMOTE_ADDR':'127.0.0.1'}):
   session['username']='tester'
   audit(cur,'UPDATE_RULE',{'id':7,'flatline_seconds':600},{'id':7,'flatline_seconds':1200})
  sql,args=cur.execute.call_args.args
  self.assertEqual(args[:4],('UPDATE_RULE',7,'tester','127.0.0.1'))
  self.assertEqual(json.loads(args[4])['flatline_seconds'],600)
  self.assertEqual(json.loads(args[5])['flatline_seconds'],1200)
