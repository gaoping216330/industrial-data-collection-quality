import sys,json
from pathlib import Path
sys.path.insert(0,str(Path('监测平台').resolve()))
from quality_engine import db_connect
context=json.loads(Path('监测平台/training_context_eids.json').read_text(encoding='utf-8'))['groups']
with db_connect() as db:
 with db.cursor() as c:
  c.execute("SHOW COLUMNS FROM dq_model_training_info LIKE 'state_eids_json'")
  if not c.fetchone():c.execute('ALTER TABLE dq_model_training_info ADD COLUMN state_eids_json text')
  c.execute("SHOW COLUMNS FROM dq_model_training_peer LIKE 'relation_role'")
  if not c.fetchone():c.execute("ALTER TABLE dq_model_training_peer ADD COLUMN relation_role varchar(32) NOT NULL DEFAULT 'numeric_peer'")
  c.execute('SELECT eid,group_id FROM dq_model_training_info WHERE model_generated=1');targets=c.fetchall()
  rows=[];updates=[]
  for t in targets:
   eids=context.get(t['group_id'],[]);updates.append((json.dumps(eids),t['eid']))
   rows.extend((t['eid'],eid,i+1001) for i,eid in enumerate(eids) if eid!=t['eid'])
  for offset in range(0,len(updates),500):c.executemany('UPDATE dq_model_training_info SET state_eids_json=%s WHERE eid=%s',updates[offset:offset+500])
  for offset in range(0,len(rows),500):c.executemany("INSERT IGNORE INTO dq_model_training_peer(eid,peer_eid,rank_no,relation_role) VALUES(%s,%s,%s,'state_context')",rows[offset:offset+500])
  c.execute('SHOW CREATE VIEW autosensorAI_training');definition=c.fetchone()['Create View'];definition=definition.replace('CREATE ALGORITHM','CREATE OR REPLACE ALGORITHM');definition=definition if '`state_eids_json` AS `state_eids_json`' in definition else definition.replace('`t`.`peer_count` AS `peer_count`,','`t`.`peer_count` AS `peer_count`,`t`.`state_eids_json` AS `state_eids_json`,');c.execute(definition)
 db.commit()
print('Imported group state context associations:',len(rows))
