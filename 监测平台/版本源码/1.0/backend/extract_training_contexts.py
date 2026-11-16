import sys,json
from pathlib import Path
R=Path(__file__).resolve().parent;P=R.parent
sys.path.insert(0,str(P/'模型在线检测服务/.deps'));sys.path.insert(0,str(P))
import joblib
base=P/'outputs/all_sensor_typeai_ptf/models_53d'
m=json.loads((base/'manifest.json').read_text(encoding='utf-8-sig'));groups={};errors=[]
for gid in m['group_models']:
 try:
  bundle=joblib.load(base/'groups'/(gid+'.joblib'))
  groups[gid]=list(getattr(bundle,'state_eids',[]));del bundle
 except Exception as e:errors.append({'group':gid,'error':str(e)})
(R/'training_context_eids.json').write_text(json.dumps({'groups':groups,'errors':errors},ensure_ascii=False),encoding='utf-8')
print('Context groups:',len(groups),'errors:',len(errors),'total context references:',sum(len(x) for x in groups.values()))
