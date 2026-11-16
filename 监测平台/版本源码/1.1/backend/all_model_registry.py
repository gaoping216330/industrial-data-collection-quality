"""Load only validated models; keep all training artifacts intact."""
import csv
import json
from pathlib import Path

FAMILIES = ['all_sensor_typeai_ptf/models_53d']

def select_candidates(root):
    chosen={};families=[]
    for priority,folder in enumerate(FAMILIES):
        path=root/'outputs'/folder
        if not (path/'manifest.json').exists():
            continue
        with (path/'model_summary.csv').open(encoding='utf-8-sig',newline='') as f:
            rows=list(csv.DictReader(f))
        families.append({'folder':folder,'scope':len(rows),'trained':sum(bool(r.get('model_kind')) for r in rows)})
        for row in rows:
            if not row.get('model_kind') or row.get('deployment_status')!='ready':
                continue
            eid=row['eid'];rank=(row.get('deployment_status')!='ready',priority)
            if eid not in chosen or rank<chosen[eid][0]:
                chosen[eid]=(rank,folder,row)
    return chosen,families

class AllModelRegistry:
    def __init__(self,root):
        import joblib
        from online_model_detector import ActiveGroup
        chosen,families=select_candidates(root)
        self.metadata={eid:{**row,'model_origin':folder} for eid,(_,folder,row) in chosen.items()}
        self.ready_eids=set(chosen) # Existing detector uses this as its execution roster.
        self.validated_eids={eid for eid,row in self.metadata.items() if row.get('deployment_status')=='ready'}
        self.required_eids=set();self.groups=[];self.errors=[];loaded=set();self.bin_seconds=10
        for folder in FAMILIES:
            path=root/'outputs'/folder
            if not (path/'manifest.json').exists():continue
            manifest=json.loads((path/'manifest.json').read_text(encoding='utf-8-sig'))
            if int(manifest['bin_seconds'])!=self.bin_seconds:raise ValueError('Incompatible model bin size')
            for gid,configured in manifest['group_models'].items():
                selected={eid for eid,(_,f,row) in chosen.items() if f==folder and row['group_id']==gid}
                if not selected:continue
                local=path/'groups'/(gid+'.joblib')
                model_path=local if local.exists() else Path(configured)
                try:
                    bundle=joblib.load(model_path)
                    targets={eid:m for eid,m in bundle.target_models.items() if eid in selected}
                    required=set(bundle.state_eids)
                    for model in targets.values():required.add(model.eid);required.update(model.peer_eids)
                    self.groups.append(ActiveGroup(folder+':'+gid,bundle,targets,required))
                    self.required_eids.update(required);loaded.update(targets)
                except Exception as exc:
                    self.errors.append({'group':folder+':'+gid,'error':str(exc)})
        self.unloaded_eids=self.ready_eids-loaded
        self.audit={'families':families,'selected_models':len(chosen),'loaded_models':len(loaded),'validated_models':len(self.validated_eids),'required_eids':len(self.required_eids),'errors':self.errors,'missing_handover_directory':str(root/'outputs/all_sensor_typeai_ptf/models_53d'),'handover_directory_present':(root/'outputs/all_sensor_typeai_ptf/models_53d').exists()}
        (root/'监测平台/model_inventory.json').write_text(json.dumps(self.audit,ensure_ascii=False,indent=2),encoding='utf-8')
