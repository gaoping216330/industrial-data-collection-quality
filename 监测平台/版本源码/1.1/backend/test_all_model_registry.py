import csv
import json
import tempfile
import unittest
from pathlib import Path
from all_model_registry import select_candidates,FAMILIES

class AllModelRegistryTests(unittest.TestCase):
    def test_only_validated_models_are_selected(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);path=root/'outputs'/FAMILIES[0];path.mkdir(parents=True)
            (path/'manifest.json').write_text('{}')
            with (path/'model_summary.csv').open('w',newline='') as f:
                w=csv.DictWriter(f,fieldnames=['eid','model_kind','deployment_status']);w.writeheader()
                for eid,status in [('a','ready'),('b','observation_only'),('c','needs_recalibration')]:
                    w.writerow({'eid':eid,'model_kind':'ridge','deployment_status':status})
                w.writerow({'eid':'none','model_kind':'','deployment_status':'untrainable'})
            selected,families=select_candidates(root)
            self.assertEqual(set(selected),{'a'})
            self.assertEqual(families[0]['trained'],3)
