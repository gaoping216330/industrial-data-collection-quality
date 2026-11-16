"""Business display grouping; collector identities used for diagnosis stay unchanged."""
import hashlib
import json

def display_group_token(row):
    parts=[str(row.get(field) or '').strip() for field in ('department','process','equipment','group')]
    fallback='' if any(parts) else str(row.get('device_key') or 'eid:'+str(row.get('eid','')))
    payload=[*parts,fallback if any(parts) else str(row.get('source',''))+':'+fallback]
    return hashlib.sha256(json.dumps(payload,ensure_ascii=False,separators=(',',':')).encode()).hexdigest()
