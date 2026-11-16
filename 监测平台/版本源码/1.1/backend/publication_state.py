"""Publish a UI revision only after the monitoring transaction has committed."""
import json
from pathlib import Path
import time

PATH = Path(__file__).resolve().parent/'publication.json'


def write_publication(summary, kind='judgment'):
    payload={'version':str(time.time_ns()),'kind':kind,'summary':summary}
    temporary=PATH.with_suffix('.tmp')
    temporary.write_text(json.dumps(payload,ensure_ascii=False),encoding='utf-8')
    temporary.replace(PATH)
    return payload


def read_publication():
    try:
        return json.loads(PATH.read_text(encoding='utf-8'))
    except (OSError,ValueError):
        return None
