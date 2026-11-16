"""Bounded nontransactional observation reads; never treat a failed batch as missing data."""
import json
import logging
import time
import redis
LOG=logging.getLogger('quality_engine')


def decode(value):
    if value is None:return None
    return json.loads(value)


def read_observation(cache,rows,include_latest,prefix,batch_size=500):
    if batch_size<1:raise ValueError('batch_size must be positive')
    receipts={};latest={}
    started=time.monotonic()
    for offset in range(0,len(rows),batch_size):
        part=rows[offset:offset+batch_size];eids=[r['eid'] for r in part]
        candidates=[r['eid'] for r in part if include_latest(r)]
        for attempt in range(2):
            try:
                with cache.pipeline(transaction=False) as pipe:
                    pipe.hmget(prefix+'receipt',eids)
                    if candidates:pipe.mget([prefix+'latest:'+eid for eid in candidates])
                    result=pipe.execute()
                break
            except (redis.ConnectionError,redis.TimeoutError):
                if attempt:
                    LOG.error('observation_batch_failed offset=%s count=%s total=%s elapsed_ms=%s',offset,len(part),len(rows),round((time.monotonic()-started)*1000))
                    raise
                LOG.warning('observation_batch_retry offset=%s count=%s',offset,len(part))
        if len(result[0])!=len(eids) or (candidates and len(result[1])!=len(candidates)):
            raise ValueError('Incomplete observation response; do not publish partial state')
        receipts.update(zip(eids,map(decode,result[0])))
        latest.update(dict.fromkeys(eids))
        if candidates:latest.update(zip(candidates,map(decode,result[1])))
    elapsed=time.monotonic()-started
    if elapsed>8:LOG.warning('observation_read_slow points=%s elapsed_ms=%s',len(rows),round(elapsed*1000))
    return receipts,latest
