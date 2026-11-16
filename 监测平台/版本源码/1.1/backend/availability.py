"""Observe actual data availability independently from alarm muting."""
from confirmation_window import ConfirmationWindow

MISSING={'从未收到','停止发送'}
class AvailabilityTracker:
    def __init__(self,reset_ms=0):
        self.reset_ms=int(reset_ms)
        self.window=ConfirmationWindow()
        self.known={}
        self.since={}
    def observe(self,eid,receipt,now_ms,started_ms,unknown=False):
        received=receipt.get('received_time') if receipt else None
        if received is not None and now_ms-int(received)<=10000:
            self.window.observe(eid,set(),now_ms)
            self.since.pop(eid,None)
            return '正常'
        if unknown:return self.known.get(eid) if self.known.get(eid) in MISSING else '待判定'
        reason='从未收到' if received is None else '停止发送'
        effective_received=max(int(received),self.reset_ms) if received is not None else None
        if received is not None:self.since[eid]=effective_received
        else:self.since.setdefault(eid,max(started_ms,self.reset_ms))
        if received is None and now_ms-started_ms<=10000:return '待判定'
        confirmed=self.window.observe(eid,{reason},now_ms)
        if self.known.get(eid)==reason or reason in confirmed or (received is not None and now_ms-effective_received>300000):return reason
        return '断线待确认'
