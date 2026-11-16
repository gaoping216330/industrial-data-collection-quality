"""Confirm each reason only after repeated observations span a full window."""


class ConfirmationWindow:
    def __init__(self, duration_ms=300_000, max_gap_ms=40_000):
        self.duration_ms = duration_ms
        self.max_gap_ms = max_gap_ms
        self.pending = {}

    def observe(self, eid, reasons, now_ms):
        confirmed = set()
        for key in list(self.pending.get(eid, {})):
            if key not in reasons:
                del self.pending[eid][key]
        tracked = self.pending.setdefault(eid, {})
        for reason in reasons:
            previous = tracked.get(reason)
            if previous is None or not 0 <= now_ms - previous[1] <= self.max_gap_ms:
                started = now_ms
            else:
                started = previous[0]
            tracked[reason] = (started, now_ms)
            if now_ms - started >= self.duration_ms:
                confirmed.add(reason)
        if not tracked:
            self.pending.pop(eid, None)
        return confirmed


class ConfirmedObservations:
    """Keep event transitions between publications, including recovered episodes."""

    def __init__(self, active, recovery_ms=0):
        self.recovery_ms=recovery_ms
        self.recovering={}
        self.active = active
        self.window = ConfirmationWindow()
        self.events = []
        self.closes = []
        self.open_buffer = {}

    def observe(self, eid, reasons, now_ms, starts=None, ends=None, suspended=False, force=False):
        old = self.active.get(eid, set())
        if suspended:
            self.window.pending.pop(eid,None)
            for key in [k for k in self.recovering if k[0]==eid]:self.recovering.pop(key,None)
            return set(old)
        reasons=set(reasons)
        for reason in old:
            key=(eid,reason)
            if reason in reasons:self.recovering.pop(key,None)
            elif not force and self.recovery_ms:
                previous=self.recovering.get(key)
                first=now_ms if previous is None or not 0<=now_ms-previous[1]<=40000 else previous[0]
                self.recovering[key]=(first,now_ms)
                if now_ms-first<self.recovery_ms:reasons.add(reason)
                else:
                    ends=dict(ends or {});ends[reason]=first
                    self.recovering.pop(key,None)
        confirmed = self.window.observe(eid, reasons, now_ms) | (old & reasons)
        for reason in confirmed - old:
            event = {'eid': eid, 'reason': reason,
                     'opened_ms': (starts or {}).get(reason, self.window.pending[eid][reason][0]),
                     'closed_ms': None}
            self.events.append(event)
            self.open_buffer[eid, reason] = event
        for reason in old - confirmed:
            ended = (ends or {}).get(reason, now_ms)
            event = self.open_buffer.pop((eid, reason), None)
            if event is None:
                self.closes.append((eid, reason, ended))
            else:
                event['closed_ms'] = max(event['opened_ms'], ended)
        self.active[eid] = confirmed
        return confirmed

    def committed(self):
        self.events.clear()
        self.closes.clear()
        self.open_buffer.clear()
