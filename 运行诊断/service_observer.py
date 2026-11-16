"""Observe existing processes without restarting or stopping them (Windows)."""
import ctypes
from ctypes import wintypes
import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import socket
import time
import os
from redis_recovery import Recovery

ROOT=Path(__file__).resolve().parent.parent
LOGDIR=ROOT/'运行诊断/logs'
SERVICES={'web_app':'监测平台/web_app.pid','quality_engine':'监测平台/quality_engine.pid','model_worker':'监测平台/model_worker.pid','mqtt_duplicate_monitor':'监测平台/mqtt_duplicate_monitor.pid','receiver':'实时数据缓存服务/receiver.pid','directory_sync':'点位目录同步/sync.pid'}
K=ctypes.WinDLL('kernel32',use_last_error=True)
K.OpenProcess.argtypes=[wintypes.DWORD,wintypes.BOOL,wintypes.DWORD];K.OpenProcess.restype=wintypes.HANDLE
K.GetExitCodeProcess.argtypes=[wintypes.HANDLE,ctypes.POINTER(wintypes.DWORD)];K.GetExitCodeProcess.restype=wintypes.BOOL
K.WaitForSingleObject.argtypes=[wintypes.HANDLE,wintypes.DWORD];K.WaitForSingleObject.restype=wintypes.DWORD
K.CloseHandle.argtypes=[wintypes.HANDLE]
K.CreateMutexW.argtypes=[wintypes.LPVOID,wintypes.BOOL,wintypes.LPCWSTR];K.CreateMutexW.restype=wintypes.HANDLE
K.GetTickCount64.restype=ctypes.c_ulonglong


def probe(port,redis=False):
    try:
        with socket.create_connection(('127.0.0.1',port),timeout=2) as s:
            if redis:
                s.settimeout(2);s.sendall(b'*1\r\n$4\r\nPING\r\n');answer=s.recv(200)
                return '正常' if answer.startswith(b'+PONG') else '加载中' if b'LOADING' in answer else 'Redis响应异常'
            return '端口可连接'
    except OSError as exc:return '连接失败:'+type(exc).__name__


def main():
    mutex=K.CreateMutexW(None,False,'Local\\DataQualityDiagnosticObserver')
    if ctypes.get_last_error()==183:return
    LOGDIR.mkdir(exist_ok=True)
    log=logging.getLogger('observer');log.setLevel(logging.INFO)
    handler=RotatingFileHandler(LOGDIR/'service-observer.log',maxBytes=5*1024**2,backupCount=3,encoding='utf-8');handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'));log.addHandler(handler)
    def event(kind,**fields):
        if kind in ('OBSERVER_START','PROCESS_OBSERVED','HEARTBEAT'):return
        if kind=='SERVICE_STATE' and fields.get('state')=='运行' and fields.get('service') not in states:return
        if kind=='DEPENDENCY_STATE' and fields.get('state') in ('正常','端口可连接') and fields.get('dependency') not in states:return
        if kind=='PROCESS_EXIT' and fields.get('exit_code')==0:return
        log.warning(json.dumps(dict(event=kind,**fields),ensure_ascii=False))
    event('OBSERVER_START',pid=os.getpid(),system_uptime_seconds=round(K.GetTickCount64()/1000),mode='观察业务服务；确认Redis转发故障时修复专用WSL连接')
    tracked={};states={};last_heartbeat=0
    recovery=Recovery(event)
    while True:
        try:
            for name,path in SERVICES.items():
                try:pid=int((ROOT/path).read_text(encoding='utf-8-sig').strip())
                except (OSError,ValueError):pid=None
                previous=tracked.get(name)
                if previous:
                    old_pid,handle=previous
                    if K.WaitForSingleObject(handle,0)==0:
                        code=wintypes.DWORD();ok=K.GetExitCodeProcess(handle,ctypes.byref(code))
                        event('PROCESS_EXIT',service=name,pid=old_pid,exit_code=int(code.value) if ok else None,exit_code_hex=hex(code.value) if ok else None,note='退出码不代表操作人；需结合生命周期日志及Windows事件')
                        K.CloseHandle(handle);tracked.pop(name,None)
                    elif old_pid!=pid:
                        event('PID_FILE_CHANGED_WHILE_OLD_ALIVE',service=name,old_pid=old_pid,new_pid=pid);K.CloseHandle(handle);tracked.pop(name,None)
                if name not in tracked and pid:
                    handle=K.OpenProcess(0x1000|0x100000,False,pid)
                    if handle and K.WaitForSingleObject(handle,0)==258:
                        tracked[name]=(pid,handle);event('PROCESS_OBSERVED',service=name,pid=pid)
                    elif handle:K.CloseHandle(handle)
                state='运行' if name in tracked else '未运行或不可访问'
                if states.get(name)!=state:event('SERVICE_STATE',service=name,state=state,pid=pid);states[name]=state
            dependencies={name:probe(port,is_redis) for name,port,is_redis in [('redis',6379,True),('mysql_local',3307,False),('web_port',8789,False),('receiver_port',8788,False)]}
            for name,state in dependencies.items():
                if states.get(name)!=state:event('DEPENDENCY_STATE',dependency=name,state=state);states[name]=state
            recovery.check(dependencies['redis'],lambda:probe(6379,True)=='正常')
            if recovery.internal:states['redis_wsl_last_diagnosis']=recovery.internal
            if time.monotonic()-last_heartbeat>=300:
                event('HEARTBEAT',states=states);last_heartbeat=time.monotonic()
            (LOGDIR/'current-status.json').write_text(json.dumps(dict(observed_at=time.strftime('%Y-%m-%d %H:%M:%S'),states=states),ensure_ascii=False,indent=2),encoding='utf-8')
        except Exception:log.exception('OBSERVER_CHECK_FAILED')
        time.sleep(10)

if __name__=='__main__':main()
