"""Conservative two-sided Redis diagnostics; repair only a confirmed WSL relay fault."""
import json
from pathlib import Path
import subprocess
import time


def run(arguments,timeout=8):
    result=subprocess.run(arguments,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=timeout,creationflags=subprocess.CREATE_NO_WINDOW)
    return result.returncode,result.stdout.decode('utf-8',errors='replace').strip()


class Recovery:
    def __init__(self,log):
        self.log=log;self.failures=0;self.started=time.monotonic();self.last_attempt=-10000;self.internal=None

    def check(self,windows_state,confirm_windows=None):
        if windows_state=='正常':
            if self.failures>=3:self.log('REDIS_CONNECTION_RECOVERED',failed_checks=self.failures)
            self.failures=0;return
        self.failures+=1
        if self.failures<3:return
        # Avoid launching diagnostic subprocesses on every polling cycle.
        now=time.monotonic()
        if now-self.last_attempt<600 or now-self.started<120:return
        self.last_attempt=now
        try:
            code,reply=run(['wsl.exe','-d','DataQualityRedis','-u','root','--','timeout','4','redis-cli','--raw','ping'])
            self.internal='正常' if code==0 and reply=='PONG' else '内部未响应'
            self.log('REDIS_TWO_SIDED_DIAGNOSIS',windows=windows_state,wsl=self.internal)
            if self.internal!='正常':return
            if confirm_windows and confirm_windows():
                self.failures=0;self.log('REDIS_CONNECTION_RECOVERED',note='复查Windows连接已恢复，取消WSL重启');return
            # Full WSL shutdown is allowed only while this is its sole distribution.
            result=subprocess.run(['wsl.exe','--list','--quiet'],stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=8,creationflags=subprocess.CREATE_NO_WINDOW)
            raw=result.stdout
            names=[x.strip().lstrip('\ufeff') for x in raw.decode('utf-16-le' if b'\x00' in raw else 'utf-8',errors='replace').splitlines() if x.strip()]
            if result.returncode or names!=['DataQualityRedis']:
                self.log('REDIS_REPAIR_SKIPPED',reason='存在其他WSL实例或无法确认专用环境');return
            self.log('REDIS_RELAY_REPAIR_BEGIN',reason='Windows连续失败，WSL内部PONG正常',cooldown_seconds=600)
            code,_=run(['wsl.exe','-d','DataQualityRedis','-u','root','--','timeout','20','redis-cli','shutdown'],timeout=25)
            if code:self.log('REDIS_REPAIR_ABORTED',reason='Redis正常关闭未成功，不强制终止');return
            code,_=run(['wsl.exe','--shutdown'],timeout=15)
            if code:self.log('REDIS_REPAIR_ABORTED',reason='WSL关闭失败');return
            code,_=run(['wsl.exe','-d','DataQualityRedis','-u','root','--','service','redis-server','start'],timeout=15)
            self.log('REDIS_RELAY_REPAIR_REQUESTED',success=code==0,note='等待持久化加载与Windows端PING通过，不提前宣告恢复')
        except Exception as exc:self.log('REDIS_REPAIR_FAILED',error_type=type(exc).__name__)
