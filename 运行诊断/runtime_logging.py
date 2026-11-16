"""Small, bounded service lifecycle and crash logs. No request payloads/credentials."""
import atexit
import faulthandler
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import sys
import threading
from datetime import datetime

_HANDLES=[]

def install(name, configure_web=False):
    root=Path(__file__).resolve().parent/'logs';root.mkdir(exist_ok=True)
    logger=logging.getLogger('lifecycle.'+name);logger.setLevel(logging.INFO);logger.propagate=False
    handler=RotatingFileHandler(root/(name+'.lifecycle.log'),maxBytes=5*1024**2,backupCount=3,encoding='utf-8')
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'));logger.addHandler(handler)
    def record(event,**fields):logger.info(json.dumps(dict(event=event,pid=os.getpid(),**fields),ensure_ascii=False))
    # Normal startup and shutdown are intentionally silent.
    fault_path=root/(name+'.fault.log')
    if fault_path.exists() and fault_path.stat().st_size>2*1024**2:
        for n in range(3,0,-1):
            src=fault_path if n==1 else Path(str(fault_path)+'.'+str(n-1))
            if src.exists():src.replace(Path(str(fault_path)+'.'+str(n)))
    fault=open(fault_path,'a',encoding='utf-8');_HANDLES.append(fault)
    faulthandler.enable(file=fault,all_threads=True)
    previous=sys.excepthook
    def unhandled(kind,value,tb):
        logger.critical('UNHANDLED_EXCEPTION pid=%s',os.getpid(),exc_info=(kind,value,tb));handler.flush();previous(kind,value,tb)
    sys.excepthook=unhandled
    old_thread=threading.excepthook
    def thread_error(args):
        logger.error('THREAD_CRASH name=%s pid=%s',args.thread.name,os.getpid(),exc_info=(args.exc_type,args.exc_value,args.exc_traceback));handler.flush();old_thread(args)
    threading.excepthook=thread_error

    errors=RotatingFileHandler(root/(name+'.errors.log'),maxBytes=5*1024**2,backupCount=3,encoding='utf-8')
    errors.setLevel(logging.WARNING)
    errors.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s'))
    logging.getLogger().addHandler(errors);logging.getLogger().setLevel(logging.WARNING)
    class ErrorStream:
        def write(self,text):
            if text and text.strip():logging.getLogger('stderr').warning(text.rstrip())
            return len(text)
        def flush(self):errors.flush()
    class QuietStream:
        def write(self,text):return len(text)
        def flush(self):pass
    sys.stderr=ErrorStream();sys.stdout=QuietStream()
    return record
