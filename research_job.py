"""One bounded fixed-pool research job per account; immutable public receipts."""
import copy
from datetime import datetime
import hashlib
import json
from pathlib import Path
import os
import re
import tempfile
import threading

from app_manifest import APP_VERSION
import historical_data
import research_execution

ROOT = Path(__file__).resolve().parent / "research_records"
_LOCK = threading.Lock()
_JOB = {"running":False,"stage":"idle","done":0,"total":0,"message":"尚未运行冻结研究",
        "result":None,"error":None,"started_at":None,"finished_at":None}


def _save(value, prefix):
    raw=json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(",",":"),allow_nan=False).encode("utf-8")
    identifier=prefix+"_"+hashlib.sha256(raw).hexdigest()
    ROOT.mkdir(exist_ok=True)
    target=ROOT/(identifier+".json")
    with tempfile.NamedTemporaryFile(dir=ROOT,prefix=".research-",suffix=".tmp",delete=False) as handle:
        temporary=Path(handle.name);handle.write(raw)
    try:
        try:os.link(temporary,target)
        except FileExistsError:
            if target.read_bytes()!=raw:raise ValueError("研究内容凭据损坏，停止覆盖")
    finally:temporary.unlink(missing_ok=True)
    return identifier


def _read(identifier, prefix):
    if not isinstance(identifier,str) or not re.fullmatch(prefix+r"_[0-9a-f]{64}",identifier):
        raise ValueError("invalid public research receipt id")
    raw=(ROOT/(identifier+".json")).read_bytes()
    if hashlib.sha256(raw).hexdigest()!=identifier.rsplit("_",1)[1]:
        raise ValueError("public research content hash mismatch")
    return json.loads(raw)


def _progress(stage,done,total,message):
    with _LOCK:_JOB.update({"stage":stage,"done":done,"total":total,"message":message})


def _worker():
    try:
        payload=historical_data.fetch_inputs(progress=_progress)
        input_record=_save(payload,"input")
        result=research_execution.study(payload,progress=_progress)
        result.update({"input_record_id":input_record,"produced_app_version":APP_VERSION,
                       "captured_at":datetime.now().astimezone().isoformat(timespec="seconds")})
        result_record=_save(result,"result")
        _read(input_record,"input")
        import private_storage
        private_storage.write(ROOT/"latest.json",{"result_record_id":result_record})
        with _LOCK:
            _JOB.update({"stage":"done","message":"冻结历史情景已完成；模型仍未验证",
                         "result":result,"result_record_id":result_record,"error":None})
    except Exception as error:
        with _LOCK:
            _JOB.update({"stage":"error","message":"历史研究失败，未重置既有凭据",
                         "error":"历史研究未完成（"+type(error).__name__+"），请核对输入与本机运行状态",
                         "result":None})
    finally:
        with _LOCK:
            _JOB.update({"running":False,"finished_at":datetime.now().astimezone().isoformat(timespec="seconds")})


def start():
    with _LOCK:
        if _JOB["running"]:return False
        _JOB.update({"running":True,"stage":"inputs","done":0,"total":4,"message":"开始读取固定四股公开历史输入",
                     "result":None,"error":None,"result_record_id":None,
                     "started_at":datetime.now().astimezone().isoformat(timespec="seconds"),"finished_at":None})
    try:threading.Thread(target=_worker,daemon=True,name="fixed-history-research").start()
    except Exception:
        with _LOCK:_JOB.update({"running":False,"stage":"error","error":"无法启动历史研究任务"})
        raise
    return True


def status():
    with _LOCK:job=copy.deepcopy(_JOB)
    saved_head=(ROOT/"latest.json").is_file()
    if job["stage"]=="done" and not saved_head:
        raise ValueError("completed research receipt head missing")
    if job["stage"] in ("idle","done","saved") and saved_head:
        import private_storage
        head=private_storage.read(ROOT/"latest.json",{})
        identifier=head.get("result_record_id")
        result=_read(identifier,"result")
        inputs=_read(result.get("input_record_id"),"input")
        if historical_data.content_id(inputs)!=result.get("input_bundle_id"):
            raise ValueError("research input reference does not match")
        job.update({"stage":"saved" if job["stage"]=="idle" else job["stage"],"result":result,"result_record_id":identifier,
                    "message":"读取已保存的冻结研究；版本与时点见记录"})
    return job
