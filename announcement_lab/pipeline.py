"""A deep workflow module: submit, execute/resume, query via one durable repo."""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import sqlite3
import threading
from typing import TypedDict
import uuid
from langgraph.graph import StateGraph,START,END
from langgraph.checkpoint.sqlite import SqliteSaver
from .contracts import Submit
from .sources import acquire,validate_url,load_sample
from .extraction import BaselineExtractor,DeepSeekExtractor,check_candidate
from .storage import Repository,Conflict,CallUncertain,LeaseLost,SharedExtractionPending,IntegrityError


class EvidenceBlocked(RuntimeError):pass


class State(TypedDict):
    run_id:str


class Workflow:
    def __init__(self,root,model_key='',model_extractor=None,fetcher=None,fault=None,lease_seconds=120):
        self.repo=Repository(root,lease_seconds);self.owner=uuid.uuid4().hex
        self.model_configured=bool(model_key or model_extractor)
        self.extractors={'baseline':BaselineExtractor(),'model':model_extractor or DeepSeekExtractor(model_key)}
        self.fetcher=fetcher or self._fetch_source;self.fault=fault
        checkpoint_path=self.repo.root/'checkpoints.sqlite3'
        self.connection=sqlite3.connect(checkpoint_path,check_same_thread=False)
        try:
            if self.connection.execute('PRAGMA quick_check').fetchone()[0]!='ok':raise sqlite3.DatabaseError('checkpoint integrity')
        except sqlite3.DatabaseError as error:
            self.connection.close();raise IntegrityError('检查点数据库损坏，未重置') from error
        self.saver=SqliteSaver(self.connection)
        builder=StateGraph(State)
        for name,node in [('fetch',self._fetch),('extract',self._extract),('validate',self._validate),('store',self._store)]:builder.add_node(name,node)
        builder.add_edge(START,'fetch');builder.add_edge('fetch','extract');builder.add_edge('extract','validate')
        builder.add_edge('validate','store');builder.add_edge('store',END)
        self.graph=builder.compile(checkpointer=self.saver)
        self.pool=ThreadPoolExecutor(max_workers=2,thread_name_prefix='announcement')
        self._lock=threading.Lock();self._active=set()

    @staticmethod
    def _fetch_source(spec):
        return load_sample() if spec['use_sample'] else acquire(spec['source_url'],spec['text'])

    def submit(self,request):
        request=Submit.model_validate(request);validate_url(request.source_url)
        if request.mode=='model' and (not request.consent_paid_call or not self.model_configured):
            raise Conflict('真实模型模式须先配置独立Key并明确同意本次调用；不会读取行情项目Key')
        if request.use_sample:
            from .sources import EXAMPLE_URL
            if request.source_url!=EXAMPLE_URL or request.text is not None:raise Conflict('留存示例不能与其他链接或粘贴文本混用')
        payload=request.model_dump();key=payload.pop('idempotency_key')
        run_id,reused=self.repo.submit(payload,key)
        return {'run_id':run_id,'reused':reused,'run':self.repo.run(run_id)}

    def schedule(self,run_id):
        with self._lock:
            if run_id in self._active:return False
            self._active.add(run_id)
            future=self.pool.submit(self.execute,run_id)
        def clear(_):
            with self._lock:self._active.discard(run_id)
        future.add_done_callback(clear)
        return True

    def execute(self,run_id,acknowledge_uncertain=False):
        if acknowledge_uncertain:self.repo.acknowledge_uncertain(run_id)
        if not self.repo.claim(run_id,self.owner):return self.repo.run(run_id)
        config={'configurable':{'thread_id':run_id}}
        try:
            previous=self.graph.get_state(config)
            self.graph.invoke(None if previous.values else {'run_id':run_id},config)
        except EvidenceBlocked:self.repo.fail(run_id,self.owner,'needs_review','source_evidence_mismatch','字段原文核验未通过；查看问题并修订，原始候选保留')
        except CallUncertain:self.repo.fail(run_id,self.owner,'call_uncertain','external_call_unknown','外部调用可能已计费但结果未保存；先核对服务商，禁止自动重复调用')
        except SharedExtractionPending:self.repo.fail(run_id,self.owner,'waiting_dependency','same_content_running','相同正文正在另一任务抽取；等待其完成，再恢复以复用结果')
        except LeaseLost:pass
        except Exception as error:
            self.repo.fail(run_id,self.owner,'failed',type(error).__name__,'该阶段失败，原始输入保留；修复原因后恢复，不重置账本或其他任务')
        return self.repo.run(run_id)

    def _before(self,run_id,stage):
        self.repo.stage(run_id,self.owner,stage)
        if self.fault:self.fault(stage,'before')

    def _after(self,stage):
        if self.fault:self.fault(stage,'after')

    def _fetch(self,state):
        run_id=state['run_id'];self._before(run_id,'fetch');run=self.repo.run(run_id)
        if run['document_id']:self.repo.document(run['document_id'])
        else:self.repo.attach_document(run_id,self.owner,self.fetcher(run['spec']))
        self._after('fetch');return state

    def _extract(self,state):
        run_id=state['run_id'];self._before(run_id,'extract');run=self.repo.run(run_id)
        if self.repo.active_candidate(run_id) is None:
            # A completed durable response is authoritative even when its graph
            # checkpoint failed. A started call without response is ambiguous.
            extractor=self.extractors[run['spec']['mode']]
            extraction_key,saved=self.repo.claim_extraction(run_id,self.owner,run['document_id'],extractor.identity,external=run['spec']['mode']=='model')
            if saved:self.repo.candidate(run_id,self.owner,saved)
            else:
                if run['spec']['mode']=='model':
                    call_id=self.repo.start_call(run_id,self.owner,extraction_key)
                    try:envelope=extractor.extract(self.repo.document(run['document_id']))
                    except Exception as error:
                        self.repo.finish_call(call_id,error_code=type(error).__name__)
                        raise CallUncertain('model response not durably known') from error
                    self.repo.finish_call(call_id,envelope=envelope)
                else:
                    try:envelope=extractor.extract(self.repo.document(run['document_id']))
                    except Exception:
                        self.repo.finish_local_extraction(extraction_key,run_id,self.owner);raise
                    self.repo.finish_local_extraction(extraction_key,run_id,self.owner,envelope)
                self.repo.candidate(run_id,self.owner,envelope)
        self._after('extract');return state

    def _validate(self,state):
        run_id=state['run_id'];self._before(run_id,'validate');run=self.repo.run(run_id)
        report=check_candidate(self.repo.active_candidate(run_id),self.repo.document(run['document_id']))
        self.repo.validation(run_id,self.owner,report)
        if not report['mechanical_pass']:raise EvidenceBlocked()
        self._after('validate');return state

    def _store(self,state):
        run_id=state['run_id'];self._before(run_id,'store');run=self.repo.run(run_id)
        revised=bool(run['spec'].get('revision_of_run'))
        self.repo.complete(run_id,self.owner,'human_revision_v1' if revised else self.extractors[run['spec']['mode']].identity,
                           'human_revision' if revised else run['spec']['mode'])
        self._after('store');return state

    def close(self):
        self.pool.shutdown(wait=True);self.connection.close()
