"""Typed, owner-local announcement interface; optional to the stock application."""
from contextlib import asynccontextmanager
import hmac
import os
from pathlib import Path
import re
from urllib.parse import urlsplit
from fastapi import FastAPI,HTTPException,Query
from fastapi.responses import HTMLResponse,JSONResponse
from starlette.middleware.trustedhost import TrustedHostMiddleware
from . import VERSION
from .contracts import Submit,Resume,Revision,Review
from .pipeline import Workflow
from .sources import EXAMPLE_URL
from .storage import Conflict,IntegrityError


class Guard:
    def __init__(self,app,token):self.app=app;self.token=token
    async def __call__(self,scope,receive,send):
        if scope['type']!='http':return await self.app(scope,receive,send)
        headers={k.decode('latin1').lower():v.decode('latin1') for k,v in scope['headers']}
        path=scope['path'];method=scope['method']
        async def reject(status,message):return await JSONResponse({'error':message},status_code=status)(scope,receive,send)
        if path.startswith('/api/') and self.token:
            value=headers.get('authorization','')
            if not hmac.compare_digest(value.encode('latin1'),('Bearer '+self.token).encode('utf-8')):return await reject(401,'需要公告模块机主访问令牌')
        if method in ('POST','PUT','PATCH','DELETE'):
            origin=headers.get('origin');expected=scope['scheme']+'://'+headers.get('host','')
            if origin and origin!=expected:return await reject(403,'不接受跨站修改请求')
            if headers.get('sec-fetch-site')=='cross-site':return await reject(403,'不接受跨站修改请求')
            if not headers.get('content-type','').lower().startswith('application/json'):return await reject(415,'仅接受JSON请求')
        # Bound the body before FastAPI parses it; receive exceptions otherwise
        # may be converted into a generic parser error instead of HTTP 413.
        length=headers.get('content-length')
        if length:
            try:declared=int(length)
            except ValueError:return await reject(400,'Content-Length无效')
            if declared<0:return await reject(400,'Content-Length无效')
            if declared>512000:return await reject(413,'请求超过512KB限制')
        chunks=[];size=0
        while True:
            message=await receive()
            if message['type']=='http.disconnect':return
            size+=len(message.get('body',b''))
            if size>512000:return await reject(413,'请求超过512KB限制')
            chunks.append(message.get('body',b''))
            if not message.get('more_body',False):break
        sent=False
        async def buffered_receive():
            nonlocal sent
            if sent:return await receive()
            sent=True;return {'type':'http.request','body':b''.join(chunks),'more_body':False}
        await self.app(scope,buffered_receive,send)


def create_app(root=None,workflow=None,token=None,model_key=None):
    token=os.getenv('ANNOUNCEMENT_ACCESS_TOKEN','') if token is None else token
    model_key=os.getenv('ANNOUNCEMENT_MODEL_KEY','') if model_key is None else model_key
    bind=os.getenv('ANNOUNCEMENT_BIND_HOST','127.0.0.1')
    if bind not in ('127.0.0.1','localhost','::1') and len(token)<24:raise RuntimeError('非本机监听必须设置至少24字符机主令牌')
    data_root=Path(root or os.getenv('ANNOUNCEMENT_DATA_ROOT') or Path(__file__).resolve().parent.parent/'announcement_data')
    owned=workflow is None;engine=workflow or Workflow(data_root,model_key=model_key)
    @asynccontextmanager
    async def lifespan(app):
        yield
        if owned:engine.close()
    app=FastAPI(title='公开公告证据研究',version=VERSION,docs_url=None,redoc_url=None,openapi_url=None,lifespan=lifespan)
    app.state.workflow=engine
    app.add_middleware(Guard,token=token)
    allowed=['127.0.0.1','localhost','[::1]']
    allowed.extend(x.strip() for x in os.getenv('ANNOUNCEMENT_ALLOWED_HOSTS','').split(',') if x.strip() and '*' not in x)
    app.add_middleware(TrustedHostMiddleware,allowed_hosts=allowed)
    @app.exception_handler(IntegrityError)
    async def integrity(request,error):return JSONResponse({'error':str(error)},status_code=409)
    @app.exception_handler(Conflict)
    async def conflict(request,error):return JSONResponse({'error':str(error)},status_code=409)
    @app.exception_handler(KeyError)
    async def missing(request,error):return JSONResponse({'error':'记录不存在'},status_code=404)
    @app.exception_handler(ValueError)
    async def invalid(request,error):return JSONResponse({'error':str(error)},status_code=400)
    @app.get('/healthz')
    def health():
        engine.repo.counts()
        return {'ok':True,'module':VERSION,'scope':'announcement_store_not_market_or_accounts'}
    @app.get('/',response_class=HTMLResponse)
    def index():return HTMLResponse((Path(__file__).parent/'index.html').read_text(encoding='utf-8'),headers={
        'X-Content-Type-Options':'nosniff','Referrer-Policy':'no-referrer',
        'Content-Security-Policy':"default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"})
    @app.get('/api/status')
    def status():return {'module_version':VERSION,'baseline_available':True,'model_configured':engine.model_configured,
        'owner_token_configured':bool(token),'example_url':EXAMPLE_URL,'counts':engine.repo.counts(),
        'real_model_evaluation_passed':False,'human_gold_labels':False,'trading_integration':False}
    @app.post('/api/runs',status_code=202)
    def submit(request:Submit):
        if request.mode=='model' and not token:raise HTTPException(403,'真实模型调用须先启用公告机主令牌')
        result=engine.submit(request)
        if result['run']['status']!='completed':engine.schedule(result['run_id'])
        return result
    @app.get('/api/runs')
    def runs():return {'rows':engine.repo.runs()}
    @app.get('/api/runs/{run_id}')
    def run(run_id:str):return engine.repo.run(run_id)
    @app.post('/api/runs/{run_id}/resume',status_code=202)
    def resume(run_id:str,request:Resume):
        current=engine.repo.run(run_id)
        if current['spec']['mode']=='model' and not current['spec'].get('revision_of_run') and not token:raise HTTPException(403,'模型任务恢复须启用公告机主令牌')
        if request.acknowledge_uncertain_call:engine.repo.acknowledge_uncertain(run_id)
        engine.schedule(run_id);return {'run_id':run_id,'run':engine.repo.run(run_id)}
    @app.post('/api/runs/{run_id}/revisions')
    def revise(run_id:str,request:Revision):
        revised_id=engine.repo.revise(run_id,request.candidate.model_dump(),request.reason)
        return {'ok':True,'run_id':revised_id,'original_candidate_preserved':True}
    @app.get('/api/documents/{document_id}')
    def document(document_id:str):return engine.repo.document(document_id)
    @app.get('/api/events')
    def events(code:str|None=Query(default=None,pattern=r'^\d{6}$'),event_type:str|None=None,
               published_after:str|None=Query(default=None,pattern=r'^\d{4}-\d{2}-\d{2}$')):
        if published_after:
            from datetime import date
            date.fromisoformat(published_after)
        return {'rows':engine.repo.events(code,event_type,published_after)}
    @app.post('/api/events/{event_id}/reviews')
    def review(event_id:str,request:Review):
        engine.repo.review(event_id,request.decision,request.note);return {'ok':True,'decision':request.decision}
    return app
