"""Independent homepage blocks run concurrently, preserving all failure states."""
from concurrent.futures import ThreadPoolExecutor
import logging
import time
import holdings
import market
import market_clock
import private_storage
import screener

_POOL=ThreadPoolExecutor(max_workers=4,thread_name_prefix='brief-block')
_LOG=logging.getLogger(__name__)


def _holdings():
    configured=holdings.load();rows=holdings.quotes()
    return {'rows':rows,'alerts':[r for r in rows if r.get('below_ma10')],'count':len(rows),
        'configured_count':len(configured),'missing_codes':[h.get('code') for h in configured
            if h.get('code') not in {r.get('code') for r in rows}],'error':None}


def _timed(fn):
    start=time.monotonic()
    try:return fn(),None,round((time.monotonic()-start)*1000,1)
    except Exception as error:return None,error,round((time.monotonic()-start)*1000,1)


def build():
    result={'generated_at':market_clock.now().isoformat(timespec='seconds'),
        'holdings':{'rows':[],'alerts':[],'error':None,'configured_count':0,'missing_codes':[]},
        'market':{'indexes':[],'regime':None,'index_error':None},'candidates':None,'component_elapsed_ms':{}}
    calls={'holdings':_holdings,'indexes':market._indexes,'regime':market._regime,
           'candidates':lambda:screener.cached_candidates(limit=10)}
    futures={name:_POOL.submit(_timed,fn) for name,fn in calls.items()}
    for name,future in futures.items():
        value,error,elapsed=future.result();result['component_elapsed_ms'][name]=elapsed
        if isinstance(error,private_storage.DataReadError):raise error
        if error is not None:
            _LOG.error('brief %s failed: %s',name,type(error).__name__)
            if name=='holdings':result['holdings']['error']='持仓行情暂不可用，请稍后刷新'
            elif name=='indexes':result['market']['index_error']='指数实时行情暂不可用'
            elif name=='regime':result['market']['regime']={'label':'数据暂缺','ic':None,'note':''}
            else:result['candidates']={'error':'因子候选暂不可用，请稍后刷新','rows':[]}
        elif name in ['indexes','regime']:result['market'][name]=value
        else:result[name]=value
    return result
