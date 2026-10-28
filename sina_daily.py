"""Public Sina OHLCV + qfq factors, without unused share-capital requests.

The fixed decoder comes from the pinned AKShare package. Provider text is data:
it is never eval'd as code. Both independent inputs are required before output.
"""
from concurrent.futures import ThreadPoolExecutor
import json
import math
import re

import pandas as pd
import requests

ADAPTER_VERSION='sina_ohlcv_qfq_parallel_v224'
MAX_BODY_BYTES=2*1024*1024
COLUMNS=['date','open','high','low','close','volume']


def _read(url,timeout):
    response=requests.get(url,timeout=timeout)
    response.raise_for_status();raw=response.content
    if len(raw)>MAX_BODY_BYTES:raise ValueError('Sina response exceeds parser limit')
    return raw.decode('utf-8-sig')


def _decode_history(text):
    from akshare.stock.cons import hk_js_decode
    import py_mini_racer
    match=re.match(r'^\s*var\s+[A-Za-z_$][\w$]*\s*=\s*[\"\']([^\"\']+)[\"\']\s*;',text)
    if match is None:raise ValueError('Sina encoded daily assignment changed')
    decoder=py_mini_racer.MiniRacer()
    try:
        decoder.eval(hk_js_decode)  # Fixed installed library code, not remote JS.
        rows=decoder.call('d',match.group(1))
    finally:
        close=getattr(decoder,'close',None)
        if callable(close):close()
    return rows


def _factor_rows(text):
    # Sina's published assignment contains strict JSON after the first '='.
    _,separator,body=text.partition('=')
    if not separator:raise ValueError('Sina qfq factor assignment missing')
    body=body.lstrip();value,end=json.JSONDecoder().raw_decode(body)
    suffix=body[end:]
    if not re.fullmatch(r'\s*;?\s*(?:(?:/\*[\s\S]*?\*/|//[^\n]*(?:\n|$))\s*)*',suffix):
        raise ValueError('Sina qfq assignment trailing content changed')
    rows=value.get('data') if isinstance(value,dict) else None
    if not isinstance(rows,list) or not rows:raise ValueError('Sina qfq factor missing')
    result=[]
    for row in rows:
        if not isinstance(row,dict) or set(row)!={'d','f'}:raise ValueError('Sina qfq factor schema changed')
        result.append({'date':row['d'],'qfq_factor':row['f']})
    return result


def normalize(rows,factors,start,end):
    """Use only real daily rows; factor dates never fabricate price bars."""
    frame=pd.DataFrame(rows)
    if frame.empty:return pd.DataFrame(columns=COLUMNS)
    if not all(c in frame for c in COLUMNS):raise ValueError('Sina daily OHLCV fields missing')
    # Amount belongs to the raw duplicate identity, matching the pinned adapter.
    columns=[*COLUMNS,*(['amount'] if 'amount' in frame else [])]
    frame=frame[columns].copy()
    # MiniRacer returns JS dates at UTC midnight; the source represents daily
    # calendar labels. Strip that serialization timezone, never use local time.
    frame['date']=pd.to_datetime(frame['date'],errors='raise',utc=True).dt.tz_convert(None).dt.normalize()
    for column in columns[1:]:frame[column]=pd.to_numeric(frame[column],errors='raise')
    frame=frame.drop_duplicates().sort_values('date')
    if frame['date'].duplicated().any():raise ValueError('Sina conflicting rows on same date')
    factor=pd.DataFrame(factors)
    if factor.empty or set(factor)!={'date','qfq_factor'}:raise ValueError('Sina qfq factor missing')
    factor['date']=pd.to_datetime(factor['date'],errors='raise').dt.normalize()
    factor['qfq_factor']=pd.to_numeric(factor['qfq_factor'],errors='raise')
    if (factor['date'].isna().any() or factor['qfq_factor'].isna().any()
            or not factor['qfq_factor'].map(lambda x:math.isfinite(x) and x>0).all()):
        raise ValueError('Sina qfq factor invalid')
    factor=factor.drop_duplicates().sort_values('date')
    if factor['date'].duplicated().any():raise ValueError('Sina conflicting qfq factors')
    frame=frame.loc[frame['date'].between(pd.Timestamp(start),pd.Timestamp(end))]
    joined=pd.merge_asof(frame,factor,on='date',direction='backward')
    if joined['qfq_factor'].isna().any():raise ValueError('Sina daily row has no dated qfq factor')
    for column in ['open','high','low','close']:joined[column]=(joined[column]/joined['qfq_factor']).round(2)
    return joined[COLUMNS].reset_index(drop=True)


def daily(symbol,start,end,timeout=5):
    if not re.fullmatch(r'(sh|sz|bj)\d{6}',symbol):raise ValueError('Sina symbol invalid')
    if not math.isfinite(float(timeout)) or float(timeout)<=0:raise ValueError('Sina timeout invalid')
    from akshare.stock.cons import zh_sina_a_stock_hist_url,zh_sina_a_stock_qfq_url
    # Only two requests per call. Each call's executor and decoder are closed;
    # caller concurrency is bounded by the existing scan/secondary workers.
    with ThreadPoolExecutor(max_workers=2,thread_name_prefix='sina-public-input') as pool:
        history=pool.submit(_read,zh_sina_a_stock_hist_url.format(symbol),timeout)
        factor=pool.submit(_read,zh_sina_a_stock_qfq_url.format(symbol),timeout)
        raw_history=history.result();raw_factor=factor.result()
    frame=normalize(_decode_history(raw_history),_factor_rows(raw_factor),start,end)
    frame.attrs.update({'provider_http_requests':2,'provider_price_fields':['sina_raw_divided_by_dated_qfq'],
                        'provider_adapter':ADAPTER_VERSION})
    return frame
