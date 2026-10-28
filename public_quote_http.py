"""Reuse transport for public Tencent quotes; never cache prices/credentials."""
from contextlib import contextmanager
from http.cookiejar import DefaultCookiePolicy
import math
from queue import LifoQueue, Empty
import re
from urllib.parse import urlsplit

import requests
from requests.auth import AuthBase


class _NoAuth(AuthBase):
    def __call__(self,request):
        request.headers.pop('Authorization',None)
        request.headers.pop('Cookie',None)
        return request


class _NoCookies(DefaultCookiePolicy):
    def set_ok(self,*args,**kwargs):return False
    def return_ok(self,*args,**kwargs):return False


def _session():
    session=requests.Session()
    session.auth=_NoAuth()  # Prevent implicit .netrc credentials, too.
    session.cookies.set_policy(_NoCookies())
    return session


class _Connections:
    def __init__(self,size=4,factory=_session):
        self.size=size;self.available=LifoQueue(maxsize=size)
        for _ in range(size):self.available.put(factory())

    @contextmanager
    def borrow(self,timeout):
        try:session=self.available.get(timeout=timeout)
        except Empty:raise TimeoutError('public quote connections busy') from None
        try:yield session
        finally:self.available.put(session)


_POOL=_Connections()


def get(url,timeout):
    parsed=urlsplit(url)
    if (parsed.scheme!='https' or parsed.netloc!='qt.gtimg.cn' or parsed.query or parsed.fragment
            or not re.fullmatch(r'/q=(?:sh|sz|bj)\d{6}(?:,(?:sh|sz|bj)\d{6})*',parsed.path)):
        raise ValueError('only public Tencent quote URLs allowed')
    if not isinstance(timeout,(int,float)) or not math.isfinite(timeout) or timeout<=0:
        raise ValueError('invalid quote timeout')
    with _POOL.borrow(timeout) as session:
        response=session.get(url,timeout=timeout,allow_redirects=False)
        try:
            response.raise_for_status()
            if 300<=response.status_code<400:raise ValueError('quote redirects rejected')
            if len(response.content)>1_000_000:raise ValueError('quote response exceeds parser bound')
            return response
        finally:response.close()  # Body is loaded; release the socket for reuse.
