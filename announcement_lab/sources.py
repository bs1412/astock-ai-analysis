"""Small allowlisted public-source adapter; source text never becomes instructions."""
from datetime import datetime,timezone,timedelta,date
import hashlib
import re
from urllib.parse import urlsplit
import httpx
from bs4 import BeautifulSoup

EXAMPLE_URL='https://www.sse.com.cn/disclosure/announcement/listing/c/c_20201027_5242851.shtml'
MAX_BYTES=1024*1024


def now():return datetime.now(timezone(timedelta(hours=8))).isoformat(timespec='seconds')


def validate_url(url):
    u=urlsplit(url)
    if (u.scheme!='https' or u.hostname!='www.sse.com.cn' or u.username or u.password
        or u.port not in (None,443) or u.query or u.fragment
        or not re.fullmatch(r'/disclosure/announcement/[a-zA-Z0-9/_-]+\.shtml',u.path)):
        raise ValueError('首版仅接受上交所公开公告HTTPS正文链接；不访问登录页或任意地址')
    return url


def digest(raw):return hashlib.sha256(raw).hexdigest()


def _document(url,raw,title,body,published_date,origin):
    if not body.strip():raise ValueError('公告正文为空')
    heading=title+'\n'
    publication=None
    if published_date:
        date.fromisoformat(published_date)
        heading+='发布日期：'+published_date+'\n'
        start=heading.index(published_date);publication={'start':start,'end':start+10,'quote':published_date}
    text=heading+body.strip()
    if len(text)>100000:raise ValueError('正文超过本轮处理上限')
    checksum=digest(text.encode('utf-8'))
    return {'document_id':digest((url+'\n'+checksum).encode('utf-8')),'source_url':url,
        'title':title,'text':text,'published_date':published_date,'publication_evidence':publication,
        'raw_sha256':digest(raw),'text_sha256':checksum,'retrieved_at':now(),'origin':origin,
        'source_verified':origin=='fetched_official_html','raw_bytes':raw,
        'parser_version':'sse_article_v1'}


def parse_html(url,raw):
    validate_url(url)
    soup=BeautifulSoup(raw.decode('utf-8'),'html.parser')
    article=soup.select_one('.article-infor')
    if article is None:raise ValueError('未识别官方公告正文结构，停止抽取')
    title_node=article.find(['h1','h2']) or soup.select_one('#searchTitle')
    opt=article.select_one('.article_opt')
    published=re.search(r'\b\d{4}-\d{2}-\d{2}\b',opt.get_text(' ',strip=True)) if opt else None
    if title_node is None or published is None:raise ValueError('缺少可区分的公告标题或网页发布日期')
    paragraphs=[p.get_text(' ',strip=True) for p in article.find_all('p')]
    body='\n'.join(p for p in paragraphs if p and '微信扫一扫' not in p)
    return _document(url,raw,title_node.get_text(' ',strip=True),body,published.group(),'fetched_official_html')


def acquire(url,text=None,client=None):
    validate_url(url)
    if text is not None:
        # A pasted source link does not prove the text came from that URL.
        return _document(url,text.encode('utf-8'),'手动提供的公开公告文本',text,None,'manual_text_unverified')
    own=client is None
    client=client or httpx.Client(timeout=httpx.Timeout(15,connect=5),follow_redirects=False)
    try:
        target=url
        for _ in range(4):
            with client.stream('GET',target,headers={'User-Agent':'AnnouncementEvidenceResearch/1.0'}) as response:
                if response.status_code in (301,302,303,307,308):
                    from urllib.parse import urljoin
                    target=validate_url(urljoin(target,response.headers.get('location','')));continue
                response.raise_for_status()
                if 'text/html' not in response.headers.get('content-type',''):raise ValueError('首版仅采集HTML公告正文')
                chunks=[];size=0
                for chunk in response.iter_bytes():
                    size+=len(chunk)
                    if size>MAX_BYTES:raise ValueError('公告超过1MiB限制')
                    chunks.append(chunk)
                document=parse_html(target,b''.join(chunks));document['requested_url']=url;return document
        raise ValueError('公告跳转次数超限')
    finally:
        if own:client.close()


def load_sample():
    import base64,json
    from pathlib import Path
    path=Path(__file__).resolve().parent/'samples/official_listing.json'
    saved=json.loads(path.read_text(encoding='utf-8'))
    raw=base64.b64decode(saved['raw_base64'],validate=True)
    if digest(raw)!=saved['raw_sha256']:raise ValueError('留存官方示例校验失败')
    document=parse_html(saved['source_url'],raw)
    if document['text_sha256']!=saved['text_sha256']:raise ValueError('留存正文校验失败')
    document.update({'retrieved_at':saved['retrieved_at'],'origin':'frozen_official_snapshot',
        'source_verified':True,'sample_reference_status':'agent_reference_not_human_gold'})
    return document
