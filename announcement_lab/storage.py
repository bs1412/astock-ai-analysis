"""Transactional documents, calls, facts and append-only reviews; no market data."""
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import sqlite3
import time
import uuid
from .sources import now,digest,validate_url


def encode(value):return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)
def identifier(value):return digest(encode(value).encode('utf-8'))


class IntegrityError(RuntimeError):pass
class Conflict(RuntimeError):pass
class LeaseLost(RuntimeError):pass
class CallUncertain(RuntimeError):pass
class SharedExtractionPending(RuntimeError):pass


class Repository:
    def __init__(self,root,lease_seconds=120):
        self.root=Path(root).resolve();self.path=self.root/'announcements.sqlite3';self.lease_seconds=lease_seconds
        self.root.mkdir(parents=True,exist_ok=True);existed=self.path.exists()
        try:
            with self.connection() as db:
                if existed:
                    if db.execute('PRAGMA quick_check').fetchone()[0]!='ok':raise IntegrityError('公告数据库完整性检查失败，原文件保留')
                    version=db.execute('SELECT value FROM metadata WHERE key=?',('schema',)).fetchone()
                    if version is None or version[0]!='2':raise IntegrityError('公告数据库模式未支持，原文件保留；不覆盖旧原型数据库')
                else:
                    db.executescript('''
                    CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                    INSERT INTO metadata VALUES('schema','2');
                    CREATE TABLE documents(id TEXT PRIMARY KEY,url TEXT NOT NULL,text_sha TEXT NOT NULL,
                        raw_sha TEXT NOT NULL,raw BLOB NOT NULL,payload TEXT NOT NULL,payload_sha TEXT NOT NULL,
                        previous_url_version TEXT,created TEXT NOT NULL,UNIQUE(url,text_sha));
                    CREATE TABLE runs(id TEXT PRIMARY KEY,idempotency_key TEXT UNIQUE NOT NULL,
                        request_sha TEXT NOT NULL,spec TEXT NOT NULL,status TEXT NOT NULL,stage TEXT NOT NULL,
                        document_id TEXT REFERENCES documents(id),candidate TEXT,validation TEXT,event_id TEXT,
                        error_code TEXT,message TEXT,owner TEXT,lease_until REAL NOT NULL DEFAULT 0,
                        created TEXT NOT NULL,updated TEXT NOT NULL);
                    CREATE TABLE calls(id TEXT PRIMARY KEY,run_id TEXT NOT NULL REFERENCES runs(id),
                        extraction_key TEXT,state TEXT NOT NULL,envelope TEXT,error_code TEXT,created TEXT NOT NULL,finished TEXT);
                    CREATE TABLE extractions(id TEXT PRIMARY KEY,document_id TEXT NOT NULL,extractor TEXT NOT NULL,
                        owner_run TEXT NOT NULL,state TEXT NOT NULL,active_call TEXT,envelope TEXT,updated TEXT NOT NULL);
                    CREATE TABLE retrievals(run_id TEXT PRIMARY KEY,document_id TEXT NOT NULL REFERENCES documents(id),
                        retrieved_at TEXT NOT NULL,origin TEXT NOT NULL,raw_sha TEXT NOT NULL,raw BLOB NOT NULL);
                    CREATE TABLE steps(id INTEGER PRIMARY KEY,run_id TEXT NOT NULL REFERENCES runs(id),
                        stage TEXT NOT NULL,state TEXT NOT NULL,at TEXT NOT NULL);
                    CREATE TABLE revisions(id TEXT PRIMARY KEY,run_id TEXT NOT NULL REFERENCES runs(id),
                        candidate TEXT NOT NULL,reason TEXT NOT NULL,created TEXT NOT NULL);
                    CREATE TABLE facts(id TEXT PRIMARY KEY,document_id TEXT NOT NULL REFERENCES documents(id),
                        extractor TEXT NOT NULL,mode TEXT NOT NULL,payload TEXT NOT NULL,validation TEXT NOT NULL,
                        code TEXT NOT NULL,event_type TEXT,published_date TEXT,human_review TEXT NOT NULL DEFAULT 'pending',
                        created TEXT NOT NULL,parent_event_id TEXT REFERENCES facts(id));
                    CREATE TABLE reviews(id TEXT PRIMARY KEY,event_id TEXT NOT NULL REFERENCES facts(id),
                        decision TEXT NOT NULL,note TEXT NOT NULL,created TEXT NOT NULL);
                    CREATE INDEX facts_lookup ON facts(code,event_type,published_date);
                    ''')
                db.execute('PRAGMA journal_mode=WAL')
        except sqlite3.DatabaseError as error:raise IntegrityError('公告数据库无法读取，原文件未重置') from error

    @contextmanager
    def connection(self,write=False):
        db=None
        try:
            db=sqlite3.connect(self.path,timeout=10);db.row_factory=sqlite3.Row
            db.execute('PRAGMA foreign_keys=ON')
            if write:db.execute('BEGIN IMMEDIATE')
            yield db
            db.commit()
        except sqlite3.DatabaseError as error:
            if db:db.rollback()
            raise IntegrityError('公告存储失败，原文件保留') from error
        finally:
            if db:db.close()

    def submit(self,spec,key):
        fingerprint=identifier(spec);stamp=now()
        with self.connection(True) as db:
            existing=db.execute('SELECT id,request_sha FROM runs WHERE idempotency_key=?',(key,)).fetchone()
            if existing:
                if existing['request_sha']!=fingerprint:raise Conflict('相同幂等键不能用于不同输入')
                return existing['id'],True
            run_id=uuid.uuid4().hex
            db.execute('INSERT INTO runs(id,idempotency_key,request_sha,spec,status,stage,created,updated) VALUES(?,?,?,?,?,?,?,?)',
                       (run_id,key,fingerprint,encode(spec),'queued','queued',stamp,stamp))
        return run_id,False

    def run(self,run_id):
        with self.connection() as db:
            row=db.execute('SELECT * FROM runs WHERE id=?',(run_id,)).fetchone()
            if row is None:raise KeyError('任务不存在')
            result=dict(row)
            for field in ['spec','candidate','validation']:
                result[field]=json.loads(result[field]) if result[field] else None
            result['steps']=[dict(r) for r in db.execute('SELECT stage,state,at FROM steps WHERE run_id=? ORDER BY id',(run_id,))]
            result['calls']=[{**dict(r),'envelope':json.loads(r['envelope']) if r['envelope'] else None}
                for r in db.execute('SELECT id,state,envelope,error_code,created,finished FROM calls WHERE run_id=? ORDER BY created,id',(run_id,))]
            result['revisions']=[{**dict(r),'candidate':json.loads(r['candidate'])} for r in db.execute('SELECT * FROM revisions WHERE run_id=? ORDER BY created,id',(run_id,))]
            retrieval=db.execute('SELECT document_id,retrieved_at,origin,raw_sha FROM retrievals WHERE run_id=?',(run_id,)).fetchone()
            result['retrieval']=dict(retrieval) if retrieval else None
            return result

    def _owned(self,db,run_id,owner):
        row=db.execute('SELECT owner,lease_until FROM runs WHERE id=?',(run_id,)).fetchone()
        if row is None or row['owner']!=owner or row['lease_until']<time.time():raise LeaseLost('任务租约已变更，停止旧执行写入')

    def claim(self,run_id,owner):
        with self.connection(True) as db:
            row=db.execute('SELECT status,lease_until FROM runs WHERE id=?',(run_id,)).fetchone()
            if row is None:raise KeyError('任务不存在')
            if row['status']=='completed' or row['lease_until']>time.time():return False
            db.execute('UPDATE runs SET owner=?,lease_until=?,status=?,error_code=NULL,message=NULL,updated=? WHERE id=?',
                (owner,time.time()+self.lease_seconds,'running',now(),run_id))
        return True

    def stage(self,run_id,owner,stage):
        with self.connection(True) as db:
            self._owned(db,run_id,owner)
            db.execute('UPDATE runs SET stage=?,lease_until=?,updated=? WHERE id=?',(stage,time.time()+self.lease_seconds,now(),run_id))
            db.execute('INSERT INTO steps(run_id,stage,state,at) VALUES(?,?,?,?)',(run_id,stage,'entered',now()))

    def attach_document(self,run_id,owner,document):
        payload={k:v for k,v in document.items() if k!='raw_bytes'}
        with self.connection(True) as db:
            self._owned(db,run_id,owner)
            previous=db.execute('SELECT id FROM documents WHERE url=? AND id<>? ORDER BY created DESC LIMIT 1',
                                (document['source_url'],document['document_id'])).fetchone()
            db.execute('INSERT OR IGNORE INTO documents VALUES(?,?,?,?,?,?,?,?,?)',
                (document['document_id'],document['source_url'],document['text_sha256'],document['raw_sha256'],
                 document['raw_bytes'],encode(payload),identifier(payload),previous['id'] if previous else None,now()))
            db.execute('UPDATE runs SET document_id=? WHERE id=?',(document['document_id'],run_id))
            db.execute('INSERT OR IGNORE INTO retrievals VALUES(?,?,?,?,?,?)',(run_id,document['document_id'],
                document['retrieved_at'],document['origin'],document['raw_sha256'],document['raw_bytes']))

    def document(self,document_id):
        with self.connection() as db:row=db.execute('SELECT * FROM documents WHERE id=?',(document_id,)).fetchone()
        if row is None:raise KeyError('公告不存在')
        try:
            value=json.loads(row['payload']);validate_url(value['source_url'])
            valid=(identifier(value)==row['payload_sha'] and digest(row['raw'])==row['raw_sha']
                and digest(value['text'].encode('utf-8'))==row['text_sha']
                and value['source_url']==row['url'] and value['document_id']==row['id']
                and value['raw_sha256']==row['raw_sha'] and value['text_sha256']==row['text_sha']
                and digest((row['url']+'\n'+row['text_sha']).encode('utf-8'))==row['id'])
        except (ValueError,KeyError,TypeError):valid=False
        if not valid:raise IntegrityError('公告内容或元数据凭据不匹配，停止使用并保留原文件')
        value['previous_url_version']=row['previous_url_version']
        value['correction_relation']='not_inferred_url_version_only'
        return value

    def candidate(self,run_id,owner,envelope):
        # The response ledger is written first; a checkpoint failure can reuse it.
        with self.connection(True) as db:
            self._owned(db,run_id,owner)
            db.execute('UPDATE runs SET candidate=?,updated=? WHERE id=?',(encode(envelope['candidate']),now(),run_id))

    def active_candidate(self,run_id):
        row=self.run(run_id)
        return row['revisions'][-1]['candidate'] if row['revisions'] else row['candidate']

    def claim_extraction(self,run_id,owner,document_id,extractor,external=True):
        key=identifier([document_id,extractor])
        with self.connection(True) as db:
            self._owned(db,run_id,owner)
            row=db.execute('SELECT * FROM extractions WHERE id=?',(key,)).fetchone()
            if row and row['state']=='returned':return key,json.loads(row['envelope'])
            if row and row['state'] in ('started','uncertain'):
                if not external and row['owner_run']==run_id:
                    pass
                elif row['state']=='uncertain' or row['owner_run']==run_id:
                    raise CallUncertain('相同内容的外部调用结果未知，先处理原任务：'+row['owner_run'])
                else:raise SharedExtractionPending('相同内容已由另一任务处理，完成后恢复可复用：'+row['owner_run'])
            db.execute('INSERT INTO extractions(id,document_id,extractor,owner_run,state,updated) VALUES(?,?,?,?,?,?) '
                'ON CONFLICT(id) DO UPDATE SET owner_run=excluded.owner_run,state=excluded.state,active_call=NULL,envelope=NULL,updated=excluded.updated',
                (key,document_id,extractor,run_id,'started',now()))
        return key,None

    def finish_local_extraction(self,key,run_id,owner,envelope=None):
        with self.connection(True) as db:
            self._owned(db,run_id,owner)
            db.execute('UPDATE extractions SET state=?,envelope=?,updated=? WHERE id=? AND owner_run=? AND active_call IS NULL',
                ('returned' if envelope is not None else 'failed',encode(envelope) if envelope is not None else None,now(),key,run_id))

    def start_call(self,run_id,owner,extraction_key):
        with self.connection(True) as db:
            self._owned(db,run_id,owner)
            pending=db.execute("SELECT id FROM calls WHERE run_id=? AND state IN ('started','uncertain')",(run_id,)).fetchone()
            if pending:raise CallUncertain('外部调用结果未知；先核对服务商，不能自动再次付费')
            call_id=uuid.uuid4().hex
            db.execute('INSERT INTO calls(id,run_id,extraction_key,state,created) VALUES(?,?,?,?,?)',(call_id,run_id,extraction_key,'started',now()))
            db.execute('UPDATE extractions SET active_call=? WHERE id=? AND owner_run=?',(call_id,extraction_key,run_id))
        return call_id

    def finish_call(self,call_id,envelope=None,error_code=None):
        with self.connection(True) as db:
            row=db.execute('SELECT state,extraction_key FROM calls WHERE id=?',(call_id,)).fetchone()
            state=('late_returned' if row and row['state']=='abandoned' else 'returned') if envelope is not None else 'uncertain'
            db.execute('UPDATE calls SET state=?,envelope=?,error_code=?,finished=? WHERE id=?',
                (state,encode(envelope) if envelope is not None else None,error_code,now(),call_id))
            if row:
                db.execute('UPDATE extractions SET state=?,envelope=?,updated=? WHERE id=? AND active_call=?',
                    (state,encode(envelope) if envelope is not None else None,now(),row['extraction_key'],call_id))

    def acknowledge_uncertain(self,run_id):
        with self.connection(True) as db:
            row=db.execute('SELECT lease_until FROM runs WHERE id=?',(run_id,)).fetchone()
            if row is None:raise KeyError('任务不存在')
            if row['lease_until']>time.time():raise Conflict('任务租约仍有效，不能接管正在执行的阶段')
            db.execute("UPDATE calls SET state='abandoned' WHERE run_id=? AND state IN ('started','uncertain')",(run_id,))
            db.execute("UPDATE extractions SET state='abandoned' WHERE owner_run=? AND state IN ('started','uncertain')",(run_id,))

    def validation(self,run_id,owner,report):
        with self.connection(True) as db:
            self._owned(db,run_id,owner);db.execute('UPDATE runs SET validation=? WHERE id=?',(encode(report),run_id))

    def complete(self,run_id,owner,extractor,mode):
        row=self.run(run_id);report=row['validation']
        if not report or not report['mechanical_pass']:raise Conflict('原文核验未通过，不能入事实表')
        candidate=report['normalized'];event_id=identifier([row['document_id'],extractor,candidate])
        with self.connection(True) as db:
            self._owned(db,run_id,owner)
            db.execute('INSERT OR IGNORE INTO facts(id,document_id,extractor,mode,payload,validation,code,event_type,published_date,created,parent_event_id) VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                (event_id,row['document_id'],extractor,mode,encode(candidate),encode(report),candidate['security_code']['value'],
                 candidate['event_type']['value'],candidate['published_date']['value'],now(),row['spec'].get('revision_of_event')))
            db.execute('UPDATE runs SET event_id=?,status=?,stage=?,owner=NULL,lease_until=0,updated=? WHERE id=?',
                (event_id,'completed','stored',now(),run_id))
            db.execute('INSERT INTO steps(run_id,stage,state,at) VALUES(?,?,?,?)',(run_id,'stored','completed',now()))
        return event_id

    def fail(self,run_id,owner,status,code,message):
        with self.connection(True) as db:
            db.execute("UPDATE runs SET status=?,error_code=?,message=?,owner=NULL,lease_until=0,updated=? WHERE id=? AND owner=? AND status<>'completed'",
                (status,code,message,now(),run_id,owner))

    def revise(self,run_id,candidate,reason):
        with self.connection(True) as db:
            row=db.execute('SELECT * FROM runs WHERE id=?',(run_id,)).fetchone()
            if row is None:raise KeyError('任务不存在')
            if row['lease_until']>time.time():raise Conflict('运行中任务不能修订')
            if row['status']=='completed':
                spec=json.loads(row['spec']);spec.update(revision_of_run=run_id,revision_of_event=row['event_id'])
                key='revision:'+identifier([run_id,candidate,reason])
                old=db.execute('SELECT id FROM runs WHERE idempotency_key=?',(key,)).fetchone()
                if old:return old['id']
                child=uuid.uuid4().hex;stamp=now()
                db.execute('INSERT INTO runs(id,idempotency_key,request_sha,spec,status,stage,document_id,candidate,created,updated) VALUES(?,?,?,?,?,?,?,?,?,?)',
                    (child,key,identifier(spec),encode(spec),'queued','human_revision',row['document_id'],row['candidate'],stamp,stamp))
                db.execute('INSERT INTO revisions VALUES(?,?,?,?,?)',(uuid.uuid4().hex,child,encode(candidate),reason,stamp))
                db.execute('INSERT INTO retrievals SELECT ?,document_id,retrieved_at,origin,raw_sha,raw FROM retrievals WHERE run_id=?',(child,run_id))
                return child
            db.execute('INSERT INTO revisions VALUES(?,?,?,?,?)',(uuid.uuid4().hex,run_id,encode(candidate),reason,now()))
        return run_id

    def events(self,code=None,event_type=None,published_after=None):
        clauses=[];values=[]
        for column,value in [('code',code),('event_type',event_type)]:
            if value:clauses.append(column+'=?');values.append(value)
        if published_after:clauses.append('published_date>=?');values.append(published_after)
        query="SELECT facts.*,(SELECT id FROM runs WHERE event_id=facts.id AND status='completed' ORDER BY created,id LIMIT 1) AS run_id FROM facts"+(' WHERE '+' AND '.join(clauses) if clauses else '')+' ORDER BY created DESC LIMIT 100'
        with self.connection() as db:rows=db.execute(query,values).fetchall()
        return [{**dict(r),'payload':json.loads(r['payload']),'validation':json.loads(r['validation'])} for r in rows]

    def review(self,event_id,decision,note):
        with self.connection(True) as db:
            if not db.execute('SELECT 1 FROM facts WHERE id=?',(event_id,)).fetchone():raise KeyError('事件不存在')
            db.execute('INSERT INTO reviews VALUES(?,?,?,?,?)',(uuid.uuid4().hex,event_id,decision,note,now()))
            db.execute('UPDATE facts SET human_review=? WHERE id=?',(decision,event_id))

    def runs(self):
        with self.connection() as db:
            return [dict(r) for r in db.execute('SELECT id,status,stage,error_code,message,document_id,event_id,created,updated FROM runs ORDER BY created DESC LIMIT 100')]

    def counts(self):
        with self.connection() as db:return {name:db.execute('SELECT COUNT(*) FROM '+name).fetchone()[0] for name in ['documents','runs','facts','calls','extractions','revisions','reviews','retrievals']}

    def backup(self,destination):
        target=Path(destination)
        if target.exists():raise Conflict('备份目标已存在，禁止覆盖')
        target.parent.mkdir(parents=True,exist_ok=True)
        # Exclusive reservation prevents concurrent replacement of an existing file.
        with target.open('xb'):pass
        with self.connection() as source:
            sink=sqlite3.connect(target)
            try:
                source.backup(sink)
                if sink.execute('PRAGMA integrity_check').fetchone()[0]!='ok':raise IntegrityError('备份完整性失败')
            finally:sink.close()
        return {'path':str(target),'sha256':digest(target.read_bytes()),'scope':'consistent_business_database_not_graph_checkpoints_or_source_code'}

    @classmethod
    def restore_new(cls,backup,new_root,expected_sha):
        raw=Path(backup).read_bytes()
        if digest(raw)!=expected_sha:raise IntegrityError('备份哈希不符，未恢复')
        target=Path(new_root)
        target.mkdir(parents=False,exist_ok=False)
        with (target/'announcements.sqlite3').open('xb') as handle:handle.write(raw)
        return cls(target)
