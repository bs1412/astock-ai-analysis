"""Allowlisted release build/verify/unpack; standard library, no account reads.

Content hashes are integrity checks, not publisher signatures. Verify a separately
obtained archive SHA-256 before unpacking. Never merges into an existing tree.
"""
import argparse
import ast
from datetime import datetime
import hashlib
import importlib.metadata
import io
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import shutil
import stat
import tempfile
import zipfile

from app_manifest import APP_FILES, APP_VERSION, OWNER_MODULE_FILES

ROOT=Path(__file__).resolve().parent
RELEASE_FILES=(*APP_FILES,'share_gateway.py','release_bundle.py','requirements.txt',
               '发行说明.md','启动分享服务.bat','启动网页版.bat',*OWNER_MODULE_FILES)
MANIFEST='release_manifest.json'
MAX_FILE=4*1024*1024
MAX_TOTAL=16*1024*1024
MAX_ARCHIVE=16*1024*1024
DEPENDENCIES=('akshare','pandas','numpy','requests','plotly','tzdata')


def _digest(raw):return hashlib.sha256(raw).hexdigest()


def _json(value):
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode('utf-8')


def _unique(pairs):
    value={}
    for key,item in pairs:
        if key in value:raise ValueError('duplicate JSON key')
        value[key]=item
    return value


def _path(name):
    if not isinstance(name,str) or not name or any(ord(c)<32 for c in name) or '\\' in name or ':' in name:
        raise ValueError('invalid release path')
    path=PurePosixPath(name)
    if path.is_absolute() or path.as_posix()!=name or any(part in ('.','..') or part.endswith((' ','.')) for part in path.parts):
        raise ValueError('ambiguous release path')
    reserved={'CON','PRN','AUX','NUL',*(f'COM{i}' for i in range(1,10)),*(f'LPT{i}' for i in range(1,10))}
    if any(part.split('.')[0].upper() in reserved for part in path.parts):raise ValueError('reserved Windows path')
    return name


def _read_allowed(root,name):
    _path(name)
    target=root/name
    for part in (target,*target.parents):
        if part.is_symlink() or (hasattr(part,'is_junction') and part.is_junction()):
            raise ValueError('release input may not be a link')
        if part==root:break
    if not target.is_file() or target.stat().st_size>MAX_FILE:raise ValueError('release input missing or too large: '+name)
    raw=target.read_bytes()
    if len(raw)>MAX_FILE:raise ValueError('release input changed beyond size limit')
    return raw


def _constant(raw,name):
    for node in ast.parse(raw.decode('utf-8-sig')).body:
        if isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id==name for t in node.targets):
            return ast.literal_eval(node.value)
    raise ValueError('missing source constant '+name)


def _source_info(files):
    version=_constant(files['app_manifest.py'],'APP_VERSION')
    business=_constant(files['app_manifest.py'],'BUSINESS_FILES')
    if version!=APP_VERSION or (*business,'web/index.html')!=APP_FILES:
        raise ValueError('release source manifest differs from this trusted builder')
    badge=re.search(r'class="ver">(V[0-9.]+)',files['web/index.html'].decode('utf-8'))
    if not badge or badge.group(1)!=version:raise ValueError('page and backend versions differ')
    hashes={name:_digest(files[name]) for name in APP_FILES}
    return {'app_version':version,'source_code_sha256':_digest(json.dumps(hashes,sort_keys=True,separators=(',',':')).encode()),
            'rule_version':_constant(files['rule_contract.py'],'RULE_VERSION'),
            'rule_source_sha256':{n:_digest(files[n]) for n in ('rule_contract.py','entry_signals.py','exit_signals.py','signal_snapshot.py')}}


def _requirements(raw):
    pins={}
    for line in raw.decode('utf-8').splitlines():
        line=line.strip()
        if not line or line.startswith('#'):continue
        match=re.fullmatch(r'([a-z]+)==([0-9]+(?:\.[0-9]+)+)',line)
        if not match or match.group(1) in pins:raise ValueError('direct dependency pins are ambiguous')
        pins[match.group(1)]=match.group(2)
    if set(pins)!=set(DEPENDENCIES):raise ValueError('direct dependency allowlist differs')
    return pins


def build(destination,source=ROOT):
    source=Path(source).absolute()
    files={name:_read_allowed(source,name) for name in RELEASE_FILES}
    if len(files)!=len(RELEASE_FILES) or len({n.casefold() for n in files})!=len(files):raise ValueError('duplicate release allowlist')
    if sum(map(len,files.values()))>MAX_TOTAL:raise ValueError('release total size limit')
    info=_source_info(files)
    dependencies={n:importlib.metadata.version(n) for n in DEPENDENCIES}
    if dependencies!=_requirements(files['requirements.txt']):raise ValueError('build environment differs from direct dependency pins')
    manifest={'schema':'ashare_release_v1',**info,
        'built_at':datetime.now().astimezone().isoformat(timespec='seconds'),
        'environment':{'python':platform.python_version(),'system':platform.system(),
                       'dependencies':dependencies},
        'files':{name:{'sha256':_digest(raw),'size':len(raw)} for name,raw in files.items()},
        'integrity_scope':'content_only_not_signature_no_private_data_or_runtime_state'}
    files[MANIFEST]=_json(manifest)
    destination=Path(destination)
    # Exclusive creation: a build may never replace a previous release.
    with destination.open('xb') as handle:
        try:
            with zipfile.ZipFile(handle,'w',compression=zipfile.ZIP_DEFLATED) as bundle:
                for name,raw in sorted(files.items()):
                    member=zipfile.ZipInfo(name,date_time=(2026,1,1,0,0,0))
                    member.create_system=3;member.external_attr=(stat.S_IFREG|0o644)<<16
                    member.compress_type=zipfile.ZIP_DEFLATED
                    bundle.writestr(member,raw)
            handle.flush()
        except Exception:
            handle.close();destination.unlink(missing_ok=True);raise
    digest=_digest(destination.read_bytes())
    try:verify(destination,digest)
    except Exception:
        destination.unlink(missing_ok=True);raise
    return {'archive_sha256':digest,'archive_size':destination.stat().st_size,'manifest':manifest}


def _load(path,expected_sha256):
    if not isinstance(expected_sha256,str) or not re.fullmatch('[0-9a-f]{64}',expected_sha256):
        raise ValueError('a separately obtained lowercase archive SHA-256 is required')
    path=Path(path)
    if not path.is_file() or path.stat().st_size>MAX_ARCHIVE:raise ValueError('archive missing or size limit')
    raw=path.read_bytes()
    if len(raw)>MAX_ARCHIVE or _digest(raw)!=expected_sha256:raise ValueError('archive SHA-256 mismatch')
    files={};expected=set(RELEASE_FILES)|{MANIFEST}
    with zipfile.ZipFile(io.BytesIO(raw)) as bundle:
        members=bundle.infolist()
        if len(members)!=len(expected):raise ValueError('release member count differs')
        total=0;folded=set()
        for member in members:
            if member.orig_filename!=member.filename:raise ValueError('truncated release path alias')
            name=_path(member.filename)
            kind=stat.S_IFMT(member.external_attr>>16)
            if name not in expected or name.casefold() in folded or member.is_dir():raise ValueError('extra or duplicate release member')
            if kind not in (0,stat.S_IFREG) or member.flag_bits&1:raise ValueError('linked or encrypted release member')
            if member.compress_type not in (zipfile.ZIP_STORED,zipfile.ZIP_DEFLATED):raise ValueError('unsupported release compression')
            if not 0<=member.file_size<=MAX_FILE:raise ValueError('release member size limit')
            total+=member.file_size
            if total>MAX_TOTAL:raise ValueError('release expanded size limit')
            folded.add(name.casefold())
            with bundle.open(member) as handle:data=handle.read(MAX_FILE+1)
            if len(data)!=member.file_size or len(data)>MAX_FILE:raise ValueError('release member size differs')
            files[name]=data
    if set(files)!=expected:raise ValueError('release allowlist incomplete')
    if len(files[MANIFEST])>128*1024:raise ValueError('manifest too large')
    manifest=json.loads(files.pop(MANIFEST),object_pairs_hook=_unique)
    if not isinstance(manifest,dict) or manifest.get('schema')!='ashare_release_v1':raise ValueError('unknown release schema')
    entries=manifest.get('files')
    if not isinstance(entries,dict) or set(entries)!=set(RELEASE_FILES):raise ValueError('manifest allowlist differs')
    for name,data in files.items():
        item=entries[name]
        if (not isinstance(item,dict) or set(item)!={'sha256','size'} or type(item['size']) is not int
                or item['size']!=len(data) or item['sha256']!=_digest(data)):
            raise ValueError('release file hash/size differs: '+name)
    for key,value in _source_info(files).items():
        if manifest.get(key)!=value:raise ValueError('release identity differs: '+key)
    environment=manifest.get('environment')
    if not isinstance(environment,dict) or environment.get('dependencies')!=_requirements(files['requirements.txt']):
        raise ValueError('recorded dependency environment differs from pins')
    return manifest,files


def verify(path,expected_sha256):
    manifest,_=_load(path,expected_sha256)
    return manifest


def unpack(path,destination,expected_sha256):
    manifest,files=_load(path,expected_sha256)
    destination=Path(destination).absolute()
    if destination.exists() or destination.is_symlink():raise FileExistsError('unpack requires a new destination')
    if not destination.parent.is_dir():raise ValueError('destination parent must exist')
    # Windows rename refuses an existing destination, including an empty one.
    # Other platforms need equivalent no-replace publication before support.
    if os.name!='nt':raise NotImplementedError('atomic new-directory unpack currently verified on Windows only')
    staging=Path(tempfile.mkdtemp(prefix='.release-stage-',dir=destination.parent)).resolve()
    try:
        for name,data in {**files,MANIFEST:_json(manifest)}.items():
            target=staging/name;target.parent.mkdir(parents=True,exist_ok=True)
            with target.open('xb') as handle:handle.write(data)
        if {p.relative_to(staging).as_posix() for p in staging.rglob('*') if p.is_file()}!=set(files)|{MANIFEST}:
            raise ValueError('staged release differs')
        for name,data in files.items():
            if (staging/name).read_bytes()!=data:raise ValueError('staged release write differs')
        os.rename(staging,destination)
    finally:
        if staging.exists():
            assert staging.parent==destination.parent.resolve() and staging.name.startswith('.release-stage-')
            shutil.rmtree(staging)
    return manifest


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    commands=parser.add_subparsers(dest='command',required=True)
    commands.add_parser('build').add_argument('--output',required=True)
    for name in ('verify','unpack'):
        command=commands.add_parser(name);command.add_argument('archive');command.add_argument('--sha256',required=True)
        if name=='unpack':command.add_argument('--destination',required=True)
    args=parser.parse_args()
    if args.command=='build':result=build(args.output)
    elif args.command=='verify':result={'manifest':verify(args.archive,args.sha256)}
    else:result={'manifest':unpack(args.archive,args.destination,args.sha256)}
    print(json.dumps(result,ensure_ascii=False,indent=2))


if __name__=='__main__':main()
