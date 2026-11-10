#!/usr/bin/env python3
"""Import a precomputed, value-free offline prefix JSON report; never scan Redis."""
import argparse,json,re,sys,urllib.request,urllib.parse
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parent.parent))
from app.redis_protocol import key_shape

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):return None

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--profile',required=True,type=Path)
    p.add_argument('--endpoint',required=True,help='base HTTPS URL, or http://127.0.0.1:8769')
    p.add_argument('--token-file',required=True,type=Path)
    a=p.parse_args();u=urllib.parse.urlsplit(a.endpoint)
    if u.scheme!='https' and not(u.scheme=='http' and u.hostname in ('127.0.0.1','localhost','::1')):p.error('HTTPS required outside loopback')
    if u.username or u.password or u.query or u.fragment or u.path not in ('','/'):p.error('use a base URL without credentials/query/path')
    if a.token_file.is_symlink() or a.token_file.stat().st_mode&0o077:p.error('token file must be private (0600) and not a symlink')
    if a.token_file.stat().st_size>4096:p.error('token file exceeds private configuration budget')
    if a.profile.stat().st_size>1024*1024:p.error('profile JSON exceeds 1 MiB; aggregate prefixes first')
    payload=json.loads(a.profile.read_text(encoding='utf-8'));token=json.loads(a.token_file.read_text(encoding='utf-8')).get('token','')
    if not isinstance(token,str) or not re.fullmatch(r'[!-~]{32,256}',token):p.error('private token not configured')
    if not isinstance(payload,dict) or set(payload)!={'instance','node','time','rdb_sha256','complete','total_keys','groups'}:p.error('not an offline prefix report; nothing uploaded')
    if not isinstance(payload['groups'],dict):p.error('groups must be numeric prefix statistics')
    for prefix,g in payload['groups'].items():
        if key_shape(prefix)!=prefix or not isinstance(g,dict) or set(g)!={'keys','estimated_bytes','expires','types'}:p.error('raw identifiers/unknown fields rejected before upload')
        if any(not isinstance(g[k],int) or isinstance(g[k],bool) or g[k]<0 for k in ('keys','estimated_bytes','expires')):p.error('only numeric statistics may be uploaded')
        if not isinstance(g['types'],dict) or set(g['types'])-{'string','hash','list','set','zset','stream','module','other'} or any(not isinstance(v,int) or isinstance(v,bool) or v<0 for v in g['types'].values()):p.error('invalid type counters')
    data=json.dumps(payload,separators=(',',':')).encode();request=urllib.request.Request(a.endpoint.rstrip('/')+'/api/ingest/redis-profiles',data=data,headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'})
    with urllib.request.build_opener(NoRedirect).open(request,timeout=20) as r:result=json.load(r)
    if result.get('ok') is not True:raise SystemExit('profile not acknowledged')
    print(json.dumps({'ok':True,'receipt':result.get('data')},ensure_ascii=False))
if __name__=='__main__':main()
