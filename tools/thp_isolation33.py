"""CI-only partially mapped THP reproducer plus single-factor ClickHouse ABBA."""
from __future__ import annotations
import argparse,ctypes,hashlib,json,mmap,os,subprocess,sys,tempfile,time,uuid
from pathlib import Path
from tools import memory_isolation33 as memory

HUGE=2*1024**2
FOLIOS=64
KEEP=HUGE//4
WRAPPER=r'''#include <sys/prctl.h>
#include <unistd.h>
#include <stdio.h>
int main(int argc,char **argv) {
    if(argc<2) return 64;
    if(prctl(PR_SET_THP_DISABLE,1,0,0,0)!=0) {perror("PR_SET_THP_DISABLE"); return 78;}
    if(prctl(PR_GET_THP_DISABLE,0,0,0,0)!=1) return 79;
    fprintf(stderr,"isolated THP disabled for process and descendants\n");
    execvp(argv[1],argv+1); perror("execvp"); return 80;
}
'''

def gap(snapshot):
    stat=snapshot['stat']
    return stat['active_anon']+stat['inactive_anon']-stat['anon']-stat.get('shmem',0)-stat.get('swapcached',0)

def worker(disabled):
    memory.fixture_only();assert sys.platform=='linux'
    libc=ctypes.CDLL(None,use_errno=True)
    libc.prctl.argtypes=[ctypes.c_int,ctypes.c_ulong,ctypes.c_ulong,ctypes.c_ulong,ctypes.c_ulong]
    libc.madvise.argtypes=[ctypes.c_void_p,ctypes.c_size_t,ctypes.c_int]
    if disabled:assert libc.prctl(41,1,0,0,0)==0,ctypes.get_errno()
    actual=libc.prctl(42,0,0,0,0);assert actual==int(disabled)
    def sample():
        stat=memory.counters(Path('/sys/fs/cgroup/memory.stat'))
        smaps={line.split(':')[0]:int(line.split()[1])*1024 for line in Path('/proc/self/smaps_rollup').read_text().splitlines() if line.startswith(('Rss:','Anonymous:','AnonHugePages:'))}
        return {'current':int(Path('/sys/fs/cgroup/memory.current').read_text()),'stat':stat,'smaps':smaps,'events':memory.counters(Path('/sys/fs/cgroup/memory.events'))}
    out={'disabled':disabled,'prGetThpDisable':actual,'before':sample(),'logicalBytes':FOLIOS*HUGE}
    buf=mmap.mmap(-1,FOLIOS*HUGE+HUGE,flags=mmap.MAP_PRIVATE|mmap.MAP_ANONYMOUS,prot=mmap.PROT_READ|mmap.PROT_WRITE)
    address=ctypes.addressof(ctypes.c_char.from_buffer(buf));base=(address+HUGE-1)//HUGE*HUGE
    assert libc.madvise(base,FOLIOS*HUGE,14)==0 # MADV_HUGEPAGE is per mapping, not a host setting.
    ctypes.memset(base,90,FOLIOS*HUGE)
    collapsed=libc.madvise(base,FOLIOS*HUGE,25) # MADV_COLLAPSE, Linux >= 6.1.
    out['collapse']={'result':collapsed,'errno':ctypes.get_errno() if collapsed else 0}
    out['fullyMapped']=sample()
    for i in range(FOLIOS):assert libc.madvise(base+i*HUGE+KEEP,HUGE-KEEP,4)==0 # MADV_DONTNEED
    out['samples']=[]
    for _ in range(5):time.sleep(.2);out['samples'].append(sample())
    expected=hashlib.sha256(b'Z'*4096).hexdigest()
    out['retainedOracle']=all(hashlib.sha256(ctypes.string_at(base+i*HUGE,4096)).hexdigest()==expected for i in range(FOLIOS))
    out['anonymousLruGaps']=[gap(s) for s in out['samples']]
    print(json.dumps(out),flush=True)
    buf.close()

def reproducer_gate(phases):
    return len(phases)==4 and all(p['retainedOracle'] and all(s['events'].get('oom_kill',0)==0 for s in p['samples']) and
        ((max(p['anonymousLruGaps'])<8*1024**2 and p['prGetThpDisable']==1) if p['disabled'] else
         (min(p['anonymousLruGaps'])>64*1024**2 and p['fullyMapped']['smaps']['AnonHugePages']>=96*1024**2)) for p in phases)

def main():
    assert os.environ.get('GITHUB_ACTIONS')=='true','isolated CI only'
    source=Path.cwd();phases=[];failure=None
    memory.run(['docker','pull',memory.APP],240)
    try:
        for disabled in [False,True,True,False]:
            name='thp-isolation-'+uuid.uuid4().hex[:12]
            try:
                raw=memory.run(['docker','run','--name',name,'--label','scope=thp-isolation-ci','--network','none','--memory','3g','--memory-swap','3g','--cpus','2','--restart','no','--entrypoint','python','--workdir','/src','-v',f'{source}:/src:ro','-e','MEMORY_ISOLATION_FIXTURE=1',memory.APP,'-m','tools.thp_isolation33','--worker',str(int(disabled))],30)
                phases.append(json.loads(raw));print(json.dumps({'thpDisabled':disabled,'gaps':phases[-1]['anonymousLruGaps']}),flush=True)
            finally:
                c=memory.inspect(name);assert c['Config']['Labels'].get('scope')=='thp-isolation-ci'
                if c['State']['Running']:memory.run(['docker','stop','--time','10',name],15)
                memory.run(['docker','rm',name],10)
        assert reproducer_gate(phases),'THP mechanism not reproduced; never treat missing pressure as a pass'
        with tempfile.TemporaryDirectory(prefix='thp-isolation-') as tmp:
            c=Path(tmp)/'thp-exec.c';exe=Path(tmp)/'thp-exec';c.write_text(WRAPPER)
            memory.run(['cc','-static','-O2','-o',str(exe),str(c)],30)
            executable=exe.read_bytes();executable_sha=hashlib.sha256(executable).hexdigest()
            os.environ['THP_ISOLATION_EXEC']=str(exe)
            memory.main()
            assert exe.read_bytes()==executable,'tested helper changed'
            root=Path('memory-evidence');(root/'thp-exec').write_bytes(executable)
            (root/'thp-exec.c').write_text(WRAPPER)
            (root/'thp-exec.sha256').write_text(executable_sha+'  thp-exec\n')
    except Exception as exc:
        failure=str(exc);raise
    finally:
        root=Path('memory-evidence');root.mkdir(exist_ok=True)
        detail={'scope':'synthetic per-process THP mechanism test, not a replay of incident 31','phases':phases,'gate':reproducer_gate(phases),'failure':failure,'hostThpSettingsChanged':False}
        raw=json.dumps(detail,indent=2).encode();(root/'thp-mechanism33.json').write_bytes(raw)
        (root/'thp-mechanism33.json.sha256').write_text(hashlib.sha256(raw).hexdigest()+'  thp-mechanism33.json\n')
        p=root/'memory-isolated33.json';evidence=json.loads(p.read_text()) if p.exists() else {'gate':{}}
        evidence['thpMechanismSha256']=hashlib.sha256(raw).hexdigest();evidence['gate']['thpMechanismReproduced']=detail['gate'] and not failure
        if (root/'thp-exec').exists():evidence['testedThpExecSha256']=hashlib.sha256((root/'thp-exec').read_bytes()).hexdigest()
        raw=json.dumps(evidence,indent=2).encode();p.write_bytes(raw);(root/'memory-isolated33.json.sha256').write_text(hashlib.sha256(raw).hexdigest()+'  memory-isolated33.json\n')

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--worker',type=int,choices=[0,1]);args=parser.parse_args()
    if args.worker is not None:worker(bool(args.worker))
    else:main()
