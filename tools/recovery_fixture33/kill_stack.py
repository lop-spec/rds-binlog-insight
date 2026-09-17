"""Host PID lifetime-checked SIGKILL, only for this GitHub-hosted fixture graph."""
import json,os,signal,subprocess,sys,time
from pathlib import Path
from tools.recovery_isolation33 import SCOPE,inspect,require_ci,write_json

def kill(root,names):
    require_ci();assert os.environ.get('RUNNER_ENVIRONMENT')=='github-hosted';assert os.geteuid()==0
    root=Path(root).resolve();workspace=Path(os.environ['GITHUB_WORKSPACE']).resolve()
    assert root.is_relative_to(workspace/'recovery-evidence')
    assert len(names)==6 and len(set(names))==6
    handles=[];proof=[]
    try:
        for name in names:
            c=inspect(name);labels=c['Config']['Labels'];assert labels.get('scope')==SCOPE and labels.get('run')==os.environ['GITHUB_RUN_ID']
            assert c['State']['Running'] and c['HostConfig']['RestartPolicy']['Name']=='unless-stopped'
            for m in c['Mounts']:
                if m['Type']=='bind':assert Path(m['Source']).resolve().is_relative_to(workspace)
            for net in c['NetworkSettings']['Networks']:
                n=json.loads(subprocess.check_output(['docker','network','inspect',net],timeout=10))[0]
                assert n['Internal'] is True and n['Labels']['scope']==SCOPE
            pid=c['State']['Pid'];assert pid>1
            stat=Path(f'/proc/{pid}/stat').read_text();ticks=stat.rsplit(')',1)[1].split()[19]
            fd=os.pidfd_open(pid);handles.append(fd)
            current=inspect(name)
            assert current['Id']==c['Id'] and current['State']['Pid']==pid and current['State']['StartedAt']==c['State']['StartedAt']
            assert Path(f'/proc/{pid}/stat').read_text().rsplit(')',1)[1].split()[19]==ticks
            proof.append({'name':name,'pid':pid,'startTicks':ticks,'containerId':c['Id'],'startedAt':c['State']['StartedAt'],'restartCount':c['RestartCount']})
        # All six scopes and lifetimes must pass before the first signal. No docker kill/stop/start.
        started=time.monotonic()
        for fd in handles:signal.pidfd_send_signal(fd,signal.SIGKILL)
        write_json(root/'fault-signals.json',{'signal':'SIGKILL','pidfdVerified':True,'signalSpanSeconds':time.monotonic()-started,'services':proof,'manualStartCalls':0})
    finally:
        for fd in handles:os.close(fd)

if __name__=='__main__':kill(sys.argv[1],sys.argv[2:])
