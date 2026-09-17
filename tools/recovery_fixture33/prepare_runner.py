"""Free disposable hosted-runner SDK space, without lowering the application's fuse."""
import json,os,shutil
from pathlib import Path
from app.clickhouse_client import ClickHouseConfig
from tools.recovery_isolation33 import require_ci,run,write_json

# Only provider-installed SDKs unused by this immutable-image experiment.
# Never remove toolcache Python/Node, runner internals, Docker, repository or data.
SDK_ROOTS=(
    '/usr/local/lib/android','/usr/share/dotnet','/usr/share/swift',
    '/usr/local/.ghcup','/opt/ghc','/usr/local/share/powershell',
    '/opt/hostedtoolcache/CodeQL','/opt/hostedtoolcache/Java_Temurin-Hotspot_jdk',
    '/opt/hostedtoolcache/Go','/opt/hostedtoolcache/Ruby',
)

def require_hosted():
    require_ci()
    assert os.environ.get('RUNNER_ENVIRONMENT')=='github-hosted','not a disposable hosted runner'
    assert os.environ.get('GITHUB_REPOSITORY')=='lop-spec/rds-binlog-insight','wrong repository'
    assert Path(os.environ['GITHUB_WORKSPACE']).resolve()==Path.cwd().resolve(),'wrong working directory'
    temp=Path(os.environ['RUNNER_TEMP']).resolve()
    workspace=Path.cwd().resolve()
    assert temp.is_dir() and temp.name=='_temp','unexpected runner temporary directory'
    assert workspace.is_relative_to(temp.parent) and not workspace.is_relative_to(temp),'workspace outside runner work root'

def main():
    require_hosted()  # All local/service-host execution stops before any command.
    floor=ClickHouseConfig.from_env().min_free_gb*1024**3
    target=floor+8*1024**3  # Image pulls and transient fixture volumes also need room.
    docker_root=Path(run(['docker','info','--format','{{.DockerRootDir}}']).strip())
    assert docker_root.is_absolute() and docker_root.is_dir()
    free=lambda:shutil.disk_usage(docker_root).free
    protected=[Path(shutil.which(name)).resolve() for name in ['python3','node','git','docker','openssl']]
    proof={'scope':'disposable GitHub-hosted runner only','diskSafetyFloorBytes':floor,'targetFreeBytes':target,'totalBytes':shutil.disk_usage(docker_root).total,'beforeFreeBytes':free(),'inventory':[],'removed':[]}
    try:
        print(json.dumps({'event':'hosted_disk_preflight',**proof}),flush=True)
        assert proof['totalBytes']>=target,'physical runner disk cannot meet the safety floor; no SDK removal attempted'
        for name in SDK_ROOTS:
            p=Path(name)
            if not p.exists():continue
            assert p.is_absolute() and p.is_dir() and not p.is_symlink() and str(p.resolve())==name
            assert not any(binary.is_relative_to(p) for binary in protected),'required runtime in SDK root'
            print(json.dumps({'event':'inventory_sdk','path':name}),flush=True)
            size=int(run(['sudo','du','-s','-B1','--',name],60).split()[0])
            proof['inventory'].append({'path':name,'bytes':size})
        print(json.dumps({'event':'hosted_sdk_inventory',**proof}),flush=True)
        for item in proof['inventory']:
            if free()>=target:break
            # Explicit allowlist above, inventoried first; nothing here is user data.
            run(['sudo','rm','-rf','--',item['path']],120)
            proof['removed'].append(item)
            print(json.dumps({'event':'hosted_sdk_removed',**item,'freeBytes':free()}),flush=True)
        proof['afterFreeBytes']=free()
        assert proof['afterFreeBytes']>=target,'insufficient disk after scoped cleanup; do not bypass the safety fuse'
        proof['passed']=True
        print(json.dumps({'event':'hosted_disk_ready','freeBytes':proof['afterFreeBytes'],'removedRoots':len(proof['removed'])}),flush=True)
    except BaseException as exc:
        proof['failure']=str(exc);raise
    finally:write_json(Path('runner-disk33.json'),proof)

if __name__=='__main__':main()
