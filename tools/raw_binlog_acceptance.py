"""Read-only capacity gate. Data recovery is a separate, explicitly open gate."""
import json
import sys
import urllib.request


def check(status):
    raw=status['data']['sync']['rawBinlog']
    assert raw['enabled'], 'raw archive mode is not active'
    assert raw['sample_seconds']>=900, 'need at least 15 minutes of wall-clock samples'
    assert raw['sample_files']>=100, 'need at least 100 completed files'
    assert raw['max_query_files']==16 and raw['max_query_bytes']==8*1024**3
    assert raw['within_24h'], 'available backlog + ongoing source production exceeds one day'
    all_known=raw['estimated_including_known_unavailable_seconds']
    assert all_known is not None and all_known<=86400, 'capacity including known source gaps exceeds one day'
    return {k:raw[k] for k in ('sample_seconds','sample_files','files_per_hour','bytes_per_second',
        'source_bytes_per_second','pending_files','pending_bytes','unavailable_files','unavailable_bytes',
        'estimated_catchup_seconds','estimated_including_known_unavailable_seconds','source_gaps_verified')}


if __name__=='__main__':
    with urllib.request.urlopen(sys.argv[1],timeout=15) as response:
        status=json.load(response)
    print(json.dumps(check(status),ensure_ascii=False,indent=2))
