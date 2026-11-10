"""Public engine/capability catalogue, without network access or credential paths."""


def instance_registry(known, starrocks_entries):
    rows = {identity: {'id': identity, 'label': identity,
                      'engine': 'mongodb' if identity.startswith(('dds-', 'mongodb')) else 'mysql',
                      'readMode': 'retained_events'} for identity in known}
    for entry in starrocks_entries:
        if not entry.get('enabled'):
            continue
        identity = entry['instanceId']
        rows[identity] = {'id': identity, 'label': entry.get('label') or identity,
                          'engine': 'starrocks', 'readMode': 'source_readonly',
                          'capabilities': ['audit', 'slow', 'workload', 'loads', 'metrics'],
                          'runningQueries': 'permission_required', 'rowChanges': False}
    return [rows[key] for key in sorted(rows)]
