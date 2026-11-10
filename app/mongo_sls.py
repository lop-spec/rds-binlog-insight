"""Bounded SLS GET adapter; targets come only from the instance configuration."""
from __future__ import annotations

import base64
from collections import Counter
from email.utils import formatdate
import hashlib
import hmac
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request

from .rds_api import shared_ssl_context

MAX_BODY = 4 * 1024 * 1024


class NoSlsRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise RuntimeError('sls_redirect_rejected')


def validate_sources(sources):
    if not isinstance(sources, list) or len(sources) > 4:
        raise ValueError('invalid_mongo_log_sources')
    for s in sources:
        if not isinstance(s, dict) or not re.fullmatch(r'[a-z][a-z0-9_-]{0,31}', str(s.get('id', ''))):
            raise ValueError('invalid_mongo_log_source_id')
        for key, pattern in [('project', r'[a-z][a-z0-9-]{2,62}'), ('logstore', r'[A-Za-z0-9_-]{2,100}')]:
            if not re.fullmatch(pattern, str(s.get(key, ''))):
                raise ValueError('invalid_mongo_log_target')
        if s.get('kind','errors') not in ('errors','http'):
            raise ValueError('invalid_mongo_log_kind')
        if s.get('kind') == 'http':
            token = s.get('pathToken', '')
            if not isinstance(token,str) or not re.fullmatch(r'[A-Za-z][A-Za-z0-9_-]{2,80}',token):
                raise ValueError('mongo_http_path_token_required')
            limit = s.get('maxConcurrent')
            if limit is not None and (not isinstance(limit,int) or isinstance(limit,bool) or not 1 <= limit <= 1000):
                raise ValueError('invalid_expected_concurrency')
        else:
            tokens = s.get('endpointTokens')
            if not isinstance(tokens, list) or not 1 <= len(tokens) <= 6 or any(
                    not isinstance(t, str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,253}', t) for t in tokens):
                raise ValueError('mongo_log_endpoint_tokens_required')
    if len({s['id'] for s in sources}) != len(sources):
        raise ValueError('duplicate_mongo_log_source_id')
    return sources


class SlsReadonly:
    def __init__(self, region, credential):
        if not re.fullmatch(r'[a-z]+-[a-z]+(?:-\d+)?', region):
            raise ValueError('invalid_sls_region')
        self.region, self.credential = region, credential

    @staticmethod
    def query(source):
        return ('"' + source['pathToken'] + '"' if source.get('kind') == 'http' else
                '(' + ' or '.join('"' + t + '"' for t in source['endpointTokens']) + ') and ("timeout" or "deadline")')

    def request(self, source, start_us, end_us, offset, *, analysis=''):
        validate_sources([source])
        k = self.credential.current()
        # analysis is an internal, constant SQL suffix, never a caller query.
        query = self.query(source) + analysis
        params = dict(type='log', **{'from': str(start_us // 1_000_000), 'to': str((end_us + 999999) // 1_000_000)},
                      query=query, line='100', offset=str(offset), reverse='false')
        path = '/logstores/' + source['logstore']
        resource = path + '?' + '&'.join(k + '=' + str(v) for k, v in sorted(params.items()))
        date = formatdate(usegmt=True)
        headers = {'x-log-apiversion': '0.6.0', 'x-log-signaturemethod': 'hmac-sha1', 'x-log-bodyrawsize': '0'}
        if k.security_token:
            headers['x-acs-security-token'] = k.security_token
        canonical = ''.join(key + ':' + value + '\n' for key, value in sorted(headers.items()))
        signature = base64.b64encode(hmac.new(k.access_key_secret.encode(),
            ('GET\n\n\n' + date + '\n' + canonical + resource).encode(), hashlib.sha1).digest()).decode()
        headers.update(Date=date, Authorization='LOG ' + k.access_key_id + ':' + signature)
        url = ('https://' + source['project'] + '.' + self.region + '.log.aliyuncs.com' + path + '?' +
               urllib.parse.urlencode(params))
        return urllib.request.Request(url, headers=headers, method='GET')

    def page(self, source, start_us, end_us, offset, *, analysis=''):
        request = self.request(source, start_us, end_us, offset, analysis=analysis)
        opener = urllib.request.build_opener(NoSlsRedirect(), urllib.request.HTTPSHandler(context=shared_ssl_context()))
        with opener.open(request, timeout=8) as response:
            body = response.read(MAX_BODY + 1)
            if len(body) > MAX_BODY:
                raise RuntimeError('sls_response_byte_limit')
            rows = json.loads(body)
            if not isinstance(rows, list) or len(rows) > (400 if analysis else 100):
                raise RuntimeError('invalid_sls_log_response')
            return rows, response.headers.get('x-log-progress', '') == 'Complete'

    def collect(self, source, start_us, end_us, *, max_pages=10, deadline=None):
        aggregate = None
        if source.get('kind','errors') == 'errors' and (deadline is None or time.monotonic() < deadline-16):
            try:
                totals, totals_complete = self.page(source,start_us,end_us,0,analysis=
                    ' | select count(*) as n, count(distinct __source__) as nodes limit 1')
                buckets, buckets_complete = self.page(source,start_us,end_us,0,analysis=
                    ' | select __time__ - __time__ % 5 as bucket, count(*) as n, count(distinct __source__) as nodes group by bucket order by bucket limit 400')
                if totals_complete and buckets_complete and len(totals)==1:
                    count = int(totals[0]['n'])
                    histogram = [dict(timestamp=int(b['bucket'])*1000,category='all_matching_timeouts',count=int(b['n']),node_count=int(b['nodes'])) for b in buckets]
                    if sum(b['count'] for b in histogram)==count:
                        aggregate = dict(rows=count,node_count=int(totals[0]['nodes']),buckets=histogram)
            except (ValueError,KeyError,TypeError,RuntimeError,urllib.error.HTTPError):
                # The raw sampling lane remains usable without SQL analysis.
                aggregate = None
        rows, complete = [], False
        for page in range(max_pages):
            if deadline is not None and time.monotonic() >= deadline-8:
                break
            batch, query_complete = self.page(source, start_us, end_us, page * 100)
            rows.extend(batch)
            if not query_complete:
                break
            if len(batch) < 100:
                complete = True; break
        result = (summarize_http(rows, complete, source, start_us=start_us, end_us=end_us) if source.get('kind') == 'http' else
                  summarize_logs(rows, complete, source['id']))
        result.update(sample_complete=result['complete'],sample_rows=len(rows),aggregation_complete=aggregate is not None)
        if aggregate is not None:
            result.update(aggregate)
            result['warning'] += ' 总量/五秒分布来自完整系统字段聚合，原始明细仅有界抽样，两种完整性分别标注。'
        return result


def summarize_http(rows, complete, source, *, start_us=None, end_us=None):
    requests, rejected, unknown = [], 0, 0
    for row in rows:
        text = str(row.get('content') or '')
        m = re.search(r'\|\s*(\d{3})\s*\|\s*([^|]+)\|[^|]+\|\s*GET\s+"([^"\s]+)"', text)
        if not m:
            unknown += 1; continue
        try:
            end = int(row['__time__']) * 1000
            durations = re.findall(r'([\d.]+)(ns|µs|us|ms|s|m|h)',m[2])
            if not durations: raise ValueError('missing_duration')
            duration = sum(float(n)*{'ns':1e-6,'µs':.001,'us':.001,'ms':1,'s':1000,'m':60000,'h':3600000}[u]
                           for n,u in durations)
            url = urllib.parse.urlsplit(m[3]); path = url.path
            if source['pathToken'] not in path.split('/'):
                unknown += 1; continue
            shop = path.rsplit('/',1)[-1]
            if not shop.isdigit(): shop = 'unknown'
            query = urllib.parse.parse_qs(url.query)
            page = query.get('page',[''])[0]
            if not page.isdigit(): page = None
            status = int(m[1])
            if status == 429:
                rejected += 1; continue
            # Exclude one second at each end: SLS timestamps may be whole seconds.
            requests.append(dict(shop=shop, page=page,
                                 start_ms=max(end-duration+1000,start_us/1000) if start_us is not None else end-duration+1000,
                                 end_ms=min(end-1000,end_us/1000) if end_us is not None else end-1000,
                                 duration_ms=duration, status=status))
        except (ValueError, TypeError, KeyError, OverflowError):
            unknown += 1
    tenants = []
    for shop in sorted({r['shop'] for r in requests}):
        selected = [r for r in requests if r['shop'] == shop]
        events = [(r['start_ms'],1) for r in selected if r['end_ms']>r['start_ms']]
        events += [(r['end_ms'],-1) for r in selected if r['end_ms']>r['start_ms']]
        current = peak = 0
        for _, delta in sorted(events):
            current += delta; peak = max(peak,current)
        limit = source.get('maxConcurrent')
        tenants.append(dict(shop=shop, requests=len(selected), observed_overlap_lower_bound=peak,
                            expected_concurrency=limit, exceeded=peak>limit if limit is not None else None,
                            max_duration_ms=max(r['duration_ms'] for r in selected)))
    return dict(source=source['id'],kind='http',complete=complete and not unknown,rows=len(rows),
                rejected=rejected,unparsed=unknown,tenants=tenants[:50],samples=requests[:20],
                warning='并发仅由本日志范围内已结束请求的保守重叠区间计算；缺未结束请求及其他日志源，不等于全局峰值。超限不直接证明租约根因。')


def summarize_logs(rows, complete, source_id):
    counts = Counter(); nodes = set(); pods = Counter(); events = []
    for r in rows:
        try:
            t = int(r['__time__']) * 1000
        except (ValueError, KeyError, TypeError):
            complete = False; continue
        node = str(r.get('__tag__:_node_ip_') or r.get('__source__') or '')
        pod = str(r.get('_pod_name_') or r.get('__tag__:_pod_name_') or '')
        raw = str(r.get('content') or '')
        # Match only known diagnostics. Never return arbitrary log text or IDs.
        category = ('response_header_timeout' if 'incomplete read of message header' in raw else
                    'pool_wait' if re.search(r'connection pool|checkout', raw, re.I) else
                    'network_failure' if re.search(r'connection refused|connection reset|no route to host', raw, re.I) else
                    'deadline_or_timeout')
        counts[(t // 5000 * 5000, category)] += 1
        if node: nodes.add(node)
        if pod: pods[pod] += 1
        if len(events) < 20:
            events.append(dict(timestamp=t, node=node[:128], pod=pod[:253], category=category))
    return dict(source=source_id, complete=complete, rows=len(rows), node_count=len(nodes),
                count_scope='matched_log_rows_not_unique_requests',
                buckets=[dict(timestamp=t, category=c, count=n) for (t, c), n in sorted(counts.items())],
                pods=[dict(pod=p, count=n) for p, n in pods.most_common(20)], samples=events,
                warning='仅统计配置日志源及endpointTokens命中的记录；达到页数/字节/时间上限为部分结果，零命中不能排除网络故障。')
