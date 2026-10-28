"""One public HTTP attempt in a foreground child with kill+wait deadline.

No retries here. tpex_root_core.fetch owns the only retry budget.
Not called against any endpoint during local verification.
"""
import base64
import http.client
import json
from pathlib import Path
import subprocess
import sys
import urllib.error
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parent))
from tpex_root_core import Response, TransientError, PermanentError, BudgetExceeded

ALLOWED = frozenset({
    'https://www.tpex.org.tw/web/bulletin/disposal_information/disposal_information_result.php?l=zh-tw&o=data',
    'https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O',
    'https://www.tpex.org.tw/openapi/v1/tpex_3insti_daily_trading',
    'https://www.tpex.org.tw/openapi/v1/tpex_mainboard_margin_balance',
    'https://openapi-t.tdcc.com.tw/v1/opendata/1-5',
})
MAX_ATTEMPT_BYTES = 32*1024*1024


def read_once(url, timeout_seconds, maximum_bytes):
    if url not in ALLOWED:
        raise PermanentError('URL outside original five-source set')
    if not 0 < timeout_seconds <= 60:
        raise ValueError('invalid time bound')
    if type(maximum_bytes) is not int or not 1 <= maximum_bytes <= MAX_ATTEMPT_BYTES:
        raise ValueError('invalid body limit')
    request = json.dumps({'url': url, 'timeout': timeout_seconds, 'maximum_bytes': maximum_bytes})
    try:
        result = subprocess.run(
            [sys.executable, '-I', '-B', '-X', 'utf8', str(Path(__file__).resolve()), '--one-attempt'],
            input=request.encode(), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            timeout=timeout_seconds, check=False,
        )
    except subprocess.TimeoutExpired as error:
        # subprocess.run kills and waits for its owned child before raising.
        # Bytes consumed before timeout are unknown: conservatively charge cap.
        raise TransientError('hard child timeout', partial_bytes=maximum_bytes) from error
    if len(result.stdout) > maximum_bytes*2+8192 or len(result.stderr) > 8192:
        raise PermanentError('bounded child protocol exceeded')
    try:
        value = json.loads(result.stdout)
        if value.get('error') == 'limit':
            raise BudgetExceeded('single attempt body limit')
        if value.get('error') == 'transient':
            amount = value.get('bytes_received')
            if type(amount) is not int or not 0 <= amount <= maximum_bytes:
                amount = maximum_bytes
            raise TransientError('bounded child source failure', partial_bytes=amount)
        if value.get('error') or result.returncode != 0:
            raise PermanentError('bounded child protocol/permanent failure')
        body = base64.b64decode(value['body_b64'], validate=True)
        if len(body) > maximum_bytes:
            raise BudgetExceeded('single attempt body limit')
        return Response(value['status'], body, value['headers'], value['final_url'])
    except (ValueError, KeyError, TypeError) as error:
        raise PermanentError('invalid bounded child receipt') from error


def one_attempt(spec):
    url = spec['url']; timeout = spec['timeout']; cap = spec['maximum_bytes']
    if url not in ALLOWED or not 0 < timeout <= 60 or type(cap) is not int or not 1 <= cap <= MAX_ATTEMPT_BYTES:
        return {'error': 'permanent'}
    request = urllib.request.Request(url, headers={
        'User-Agent': 'tpex-data-relay/1.4', 'Accept': '*/*',
        'Referer': ('https://www.tpex.org.tw/zh-tw/announce/market/disposal.html'
                    if 'disposal_information_result.php' in url
                    else 'https://www.tpex.org.tw/openapi/'),
        'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.5', 'Accept-Encoding': 'identity',
    })
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            headers = {key.lower(): value for key, value in response.headers.items()}
            lengths = response.headers.get_all('Content-Length', [])
            if len(lengths) > 1:
                headers['content-length'] = ','.join(lengths)
            length = headers.get('content-length')
            if isinstance(length, str) and length.isdecimal() and int(length) > cap:
                return {'error': 'limit'}
            raw = response.read(cap)
            # At a headerless/ambiguous exact cap, fail closed without reading
            # another byte beyond the admitted body limit just to test EOF.
            if len(raw) == cap and length != str(cap):
                return {'error': 'limit'}
            return {'status': response.getcode(), 'body_b64': base64.b64encode(raw).decode('ascii'),
                    'headers': headers, 'final_url': response.geturl()}
    except urllib.error.HTTPError as error:
        with error:
            return {'status': error.code, 'body_b64': '',
                    'headers': {key.lower(): value for key, value in error.headers.items()},
                    'final_url': error.geturl()}
    except http.client.IncompleteRead as error:
        return {'error': 'transient', 'bytes_received': min(cap, len(error.partial))}
    except (OSError, http.client.HTTPException, urllib.error.URLError):
        return {'error': 'transient', 'bytes_received': cap}


if __name__ == '__main__':
    # This branch is exclusively the parent's single-attempt worker protocol.
    # It is not exercised with a real URL by the offline test suite.
    if sys.argv[1:] != ['--one-attempt']:
        raise SystemExit('No downloader entry point; original source binding is required.')
    try:
        raw_spec = sys.stdin.buffer.read(4097)
        if len(raw_spec) > 4096:
            raise ValueError('oversized protocol input')
        result = one_attempt(json.loads(raw_spec))
    except Exception:
        result = {'error': 'permanent'}
    sys.stdout.write(json.dumps(result, separators=(',', ':')))
