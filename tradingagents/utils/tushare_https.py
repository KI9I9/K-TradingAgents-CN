"""HTTPS-only transport for the pinned Tushare 1.4.29 DataApi interface.

Keep the installed SDK's authority, path and payload (including ts_type_name).
Only transport changes: verified TLS, no redirects, retries or ambient proxy/
netrc credentials. No token persistence or process-global monkeypatches.
"""
from functools import partial
import re
import unicodedata

import pandas as pd
from tradingagents.utils.tushare_egress import MainlandTransport, TushareEgressStopped


class TushareTransportError(RuntimeError):
    """Fixed diagnostics only: never include server messages or request objects."""


_REJECTION_RULES = (
    ('usage_limit', re.compile(
        r'(?:次数|频次).{0,40}(?:上限|限制|超限|超出|用完|耗尽)|'
        r'(?:上限|限制|超出|超过).{0,40}(?:次数|频次)|'
        r'每.{0,12}(?:分钟|小时|天|日|秒).{0,30}(?:最多|只能|仅能|上限)|'
        r'(?:频率|频次).{0,20}(?:超|限制)|请求过于频繁|'
        r'too many requests|rate[ _-]?limit|quota.{0,20}(?:exceed|exhaust)|'
        r'(?:request|call).{0,20}limit.{0,20}(?:reach|exceed)', re.I)),
    ('insufficient_points', re.compile(
        r'积分.{0,20}(?:不足|不够|低于|至少|大于|达到)|'
        r'(?:至少|需要|不足|低于).{0,12}\d*.{0,4}积分|insufficient points', re.I)),
    ('permission_denied', re.compile(
        r'没有.{0,30}权限|无权访问|无.{0,15}访问权限|权限不足|未开通|未授权|'
        r'permission denied|not authorized|not permitted|unauthorized|access denied', re.I)),
    ('token_invalid', re.compile(
        r'(?:token|令牌).{0,20}(?:无效|错误|过期|失效|不正确|不存在)|'
        r'(?:无效|错误|过期).{0,15}(?:token|令牌)|'
        r'invalid.{0,10}token|token.{0,10}(?:invalid|expired|incorrect)', re.I)),
    ('ip_restriction', re.compile(
        r'(?:\bip\b|ip地址).{0,25}(?:限制|封禁|白名单|不允许|blocked|restricted|whitelist)|'
        r'(?:限制|封禁|白名单).{0,25}(?:\bip\b|ip地址)', re.I)),
    ('invalid_parameters', re.compile(
        r'参数.{0,20}(?:错误|无效|缺失|不合法)|invalid.{0,15}param|missing.{0,15}param', re.I)),
    ('service_unavailable', re.compile(
        r'服务暂不可用|服务异常|系统异常|内部错误|维护中|'
        r'service unavailable|internal server error|under maintenance', re.I)),
)
_USAGE_WINDOWS = (
    ('second', re.compile(r'每(?:\d{1,4}|一)?秒|per second', re.I)),
    ('minute', re.compile(r'每(?:\d{1,4}|一)?分钟|per minute', re.I)),
    ('hour', re.compile(r'每(?:\d{1,4}|一)?小时|per hour', re.I)),
    ('day', re.compile(r'每(?:\d{1,4}|一)?(?:天|日)|per day', re.I)),
)


def _rejection_diagnostic(result, token):
    """Return bounded code + allowlisted labels, never arbitrary upstream text.

    The code is informational only, not an undocumented code-to-cause mapping.
    Messages are used in memory for classification, never attached to the error.
    A window unit is not an account quota or a suggested retry interval.
    """
    code = result.get('code')
    code_label = str(code) if type(code) is int and -1000000 <= code <= 1000000 else 'unknown'
    parts = []
    normalized_token = unicodedata.normalize('NFKC', token).casefold()
    for field in ('msg', 'message', 'error', 'detail', 'reason'):
        value = result.get(field)
        if not isinstance(value, str) or len(value) > 4096:
            continue
        # Do not let an echoed Token itself determine a diagnostic category.
        value = value.replace(token, ' ')
        value = unicodedata.normalize('NFKC', value).casefold()
        if normalized_token:
            value = value.replace(normalized_token, ' ')
        parts.append(value)
    message = '\n'.join(parts)
    reasons = [name for name, rule in _REJECTION_RULES if rule.search(message)]
    windows = [name for name, rule in _USAGE_WINDOWS if rule.search(message)] if 'usage_limit' in reasons else []
    # Preserve uncertainty if more than one time unit appears in the message.
    window = windows[0] if len(windows) == 1 else 'unknown'
    return ('tushare_api_rejected code=' + code_label
            + ' reason=' + (','.join(reasons) if reasons else 'unclassified')
            + ' window=' + window)


class HttpsTushareClient:
    def __init__(self, token, timeout=30):
        if not isinstance(token, str) or not token.strip():
            raise TushareEgressStopped('token_invalid')
        if not isinstance(timeout, (int, float)) or not 0 < timeout <= 300:
            raise TushareEgressStopped('invalid_request')
        self.__token = token
        self.__timeout = timeout

    def query(self, api_name, fields='', **kwargs):
        # Candidate is closed unless BOTH the route and a finite permit are enabled.
        # No direct/HTTP/alternate-Token fallback exists in this query path.
        transport = MainlandTransport(self.__token, _rejection_diagnostic, self.__timeout)
        table = transport.query(api_name, fields=fields, **kwargs)
        return pd.DataFrame(table['items'], columns=table['fields'])

    def __getattr__(self, name):
        if name.startswith('_'):
            raise AttributeError(name)
        return partial(self.query, name)


def create_tushare_client(token, timeout=30):
    """Explicit token only; never use upass/tk.csv or create a global SDK client."""
    return HttpsTushareClient(token, timeout=timeout)
