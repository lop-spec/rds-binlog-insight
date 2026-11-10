"""Conservative credential masking at the audit trust boundary; not general PII removal."""
from __future__ import annotations

import re

_LITERAL = r"(?:'(?:''|\\.|[^'\\])*'|\"(?:\"\"|\\.|[^\"\\])*\")"
_PATTERNS = [
    re.compile(r'(\bIDENTIFIED\s+(?:WITH\s+[\w`]+\s+)?(?:BY|AS)\s+)' + _LITERAL, re.I | re.S),
    re.compile(r'(\bPASSWORD\s*\(\s*)' + _LITERAL, re.I | re.S),
    re.compile(r'(\b(?:PASSWORD|ACCESS_KEY_SECRET|ACCESS_TOKEN|API_KEY|CLIENT_SECRET|SECRET_ACCESS_KEY|TOKEN|PWD)\b[\s`"\']*[:=]\s*)' + _LITERAL, re.I | re.S),
    re.compile(r'(\b(?:mysql|mongodb(?:\+srv)?|postgres(?:ql)?|redis)://[^\s:@/]+:)[^\s@]+(?=@)', re.I),
    re.compile(r'(\bAuthorization\s*[:=]\s*["\']?Bearer\s+)[A-Za-z0-9._~+/=-]+', re.I),
    re.compile(r'(-p)' + _LITERAL, re.I | re.S),
    re.compile(r'(\bREQUIRE\s+(?:CIPHER|ISSUER|SUBJECT)\s+)' + _LITERAL, re.I | re.S),
]
_CREDENTIAL_INSERT = re.compile(r'\b(?:INSERT|REPLACE)\s+(?:IGNORE\s+)?INTO\s+[^;]+\([^)]*\b(?:password|pwd|secret|access_token|api_key)\b[^)]*\)\s*VALUES\b', re.I | re.S)


def redact_secrets(text: str) -> tuple[str, bool]:
    original = str(text or '')
    result = original
    if _CREDENTIAL_INSERT.search(result):
        result = re.sub(r'(\bVALUES\b).*', r'\1 /* credential values redacted */', result, flags=re.I | re.S)
    for pattern in _PATTERNS:
        result = pattern.sub(lambda match: match.group(1) + "'<redacted>'", result)
    result = re.sub(r'\b(?:LTAI[A-Za-z0-9]{12,}|eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+)\b', '<redacted>', result)
    return result, result != original
