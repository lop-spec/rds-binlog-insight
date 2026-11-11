"""Small lexical scanner; not an authorization parser and never evaluates SQL."""
from __future__ import annotations
import re

_WORD = re.compile(r'[A-Za-z_$][A-Za-z0-9_$]*|(?:\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)')
_SAFE_CALLS = frozenset('COUNT SUM AVG MIN MAX ABS ROUND FLOOR CEIL CEILING COALESCE IFNULL NULLIF IF CAST CONVERT CONCAT CONCAT_WS LOWER UPPER LENGTH CHAR_LENGTH SUBSTRING SUBSTR LEFT RIGHT TRIM DATE DATE_FORMAT DATE_ADD DATE_SUB DATEDIFF TIMESTAMPDIFF NOW CURRENT_TIMESTAMP YEAR MONTH DAY HOUR MINUTE SECOND UNIX_TIMESTAMP FROM_UNIXTIME JSON_EXTRACT JSON_UNQUOTE JSON_LENGTH JSON_CONTAINS JSON_OBJECT JSON_ARRAY GREATEST LEAST ROW_NUMBER RANK DENSE_RANK'.split())
_STRUCTURAL = frozenset('SELECT IN EXISTS AS WITH OVER PARTITION WHEN THEN ELSE DISTINCT AND OR NOT VALUES'.split())


def tokens(sql):
    text=str(sql or '');i=0;out=[]
    while i<len(text):
        ch=text[i]
        if ch.isspace():i+=1;continue
        if text.startswith('/*',i):
            end=text.find('*/',i+2)
            if end<0:raise ValueError('unterminated_sql_comment')
            if text.startswith('/*!',i):raise ValueError('executable_sql_comment_rejected')
            i=end+2;continue
        if ch=='#' or text.startswith('--',i) and (i+2==len(text) or text[i+2].isspace()):
            end=text.find('\n',i);i=len(text) if end<0 else end+1;continue
        if ch in "'\"`":
            quote=ch;start=i;i+=1
            while i<len(text):
                if text[i]=='\\' and quote!='`':i+=2;continue
                if text[i]==quote:
                    if i+1<len(text) and text[i+1]==quote:i+=2;continue
                    i+=1;break
                i+=1
            else:raise ValueError('unterminated_sql_literal')
            out.append(('identifier' if quote=='`' else 'literal',text[start:i]));continue
        match=_WORD.match(text,i)
        if match:
            value=match[0];out.append(('word',value));i+=len(value)
        else:out.append(('symbol',ch));i+=1
    return out


def statement_verb(sql):
    parts=tokens(sql);depth=0;first=''
    for kind,value in parts:
        if value=='(':depth+=1;continue
        if value==')':depth=max(depth-1,0);continue
        if kind!='word':continue
        word=value.upper()
        if not first:first=word
        if first=='WITH':
            if depth==0 and word in {'SELECT','INSERT','UPDATE','DELETE','REPLACE'}:return word
        elif first:return first
    return first


def explain_safe(sql):
    parts=tokens(sql)
    if statement_verb(sql)!='SELECT':return False
    body=parts[:-1] if parts and parts[-1][1]==';' else parts
    if any(value==';' for kind,value in body if kind=='symbol'):return False
    words=[value.upper() for kind,value in body if kind=='word']
    forbidden={'SLEEP','BENCHMARK','GET_LOCK','RELEASE_LOCK','LOAD_FILE','OUTFILE','DUMPFILE','ANALYZE'}
    if set(words)&forbidden:return False
    for i,(kind,value) in enumerate(body[:-1]):
        if body[i+1][1] == '(' and kind in {'word','identifier','literal'}:
            if kind != 'word' or value.upper() not in _SAFE_CALLS | _STRUCTURAL:
                return False
            if i and body[i-1][1] == '.':
                return False
    if any(value=='@' for kind,value in body if kind=='symbol'):return False
    return not any(words[i:i+2] in [['FOR','UPDATE'],['LOCK','IN']] for i in range(len(words)))
