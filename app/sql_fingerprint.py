"""SQL 归一化与指纹。

Binlog 只在两种情况下携带真实 SQL 文本：QueryEvent（DDL、非行模式语句、
事务边界）和开启 `binlog_rows_query_log_events` 后的 RowsQueryEvent。行事件
本身只有行镜像，没有语句文本。因此指纹分三档，且必须在结果里如实标注来源：

- `original`：QueryEvent 原文归一化。
- `rows-query`：RowsQueryEvent 原文归一化（对应一批行事件）。
- `synthetic`：既无原文也无 RowsQuery 时，按 `操作 + 库.表` 合成模板。它只能
  回答「哪张表被怎么改了多少次」，不能冒充真实语句文本。

归一化目标是让同一条语句的不同参数落到同一个指纹，同时不引入跨语句碰撞。
"""

from __future__ import annotations

import hashlib
import re

# 2：事务边界按语句动作判定（MySQL 的 BEGIN 是普通 QueryEvent）、行事件的重建
# SQL 不再标成 original、列序号 `@1` 不再被参数化。指纹口径变化必须升版本，否则
# 旧口径的聚合会继续留在索引里被查询到。
FINGERPRINT_FORMAT_VERSION = 2

# 归一化后的最大保留长度。超长语句（大 IN 列表、生成式 SQL）截断后仍保留
# 足够前缀区分语句形态，避免把整段正文塞进索引。
MAX_NORMALIZED_CHARS = 4096
MAX_SAMPLE_CHARS = 2048

_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.S)
_LINE_COMMENT = re.compile(r"(?:--[ \t\r\n][^\n]*|--$|#[^\n]*)", re.M)
# 先字符串后数字：字符串内的数字不能被单独替换。
_STRING = re.compile(r"'(?:[^'\\]|\\.|'')*'|\"(?:[^\"\\]|\\.|\"\")*\"")
_HEX = re.compile(r"\b[xX]'[0-9a-fA-F]*'|\b0[xX][0-9a-fA-F]+\b")
_BIT = re.compile(r"\b[bB]'[01]*'")
# `@1`、`@2` 是列名元数据缺失时的列序号，不是字面量。参数化它们会把不同列的
# 更新错误地归并成同一指纹，因此 `@` 也要排除在数字归一化之外。
_NUMBER = re.compile(r"(?<![A-Za-z0-9_$.@])[+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?\b")
_PLACEHOLDER_LIST = re.compile(r"\?(?:\s*,\s*\?)+")
_VALUES_GROUPS = re.compile(
    r"\bVALUES\b\s*\(\s*\?\s*\)(?:\s*,\s*\(\s*\?\s*\))+",
    re.I,
)
_WHITESPACE = re.compile(r"\s+")
_SPACE_BEFORE = re.compile(r"\s+([,;)])")
_SPACE_AFTER = re.compile(r"([(])\s+")
# 比较/赋值操作符两侧的空格是纯书写风格差异。不归一的话，`a=?` 与 `a = ?`
# 会分裂成两个指纹，同一条语句的统计被拆散。长操作符必须排在前面。
_OPERATORS = re.compile(r"\s*(<=>|<>|!=|<=|>=|=|<|>)\s*")

_ACTION_RE = re.compile(
    r"^\s*(?:/\*.*?\*/\s*)*"
    r"(?P<verb>[A-Za-z_]+)",
    re.S,
)

_DDL_VERBS = {
    "CREATE",
    "ALTER",
    "DROP",
    "RENAME",
    "TRUNCATE",
    "COMMENT",
}
_DML_VERBS = {
    "INSERT",
    "UPDATE",
    "DELETE",
    "REPLACE",
    "SELECT",
    "LOAD",
    "MERGE",
}
_TX_VERBS = {
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "START",
    "SAVEPOINT",
    "RELEASE",
    "XA",
}
_ADMIN_VERBS = {
    "GRANT",
    "REVOKE",
    "SET",
    "FLUSH",
    "ANALYZE",
    "OPTIMIZE",
    "REPAIR",
    "CHECK",
    "KILL",
    "LOCK",
    "UNLOCK",
    "CALL",
    "DO",
    "USE",
}


def normalize_sql(text: str) -> str:
    """把语句归一化成可比较的形态：去注释、参数化字面量、折叠列表、压空白。"""

    value = str(text or "")
    if not value.strip():
        return ""
    value = _BLOCK_COMMENT.sub(" ", value)
    value = _LINE_COMMENT.sub(" ", value)
    value = _STRING.sub("?", value)
    value = _HEX.sub("?", value)
    value = _BIT.sub("?", value)
    value = _NUMBER.sub("?", value)
    # 先折叠 VALUES 的多组占位，再折叠剩余的 (?, ?, ?) 列表，
    # 否则 VALUES (?),(?) 会先被压成 VALUES (?) 之外的形态。
    value = _PLACEHOLDER_LIST.sub("?", value)
    value = _VALUES_GROUPS.sub("VALUES (?)", value)
    value = _WHITESPACE.sub(" ", value).strip()
    value = _SPACE_BEFORE.sub(r"\1", value)
    value = _SPACE_AFTER.sub(r"\1", value)
    value = _OPERATORS.sub(r"\1", value)
    if len(value) > MAX_NORMALIZED_CHARS:
        value = value[:MAX_NORMALIZED_CHARS] + " …"
    return value


def sql_action(text: str) -> str:
    """取语句动作分类，用于按 DDL / DML / 事务 / 管理语句分组。"""

    match = _ACTION_RE.match(str(text or ""))
    if not match:
        return "OTHER"
    verb = match.group("verb").upper()
    if verb in _DDL_VERBS:
        return "DDL"
    if verb in _DML_VERBS:
        return verb
    if verb in _TX_VERBS:
        return "TRANSACTION"
    if verb in _ADMIN_VERBS:
        return "ADMIN"
    return "OTHER"


def synthetic_statement(operation: str, database: str, table: str) -> str:
    """行事件没有语句文本时的合成模板，只表达操作与目标对象。"""

    op = str(operation or "").strip().upper() or "UNKNOWN"
    target = ".".join(
        part for part in (str(database or "").strip(), str(table or "").strip()) if part
    )
    target = target or "(unknown)"
    if op == "INSERT":
        return f"INSERT INTO {target} (行事件)"
    if op == "UPDATE":
        return f"UPDATE {target} (行事件)"
    if op == "DELETE":
        return f"DELETE FROM {target} (行事件)"
    return f"{op} {target} (行事件)"


def fingerprint_of(normalized: str) -> str:
    """归一化文本 → 稳定 16 字节十六进制指纹。"""

    if not normalized:
        return ""
    digest = hashlib.sha256(normalized.encode("utf-8", "replace")).hexdigest()
    return digest[:32]


def statement_profile(
    *,
    sql_kind: str,
    sql_text: str,
    row_query: str,
    operation: str,
    database: str,
    table: str,
) -> dict[str, str]:
    """把一条事件解析成 (指纹, 归一化文本, 来源, 动作) 四元组。

    优先级：QueryEvent 原文 → RowsQueryEvent 原文 → 合成模板。来源必须随结果
    一起返回，界面据此区分「真实语句」和「按行事件合成」。
    """

    kind = str(sql_kind or "").strip().upper()
    original = str(sql_text or "").strip()
    rows_query = str(row_query or "").strip()

    if kind == "BOUNDARY":
        # BEGIN / COMMIT / XA 等事务边界。它们数量与事务数同量级，混进语句榜单
        # 会把业务 SQL 全部挤掉，因此单独归类，由事务分析负责。
        source = "boundary"
        raw = original or "(事务边界)"
    elif kind == "ORIGINAL" and original:
        source = "original"
        raw = original
    elif rows_query:
        source = "rows-query"
        raw = rows_query
    elif kind == "PSEUDO" and original:
        # 行事件的 sql_text 是解析器按行镜像生成的重建 SQL，只供阅读，不能
        # 冒充 QueryEvent 里的真实语句文本。
        source = "reconstructed"
        raw = original
    elif original:
        source = "original"
        raw = original
    else:
        source = "synthetic"
        raw = synthetic_statement(operation, database, table)

    if source == "synthetic":
        normalized = raw
        action = str(operation or "").strip().upper() or "OTHER"
    else:
        normalized = normalize_sql(raw)
        if not normalized:
            source = "synthetic"
            normalized = synthetic_statement(operation, database, table)
            action = str(operation or "").strip().upper() or "OTHER"
        else:
            action = sql_action(normalized)

    # MySQL 把 BEGIN 写成普通 QueryEvent（sql_kind='ORIGINAL'），所以光看
    # sql_kind 会漏掉绝大多数事务边界，让 BEGIN/COMMIT 按事务数量级霸占语句
    # 榜单。按语句动作兜底判定。
    if action == "TRANSACTION":
        source = "boundary"

    sample = raw if len(raw) <= MAX_SAMPLE_CHARS else raw[:MAX_SAMPLE_CHARS] + " …"
    return {
        "fingerprint": fingerprint_of(f"{source}\x00{normalized}"),
        "normalized": normalized,
        "source": source,
        "action": action,
        "sample": sample,
    }
