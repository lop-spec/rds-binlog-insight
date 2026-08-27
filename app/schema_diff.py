"""库表结构对比：从多个 RDS 实例只读拉取 information_schema，逐表比对结构差异并生成对齐 SQL。

设计约束：
- 全程只读。本模块只发 SELECT / SHOW CREATE TABLE，绝不执行生成出来的变更语句。
- 生成的 SQL 按风险分两组：safe(仅新增与放宽)与 risky(删除、收窄、字符集重建)，
  前端默认只展示 safe，risky 需要显式勾选，避免误粘贴执行。
- 列与索引定义由 information_schema 重建而非解析 SHOW CREATE TABLE 文本，
  同时保留两侧 SHOW CREATE TABLE 原文供人工核对。
"""

from __future__ import annotations

import logging
import os
import re
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

LOGGER = logging.getLogger(__name__)

_NAME_RE = re.compile(r"^[A-Za-z0-9_]{1,32}$")
_IDENT_RE = re.compile(r"^[A-Za-z0-9_$一-鿿]{1,64}$")
_TIME_DEFAULT_RE = re.compile(
    r"^(current_timestamp|now|localtime|localtimestamp)(\(\s*\d*\s*\))?$", re.IGNORECASE
)
_NUMERIC_TYPES = frozenset(
    {
        "tinyint", "smallint", "mediumint", "int", "integer", "bigint",
        "decimal", "numeric", "float", "double", "real", "bit", "year",
    }
)
_NO_DEFAULT_TYPES = frozenset(
    {"blob", "tinyblob", "mediumblob", "longblob", "text", "tinytext", "mediumtext", "longtext", "json", "geometry"}
)
# 同族类型按“容量”排序，用于判断 MODIFY 是扩容(safe)还是收窄(risky)。
_TYPE_FAMILY: dict[str, tuple[str, int]] = {
    "tinyint": ("int", 1), "smallint": ("int", 2), "mediumint": ("int", 3),
    "int": ("int", 4), "integer": ("int", 4), "bigint": ("int", 8),
    "tinytext": ("text", 1), "text": ("text", 2), "mediumtext": ("text", 3), "longtext": ("text", 4),
    "tinyblob": ("blob", 1), "blob": ("blob", 2), "mediumblob": ("blob", 3), "longblob": ("blob", 4),
    "float": ("float", 1), "double": ("float", 2), "real": ("float", 2),
    "char": ("char", 1), "varchar": ("char", 2),
    "binary": ("bin", 1), "varbinary": ("bin", 2),
    "datetime": ("dt", 1), "timestamp": ("dt", 1),
}


class SchemaDiffError(Exception):
    """结构对比过程中的可预期错误(配置缺失、连接失败、对象不存在)。"""


# --------------------------------------------------------------------------------------
# 标识符与字面量转义
# --------------------------------------------------------------------------------------


def quote_ident(name: str) -> str:
    return "`" + str(name).replace("`", "``") + "`"


def qualified(database: str, table: str) -> str:
    return f"{quote_ident(database)}.{quote_ident(table)}"


def quote_literal(value: str) -> str:
    escaped = (
        str(value)
        .replace("\\", "\\\\")
        .replace("'", "''")
        .replace("\x00", "\\0")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\x1a", "\\Z")
    )
    return f"'{escaped}'"


def _base_type(column_type: str) -> str:
    return re.split(r"[ (]", str(column_type).strip().lower(), maxsplit=1)[0]


def _type_length(column_type: str) -> int:
    match = re.search(r"\((\d+)", str(column_type))
    return int(match.group(1)) if match else -1


# --------------------------------------------------------------------------------------
# 实例配置
# --------------------------------------------------------------------------------------


@dataclass(slots=True, frozen=True)
class InstanceConfig:
    name: str
    label: str
    instance_id: str
    host: str
    port: int
    user: str
    password: str = field(repr=False, default="")
    connect_timeout: int = 10
    read_timeout: int = 120

    def public_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "label": self.label,
            "instanceId": self.instance_id,
            "host": self.host,
            "port": self.port,
            "user": self.user,
        }


def _instance_from_mapping(item: dict[str, Any], defaults: dict[str, Any]) -> InstanceConfig:
    name = str(item.get("name", "") or "").strip()
    if not _NAME_RE.fullmatch(name):
        raise SchemaDiffError(f"实例标识非法：{name or '(空)'}")
    host = str(item.get("host", "") or "").strip()
    user = str(item.get("user", "") or "").strip()
    if not host or not user:
        raise SchemaDiffError(f"实例 {name} 缺少 host 或 user")
    try:
        port = int(item.get("port", 3306) or 3306)
    except (TypeError, ValueError) as exc:
        raise SchemaDiffError(f"实例 {name} 的端口无效") from exc
    if not 1 <= port <= 65535:
        raise SchemaDiffError(f"实例 {name} 的端口超出范围")
    return InstanceConfig(
        name=name,
        label=str(item.get("label", "") or name).strip(),
        instance_id=str(item.get("instanceId", "") or item.get("instance_id", "") or "").strip(),
        host=host,
        port=port,
        user=user,
        password=str(item.get("password", "") or ""),
        connect_timeout=int(defaults.get("connect_timeout", 10)),
        read_timeout=int(defaults.get("read_timeout", 120)),
    )


def _load_instances_from_json(path_text: str, defaults: dict[str, Any]) -> list[InstanceConfig]:
    """从 JSON 文件读取实例清单。

    凭据里的 # $ ' " 空格等字符在 env_file 里会被误解析，因此优先走 JSON 文件。
    """
    import json
    from pathlib import Path as _Path

    path = _Path(path_text).expanduser()
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise SchemaDiffError(f"读取结构对比配置失败：{path} ({exc.strerror or exc})") from exc
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SchemaDiffError(f"结构对比配置不是合法 JSON：{path} 第 {exc.lineno} 行") from exc
    items = payload.get("instances") if isinstance(payload, dict) else payload
    if not isinstance(items, list):
        raise SchemaDiffError("结构对比配置缺少 instances 数组")
    instances: list[InstanceConfig] = []
    seen: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            raise SchemaDiffError("instances 数组元素必须是对象")
        config = _instance_from_mapping(item, defaults)
        if config.name in seen:
            raise SchemaDiffError(f"实例标识重复：{config.name}")
        seen.add(config.name)
        instances.append(config)
    return instances


def _read_config_json(path_text: str) -> dict[str, Any]:
    import json
    from pathlib import Path as _Path

    path = _Path(path_text).expanduser()
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise SchemaDiffError(f"读取结构对比配置失败：{path} ({exc.strerror or exc})") from exc
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SchemaDiffError(f"结构对比配置不是合法 JSON：{path} 第 {exc.lineno} 行") from exc
    return payload if isinstance(payload, dict) else {"instances": payload}


def load_default_compare(env: dict[str, str] | None = None) -> dict[str, str]:
    """页面打开时预选的基准/目标/范围。非法值一律丢弃成空，不阻断功能。"""
    source = os.environ if env is None else env
    empty = {
        "sourceInstance": "", "sourceDatabase": "",
        "targetInstance": "", "targetDatabase": "", "scope": "",
    }
    raw: dict[str, Any] = {}
    config_path = str(source.get("RDS_BINLOG_SCHEMA_CONFIG", "") or "").strip()
    if config_path:
        payload = _read_config_json(config_path)
        candidate = payload.get("defaultCompare")
        if isinstance(candidate, dict):
            raw = candidate
    else:
        raw = {
            "sourceInstance": source.get("RDS_BINLOG_SCHEMA_DEFAULT_SOURCE_INSTANCE", ""),
            "sourceDatabase": source.get("RDS_BINLOG_SCHEMA_DEFAULT_SOURCE_DATABASE", ""),
            "targetInstance": source.get("RDS_BINLOG_SCHEMA_DEFAULT_TARGET_INSTANCE", ""),
            "targetDatabase": source.get("RDS_BINLOG_SCHEMA_DEFAULT_TARGET_DATABASE", ""),
            "scope": source.get("RDS_BINLOG_SCHEMA_DEFAULT_SCOPE", ""),
        }
    result = dict(empty)
    for key in ("sourceInstance", "targetInstance"):
        value = str(raw.get(key, "") or "").strip()
        if value and _NAME_RE.fullmatch(value):
            result[key] = value
    for key in ("sourceDatabase", "targetDatabase"):
        value = str(raw.get(key, "") or "").strip()
        if value and _IDENT_RE.fullmatch(value):
            result[key] = value
    scope = str(raw.get("scope", "") or "").strip()
    if scope in {SCOPE_ALL, SCOPE_TARGET_EXISTING, SCOPE_INTERSECTION}:
        result["scope"] = scope
    return result


def load_instances(env: dict[str, str] | None = None) -> list[InstanceConfig]:
    """读取可对比的实例清单。

    优先级：RDS_BINLOG_SCHEMA_CONFIG 指向的 JSON 文件 > 逐项环境变量。
    环境变量形式：
    RDS_BINLOG_SCHEMA_INSTANCES=prod,test,hub
    RDS_BINLOG_SCHEMA_<NAME>_LABEL / _INSTANCE_ID / _HOST / _PORT / _USER / _PASSWORD
    """
    source = os.environ if env is None else env
    defaults = {
        "connect_timeout": int(str(source.get("RDS_BINLOG_SCHEMA_CONNECT_TIMEOUT", "10") or "10")),
        "read_timeout": int(str(source.get("RDS_BINLOG_SCHEMA_READ_TIMEOUT", "120") or "120")),
    }
    config_path = str(source.get("RDS_BINLOG_SCHEMA_CONFIG", "") or "").strip()
    if config_path:
        return _load_instances_from_json(config_path, defaults)
    declared = str(source.get("RDS_BINLOG_SCHEMA_INSTANCES", "") or "").strip()
    if not declared:
        return []
    instances: list[InstanceConfig] = []
    seen: set[str] = set()
    for raw_name in declared.split(","):
        name = raw_name.strip()
        if not name:
            continue
        if not _NAME_RE.fullmatch(name):
            raise SchemaDiffError(f"实例标识非法：{name}")
        if name in seen:
            raise SchemaDiffError(f"实例标识重复：{name}")
        seen.add(name)
        prefix = f"RDS_BINLOG_SCHEMA_{name.upper()}_"
        host = str(source.get(prefix + "HOST", "") or "").strip()
        user = str(source.get(prefix + "USER", "") or "").strip()
        password = str(source.get(prefix + "PASSWORD", "") or "")
        if not host or not user:
            raise SchemaDiffError(f"实例 {name} 缺少 {prefix}HOST 或 {prefix}USER")
        try:
            port = int(str(source.get(prefix + "PORT", "3306") or "3306"))
        except ValueError as exc:
            raise SchemaDiffError(f"实例 {name} 的端口无效") from exc
        if not 1 <= port <= 65535:
            raise SchemaDiffError(f"实例 {name} 的端口超出范围")
        instances.append(
            InstanceConfig(
                name=name,
                label=str(source.get(prefix + "LABEL", "") or name).strip(),
                instance_id=str(source.get(prefix + "INSTANCE_ID", "") or "").strip(),
                host=host,
                port=port,
                user=user,
                password=password,
                connect_timeout=int(str(source.get("RDS_BINLOG_SCHEMA_CONNECT_TIMEOUT", "10") or "10")),
                read_timeout=int(str(source.get("RDS_BINLOG_SCHEMA_READ_TIMEOUT", "120") or "120")),
            )
        )
    return instances


# --------------------------------------------------------------------------------------
# 结构模型
# --------------------------------------------------------------------------------------


@dataclass(slots=True)
class Column:
    name: str
    position: int
    column_type: str
    nullable: bool
    default: str | None
    extra: str
    comment: str
    charset: str | None
    collation: str | None
    generation_expression: str
    srs_id: int | None = None

    @property
    def base_type(self) -> str:
        return _base_type(self.column_type)

    @property
    def is_generated(self) -> bool:
        return bool(self.generation_expression)

    @property
    def is_auto_increment(self) -> bool:
        return "auto_increment" in self.extra.lower()

    def _default_clause(self) -> str | None:
        if self.is_generated:
            return None
        if self.default is None:
            return "DEFAULT NULL" if self.nullable else None
        text = self.default
        if _TIME_DEFAULT_RE.fullmatch(text.strip()):
            return f"DEFAULT {text}"
        if "DEFAULT_GENERATED" in self.extra.upper():
            # 8.0.13+ 表达式默认值：information_schema 里存的是表达式文本。
            return f"DEFAULT ({text})"
        if self.base_type in _NO_DEFAULT_TYPES:
            return f"DEFAULT ({quote_literal(text)})"
        if self.base_type == "bit" and re.fullmatch(r"b'[01]+'", text, re.IGNORECASE):
            return f"DEFAULT {text}"
        if self.base_type in _NUMERIC_TYPES and re.fullmatch(r"-?\d+(\.\d+)?(e[-+]?\d+)?", text, re.IGNORECASE):
            return f"DEFAULT {text}"
        return f"DEFAULT {quote_literal(text)}"

    def _on_update_clause(self) -> str | None:
        match = re.search(r"on update (current_timestamp(\(\s*\d*\s*\))?)", self.extra, re.IGNORECASE)
        return f"ON UPDATE {match.group(1)}" if match else None

    def definition(self, *, include_collation: bool = True) -> str:
        """重建列定义(不含列名)，用于 ADD/MODIFY COLUMN 与差异比对。"""
        parts: list[str] = [self.column_type]
        if include_collation and self.charset and self.collation:
            parts.append(f"CHARACTER SET {self.charset} COLLATE {self.collation}")
        if self.is_generated:
            stored = "STORED" if "STORED" in self.extra.upper() else "VIRTUAL"
            parts.append(f"GENERATED ALWAYS AS ({self.generation_expression}) {stored}")
        if self.srs_id is not None:
            parts.append(f"/*!80003 SRID {self.srs_id} */")
        parts.append("NULL" if self.nullable else "NOT NULL")
        default_clause = self._default_clause()
        if default_clause:
            parts.append(default_clause)
        if self.is_auto_increment:
            parts.append("AUTO_INCREMENT")
        on_update = self._on_update_clause()
        if on_update:
            parts.append(on_update)
        if self.comment:
            parts.append(f"COMMENT {quote_literal(self.comment)}")
        return " ".join(parts)

    def ddl(self) -> str:
        return f"{quote_ident(self.name)} {self.definition()}"

    def public_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "position": self.position,
            "definition": self.definition(),
            "type": self.column_type,
            "nullable": self.nullable,
            "default": self.default,
            "extra": self.extra,
            "comment": self.comment,
            "collation": self.collation,
        }


@dataclass(slots=True)
class IndexPart:
    column: str | None
    expression: str | None
    sub_part: int | None
    descending: bool

    def ddl(self) -> str:
        if self.expression:
            body = f"({self.expression})"
        else:
            body = quote_ident(self.column or "")
            if self.sub_part:
                body += f"({self.sub_part})"
        return body + (" DESC" if self.descending else "")


@dataclass(slots=True)
class Index:
    name: str
    unique: bool
    index_type: str
    comment: str
    parser: str | None
    parts: list[IndexPart] = field(default_factory=list)

    @property
    def is_primary(self) -> bool:
        return self.name == "PRIMARY"

    def _keyword(self) -> str:
        if self.is_primary:
            return "PRIMARY KEY"
        if self.index_type.upper() == "FULLTEXT":
            return f"FULLTEXT KEY {quote_ident(self.name)}"
        if self.index_type.upper() == "SPATIAL":
            return f"SPATIAL KEY {quote_ident(self.name)}"
        if self.unique:
            return f"UNIQUE KEY {quote_ident(self.name)}"
        return f"KEY {quote_ident(self.name)}"

    def definition(self) -> str:
        body = ",".join(part.ddl() for part in self.parts)
        text = f"{self._keyword()} ({body})"
        if self.index_type.upper() == "HASH" and not self.is_primary:
            text += " USING HASH"
        if self.parser:
            text += f" WITH PARSER {quote_ident(self.parser)}"
        if self.comment:
            text += f" COMMENT {quote_literal(self.comment)}"
        return text

    def signature(self) -> str:
        """用于判等：忽略名称之外的书写差异。"""
        body = ",".join(part.ddl() for part in self.parts)
        return f"{'PRIMARY' if self.is_primary else ('UNIQUE' if self.unique else 'INDEX')}|{self.index_type.upper()}|{body}|{self.comment}|{self.parser or ''}"

    def public_dict(self) -> dict[str, Any]:
        return {"name": self.name, "definition": self.definition(), "unique": self.unique, "type": self.index_type}


@dataclass(slots=True)
class ForeignKey:
    name: str
    columns: list[str]
    referenced_schema: str
    referenced_table: str
    referenced_columns: list[str]
    update_rule: str
    delete_rule: str

    def definition(self, *, same_schema: bool = True) -> str:
        cols = ",".join(quote_ident(c) for c in self.columns)
        ref_cols = ",".join(quote_ident(c) for c in self.referenced_columns)
        ref = (
            quote_ident(self.referenced_table)
            if same_schema
            else qualified(self.referenced_schema, self.referenced_table)
        )
        text = f"CONSTRAINT {quote_ident(self.name)} FOREIGN KEY ({cols}) REFERENCES {ref} ({ref_cols})"
        if self.delete_rule and self.delete_rule.upper() != "NO ACTION":
            text += f" ON DELETE {self.delete_rule}"
        if self.update_rule and self.update_rule.upper() != "NO ACTION":
            text += f" ON UPDATE {self.update_rule}"
        return text

    def signature(self) -> str:
        return (
            f"{','.join(self.columns)}|{self.referenced_table}|{','.join(self.referenced_columns)}"
            f"|{self.update_rule.upper()}|{self.delete_rule.upper()}"
        )


@dataclass(slots=True)
class Table:
    name: str
    table_type: str
    engine: str
    collation: str
    comment: str
    row_format: str
    columns: dict[str, Column] = field(default_factory=dict)
    indexes: dict[str, Index] = field(default_factory=dict)
    foreign_keys: dict[str, ForeignKey] = field(default_factory=dict)
    create_sql: str = ""

    @property
    def is_view(self) -> bool:
        return self.table_type.upper() != "BASE TABLE"

    def ordered_columns(self) -> list[Column]:
        return sorted(self.columns.values(), key=lambda c: c.position)

    def column_names(self) -> list[str]:
        return [c.name for c in self.ordered_columns()]


# --------------------------------------------------------------------------------------
# 只读读取
# --------------------------------------------------------------------------------------

_SYSTEM_SCHEMAS = frozenset({"information_schema", "performance_schema", "mysql", "sys", "__recycle_bin__"})


class SchemaReader:
    """单实例只读结构读取器。每次调用建立独立短连接，不做连接池。"""

    def __init__(self, config: InstanceConfig):
        self.config = config

    def _connect(self):
        import pymysql

        return pymysql.connect(
            host=self.config.host,
            port=self.config.port,
            user=self.config.user,
            password=self.config.password,
            charset="utf8mb4",
            autocommit=True,
            connect_timeout=self.config.connect_timeout,
            read_timeout=self.config.read_timeout,
            write_timeout=self.config.read_timeout,
            cursorclass=pymysql.cursors.DictCursor,
            init_command="SET SESSION TRANSACTION READ ONLY",
        )

    def identity(self) -> dict[str, Any]:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT @@hostname AS hostname, @@port AS port, CURRENT_USER() AS current_user_name,"
                    " @@read_only AS read_only, @@version AS version"
                )
                row = cursor.fetchone() or {}
        return {
            "instance": self.config.name,
            "hostname": row.get("hostname"),
            "port": row.get("port"),
            "user": row.get("current_user_name"),
            "readOnly": str(row.get("read_only")),
            "version": row.get("version"),
        }

    def list_databases(self, include_system: bool = False) -> list[dict[str, Any]]:
        sql = (
            "SELECT s.SCHEMA_NAME AS name, s.DEFAULT_CHARACTER_SET_NAME AS charset,"
            " s.DEFAULT_COLLATION_NAME AS collation,"
            " (SELECT COUNT(*) FROM information_schema.TABLES t WHERE t.TABLE_SCHEMA = s.SCHEMA_NAME) AS table_count"
            " FROM information_schema.SCHEMATA s ORDER BY s.SCHEMA_NAME"
        )
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
        result = []
        for row in rows:
            name = row["name"]
            if not include_system and name in _SYSTEM_SCHEMAS:
                continue
            result.append(
                {
                    "name": name,
                    "charset": row["charset"],
                    "collation": row["collation"],
                    "tableCount": int(row["table_count"] or 0),
                }
            )
        return result

    def read_schema(self, database: str, *, with_create_sql: Iterable[str] | None = None) -> dict[str, Table]:
        """一次性拉取整库结构。表数量再多也只发 4 条查询。"""
        if not _IDENT_RE.fullmatch(database):
            raise SchemaDiffError(f"库名非法：{database}")
        tables: dict[str, Table] = {}
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT TABLE_NAME, TABLE_TYPE, IFNULL(ENGINE,'') AS ENGINE,"
                    " IFNULL(TABLE_COLLATION,'') AS TABLE_COLLATION, IFNULL(TABLE_COMMENT,'') AS TABLE_COMMENT,"
                    " IFNULL(ROW_FORMAT,'') AS ROW_FORMAT"
                    " FROM information_schema.TABLES WHERE TABLE_SCHEMA = %s",
                    (database,),
                )
                for row in cursor.fetchall():
                    tables[row["TABLE_NAME"]] = Table(
                        name=row["TABLE_NAME"],
                        table_type=row["TABLE_TYPE"],
                        engine=row["ENGINE"],
                        collation=row["TABLE_COLLATION"],
                        comment=row["TABLE_COMMENT"],
                        row_format=row["ROW_FORMAT"],
                    )
                if not tables:
                    return tables

                cursor.execute(
                    "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_TYPE, IS_NULLABLE,"
                    " COLUMN_DEFAULT, IFNULL(EXTRA,'') AS EXTRA, IFNULL(COLUMN_COMMENT,'') AS COLUMN_COMMENT,"
                    " CHARACTER_SET_NAME, COLLATION_NAME, IFNULL(GENERATION_EXPRESSION,'') AS GENERATION_EXPRESSION,"
                    " SRS_ID"
                    " FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = %s",
                    (database,),
                )
                for row in cursor.fetchall():
                    table = tables.get(row["TABLE_NAME"])
                    if table is None:
                        continue
                    table.columns[row["COLUMN_NAME"]] = Column(
                        name=row["COLUMN_NAME"],
                        position=int(row["ORDINAL_POSITION"]),
                        column_type=row["COLUMN_TYPE"],
                        nullable=str(row["IS_NULLABLE"]).upper() == "YES",
                        default=row["COLUMN_DEFAULT"],
                        extra=row["EXTRA"],
                        comment=row["COLUMN_COMMENT"],
                        charset=row["CHARACTER_SET_NAME"],
                        collation=row["COLLATION_NAME"],
                        generation_expression=row["GENERATION_EXPRESSION"],
                        srs_id=row["SRS_ID"],
                    )

                cursor.execute(
                    "SELECT TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, NON_UNIQUE, SUB_PART,"
                    " IFNULL(INDEX_TYPE,'') AS INDEX_TYPE, COLLATION, EXPRESSION,"
                    " IFNULL(INDEX_COMMENT,'') AS INDEX_COMMENT"
                    " FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = %s"
                    " ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX",
                    (database,),
                )
                for row in cursor.fetchall():
                    table = tables.get(row["TABLE_NAME"])
                    if table is None:
                        continue
                    index = table.indexes.get(row["INDEX_NAME"])
                    if index is None:
                        index = Index(
                            name=row["INDEX_NAME"],
                            unique=int(row["NON_UNIQUE"]) == 0,
                            index_type=row["INDEX_TYPE"],
                            comment=row["INDEX_COMMENT"],
                            parser=None,
                        )
                        table.indexes[index.name] = index
                    index.parts.append(
                        IndexPart(
                            column=row["COLUMN_NAME"],
                            expression=row["EXPRESSION"],
                            sub_part=int(row["SUB_PART"]) if row["SUB_PART"] else None,
                            descending=str(row["COLLATION"] or "").upper() == "D",
                        )
                    )

                cursor.execute(
                    "SELECT rc.CONSTRAINT_NAME, rc.TABLE_NAME, rc.UPDATE_RULE, rc.DELETE_RULE,"
                    " rc.UNIQUE_CONSTRAINT_SCHEMA, rc.REFERENCED_TABLE_NAME,"
                    " kcu.COLUMN_NAME, kcu.REFERENCED_TABLE_SCHEMA, kcu.REFERENCED_COLUMN_NAME, kcu.ORDINAL_POSITION"
                    " FROM information_schema.REFERENTIAL_CONSTRAINTS rc"
                    " JOIN information_schema.KEY_COLUMN_USAGE kcu"
                    "   ON kcu.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA"
                    "  AND kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME"
                    "  AND kcu.TABLE_NAME = rc.TABLE_NAME"
                    " WHERE rc.CONSTRAINT_SCHEMA = %s"
                    " ORDER BY rc.TABLE_NAME, rc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION",
                    (database,),
                )
                for row in cursor.fetchall():
                    table = tables.get(row["TABLE_NAME"])
                    if table is None:
                        continue
                    fk = table.foreign_keys.get(row["CONSTRAINT_NAME"])
                    if fk is None:
                        fk = ForeignKey(
                            name=row["CONSTRAINT_NAME"],
                            columns=[],
                            referenced_schema=row["REFERENCED_TABLE_SCHEMA"] or database,
                            referenced_table=row["REFERENCED_TABLE_NAME"] or "",
                            referenced_columns=[],
                            update_rule=row["UPDATE_RULE"] or "",
                            delete_rule=row["DELETE_RULE"] or "",
                        )
                        table.foreign_keys[fk.name] = fk
                    fk.columns.append(row["COLUMN_NAME"])
                    if row["REFERENCED_COLUMN_NAME"]:
                        fk.referenced_columns.append(row["REFERENCED_COLUMN_NAME"])

                wanted = set(with_create_sql or ())
                for table_name in sorted(wanted & set(tables)):
                    try:
                        cursor.execute(f"SHOW CREATE TABLE {qualified(database, table_name)}")
                        row = cursor.fetchone() or {}
                        tables[table_name].create_sql = row.get("Create Table") or row.get("Create View") or ""
                    except Exception as exc:  # noqa: BLE001 - 单表失败不应中断整库读取
                        LOGGER.warning("SHOW CREATE TABLE %s.%s 失败：%s", database, table_name, exc)
        return tables


# --------------------------------------------------------------------------------------
# 差异模型与比对
# --------------------------------------------------------------------------------------

RISK_SAFE = "safe"
RISK_RISKY = "risky"


@dataclass(slots=True)
class Change:
    kind: str          # column_added / column_modified / column_dropped / index_* / fk_* / option_*
    object_name: str
    risk: str
    detail: str
    source_value: str = ""
    target_value: str = ""
    sql: str = ""


@dataclass(slots=True)
class TableDiff:
    table: str
    status: str        # missing_in_target / extra_in_target / changed / same
    changes: list[Change] = field(default_factory=list)
    source_create_sql: str = ""
    target_create_sql: str = ""

    @property
    def safe_sql(self) -> list[str]:
        return [c.sql for c in self.changes if c.sql and c.risk == RISK_SAFE]

    @property
    def risky_sql(self) -> list[str]:
        return [c.sql for c in self.changes if c.sql and c.risk == RISK_RISKY]

    def public_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "status": self.status,
            "changeCount": len(self.changes),
            "safeCount": sum(1 for c in self.changes if c.risk == RISK_SAFE),
            "riskyCount": sum(1 for c in self.changes if c.risk == RISK_RISKY),
            "changes": [
                {
                    "kind": c.kind,
                    "object": c.object_name,
                    "risk": c.risk,
                    "detail": c.detail,
                    "source": c.source_value,
                    "target": c.target_value,
                    "sql": c.sql,
                }
                for c in self.changes
            ],
            "sourceCreateSql": self.source_create_sql,
            "targetCreateSql": self.target_create_sql,
        }


def _modify_risk(source: Column, target: Column) -> str:
    """判断把 target 列改成 source 列定义是扩容(safe)还是可能丢数据(risky)。"""
    src_base, tgt_base = source.base_type, target.base_type
    if src_base != tgt_base:
        src_family = _TYPE_FAMILY.get(src_base)
        tgt_family = _TYPE_FAMILY.get(tgt_base)
        if not src_family or not tgt_family or src_family[0] != tgt_family[0]:
            return RISK_RISKY
        if src_family[1] < tgt_family[1]:
            return RISK_RISKY
    else:
        src_len, tgt_len = _type_length(source.column_type), _type_length(target.column_type)
        if src_len >= 0 and tgt_len >= 0 and src_len < tgt_len:
            return RISK_RISKY
        if "unsigned" in target.column_type.lower() and "unsigned" not in source.column_type.lower():
            return RISK_RISKY
    if target.nullable and not source.nullable:
        return RISK_RISKY
    if source.collation != target.collation and source.collation and target.collation:
        # 字符集/排序规则变更会重建列数据，归为高危。
        return RISK_RISKY
    return RISK_SAFE


def _diff_columns(database: str, table_name: str, source: Table, target: Table, diff: TableDiff) -> None:
    source_columns = source.ordered_columns()
    target_names = set(target.columns)
    previous_name: str | None = None
    for column in source_columns:
        existing = target.columns.get(column.name)
        if existing is None:
            position = f" AFTER {quote_ident(previous_name)}" if previous_name else " FIRST"
            diff.changes.append(
                Change(
                    kind="column_added",
                    object_name=column.name,
                    risk=RISK_SAFE,
                    detail="目标缺少该列",
                    source_value=column.definition(),
                    target_value="",
                    sql=f"ALTER TABLE {qualified(database, table_name)} ADD COLUMN {column.ddl()}{position};",
                )
            )
        else:
            source_def = column.definition()
            target_def = existing.definition()
            if source_def != target_def:
                risk = _modify_risk(column, existing)
                diff.changes.append(
                    Change(
                        kind="column_modified",
                        object_name=column.name,
                        risk=risk,
                        detail="列定义不一致",
                        source_value=source_def,
                        target_value=target_def,
                        sql=f"ALTER TABLE {qualified(database, table_name)} MODIFY COLUMN {column.ddl()};",
                    )
                )
        previous_name = column.name

    for name in sorted(target_names - set(source.columns)):
        existing = target.columns[name]
        diff.changes.append(
            Change(
                kind="column_dropped",
                object_name=name,
                risk=RISK_RISKY,
                detail="目标多出该列(基准没有)",
                source_value="",
                target_value=existing.definition(),
                sql=f"ALTER TABLE {qualified(database, table_name)} DROP COLUMN {quote_ident(name)};",
            )
        )


def _drop_index_sql(database: str, table_name: str, index: Index) -> str:
    if index.is_primary:
        return f"ALTER TABLE {qualified(database, table_name)} DROP PRIMARY KEY;"
    return f"ALTER TABLE {qualified(database, table_name)} DROP INDEX {quote_ident(index.name)};"


def _diff_indexes(database: str, table_name: str, source: Table, target: Table, diff: TableDiff) -> None:
    for name, index in sorted(source.indexes.items()):
        existing = target.indexes.get(name)
        if existing is None:
            diff.changes.append(
                Change(
                    kind="index_added",
                    object_name=name,
                    risk=RISK_SAFE,
                    detail="目标缺少该索引",
                    source_value=index.definition(),
                    target_value="",
                    sql=f"ALTER TABLE {qualified(database, table_name)} ADD {index.definition()};",
                )
            )
        elif index.signature() != existing.signature():
            diff.changes.append(
                Change(
                    kind="index_modified",
                    object_name=name,
                    risk=RISK_RISKY,
                    detail="同名索引定义不一致(需先删后建)",
                    source_value=index.definition(),
                    target_value=existing.definition(),
                    sql=(
                        _drop_index_sql(database, table_name, existing)
                        + f"\nALTER TABLE {qualified(database, table_name)} ADD {index.definition()};"
                    ),
                )
            )
    for name in sorted(set(target.indexes) - set(source.indexes)):
        existing = target.indexes[name]
        diff.changes.append(
            Change(
                kind="index_dropped",
                object_name=name,
                risk=RISK_RISKY,
                detail="目标多出该索引(基准没有)",
                source_value="",
                target_value=existing.definition(),
                sql=_drop_index_sql(database, table_name, existing),
            )
        )


def _diff_foreign_keys(database: str, table_name: str, source: Table, target: Table, diff: TableDiff) -> None:
    for name, fk in sorted(source.foreign_keys.items()):
        existing = target.foreign_keys.get(name)
        if existing is None:
            diff.changes.append(
                Change(
                    kind="fk_added",
                    object_name=name,
                    risk=RISK_SAFE,
                    detail="目标缺少该外键",
                    source_value=fk.definition(),
                    target_value="",
                    sql=f"ALTER TABLE {qualified(database, table_name)} ADD {fk.definition()};",
                )
            )
        elif fk.signature() != existing.signature():
            diff.changes.append(
                Change(
                    kind="fk_modified",
                    object_name=name,
                    risk=RISK_RISKY,
                    detail="同名外键定义不一致",
                    source_value=fk.definition(),
                    target_value=existing.definition(),
                    sql=(
                        f"ALTER TABLE {qualified(database, table_name)} DROP FOREIGN KEY {quote_ident(name)};"
                        f"\nALTER TABLE {qualified(database, table_name)} ADD {fk.definition()};"
                    ),
                )
            )
    for name in sorted(set(target.foreign_keys) - set(source.foreign_keys)):
        diff.changes.append(
            Change(
                kind="fk_dropped",
                object_name=name,
                risk=RISK_RISKY,
                detail="目标多出该外键(基准没有)",
                source_value="",
                target_value=target.foreign_keys[name].definition(),
                sql=f"ALTER TABLE {qualified(database, table_name)} DROP FOREIGN KEY {quote_ident(name)};",
            )
        )


def _diff_options(database: str, table_name: str, source: Table, target: Table, diff: TableDiff) -> None:
    if source.engine and source.engine != target.engine:
        diff.changes.append(
            Change(
                kind="option_engine",
                object_name="ENGINE",
                risk=RISK_RISKY,
                detail="存储引擎不一致(整表重建)",
                source_value=source.engine,
                target_value=target.engine,
                sql=f"ALTER TABLE {qualified(database, table_name)} ENGINE={source.engine};",
            )
        )
    if source.collation and source.collation != target.collation:
        charset = source.collation.split("_", 1)[0]
        diff.changes.append(
            Change(
                kind="option_collation",
                object_name="COLLATE",
                risk=RISK_RISKY,
                detail="表默认排序规则不一致(仅影响后续新增列，不改动已有列)",
                source_value=source.collation,
                target_value=target.collation,
                sql=(
                    f"ALTER TABLE {qualified(database, table_name)} "
                    f"DEFAULT CHARACTER SET {charset} COLLATE {source.collation};"
                ),
            )
        )
    if source.comment != target.comment:
        diff.changes.append(
            Change(
                kind="option_comment",
                object_name="COMMENT",
                risk=RISK_SAFE,
                detail="表注释不一致",
                source_value=source.comment,
                target_value=target.comment,
                sql=f"ALTER TABLE {qualified(database, table_name)} COMMENT={quote_literal(source.comment)};",
            )
        )
    if source.row_format and target.row_format and source.row_format != target.row_format:
        diff.changes.append(
            Change(
                kind="option_row_format",
                object_name="ROW_FORMAT",
                risk=RISK_RISKY,
                detail="行格式不一致(整表重建)",
                source_value=source.row_format,
                target_value=target.row_format,
                sql=f"ALTER TABLE {qualified(database, table_name)} ROW_FORMAT={source.row_format};",
            )
        )


def _rewrite_create_sql(create_sql: str, source_table: str, target_database: str) -> str:
    """把基准库的 SHOW CREATE TABLE 原文改写成目标库的建表语句。"""
    text = create_sql.strip()
    text = re.sub(r"\bAUTO_INCREMENT=\d+\s*", "", text)
    pattern = re.compile(
        r"^CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(`(?:[^`]|``)+`|\S+)", re.IGNORECASE
    )
    replacement = f"CREATE TABLE IF NOT EXISTS {qualified(target_database, source_table)}"
    text, count = pattern.subn(replacement, text, count=1)
    if count == 0:
        raise SchemaDiffError(f"无法改写建表语句：{source_table}")
    return text.rstrip().rstrip(";") + ";"


SCOPE_ALL = "all"                 # 基准库全部表(含目标缺失的表)
SCOPE_TARGET_EXISTING = "target"  # 只处理目标库已有的表
SCOPE_INTERSECTION = "both"       # 只处理两边都有的表(等价于 target，但不报告目标多出的表)


def diff_databases(
    source_tables: dict[str, Table],
    target_tables: dict[str, Table],
    *,
    source_database: str,
    target_database: str,
    scope: str = SCOPE_ALL,
    include_views: bool = False,
) -> list[TableDiff]:
    """以 source 为基准，产出把 target 对齐到 source 的差异清单。"""
    def usable(table: Table) -> bool:
        return include_views or not table.is_view

    source_names = {name for name, t in source_tables.items() if usable(t)}
    target_names = {name for name, t in target_tables.items() if usable(t)}

    if scope == SCOPE_TARGET_EXISTING:
        compare_names = target_names
    elif scope == SCOPE_INTERSECTION:
        compare_names = source_names & target_names
    else:
        compare_names = source_names | target_names

    diffs: list[TableDiff] = []
    for name in sorted(compare_names):
        source = source_tables.get(name)
        target = target_tables.get(name)
        if source is not None and target is None:
            diff = TableDiff(table=name, status="missing_in_target", source_create_sql=source.create_sql)
            create_statement = ""
            if source.create_sql:
                try:
                    create_statement = _rewrite_create_sql(source.create_sql, name, target_database)
                except SchemaDiffError as exc:
                    LOGGER.warning("%s", exc)
            diff.changes.append(
                Change(
                    kind="table_added",
                    object_name=name,
                    risk=RISK_SAFE,
                    detail=f"目标库 {target_database} 缺少该表",
                    source_value=f"{source_database}.{name}",
                    target_value="",
                    sql=create_statement,
                )
            )
            diffs.append(diff)
            continue
        if source is None and target is not None:
            if scope == SCOPE_INTERSECTION:
                continue
            diff = TableDiff(table=name, status="extra_in_target", target_create_sql=target.create_sql)
            diff.changes.append(
                Change(
                    kind="table_extra",
                    object_name=name,
                    risk=RISK_RISKY,
                    detail=f"基准库 {source_database} 没有该表",
                    source_value="",
                    target_value=f"{target_database}.{name}",
                    sql=f"-- DROP TABLE {qualified(target_database, name)}; -- 需人工确认后再执行",
                )
            )
            diffs.append(diff)
            continue
        assert source is not None and target is not None
        diff = TableDiff(
            table=name,
            status="same",
            source_create_sql=source.create_sql,
            target_create_sql=target.create_sql,
        )
        _diff_columns(target_database, name, source, target, diff)
        _diff_indexes(target_database, name, source, target, diff)
        _diff_foreign_keys(target_database, name, source, target, diff)
        _diff_options(target_database, name, source, target, diff)
        diff.status = "changed" if diff.changes else "same"
        diffs.append(diff)
    return diffs


def _commented(row: str) -> str:
    """给 SQL 行加注释前缀；本身已是注释的不再叠加，避免出现 '-- -- DROP TABLE'。"""
    return row if row.lstrip().startswith("--") else f"-- {row}"


def render_sql_script(
    diffs: list[TableDiff],
    *,
    source_instance: str,
    source_database: str,
    target_instance: str,
    target_database: str,
    include_risky: bool = False,
    generated_at: str = "",
) -> str:
    """把差异渲染成可直接复制执行的 SQL 脚本。

    每一张有差异的表都会出现在脚本里：安全语句可直接执行，高危语句(删除/收窄/
    重建)默认以注释形式给出，确认无数据影响后手工放开。表级 DROP TABLE 无论
    include_risky 与否都只以注释形式出现。
    """
    changed = [d for d in diffs if d.status != "same"]
    lines: list[str] = [
        "-- 库表结构对齐脚本(由 RDS SQL Insight 结构对比生成)",
        f"-- 基准：{source_instance}.{source_database}",
        f"-- 目标：{target_instance}.{target_database}",
        f"-- 生成时间：{generated_at or '-'}",
        f"-- 涉及表：{len(changed)} 张全部列出；安全语句可直接执行，高危语句(删除/收窄/重建)已注释",
        "-- 执行前请确认目标实例身份：SELECT @@hostname, @@port, CURRENT_USER(), @@read_only;",
        "",
    ]
    safe_total = 0
    risky_total = 0
    extra_tables: list[str] = []
    for diff in changed:
        safe = diff.safe_sql
        risky = diff.risky_sql
        risky_total += len(risky)
        if diff.status == "extra_in_target":
            extra_tables.append(diff.table)
        header = f"-- ==== {target_database}.{diff.table} ({diff.status}) ===="
        if not safe and not risky:
            # 有差异但没有可生成的 DDL(例如无法改写的建表语句)，明写出来而不是静默丢弃。
            lines.append(header)
            lines.append(f"-- 该表有 {len(diff.changes)} 处差异，但无法生成 DDL，请人工比对建表语句")
            lines.append("")
            continue
        lines.append(header)
        for statement in safe:
            lines.append(statement)
            safe_total += 1
        if risky:
            lines.append(f"-- 高危 {len(risky)} 条：删除/收窄/重建，确认无数据影响后再手工放开")
            for statement in risky:
                for row in statement.splitlines():
                    lines.append(row if include_risky else _commented(row))
        lines.append("")
    if extra_tables:
        lines.append(f"-- ==== 目标独有的表 {len(extra_tables)} 张(基准库没有，不建议删除) ====")
        lines.append(f"-- {', '.join(extra_tables)}")
        lines.append("")
    lines.append(f"-- 合计：安全语句 {safe_total} 条，高危语句 {risky_total} 条"
                 + ("(已展开)" if include_risky else "(已注释)"))
    return "\n".join(lines) + "\n"


def summarize(diffs: list[TableDiff]) -> dict[str, Any]:
    counts = {"same": 0, "changed": 0, "missing_in_target": 0, "extra_in_target": 0}
    safe_total = 0
    risky_total = 0
    kinds: dict[str, int] = {}
    for diff in diffs:
        counts[diff.status] = counts.get(diff.status, 0) + 1
        for change in diff.changes:
            kinds[change.kind] = kinds.get(change.kind, 0) + 1
            if change.risk == RISK_SAFE:
                safe_total += 1
            else:
                risky_total += 1
    return {
        "tables": len(diffs),
        "same": counts["same"],
        "changed": counts["changed"],
        "missingInTarget": counts["missing_in_target"],
        "extraInTarget": counts["extra_in_target"],
        "safeChanges": safe_total,
        "riskyChanges": risky_total,
        "kinds": kinds,
    }


# --------------------------------------------------------------------------------------
# 服务层
# --------------------------------------------------------------------------------------


class SchemaDiffService:
    """供 HTTP 层调用的门面：实例目录、库目录、结构对比。"""

    def __init__(
        self,
        instances: list[InstanceConfig] | None = None,
        *,
        name_resolver: "Callable[[str], str | None] | None" = None,
        default_compare: dict[str, str] | None = None,
    ):
        self._instances = {item.name: item for item in (instances if instances is not None else load_instances())}
        self._default_compare = default_compare if default_compare is not None else load_default_compare()
        self._lock = threading.Lock()
        # 显示名的真值来源是 RDS 控制台的实例名称(DBInstanceDescription)，
        # resolver 取不到时才回退到配置里的 label。
        self._name_resolver = name_resolver
        self._resolved_names: dict[str, str] = {}
        self._names_resolved = False

    @property
    def enabled(self) -> bool:
        return bool(self._instances)

    def _resolve_display_names(self) -> None:
        """惰性拉取一次 RDS 实例名称；失败只记日志，不影响功能。"""
        if self._names_resolved or self._name_resolver is None:
            return
        self._names_resolved = True
        for config in self._instances.values():
            if not config.instance_id:
                continue
            try:
                name = self._name_resolver(config.instance_id)
            except Exception as exc:  # noqa: BLE001 - 取名失败不能拖垮结构对比
                LOGGER.warning("读取实例 %s 名称失败，回退配置名：%s", config.instance_id, exc)
                continue
            if name:
                self._resolved_names[config.name] = str(name).strip()

    def default_compare(self) -> dict[str, str]:
        """页面预选值；引用了不存在的实例时把该项置空，避免前端选中空选项。"""
        result = dict(self._default_compare)
        for key in ("sourceInstance", "targetInstance"):
            if result.get(key) and result[key] not in self._instances:
                result[key] = ""
        return result

    def display_name(self, name: str) -> str:
        config = self._instances.get(name)
        if config is None:
            return name
        return self._resolved_names.get(name) or config.label

    def instances(self) -> list[dict[str, Any]]:
        self._resolve_display_names()
        result = []
        for config in self._instances.values():
            item = config.public_dict()
            item["label"] = self.display_name(config.name)
            result.append(item)
        return result

    def _reader(self, name: str) -> SchemaReader:
        config = self._instances.get(name)
        if config is None:
            raise SchemaDiffError(f"未知实例：{name}")
        return SchemaReader(config)

    def databases(self, instance: str) -> list[dict[str, Any]]:
        return self._reader(instance).list_databases()

    def tables(self, instance: str, database: str) -> list[dict[str, Any]]:
        schema = self._reader(instance).read_schema(database)
        return [
            {
                "name": table.name,
                "type": table.table_type,
                "engine": table.engine,
                "collation": table.collation,
                "columns": len(table.columns),
                "indexes": len(table.indexes),
            }
            for table in sorted(schema.values(), key=lambda t: t.name)
        ]

    def identity(self, instance: str) -> dict[str, Any]:
        return self._reader(instance).identity()

    def compare(
        self,
        *,
        source_instance: str,
        source_database: str,
        target_instance: str,
        target_database: str,
        scope: str = SCOPE_ALL,
        include_views: bool = False,
        generated_at: str = "",
    ) -> dict[str, Any]:
        if source_instance == target_instance and source_database == target_database:
            raise SchemaDiffError("基准与目标是同一个库，无需对比")
        with self._lock:
            source_reader = self._reader(source_instance)
            target_reader = self._reader(target_instance)
            source_identity = source_reader.identity()
            target_identity = target_reader.identity()
            source_tables = source_reader.read_schema(source_database)
            target_tables = target_reader.read_schema(target_database)
            if not source_tables:
                raise SchemaDiffError(f"基准库不存在或没有表：{source_instance}.{source_database}")

            diffs = diff_databases(
                source_tables,
                target_tables,
                source_database=source_database,
                target_database=target_database,
                scope=scope,
                include_views=include_views,
            )
            # 只对需要建表的表补取 SHOW CREATE TABLE，避免对整库发 N 条语句。
            need_create = [d.table for d in diffs if d.status == "missing_in_target"]
            if need_create:
                enriched = source_reader.read_schema(source_database, with_create_sql=need_create)
                for diff in diffs:
                    if diff.status != "missing_in_target":
                        continue
                    table = enriched.get(diff.table)
                    if table is None or not table.create_sql:
                        continue
                    diff.source_create_sql = table.create_sql
                    for change in diff.changes:
                        if change.kind == "table_added" and not change.sql:
                            try:
                                change.sql = _rewrite_create_sql(table.create_sql, diff.table, target_database)
                            except SchemaDiffError as exc:
                                LOGGER.warning("%s", exc)

        source_label = self.display_name(source_instance)
        target_label = self.display_name(target_instance)
        return {
            "source": {
                "instance": source_instance,
                "label": source_label,
                "database": source_database,
                "identity": source_identity,
                "tableCount": len(source_tables),
            },
            "target": {
                "instance": target_instance,
                "label": target_label,
                "database": target_database,
                "identity": target_identity,
                "tableCount": len(target_tables),
            },
            "scope": scope,
            "summary": summarize(diffs),
            "diffs": [d.public_dict() for d in diffs],
            "sql": render_sql_script(
                diffs,
                source_instance=source_label,
                source_database=source_database,
                target_instance=target_label,
                target_database=target_database,
                include_risky=False,
                generated_at=generated_at,
            ),
            "sqlWithRisky": render_sql_script(
                diffs,
                source_instance=source_label,
                source_database=source_database,
                target_instance=target_label,
                target_database=target_database,
                include_risky=True,
                generated_at=generated_at,
            ),
        }
