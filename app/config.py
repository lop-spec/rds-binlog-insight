from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger(__name__)

APP_NAME = "RDS SQL Insight"
APP_ID = "rds-binlog-insight"
APP_VERSION = "1.26.2-rawoss"
DEFAULT_PORT = 8769
MAX_PORT = 8799

_INSTANCE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9-]{4,127}$")
NODE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_REGION_RE = re.compile(r"^[a-z0-9][a-z0-9-]{1,62}$")
_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$")


def app_root() -> Path:
    return Path(__file__).resolve().parent.parent


def data_root() -> Path:
    override = os.environ.get("RDS_BINLOG_DATA_DIR", "").strip()
    return Path(override).expanduser().resolve() if override else app_root() / "data"


def ensure_data_dirs(root: Path | None = None) -> dict[str, Path]:
    base = root or data_root()
    staging_override = os.environ.get("RDS_BINLOG_STAGING_DIR", "").strip()
    paths = {
        "root": base,
        "events": base / "events",
        "downloads": base / "downloads",
        "staging": (
            Path(staging_override).expanduser().resolve()
            if staging_override
            else base / "staging"
        ),
        "index": base / "index",
        "locks": base / "locks",
        "scratch": base / "scratch",
        "legacy_cache": base / "cache",
        "legacy_query_cache": base / "query-cache",
        "logs": base / "logs",
        "exports": base / "exports",
        "query_results": base / "query-results",
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    return paths


@dataclass(slots=True)
class Settings:
    region_id: str = "cn-hangzhou"
    db_instance_id: str = ""
    endpoint: str = "rds.aliyuncs.com"
    initial_lookback_days: int = 60
    retention_days: int = 60
    poll_minutes: int = 5
    auto_sync: bool = True
    prefer_intranet_download: bool = True
    page_size: int = 100
    credential_target: str = "RDS-Binlog-Insight/default"
    oss_enabled: bool = False
    oss_bucket: str = ""
    oss_region_id: str = ""
    oss_endpoint: str = ""
    oss_prefix: str = ""
    oss_auth_mode: str = "ecs_ram_role"
    oss_role_name: str = ""
    oss_retention_days: int = 60

    @classmethod
    def from_mapping(cls, value: dict[str, Any] | None) -> "Settings":
        base = cls()
        if not value:
            return base
        allowed = set(asdict(base))
        merged = asdict(base)
        merged.update({key: val for key, val in value.items() if key in allowed})
        merged["prefer_intranet_download"] = True
        return cls(**merged)

    def validate(self, require_identity: bool = False) -> None:
        if require_identity and not _INSTANCE_RE.fullmatch(self.db_instance_id):
            raise ValueError("RDS 实例 ID 缺失或格式无效")
        if not _REGION_RE.fullmatch(self.region_id):
            raise ValueError("Region ID 格式无效")
        host = self.endpoint.strip().lower()
        if host.startswith("https://") or host.startswith("http://"):
            host = host.split("://", 1)[1].split("/", 1)[0]
        if os.environ.get("RDS_BINLOG_TEST_MODE") != "1":
            if not (host == "rds.aliyuncs.com" or host.endswith(".aliyuncs.com")):
                raise ValueError("API Endpoint 必须是阿里云 aliyuncs.com 域名")
        if not 1 <= int(self.initial_lookback_days) <= 365:
            raise ValueError("首次回看天数必须在 1 到 365 之间")
        if not 1 <= int(self.retention_days) <= 365:
            raise ValueError("保留天数必须在 1 到 365 之间")
        if not 1 <= int(self.poll_minutes) <= 1440:
            raise ValueError("轮询间隔必须在 1 到 1440 分钟之间")
        if not 30 <= int(self.page_size) <= 100:
            raise ValueError("RDS API PageSize 必须在 30 到 100 之间")
        if not 1 <= int(self.oss_retention_days) <= 3650:
            raise ValueError("OSS 保留天数必须在 1 到 3650 之间")
        if self.oss_enabled:
            if not _BUCKET_RE.fullmatch(self.oss_bucket):
                raise ValueError("OSS Bucket 名称格式无效")
            if not _REGION_RE.fullmatch(self.oss_region_id):
                raise ValueError("OSS Region ID 格式无效")
            oss_host = self.oss_endpoint.strip().lower()
            if oss_host.startswith(("https://", "http://")):
                oss_host = oss_host.split("://", 1)[1].split("/", 1)[0]
            expected_oss_host = (
                f"oss-{self.oss_region_id.strip().lower()}-internal.aliyuncs.com"
            )
            if oss_host != expected_oss_host:
                raise ValueError(
                    "OSS Endpoint 必须使用与 Region 匹配的内网 Endpoint："
                    + expected_oss_host
                )
            prefix = self.oss_prefix.strip().lstrip("/")
            if (
                not prefix
                or ".." in prefix.split("/")
                or "//" in prefix
                or any(ord(char) < 32 for char in prefix)
            ):
                raise ValueError("OSS 对象前缀格式无效")
            if self.oss_auth_mode not in {"ecs_ram_role", "access_key"}:
                raise ValueError(
                    "OSS 认证方式仅支持 ECS RAM 角色或受保护 AccessKey"
                )

    def public_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["regionId"] = result.pop("region_id")
        result["dbInstanceId"] = result.pop("db_instance_id")
        result["initialLookbackDays"] = result.pop("initial_lookback_days")
        result["retentionDays"] = result.pop("retention_days")
        result["pollMinutes"] = result.pop("poll_minutes")
        result["autoSync"] = result.pop("auto_sync")
        result["preferIntranetDownload"] = result.pop("prefer_intranet_download")
        result["pageSize"] = result.pop("page_size")
        result["ossEnabled"] = result.pop("oss_enabled")
        result["ossBucket"] = result.pop("oss_bucket")
        result["ossRegionId"] = result.pop("oss_region_id")
        result["ossEndpoint"] = result.pop("oss_endpoint")
        result["ossPrefix"] = result.pop("oss_prefix")
        result["ossAuthMode"] = result.pop("oss_auth_mode")
        result["ossRoleName"] = result.pop("oss_role_name")
        result["ossRetentionDays"] = result.pop("oss_retention_days")
        result.pop("credential_target", None)
        return result


def parse_settings_payload(payload: dict[str, Any], current: Settings) -> Settings:
    mapping = asdict(current)
    aliases = {
        "regionId": "region_id",
        "dbInstanceId": "db_instance_id",
        "initialLookbackDays": "initial_lookback_days",
        "retentionDays": "retention_days",
        "pollMinutes": "poll_minutes",
        "autoSync": "auto_sync",
        "preferIntranetDownload": "prefer_intranet_download",
        "pageSize": "page_size",
        "ossEnabled": "oss_enabled",
        "ossBucket": "oss_bucket",
        "ossRegionId": "oss_region_id",
        "ossEndpoint": "oss_endpoint",
        "ossPrefix": "oss_prefix",
        "ossAuthMode": "oss_auth_mode",
        "ossRoleName": "oss_role_name",
        "ossRetentionDays": "oss_retention_days",
    }
    for key, value in payload.items():
        target = aliases.get(key, key)
        if target in mapping and target not in {
            "credential_target",
            "prefer_intranet_download",
        }:
            mapping[target] = value
    mapping["prefer_intranet_download"] = True
    for key in (
        "initial_lookback_days",
        "retention_days",
        "poll_minutes",
        "page_size",
        "oss_retention_days",
    ):
        mapping[key] = int(mapping[key])
    for key in ("auto_sync", "prefer_intranet_download", "oss_enabled"):
        value = mapping[key]
        if isinstance(value, bool):
            mapping[key] = value
        elif isinstance(value, (int, float)) and value in {0, 1}:
            mapping[key] = bool(value)
        elif isinstance(value, str) and value.strip().lower() in {
            "true",
            "false",
            "1",
            "0",
        }:
            mapping[key] = value.strip().lower() in {"true", "1"}
        else:
            raise ValueError(f"{key} 必须是布尔值")
    for key in (
        "region_id",
        "db_instance_id",
        "endpoint",
        "oss_bucket",
        "oss_region_id",
        "oss_endpoint",
        "oss_auth_mode",
        "oss_role_name",
    ):
        mapping[key] = str(mapping[key]).strip()
    mapping["oss_prefix"] = str(mapping["oss_prefix"]).strip().lstrip("/")
    if mapping["oss_prefix"] and not mapping["oss_prefix"].endswith("/"):
        mapping["oss_prefix"] += "/"
    result = Settings(**mapping)
    result.validate(require_identity=False)
    return result


# --------------------------------------------------------------------------------------
# 附加 binlog 实例(多实例同步)
# --------------------------------------------------------------------------------------

SECONDARY_INSTANCES_FILE = "binlog-instances.json"


@dataclass(slots=True)
class SecondaryInstance:
    """附加的 binlog 同步实例。

    只覆盖实例身份与同步节奏；Region、Endpoint、凭据、OSS 账号一律继承主设置，
    避免同一份云配置在两处各写一遍后漂移。OSS 前缀默认按实例 ID 派生，保证两个
    实例的 binlog 不会落进同一个对象目录。
    """

    instance_id: str
    label: str = ""
    auto_sync: bool = True
    poll_minutes: int = 10
    initial_lookback_days: int = 3
    # 只用于本实例的同步窗口计算。本地分区的保留期清理是全局动作、由 primary
    # 按主设置统一执行，这里填小值不会让本实例的分区被提前清掉。
    retention_days: int = 0  # 0 表示继承主设置
    oss_prefix: str = ""  # 空表示按实例 ID 自动派生

    def display_name(self) -> str:
        return self.label or self.instance_id

    def resolve(self, base: Settings) -> Settings:
        prefix = self.oss_prefix.strip().lstrip("/")
        if not prefix:
            prefix = f"mysql-binlog/{self.instance_id}/"
        if not prefix.endswith("/"):
            prefix += "/"
        merged = replace(
            base,
            db_instance_id=self.instance_id,
            auto_sync=bool(self.auto_sync),
            poll_minutes=int(self.poll_minutes),
            initial_lookback_days=int(self.initial_lookback_days),
            retention_days=int(self.retention_days or base.retention_days),
            oss_prefix=prefix,
        )
        merged.validate(require_identity=True)
        return merged


def load_secondary_instances(root: Path | None = None) -> list[SecondaryInstance]:
    """读 data/binlog-instances.json。

    文件不存在 = 单实例模式，行为与本功能上线前完全一致。解析失败只记日志并
    返回空列表：附加配置写坏不能让主实例的同步跟着停。
    """

    path = (root or data_root()) / SECONDARY_INSTANCES_FILE
    try:
        raw = json.loads(path.read_text("utf-8"))
    except FileNotFoundError:
        return []
    except (OSError, ValueError) as exc:
        LOGGER.warning("%s 解析失败，附加 binlog 实例未启用：%s", path, exc)
        return []
    items = raw.get("instances") if isinstance(raw, dict) else raw
    if not isinstance(items, list):
        LOGGER.warning("%s 结构无效(期望 instances 数组)，附加实例未启用", path)
        return []
    result: list[SecondaryInstance] = []
    seen: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        instance_id = str(
            item.get("instanceId") or item.get("instance_id") or ""
        ).strip()
        if not _INSTANCE_RE.fullmatch(instance_id):
            LOGGER.warning("附加实例 ID 格式无效，已跳过：%r", instance_id)
            continue
        if instance_id in seen:
            LOGGER.warning("附加实例重复，已跳过：%s", instance_id)
            continue
        enabled = item.get("enabled", True)
        if isinstance(enabled, str):
            enabled = enabled.strip().lower() not in {"0", "false", "no"}
        if not enabled:
            continue
        seen.add(instance_id)
        try:
            result.append(
                SecondaryInstance(
                    instance_id=instance_id,
                    label=str(item.get("label") or "").strip(),
                    auto_sync=bool(item.get("autoSync", item.get("auto_sync", True))),
                    poll_minutes=int(
                        item.get("pollMinutes", item.get("poll_minutes", 10))
                    ),
                    initial_lookback_days=int(
                        item.get(
                            "initialLookbackDays",
                            item.get("initial_lookback_days", 3),
                        )
                    ),
                    retention_days=int(
                        item.get("retentionDays", item.get("retention_days", 0))
                    ),
                    oss_prefix=str(
                        item.get("ossPrefix") or item.get("oss_prefix") or ""
                    ).strip(),
                )
            )
        except (TypeError, ValueError) as exc:
            LOGGER.warning("附加实例 %s 配置字段无效，已跳过：%s", instance_id, exc)
    return result


GENERAL_LOG_INSTANCES_FILE = "general-log-instances.json"


def load_general_log_instances(root: Path | None = None) -> list[dict[str, Any]]:
    """读 data/general-log-instances.json，返回逐实例的 general log 采集配置。

    凭据放文件不放 env_file：RDS 账号密码里的 `#$'"` 等字符在 env_file 里会被
    误解析(与 schema-instances.json 同样的理由)。文件不存在时返回空列表，此时
    仍按 RDS_BINLOG_GLOG_* 环境变量跑单实例，与本功能上线前一致。
    """

    path = (root or data_root()) / GENERAL_LOG_INSTANCES_FILE
    try:
        raw = json.loads(path.read_text("utf-8"))
    except FileNotFoundError:
        return []
    except (OSError, ValueError) as exc:
        LOGGER.warning("%s 解析失败，多实例 general log 未启用：%s", path, exc)
        return []
    items = raw.get("instances") if isinstance(raw, dict) else raw
    if not isinstance(items, list):
        LOGGER.warning("%s 结构无效(期望 instances 数组)", path)
        return []
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        instance_id = str(
            item.get("instanceId") or item.get("instance_id") or ""
        ).strip()
        if not _INSTANCE_RE.fullmatch(instance_id):
            LOGGER.warning("general log 实例 ID 格式无效，已跳过：%r", instance_id)
            continue
        if instance_id in seen:
            LOGGER.warning("general log 实例重复，已跳过：%s", instance_id)
            continue
        seen.add(instance_id)
        result.append(dict(item, instanceId=instance_id))
    return result


SLOW_LOG_INSTANCES_FILE = "slow-log-instances.json"


def load_slow_log_instances(root: Path | None = None) -> list[dict[str, Any]]:
    """读 data/slow-log-instances.json，返回逐实例的慢日志采集配置。

    慢日志走管控面只读 API，凭据复用 binlog 同步那一份 AccessKey，所以这个文件里
    不含任何密码；文件不存在时返回空列表，慢日志采集整体不启动。
    """

    path = (root or data_root()) / SLOW_LOG_INSTANCES_FILE
    try:
        raw = json.loads(path.read_text("utf-8"))
    except FileNotFoundError:
        return []
    except (OSError, ValueError) as exc:
        LOGGER.warning("%s 解析失败，慢日志采集未启用：%s", path, exc)
        return []
    items = raw.get("instances") if isinstance(raw, dict) else raw
    if not isinstance(items, list):
        LOGGER.warning("%s 结构无效(期望 instances 数组)", path)
        return []
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        instance_id = str(
            item.get("instanceId") or item.get("instance_id") or ""
        ).strip()
        if not _INSTANCE_RE.fullmatch(instance_id):
            LOGGER.warning("慢日志实例 ID 格式无效，已跳过：%r", instance_id)
            continue
        node_id = str(item.get("nodeId") or item.get("node_id") or "").strip()
        if node_id and not NODE_ID_RE.fullmatch(node_id):
            LOGGER.warning(
                "慢日志 Node ID 格式无效，已跳过：instance=%s node=%r",
                instance_id,
                node_id,
            )
            continue
        identity = (instance_id, node_id)
        if identity in seen:
            LOGGER.warning(
                "慢日志实例与节点重复，已跳过：instance=%s node=%s",
                instance_id,
                node_id or "(默认)",
            )
            continue
        seen.add(identity)
        result.append(dict(item, instanceId=instance_id, nodeId=node_id))
    return result


def utc_now_text() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
