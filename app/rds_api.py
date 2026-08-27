from __future__ import annotations

import base64
import hashlib
import hmac
import json
import ssl
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from .config import Settings
from .credentials import CloudCredential


class RdsApiError(RuntimeError):
    def __init__(self, message: str, *, code: str = "RDS_API_ERROR", request_id: str = ""):
        super().__init__(message)
        self.code = code
        self.request_id = request_id


@dataclass(slots=True, frozen=True)
class RemoteBinlog:
    log_file_name: str
    log_begin_utc: str
    log_end_utc: str
    file_size: int
    checksum_crc64: str
    download_link: str
    intranet_download_link: str
    link_expired_utc: str
    remote_status: str
    host_instance_id: str

    @property
    def stable_id(self) -> str:
        raw = "\x1f".join(
            (
                self.log_file_name,
                self.log_begin_utc,
                self.log_end_utc,
                str(self.file_size),
                self.host_instance_id,
            )
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def selected_url(self, _prefer_intranet: bool = True) -> str:
        if not self.intranet_download_link.strip():
            raise RdsApiError(
                "RDS 未返回内网 Binlog 下载地址；已禁止回退公网下载链接",
                code="INTRANET_DOWNLOAD_LINK_MISSING",
            )
        return self.intranet_download_link


def _percent(value: Any) -> str:
    return urllib.parse.quote(str(value), safe="~", encoding="utf-8", errors="strict")


class RdsRpcClient:
    VERSION = "2014-08-15"

    def __init__(self, settings: Settings, credential: CloudCredential, timeout: int = 30):
        self.settings = settings
        self.credential = credential
        self.timeout = timeout
        self.settings.validate(require_identity=True)
        self.credential.validate()

    @property
    def endpoint(self) -> str:
        endpoint = self.settings.endpoint.strip().rstrip("/")
        if not endpoint.startswith(("https://", "http://")):
            endpoint = "https://" + endpoint
        return endpoint

    def _signed_params(self, action: str, params: dict[str, Any]) -> dict[str, str]:
        values = {
            "AccessKeyId": self.credential.access_key_id,
            "Action": action,
            "Format": "JSON",
            "RegionId": self.settings.region_id,
            "SignatureMethod": "HMAC-SHA1",
            "SignatureNonce": str(uuid.uuid4()),
            "SignatureVersion": "1.0",
            "Timestamp": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "Version": self.VERSION,
        }
        if self.credential.security_token:
            values["SecurityToken"] = self.credential.security_token
        values.update({key: str(value) for key, value in params.items() if value is not None})
        canonical = "&".join(
            f"{_percent(key)}={_percent(values[key])}" for key in sorted(values)
        )
        string_to_sign = "GET&%2F&" + _percent(canonical)
        digest = hmac.new(
            (self.credential.access_key_secret + "&").encode("utf-8"),
            string_to_sign.encode("utf-8"),
            hashlib.sha1,
        ).digest()
        values["Signature"] = base64.b64encode(digest).decode("ascii")
        return values

    def call(self, action: str, params: dict[str, Any]) -> dict[str, Any]:
        signed = self._signed_params(action, params)
        url = self.endpoint + "/?" + urllib.parse.urlencode(signed, quote_via=urllib.parse.quote)
        request = urllib.request.Request(
            url,
            method="GET",
            headers={
                "Accept": "application/json",
                "User-Agent": "RDS-Binlog-Insight/1.0",
            },
        )
        try:
            with urllib.request.urlopen(
                request,
                timeout=self.timeout,
                context=ssl.create_default_context(),
            ) as response:
                body = response.read()
        except urllib.error.HTTPError as exc:
            body = exc.read()
            try:
                parsed = json.loads(body.decode("utf-8"))
            except Exception:
                parsed = {}
            raise RdsApiError(
                str(parsed.get("Message") or f"RDS API HTTP {exc.code}"),
                code=str(parsed.get("Code") or f"HTTP_{exc.code}"),
                request_id=str(parsed.get("RequestId") or ""),
            ) from exc
        except (urllib.error.URLError, TimeoutError) as exc:
            raise RdsApiError(f"RDS API 网络错误：{exc}", code="NETWORK_ERROR") from exc
        try:
            result = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RdsApiError("RDS API 返回了无效 JSON", code="INVALID_RESPONSE") from exc
        if result.get("Code") and result.get("Message"):
            raise RdsApiError(
                str(result["Message"]),
                code=str(result["Code"]),
                request_id=str(result.get("RequestId") or ""),
            )
        return result

    def verify_instance(self) -> dict[str, str]:
        payload = self.call(
            "DescribeDBInstanceAttribute",
            {"DBInstanceId": self.settings.db_instance_id},
        )
        items = payload.get("Items", {}).get("DBInstanceAttribute", [])
        if isinstance(items, dict):
            items = [items]
        if not items:
            raise RdsApiError("实例身份核验未返回目标实例", code="IDENTITY_NOT_FOUND")
        item = items[0]
        actual_id = str(item.get("DBInstanceId") or "")
        if actual_id != self.settings.db_instance_id:
            raise RdsApiError(
                f"实例身份不匹配：期望 {self.settings.db_instance_id}，返回 {actual_id}",
                code="IDENTITY_MISMATCH",
            )
        engine = str(item.get("Engine") or "")
        if engine.lower() not in {"mysql", "mariadb"}:
            raise RdsApiError(
                f"当前引擎为 {engine or '未知'}，本工具只解析 RDS MySQL/MariaDB Binlog",
                code="ENGINE_NOT_SUPPORTED",
            )
        return {
            "dbInstanceId": actual_id,
            "engine": engine,
            "engineVersion": str(item.get("EngineVersion") or ""),
            "description": str(item.get("DBInstanceDescription") or ""),
            "regionId": str(item.get("RegionId") or self.settings.region_id),
            "requestId": str(payload.get("RequestId") or ""),
        }

    def primary_host_instance_id(self) -> str:
        payload = self.call(
            "DescribeDBInstanceHAConfig",
            {"DBInstanceId": self.settings.db_instance_id},
        )
        raw_nodes = payload.get("HostInstanceInfos", {}).get("NodeInfo", [])
        if isinstance(raw_nodes, dict):
            raw_nodes = [raw_nodes]
        masters = [
            str(node.get("NodeId") or "")
            for node in raw_nodes
            if str(node.get("NodeType") or "").lower() == "master"
            and str(node.get("NodeId") or "")
        ]
        if len(masters) != 1:
            raise RdsApiError(
                f"无法唯一识别 RDS 主节点，返回 {len(masters)} 个 Master",
                code="PRIMARY_NODE_NOT_UNIQUE",
                request_id=str(payload.get("RequestId") or ""),
            )
        return masters[0]

    def list_binlogs(self, start_utc: str, end_utc: str) -> list[RemoteBinlog]:
        page = 1
        results: list[RemoteBinlog] = []
        while True:
            payload = self.call(
                "DescribeBinlogFiles",
                {
                    "DBInstanceId": self.settings.db_instance_id,
                    "StartTime": start_utc,
                    "EndTime": end_utc,
                    "PageNumber": page,
                    "PageSize": self.settings.page_size,
                },
            )
            raw_items = payload.get("Items", {}).get("BinLogFile", [])
            if isinstance(raw_items, dict):
                raw_items = [raw_items]
            for item in raw_items:
                results.append(
                    RemoteBinlog(
                        log_file_name=str(item.get("LogFileName") or ""),
                        log_begin_utc=str(item.get("LogBeginTime") or ""),
                        log_end_utc=str(item.get("LogEndTime") or ""),
                        file_size=int(item.get("FileSize") or 0),
                        checksum_crc64=str(item.get("Checksum") or ""),
                        download_link=str(item.get("DownloadLink") or ""),
                        intranet_download_link=str(
                            item.get("IntranetDownloadLink") or ""
                        ),
                        link_expired_utc=str(item.get("LinkExpiredTime") or ""),
                        remote_status=str(item.get("RemoteStatus") or ""),
                        host_instance_id=str(item.get("HostInstanceID") or ""),
                    )
                )
            total = int(payload.get("TotalRecordCount") or len(results))
            if len(results) >= total or not raw_items:
                break
            page += 1
            if page > 100000:
                raise RdsApiError("RDS Binlog 分页异常，已停止", code="PAGINATION_GUARD")
        return sorted(
            results,
            key=lambda item: (
                item.log_begin_utc,
                item.log_end_utc,
                item.log_file_name,
                item.host_instance_id,
            ),
        )
