"""
【模块功能】调用企业爬虫接口、轮询任务状态并调度按 PDF 触发的企业爬取。

:Author: gexinyan
:CreateTime: 2026-07-16 10:00:00
"""

from __future__ import annotations

import json
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Iterable
from urllib import parse, request
from urllib.error import HTTPError, URLError

from .database import VerificationResult
from .records import extract_company_names, normalize_text


SUCCESS_STATUSES = {"success", "succeeded", "finished", "finish", "completed", "done", "ok"}
FAILED_STATUSES = {"failed", "fail", "error", "exception", "canceled", "cancelled"}
RUNNING_STATUSES = {"running", "pending", "processing", "started", "queued", "doing", "waiting"}
TERMINAL_ENTITY_STATUSES = {"success", "failed", "existing"}
TERMINAL_EXPANSION_STATUSES = {"COMPLETED", "FAILED", "CANCELLED"}
Verifier = Callable[[str], VerificationResult]
SpiderProgressCallback = Callable[[dict[str, Any]], None]


@dataclass(frozen=True, slots=True)
class SpiderConfig:
    """
    【类功能】保存企业爬虫 HTTP 调用和状态轮询配置。
    :Attributes:
        base_url: str，爬虫服务根地址
        submit_mode: str，single 或 batch
        timeout_seconds: int，单次 HTTP 请求超时秒数
        poll_interval_seconds: float，状态轮询间隔秒数
        max_poll_seconds: float，单个企业最长轮询时长
        retry_delays: tuple[float, ...]，网络或 5xx 失败后的重试间隔
        fetch_deep_info: bool，是否采集企业深度信息
        fetch_bidding_detail: bool，是否采集招投标详情
        relation_expansion_depth: int，企业关联关系扩展层数
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    """

    base_url: str
    submit_mode: str = "single"
    timeout_seconds: int = 20
    poll_interval_seconds: float = 5.0
    max_poll_seconds: float = 180.0
    retry_delays: tuple[float, ...] = (5.0, 15.0)
    fetch_deep_info: bool = False
    fetch_bidding_detail: bool = False
    relation_expansion_depth: int = 1


@dataclass(frozen=True, slots=True)
class HttpResult:
    """
    【类功能】保存一次 HTTP 请求的状态码、响应文本和网络错误。
    :Attributes:
        status_code: int | None，HTTP 状态码，网络错误时为空
        text: str，响应正文或错误文本
        json_body: Any，解析后的 JSON 响应，非 JSON 时为空
        error: str，网络错误说明
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    """

    status_code: int | None
    text: str
    json_body: Any
    error: str = ""


@dataclass(frozen=True, slots=True)
class SpiderTaskResult:
    """
    【类功能】描述一家公司爬取提交、状态轮询和数据库回查的结果。
    :Attributes:
        source_pdf: str，触发任务的 PDF 路径
        company_name: str，当前企业名称
        request_keyword: str，提交给接口的 keyword 内容
        status: str，success、failed、timeout、empty_result 或 skipped
        message: str，状态或异常摘要
        attempts: int，提交请求尝试次数
        poll_count: int，状态轮询次数
        database_rows: int，回查到的企业信息行数
        effective_fields: dict[str, str]，回查命中的非空字段样例
        company_id: str，服务端稳定企业标识
        expansion_status: str，结果生成时的关系扩展状态
        audit_results: tuple[dict[str, Any], ...]，合并去重前的服务端审计记录
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    """

    source_pdf: str
    company_name: str
    request_keyword: str
    status: str
    message: str
    attempts: int = 0
    poll_count: int = 0
    database_rows: int = 0
    effective_fields: dict[str, str] = field(default_factory=dict)
    run_id: str = ""
    company_type: str = "root"
    raw_status: str = ""
    has_data: bool = False
    related_sources: tuple[str, ...] = ()
    company_id: str = ""
    expansion_status: str = ""
    audit_results: tuple[dict[str, Any], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        """
        【方法功能】转换为可写入运行摘要 JSON 的字典。
        :return: dict[str, Any]，可 JSON 序列化的爬虫结果
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        audit_results = self.audit_results or (
            {
                "runId": self.run_id,
                "sourcePdf": self.source_pdf,
                "requestKeyword": self.request_keyword,
                "queryStatus": self.raw_status,
                "errorMessage": self.message,
                "hasData": self.has_data,
                "expansionStatus": self.expansion_status,
            },
        )
        return {
            "sourcePdf": self.source_pdf,
            "companyName": self.company_name,
            "requestKeyword": self.request_keyword,
            "status": self.status,
            "message": self.message,
            "attempts": self.attempts,
            "pollCount": self.poll_count,
            "databaseRows": self.database_rows,
            "effectiveFields": self.effective_fields,
            "runId": self.run_id,
            "companyId": self.company_id,
            "companyType": self.company_type,
            "rawStatus": self.raw_status,
            "queryStatus": self.raw_status,
            "hasData": self.has_data,
            "relatedSources": list(self.related_sources),
            "expansionStatus": self.expansion_status,
            "auditResults": list(audit_results),
        }


@dataclass(frozen=True, slots=True)
class RunProgress:
    """
    【类功能】保存单次 runId 关联企业扩展任务的根企业、关联企业及扩展状态快照。
    :Attributes:
        run_id: str，爬虫服务任务标识
        root_total: int，根企业总数
        root_success: int，根企业成功数
        root_failed: int，根企业失败数
        root_existing: int，根企业已有数据数
        related_total: int，已发现关联企业数
        related_success: int，关联企业成功数
        related_failed: int，关联企业失败数
        related_existing: int，关联企业已有数据数
        expansion_status: str，服务端关系扩展状态
        entities: tuple[SpiderTaskResult, ...]，本次快照中的企业结果
    :Author: gexinyan
    :CreateTime: 2026-07-20 18:00:00
    """

    run_id: str
    root_total: int
    root_success: int
    root_failed: int
    root_existing: int
    related_total: int
    related_success: int
    related_failed: int
    related_existing: int
    expansion_status: str
    entities: tuple[SpiderTaskResult, ...] = ()


@dataclass(frozen=True, slots=True)
class CrawlProgress:
    """
    【类功能】保存当前任务中企业爬虫队列的实时进度快照。
    :Attributes:
        discovered: int，已从成功解析 PDF 中发现并去重的企业数
        queued: int，尚未开始爬取的企业数
        running: int，正在爬取的企业数
        completed: int，已结束的企业任务数，包含成功、失败和跳过
        failed: int，失败或超时的企业数
        skipped: int，因爬虫关闭而跳过的企业数
        phase: str，waiting_for_companies/crawling/waiting_for_completion/completed
    :Author: gexinyan
    :CreateTime: 2026-07-17 10:30:00
    """

    discovered: int = 0
    queued: int = 0
    running: int = 0
    completed: int = 0
    failed: int = 0
    skipped: int = 0
    phase: str = "waiting_for_companies"
    root_total: int = 0
    root_success: int = 0
    root_failed: int = 0
    root_existing: int = 0
    related_total: int = 0
    related_success: int = 0
    related_failed: int = 0
    related_existing: int = 0
    expansion_status: str = "WAITING"

    def to_dict(self) -> dict[str, Any]:
        """
        【方法功能】转换为可经由进程队列传输的结构化进度数据。
        :return: dict[str, Any]，包含根企业和关联企业嵌套统计的爬虫进度快照
        :Author: gexinyan
        :CreateTime: 2026-07-17 10:30:00
        """
        return {
            "discovered": self.discovered,
            "queued": self.queued,
            "running": self.running,
            "completed": self.completed,
            "failed": self.failed,
            "skipped": self.skipped,
            "phase": self.phase,
            "root": {
                "total": self.root_total,
                "success": self.root_success,
                "failed": self.root_failed,
                "existing": self.root_existing,
            },
            "related": {
                "total": self.related_total,
                "success": self.related_success,
                "failed": self.related_failed,
                "existing": self.related_existing,
            },
            "expansionStatus": self.expansion_status,
        }

def parse_json_text(value: str) -> Any:
    """
    【函数功能】尝试将 HTTP 响应文本解析为 JSON。
    :param value: str，响应文本
    :return: Any，JSON 对象；解析失败时返回 None
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: parse_json_text('{"code": 200}')
    """
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None


def truncate_message(value: Any, limit: int = 500) -> str:
    """
    【函数功能】规范化并截断接口响应，防止运行摘要写入过大内容。
    :param value: Any，原始响应或异常
    :param limit: int，最大字符数，默认 500
    :return: str，适合日志和摘要的文本
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: truncate_message("ok")
    """
    text = normalize_text(value)
    return text if len(text) <= limit else text[:limit] + "…"


def is_success_response(result: HttpResult) -> bool:
    """
    【函数功能】判断 HTTP 请求是否返回 2xx 成功状态。
    :param result: HttpResult，HTTP 请求结果
    :return: bool，状态码位于 200 至 299 时为 True
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: is_success_response(HttpResult(200, "", None))
    """
    return result.status_code is not None and 200 <= result.status_code < 300


def should_retry(result: HttpResult) -> bool:
    """
    【函数功能】判断网络错误或服务端错误是否允许重试。
    :param result: HttpResult，HTTP 请求结果
    :return: bool，可重试时返回 True
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: should_retry(HttpResult(503, "", None))
    """
    return result.status_code is None or (result.status_code is not None and result.status_code >= 500)


def flatten_status_values(value: Any) -> list[str]:
    """
    【函数功能】递归提取响应中的状态、消息和有无企业数据标记。
    :param value: Any，状态接口 JSON 响应
    :return: list[str]，小写状态候选文本列表
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: flatten_status_values({"queryStatus": "WAITING"})
    """
    values: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            normalized_key = normalize_text(key).replace("_", "").casefold()
            if normalized_key in {
                "status",
                "state",
                "taskstatus",
                "crawlstatus",
                "querystatus",
                "code",
                "message",
                "msg",
            }:
                values.append(normalize_text(item).casefold())
            if normalized_key == "hasdata" and item is True:
                values.append("has_data")
            values.extend(flatten_status_values(item))
    elif isinstance(value, list):
        for item in value:
            values.extend(flatten_status_values(item))
    return values


def classify_status(result: HttpResult) -> str:
    """
    【函数功能】将爬虫状态响应归类为 success、failed、running 或 unknown。
    :param result: HttpResult，状态接口请求结果
    :return: str，规范化任务状态
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: classify_status(HttpResult(200, '{"queryStatus":"SUCCESS"}', {"queryStatus":"SUCCESS"}))
    """
    candidates = flatten_status_values(result.json_body)
    candidates.append(normalize_text(result.text).casefold())
    joined = " ".join(candidates)
    if "has_data" in candidates:
        return "existing"
    if any(status in joined for status in FAILED_STATUSES):
        return "failed"
    if any(status in joined for status in SUCCESS_STATUSES):
        return "success"
    if any(status in joined for status in RUNNING_STATUSES):
        return "running"
    return "unknown"


def normalize_entity_status(raw_status: Any, has_data: bool) -> str:
    """
    【函数功能】按已有数据优先规则将服务端企业状态标准化为 Pipeline 状态。
    :param raw_status: Any，服务端原始状态字段
    :param has_data: bool，服务端是否明确表示已有数据
    :return: str，success、failed、existing、running 或 waiting
    :Author: gexinyan
    :CreateTime: 2026-07-20 18:00:00
    Example: normalize_entity_status("FAILED", True)
    """
    if has_data:
        return "existing"
    text = normalize_text(raw_status).casefold()
    if any(value in text for value in FAILED_STATUSES):
        return "failed"
    if any(value in text for value in SUCCESS_STATUSES):
        return "success"
    if any(value in text for value in RUNNING_STATUSES):
        return "running"
    return "waiting"


def normalize_boolean(value: Any) -> bool:
    """
    【函数功能】将服务端布尔值安全转换为 Python 布尔值，避免字符串 false 被误判为真。
    :param value: Any，布尔值、数字或字符串
    :return: bool，标准布尔值
    :Author: gexinyan
    :CreateTime: 2026-07-21 09:30:00
    Example: normalize_boolean("false")
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return normalize_text(value).casefold() in {"1", "true", "yes", "y", "是"}


def entity_error_message(value: dict[str, Any]) -> str:
    """
    【函数功能】从不同版本的服务端企业对象中提取错误信息。
    :param value: dict[str, Any]，根企业或队列节点对象
    :return: str，规范化错误信息
    :Author: gexinyan
    :CreateTime: 2026-07-21 09:30:00
    Example: entity_error_message({"errorMessage": "timeout"})
    """
    return normalize_text(
        value.get("errorMessage")
        or value.get("error")
        or value.get("errorSummary")
        or value.get("failureReason")
        or value.get("message")
        or value.get("msg")
    )


def queue_item_raw_status(value: dict[str, Any]) -> str:
    """
    【函数功能】兼容队列节点的单状态和遍历、采集双状态字段并保留原始审计值。
    :param value: dict[str, Any]，关联扩展队列节点
    :return: str，可用于标准化和审计的原始状态文本
    :Author: gexinyan
    :CreateTime: 2026-07-21 09:30:00
    Example: queue_item_raw_status({"traversalStatus": "EXPANDED", "collectionStatus": "COMPLETED"})
    """
    direct_status = normalize_text(value.get("queryStatus") or value.get("status"))
    if direct_status:
        return direct_status
    parts = [
        f"traversalStatus={normalize_text(value.get('traversalStatus'))}"
        if normalize_text(value.get("traversalStatus"))
        else "",
        f"collectionStatus={normalize_text(value.get('collectionStatus'))}"
        if normalize_text(value.get("collectionStatus"))
        else "",
    ]
    return "; ".join(part for part in parts if part)


def queue_item_has_data(value: dict[str, Any]) -> bool:
    """
    【函数功能】识别队列节点显式 hasData 或服务端数据库复用来源。
    :param value: dict[str, Any]，关联扩展队列节点
    :return: bool，节点已有可复用企业数据时返回 True
    :Author: gexinyan
    :CreateTime: 2026-07-21 09:30:00
    Example: queue_item_has_data({"source": "DATABASE_REUSE"})
    """
    source = normalize_text(value.get("source")).upper()
    return normalize_boolean(value.get("hasData", False)) or source in {
        "DATABASE_REUSE",
        "CURRENT_RUN_REUSE",
    }


def response_data(result: HttpResult) -> dict[str, Any]:
    """
    【函数功能】提取爬虫 HTTP 响应中 data 字段的字典内容。
    :param result: HttpResult，HTTP 响应结果
    :return: dict[str, Any]，响应 data 字段；结构不匹配时返回空字典
    :Author: gexinyan
    :CreateTime: 2026-07-20 18:00:00
    Example: response_data(HttpResult(200, "", {"data": {}}))
    """
    body = result.json_body
    if not isinstance(body, dict):
        return {}
    data = body.get("data")
    return data if isinstance(data, dict) else {}


def _status_counts(items: Iterable[SpiderTaskResult]) -> dict[str, int]:
    """
    【函数功能】统计企业结果中的成功、失败和已有数据数量。
    :param items: Iterable[SpiderTaskResult]，企业结果集合
    :return: dict[str, int]，按标准状态聚合的数量
    :Author: gexinyan
    :CreateTime: 2026-07-20 18:00:00
    Example: _status_counts([])
    """
    counts = {"success": 0, "failed": 0, "existing": 0}
    for item in items:
        if item.status in {"timeout", "empty_result"}:
            counts["failed"] += 1
        elif item.status in counts:
            counts[item.status] += 1
    return counts


def _result_audits(result: SpiderTaskResult) -> tuple[dict[str, Any], ...]:
    """
    【函数功能】生成企业结果的完整审计记录，供跨根企业去重时合并。
    :param result: SpiderTaskResult，单次服务端企业结果
    :return: tuple[dict[str, Any], ...]，至少包含一条审计记录
    :Author: gexinyan
    :CreateTime: 2026-07-21 09:30:00
    Example: _result_audits(SpiderTaskResult("a.pdf", "企业A", "企业A", "success", ""))
    """
    if result.audit_results:
        return result.audit_results
    return (
        {
            "runId": result.run_id,
            "sourcePdf": result.source_pdf,
            "requestKeyword": result.request_keyword,
            "queryStatus": result.raw_status,
            "errorMessage": result.message,
            "hasData": result.has_data,
            "expansionStatus": result.expansion_status,
        },
    )


def _merge_spider_results(current: SpiderTaskResult, incoming: SpiderTaskResult) -> SpiderTaskResult:
    """
    【函数功能】合并同一企业的多根来源结果，并完整保留每次服务调用审计信息。
    :param current: SpiderTaskResult，已聚合结果
    :param incoming: SpiderTaskResult，新发现的重复企业结果
    :return: SpiderTaskResult，合并后的规范化结果
    :Author: gexinyan
    :CreateTime: 2026-07-21 09:30:00
    Example: _merge_spider_results(current, incoming)
    """
    status_priority = {
        "existing": 6,
        "success": 5,
        "failed": 4,
        "timeout": 3,
        "empty_result": 2,
        "running": 1,
        "waiting": 0,
    }
    preferred = incoming if status_priority.get(incoming.status, -1) > status_priority.get(current.status, -1) else current
    messages = [message for message in (current.message, incoming.message) if message]
    related_sources = tuple(sorted(set(current.related_sources) | set(incoming.related_sources)))
    audit_results = _result_audits(current) + tuple(
        audit for audit in _result_audits(incoming) if audit not in _result_audits(current)
    )
    return replace(
        preferred,
        message="; ".join(dict.fromkeys(messages)),
        attempts=current.attempts + incoming.attempts,
        poll_count=current.poll_count + incoming.poll_count,
        database_rows=max(current.database_rows, incoming.database_rows),
        effective_fields=preferred.effective_fields or current.effective_fields or incoming.effective_fields,
        run_id=current.run_id or incoming.run_id,
        raw_status=preferred.raw_status or current.raw_status or incoming.raw_status,
        has_data=current.has_data or incoming.has_data,
        related_sources=related_sources,
        company_id=current.company_id or incoming.company_id,
        expansion_status=preferred.expansion_status or current.expansion_status or incoming.expansion_status,
        audit_results=audit_results,
    )


def consolidate_spider_results(results: Iterable[SpiderTaskResult]) -> list[SpiderTaskResult]:
    """
    【函数功能】按企业类型优先使用稳定企业 ID、缺失时使用规范化名称去重并合并来源。
    :param results: Iterable[SpiderTaskResult]，待聚合企业结果
    :return: list[SpiderTaskResult]，保持首次出现顺序的去重结果
    :Author: gexinyan
    :CreateTime: 2026-07-21 09:30:00
    Example: consolidate_spider_results(results)
    """
    consolidated: list[SpiderTaskResult] = []
    id_indexes: dict[tuple[str, str], int] = {}
    name_indexes: dict[tuple[str, str], int] = {}
    for result in results:
        company_type = result.company_type or "root"
        company_id = normalize_text(result.company_id).casefold()
        company_name = normalize_text(result.company_name).casefold()
        index = id_indexes.get((company_type, company_id)) if company_id else None
        if index is None and company_name:
            name_index = name_indexes.get((company_type, company_name))
            if not company_id:
                index = name_index
            elif name_index is not None and not consolidated[name_index].company_id:
                index = name_index
        if index is None:
            index = len(consolidated)
            consolidated.append(result)
        else:
            consolidated[index] = _merge_spider_results(consolidated[index], result)
        merged = consolidated[index]
        merged_id = normalize_text(merged.company_id).casefold()
        merged_name = normalize_text(merged.company_name).casefold()
        if merged_id:
            id_indexes[(company_type, merged_id)] = index
        if merged_name:
            name_indexes[(company_type, merged_name)] = index
    return consolidated


class SpiderClient:
    """
    【类功能】封装企业爬虫提交、状态轮询与可选数据库回查。
    :Attributes:
        config: SpiderConfig，HTTP 调用配置
        verifier: Verifier | None，可选企业信息回查函数
        sleep: Callable[[float], None]，可替换的等待函数
        monotonic: Callable[[], float]，可替换的单调时间函数
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    """

    def __init__(
        self,
        config: SpiderConfig,
        verifier: Verifier | None = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        run_progress_callback: Callable[[RunProgress], None] | None = None,
    ) -> None:
        """
        【方法功能】初始化爬虫客户端并校验提交模式。
        :param config: SpiderConfig，爬虫调用配置
        :param verifier: Verifier | None，可选数据库回查函数
        :param sleep: Callable[[float], None]，等待函数
        :param monotonic: Callable[[], float]，单调时间函数
        :return: None
        :raises ValueError: 提交模式非法时抛出
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        if config.submit_mode not in {"single", "batch"}:
            raise ValueError("爬虫提交模式仅支持 single 或 batch")
        if config.relation_expansion_depth < 0:
            raise ValueError("relation_expansion_depth 不能小于 0")
        self.config = config
        self.verifier = verifier
        self.sleep = sleep
        self.monotonic = monotonic
        self.run_progress_callback = run_progress_callback

    def crawl_document(self, source_pdf: str, company_names: Iterable[str]) -> list[SpiderTaskResult]:
        """
        【方法功能】按配置模式提交一个 PDF 提取出的企业名称并回查每家企业状态。
        :param source_pdf: str，触发爬虫的 PDF 路径
        :param company_names: Iterable[str]，已从单个 PDF 提取的企业名称
        :return: list[SpiderTaskResult]，每家企业的爬取结果
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        names = deduplicate_names(company_names)
        if not names:
            return []
        if self.config.submit_mode == "batch":
            return self._submit_keyword(source_pdf, names, ",".join(names))
        results: list[SpiderTaskResult] = []
        for name in names:
            results.extend(self._submit_keyword(source_pdf, [name], name))
        return results

    def _submit_keyword(
        self,
        source_pdf: str,
        names: list[str],
        keyword: str,
    ) -> list[SpiderTaskResult]:
        """
        【方法功能】提交 keyword 并对其中每家企业独立轮询状态。
        :param source_pdf: str，触发爬虫的 PDF 路径
        :param names: list[str]，本次 keyword 包含的企业名称
        :param keyword: str，提交给爬虫接口的 keyword
        :return: list[SpiderTaskResult]，对应企业的结果列表
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        response, attempts = self._request_with_retry(
            self._crawl_url,
            "POST",
            {
                "companyNames": names,
                "fetchDeepInfo": self.config.fetch_deep_info,
                "fetchBiddingDetail": self.config.fetch_bidding_detail,
                "relationExpansionDepth": self.config.relation_expansion_depth,
            },
        )
        if not is_success_response(response):
            message = response.error or truncate_message(response.text) or f"trigger_http_{response.status_code}"
            return [
                SpiderTaskResult(source_pdf, name, keyword, "failed", message, attempts=attempts)
                for name in names
            ]
        run_id = normalize_text(response_data(response).get("runId"))
        if run_id:
            return self._poll_run(source_pdf, names, keyword, run_id, attempts)
        return [self._poll_company(source_pdf, name, keyword, attempts) for name in names]

    def _poll_run(
        self,
        source_pdf: str,
        root_names: list[str],
        keyword: str,
        run_id: str,
        attempts: int,
    ) -> list[SpiderTaskResult]:
        """
        【方法功能】按 runId 轮询根企业及关联企业扩展任务，直至服务端扩展任务结束。
        :param source_pdf: str，触发爬取的 PDF 路径
        :param root_names: list[str，本次提交的根企业名称
        :param keyword: str，兼容旧审计字段的提交名称
        :param run_id: str，服务端关联扩展任务标识
        :param attempts: int，提交请求尝试次数
        :return: list[SpiderTaskResult]，根企业及已返回的关联企业终态结果
        :Author: gexinyan
        :CreateTime: 2026-07-20 18:00:00
        """
        deadline = self.monotonic() + self.config.max_poll_seconds
        poll_count = 0
        latest: RunProgress | None = None
        while self.monotonic() <= deadline:
            response, _ = self._request_with_retry(self._run_url(run_id), "GET", None)
            poll_count += 1
            if not is_success_response(response):
                if latest is not None:
                    return self._finalize_run_results(
                        latest,
                        attempts,
                        poll_count,
                        unresolved_status="failed",
                        unresolved_message=response.error or truncate_message(response.text) or f"run_status_http_{response.status_code}",
                    )
                return [
                    SpiderTaskResult(
                        source_pdf, name, keyword, "failed",
                        response.error or truncate_message(response.text) or f"run_status_http_{response.status_code}",
                        attempts, poll_count, run_id=run_id, company_type="root",
                    )
                    for name in root_names
                ]
            data = response_data(response)
            queue_items = self._read_run_queue(run_id)
            latest = self._build_run_progress(source_pdf, root_names, keyword, run_id, data, queue_items)
            self._emit_run_progress(latest)
            if latest.expansion_status in {"FAILED", "CANCELLED"}:
                return self._finalize_run_results(
                    latest,
                    attempts,
                    poll_count,
                    unresolved_status="failed",
                    unresolved_message=f"relation_expansion_{latest.expansion_status.casefold()}",
                )
            if latest.expansion_status == "COMPLETED" and self._run_entities_terminal(latest):
                return self._finalize_run_results(latest, attempts, poll_count)
            if self.monotonic() + self.config.poll_interval_seconds > deadline:
                break
            self.sleep(self.config.poll_interval_seconds)
        if latest is not None:
            return self._finalize_run_results(
                latest,
                attempts,
                poll_count,
                unresolved_status="timeout",
                unresolved_message="relation_expansion_timeout",
            )
        return [
            SpiderTaskResult(source_pdf, name, keyword, "timeout", "relation_expansion_timeout", attempts, poll_count, run_id=run_id, company_type="root")
            for name in root_names
        ]

    def _read_run_queue(self, run_id: str) -> list[dict[str, Any]]:
        """
        【方法功能】读取 runId 关联扩展队列中的全部企业节点快照。
        :param run_id: str，服务端关联扩展任务标识
        :return: list[dict[str, Any]]，队列节点列表；接口不可用时返回空列表
        :Author: gexinyan
        :CreateTime: 2026-07-20 18:00:00
        """
        result: list[dict[str, Any]] = []
        page_num = 1
        while True:
            response, _ = self._request_with_retry(self._queue_url(run_id, page_num), "GET", None)
            if not is_success_response(response):
                return result
            data = response_data(response)
            items = data.get("items")
            if not isinstance(items, list):
                return result
            result.extend(item for item in items if isinstance(item, dict))
            total = int(data.get("total") or len(result))
            page_size = max(1, int(data.get("pageSize") or 100))
            if len(result) >= total or not items:
                return result
            page_num += 1
            if page_num > max(1, (total + page_size - 1) // page_size):
                return result

    def _build_run_progress(
        self,
        source_pdf: str,
        root_names: list[str],
        keyword: str,
        run_id: str,
        data: dict[str, Any],
        queue_items: list[dict[str, Any]],
    ) -> RunProgress:
        """
        【方法功能】将 runId 状态与队列节点转换为前端可聚合的根企业和关联企业进度快照。
        :param source_pdf: str，触发爬取的 PDF 路径
        :param root_names: list[str，本次提交根企业名称
        :param keyword: str，兼容旧审计字段的提交名称
        :param run_id: str，服务端关联扩展任务标识
        :param data: dict[str, Any]，运行状态接口 data 内容
        :param queue_items: list[dict[str, Any]]，队列接口节点内容
        :return: RunProgress，规范化运行进度
        :Author: gexinyan
        :CreateTime: 2026-07-20 18:00:00
        """
        root_rows_value = data.get("roots")
        root_rows: list[dict[str, Any]] = (
            [row for row in root_rows_value if isinstance(row, dict)]
            if isinstance(root_rows_value, list)
            else []
        )
        rows_by_name = {
            normalize_text(row.get("companyName") or row.get("name")).casefold(): row
            for row in root_rows
            if isinstance(row, dict) and normalize_text(row.get("companyName") or row.get("name"))
        }
        queue_root_rows = []
        for item in queue_items:
            try:
                if int(item.get("depth", item.get("currentDepth", -1))) == 0:
                    queue_root_rows.append(item)
            except (TypeError, ValueError):
                continue
        root_entities: list[SpiderTaskResult] = []
        for index, name in enumerate(root_names):
            row: dict[str, Any] | None = rows_by_name.get(normalize_text(name).casefold())
            if row is None:
                row = next(
                    (
                        item for item in root_rows
                        if isinstance(item, dict) and item.get("rootOrder") == index
                    ),
                    root_rows[index] if index < len(root_rows) and isinstance(root_rows[index], dict) else {},
                )
            name_key = normalize_text(name).casefold()
            queue_row = next(
                (
                    item for item in queue_root_rows
                    if normalize_text(item.get("rootCompany") or item.get("rootCompanyName")).casefold() == name_key
                    or normalize_text(item.get("companyName") or item.get("name")).casefold() == name_key
                ),
                {},
            )
            raw_status = queue_item_raw_status(queue_row) or normalize_text(
                row.get("queryStatus") or row.get("status") or data.get("rootStatus")
            )
            has_data = queue_item_has_data(queue_row) or normalize_boolean(row.get("hasData", False)) or any(
                marker in raw_status.casefold() for marker in ("reuse", "existing", "已存在", "复用")
            )
            status = normalize_entity_status(raw_status, has_data)
            root_entities.append(
                SpiderTaskResult(
                    source_pdf,
                    name,
                    keyword,
                    status,
                    entity_error_message(queue_row) or entity_error_message(row),
                    run_id=run_id,
                    company_type="root",
                    raw_status=raw_status,
                    has_data=has_data,
                    company_id=normalize_text(
                        queue_row.get("companyId")
                        or queue_row.get("enterpriseId")
                        or queue_row.get("entId")
                        or queue_row.get("creditCode")
                        or row.get("companyId")
                        or row.get("enterpriseId")
                        or row.get("creditCode")
                    ),
                )
            )
        related_entities = self._queue_entities(source_pdf, keyword, run_id, queue_items, set(root_names))
        entities = tuple(root_entities + related_entities)
        root_counts = _status_counts(root_entities)
        related_counts = _status_counts(related_entities)
        if not related_entities:
            root_existing = root_counts["existing"]
            related_counts["existing"] = max(0, int(data.get("databaseReuseCount") or 0) - root_existing)
            related_counts["success"] = max(0, int(data.get("crawlSuccessCount") or 0) - root_counts["success"])
            related_counts["failed"] = max(0, int(data.get("failedCount") or 0) - root_counts["failed"])
            related_total = max(
                related_counts["success"] + related_counts["failed"] + related_counts["existing"] + int(data.get("waitingNodes") or 0),
                max(0, int(data.get("totalNodes") or 0) - len(root_entities)),
            )
        else:
            related_total = len(related_entities)
        expansion_status = normalize_text(data.get("expansionStatus") or data.get("status") or "WAITING").upper()
        return RunProgress(run_id, len(root_entities), root_counts["success"], root_counts["failed"], root_counts["existing"], related_total, related_counts["success"], related_counts["failed"], related_counts["existing"], expansion_status, entities)

    def _queue_entities(self, source_pdf: str, keyword: str, run_id: str, items: list[dict[str, Any]], root_names: set[str]) -> list[SpiderTaskResult]:
        """
        【方法功能】从关联扩展队列提取去重后的第一层关联企业结果。
        :param source_pdf: str，触发爬取的 PDF 路径
        :param keyword: str，兼容旧审计字段的提交名称
        :param run_id: str，服务端关联扩展任务标识
        :param items: list[dict[str, Any]]，队列节点列表
        :param root_names: set[str]，根企业名称集合
        :return: list[SpiderTaskResult]，关联企业结果
        :Author: gexinyan
        :CreateTime: 2026-07-20 18:00:00
        """
        result: list[SpiderTaskResult] = []
        seen: set[str] = set()
        root_keys = {normalize_text(name).casefold() for name in root_names}
        for item in items:
            depth = item.get("depth", item.get("currentDepth", item.get("relationDepth", 1)))
            try:
                if int(depth) != 1:
                    continue
            except (TypeError, ValueError):
                continue
            name = normalize_text(item.get("companyName") or item.get("name"))
            stable_id = normalize_text(
                item.get("companyId")
                or item.get("enterpriseId")
                or item.get("entId")
                or item.get("creditCode")
                or item.get("id")
            )
            key = stable_id or name.casefold()
            if not name or not key or name.casefold() in root_keys or key in seen:
                continue
            seen.add(key)
            raw_status = queue_item_raw_status(item)
            has_data = queue_item_has_data(item) or any(
                marker in raw_status.casefold() for marker in ("reuse", "existing", "已存在", "复用")
            )
            sources = self._related_sources(item, root_names)
            result.append(
                SpiderTaskResult(
                    source_pdf,
                    name,
                    keyword,
                    normalize_entity_status(raw_status, has_data),
                    entity_error_message(item),
                    run_id=run_id,
                    company_type="related",
                    raw_status=raw_status,
                    has_data=has_data,
                    related_sources=sources,
                    company_id=stable_id,
                )
            )
        return result

    def _related_sources(self, item: dict[str, Any], root_names: set[str]) -> tuple[str, ...]:
        """
        【方法功能】从关联节点提取根企业来源，字段缺失时回退到本次提交的根企业集合。
        :param item: dict[str, Any]，关联企业队列节点
        :param root_names: set[str]，本次 runId 根企业集合
        :return: tuple[str, ...]，排序去重后的来源企业名称
        :Author: gexinyan
        :CreateTime: 2026-07-21 09:30:00
        """
        sources: set[str] = set()
        for key in (
            "rootCompanyName",
            "rootCompany",
            "sourceRootCompany",
            "sourceCompanyName",
            "relatedFrom",
            "parentCompanyName",
            "parentCompany",
            "discoveredByCompanyName",
        ):
            value = normalize_text(item.get(key))
            if value:
                sources.add(value)
        for key in ("rootCompanyNames", "sourceRootCompanies", "relatedSources"):
            values = item.get(key)
            if isinstance(values, list):
                sources.update(normalize_text(value) for value in values if normalize_text(value))
        return tuple(sorted(sources or root_names))

    def _run_entities_terminal(self, progress: RunProgress) -> bool:
        """
        【方法功能】确认根企业和已发现关联企业数量齐全且均进入终态。
        :param progress: RunProgress，当前 runId 快照
        :return: bool，全部企业进入 success、failed 或 existing 时返回 True
        :Author: gexinyan
        :CreateTime: 2026-07-21 09:30:00
        """
        expected_total = progress.root_total + progress.related_total
        return len(progress.entities) >= expected_total and all(
            item.status in TERMINAL_ENTITY_STATUSES for item in progress.entities
        )

    def _finalize_run_results(
        self,
        progress: RunProgress,
        attempts: int,
        poll_count: int,
        unresolved_status: str = "",
        unresolved_message: str = "",
    ) -> list[SpiderTaskResult]:
        """
        【方法功能】为已结束的关联扩展任务补齐请求尝试次数和轮询次数。
        :param progress: RunProgress，终态运行快照
        :param attempts: int，提交尝试次数
        :param poll_count: int，轮询次数
        :param unresolved_status: str，非终态企业的强制结束状态
        :param unresolved_message: str，非终态企业的结束原因
        :return: list[SpiderTaskResult]，终态企业结果
        :Author: gexinyan
        :CreateTime: 2026-07-20 18:00:00
        """
        results: list[SpiderTaskResult] = []
        final_expansion_status = "FAILED" if unresolved_status else progress.expansion_status
        for item in progress.entities:
            update_status = unresolved_status if unresolved_status and item.status not in TERMINAL_ENTITY_STATUSES else item.status
            update_message = item.message
            if update_status != item.status and unresolved_message:
                update_message = "; ".join(part for part in (item.message, unresolved_message) if part)
            audit_source = replace(item, expansion_status=progress.expansion_status)
            results.append(
                replace(
                    item,
                    status=update_status,
                    message=update_message,
                    attempts=attempts,
                    poll_count=poll_count,
                    expansion_status=final_expansion_status,
                    audit_results=_result_audits(audit_source),
                )
            )
        roots = [item for item in results if item.company_type == "root"]
        related = [item for item in results if item.company_type == "related"]
        root_counts = _status_counts(roots)
        related_counts = _status_counts(related)
        self._emit_run_progress(
            RunProgress(
                progress.run_id,
                progress.root_total,
                root_counts["success"],
                root_counts["failed"],
                root_counts["existing"],
                progress.related_total,
                related_counts["success"],
                related_counts["failed"],
                related_counts["existing"],
                final_expansion_status,
                tuple(results),
            )
        )
        return results

    def _emit_run_progress(self, progress: RunProgress) -> None:
        """
        【方法功能】向调度器发送关联扩展过程中的结构化进度快照。
        :param progress: RunProgress，当前 runId 快照
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-20 18:00:00
        """
        if self.run_progress_callback is not None:
            self.run_progress_callback(progress)

    def _poll_company(
        self,
        source_pdf: str,
        company_name: str,
        keyword: str,
        attempts: int,
    ) -> SpiderTaskResult:
        """
        【方法功能】轮询单家企业的爬虫状态，完成后执行可选数据库回查。
        :param source_pdf: str，触发爬虫的 PDF 路径
        :param company_name: str，待轮询企业名称
        :param keyword: str，原始提交 keyword
        :param attempts: int，提交请求尝试次数
        :return: SpiderTaskResult，单企业爬虫结果
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        deadline = self.monotonic() + self.config.max_poll_seconds
        poll_count = 0
        last_message = ""
        while self.monotonic() <= deadline:
            query = parse.urlencode({"keyword": company_name})
            response, _ = self._request_with_retry(f"{self._status_url}?{query}", "GET", None)
            poll_count += 1
            last_message = response.error or truncate_message(response.text)
            if not is_success_response(response):
                return SpiderTaskResult(
                    source_pdf,
                    company_name,
                    keyword,
                    "failed",
                    last_message or f"status_http_{response.status_code}",
                    attempts,
                    poll_count,
                )
            status = classify_status(response)
            if status == "success":
                return self._verify_result(source_pdf, company_name, keyword, attempts, poll_count, last_message)
            if status == "failed":
                return SpiderTaskResult(
                    source_pdf,
                    company_name,
                    keyword,
                    "failed",
                    last_message or "crawler_reported_failed",
                    attempts,
                    poll_count,
                )
            if self.monotonic() + self.config.poll_interval_seconds > deadline:
                break
            self.sleep(self.config.poll_interval_seconds)
        return SpiderTaskResult(
            source_pdf,
            company_name,
            keyword,
            "timeout",
            last_message or "crawler_status_timeout",
            attempts,
            poll_count,
        )

    def _verify_result(
        self,
        source_pdf: str,
        company_name: str,
        keyword: str,
        attempts: int,
        poll_count: int,
        message: str,
    ) -> SpiderTaskResult:
        """
        【方法功能】回查成功爬虫任务的企业信息表，并区分无数据与回查异常。
        :param source_pdf: str，触发爬虫的 PDF 路径
        :param company_name: str，企业名称
        :param keyword: str，原始提交 keyword
        :param attempts: int，提交请求尝试次数
        :param poll_count: int，状态轮询次数
        :param message: str，状态接口响应摘要
        :return: SpiderTaskResult，带数据库回查信息的结果
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        if self.verifier is None:
            return SpiderTaskResult(source_pdf, company_name, keyword, "success", message, attempts, poll_count)
        try:
            verification = self.verifier(company_name)
        except Exception as exc:  # noqa: BLE001
            return SpiderTaskResult(
                source_pdf,
                company_name,
                keyword,
                "success",
                f"{message}; database_verification_error:{exc}",
                attempts,
                poll_count,
            )
        if verification.row_count <= 0:
            return SpiderTaskResult(
                source_pdf,
                company_name,
                keyword,
                "empty_result",
                message or "crawler_completed_without_visible_company_data",
                attempts,
                poll_count,
            )
        return SpiderTaskResult(
            source_pdf,
            company_name,
            keyword,
            "success",
            message,
            attempts,
            poll_count,
            verification.row_count,
            verification.effective_fields,
        )

    @property
    def _crawl_url(self) -> str:
        """
        【方法功能】生成爬虫提交接口地址。
        :return: str，POST /spider/crawl 地址
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        return self.config.base_url.rstrip("/") + "/spider/crawl"

    @property
    def _status_url(self) -> str:
        """
        【方法功能】生成爬虫状态查询接口地址。
        :return: str，GET /spider/crawl/status 地址
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        return self.config.base_url.rstrip("/") + "/spider/crawl/status"

    def _run_url(self, run_id: str) -> str:
        """
        【方法功能】生成按 runId 查询关联企业扩展状态的接口地址。
        :param run_id: str，服务端关联扩展任务标识
        :return: str，GET /spider/crawl/runs/{runId} 地址
        :Author: gexinyan
        :CreateTime: 2026-07-20 18:00:00
        """
        return self.config.base_url.rstrip("/") + "/spider/crawl/runs/" + parse.quote(run_id, safe="")

    def _queue_url(self, run_id: str, page_num: int) -> str:
        """
        【方法功能】生成关联企业扩展队列分页查询接口地址。
        :param run_id: str，服务端关联扩展任务标识
        :param page_num: int，页码，从 1 开始
        :return: str，GET 队列分页接口地址
        :Author: gexinyan
        :CreateTime: 2026-07-20 18:00:00
        """
        query = parse.urlencode({"pageNum": page_num, "pageSize": 100})
        return self._run_url(run_id) + "/queue?" + query

    def _request_with_retry(
        self,
        url: str,
        method: str,
        payload: dict[str, Any] | None,
    ) -> tuple[HttpResult, int]:
        """
        【方法功能】执行 HTTP 请求，并仅对网络错误或 5xx 响应按配置重试。
        :param url: str，请求地址
        :param method: str，HTTP 方法
        :param payload: dict[str, Any] | None，可选 JSON 请求体
        :return: tuple[HttpResult, int]，最终响应和实际尝试次数
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        attempts = 0
        result = HttpResult(None, "", None, "request_not_started")
        for delay in (*self.config.retry_delays, None):
            attempts += 1
            result = self._request(url, method, payload)
            if not should_retry(result) or delay is None:
                return result, attempts
            self.sleep(delay)
        return result, attempts

    def _request(self, url: str, method: str, payload: dict[str, Any] | None) -> HttpResult:
        """
        【方法功能】发送 UTF-8 JSON 或空请求并读取 HTTP 响应正文。
        :param url: str，请求地址
        :param method: str，HTTP 方法
        :param payload: dict[str, Any] | None，可选 JSON 请求体
        :return: HttpResult，HTTP 响应或网络错误信息
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        data = None
        headers = {"Accept": "application/json, text/event-stream, text/plain, */*"}
        if payload is not None:
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json; charset=utf-8"
        http_request = request.Request(url, data=data, headers=headers, method=method.upper())
        try:
            with request.urlopen(http_request, timeout=self.config.timeout_seconds) as response:
                text = response.read().decode("utf-8", errors="replace")
                return HttpResult(int(response.status), text, parse_json_text(text))
        except HTTPError as exc:
            text = exc.read().decode("utf-8", errors="replace")
            return HttpResult(int(exc.code), text, parse_json_text(text), f"http_{exc.code}")
        except (URLError, TimeoutError, OSError) as exc:
            return HttpResult(None, "", None, str(exc))


class CrawlDispatcher:
    """
    【类功能】在 OCR 主进程收到 PDF 完成事件时按顺序调度企业爬虫任务。
    :Attributes:
        client: SpiderClient，爬虫客户端
        enabled: bool，是否实际调用外部爬虫
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    """

    def __init__(
        self,
        client: SpiderClient | None,
        enabled: bool = True,
        progress_callback: SpiderProgressCallback | None = None,
    ) -> None:
        """
        【方法功能】初始化单线程爬虫调度器，保证默认逐企业提交不会并发压垮服务。
        :param client: SpiderClient | None，爬虫客户端；为空时仅记录跳过任务
        :param enabled: bool，是否启用实际爬虫调用
        :param progress_callback: SpiderProgressCallback|None，接收线程安全进度快照的可选回调
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        self.client = client
        self.enabled = enabled and client is not None
        self._executor = (
            ThreadPoolExecutor(max_workers=1, thread_name_prefix="bidding-spider")
            if self.enabled
            else None
        )
        self._progress_callback = progress_callback
        self._lock = threading.RLock()
        self._futures: list[tuple[str, str, Future[list[SpiderTaskResult]]]] = []
        self._completed: list[SpiderTaskResult] = []
        self._seen_names: set[str] = set()
        self._run_progresses: dict[str, RunProgress] = {}
        self._progress = CrawlProgress()
        self._waited = False
        if self.client is not None:
            self.client.run_progress_callback = self._on_run_progress

    def on_pdf_completed(self, document: Any, _: list[str]) -> None:
        """
        【方法功能】接收 OCR 单 PDF 完成回调并调度此前未提交过的企业名称。
        :param document: Any，含 pdf_path 与 records 属性的 ParsedDocument
        :param _: list[str]，OCR 告警列表，当前调度不修改其内容
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        source_pdf = str(document.pdf_path)
        names = [name for name in extract_company_names(document.records) if self._mark_new_name(name)]
        if not names:
            self._emit_progress()
            return
        with self._lock:
            if self._waited:
                raise RuntimeError("爬虫调度器已经结束，不能再提交企业任务")
            self._progress = replace(
                self._progress,
                discovered=self._progress.discovered + len(names),
                root_total=self._progress.root_total + len(names),
                phase="crawling" if self.enabled else self._progress.phase,
            )
        if not self.enabled or self.client is None or self._executor is None:
            skipped_results = [
                SpiderTaskResult(source_pdf, name, name, "skipped", "spider_disabled")
                for name in names
            ]
            with self._lock:
                self._completed.extend(skipped_results)
                self._progress = replace(
                    self._progress,
                    completed=self._progress.completed + len(skipped_results),
                    skipped=self._progress.skipped + len(skipped_results),
                )
            self._emit_progress()
            return
        for name in names:
            with self._lock:
                self._progress = replace(self._progress, queued=self._progress.queued + 1)
            future = self._executor.submit(self._crawl_company, source_pdf, name)
            with self._lock:
                self._futures.append((source_pdf, name, future))
        self._emit_progress()

    def wait(self) -> list[SpiderTaskResult]:
        """
        【方法功能】等待已调度的爬虫任务完成并收集所有成功或失败结果。
        :return: list[SpiderTaskResult]，按 PDF 调度顺序排列的爬虫结果
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        with self._lock:
            if self._waited:
                return list(self._completed)
            self._progress = replace(self._progress, phase="waiting_for_completion")
        self._emit_progress()
        try:
            for source_pdf, company_name, future in self._futures:
                try:
                    results = future.result()
                except Exception as exc:  # noqa: BLE001
                    results = [
                        SpiderTaskResult(
                            source_pdf,
                            company_name,
                            company_name,
                            "failed",
                            f"crawler_worker_error:{exc}",
                        )
                    ]
                    self._finish_company(results)
                with self._lock:
                    self._completed.extend(results)
            with self._lock:
                self._completed = consolidate_spider_results(self._completed)
                return list(self._completed)
        finally:
            if self._executor is not None:
                self._executor.shutdown(wait=True)
                self._executor = None
            with self._lock:
                self._waited = True
                self._progress = replace(self._progress, phase="completed")
            self._emit_progress()

    def _crawl_company(self, source_pdf: str, company_name: str) -> list[SpiderTaskResult]:
        """
        【方法功能】在线程池唯一工作线程中执行单个企业的爬虫并更新实时进度。
        :param source_pdf: str，触发本次企业爬取的 PDF 路径
        :param company_name: str，待爬取企业名称
        :return: list[SpiderTaskResult]，该企业的爬虫结果
        :Author: gexinyan
        :CreateTime: 2026-07-17 10:30:00
        """
        with self._lock:
            self._progress = replace(
                self._progress,
                queued=max(0, self._progress.queued - 1),
                running=self._progress.running + 1,
                phase="crawling",
            )
        self._emit_progress()
        try:
            if self.client is None:
                results = [
                    SpiderTaskResult(source_pdf, company_name, company_name, "skipped", "spider_disabled")
                ]
            else:
                results = self.client.crawl_document(source_pdf, [company_name])
                if not results:
                    results = [
                        SpiderTaskResult(
                            source_pdf,
                            company_name,
                            company_name,
                            "empty_result",
                            "crawler_returned_no_result",
                        )
                    ]
        except Exception as exc:  # noqa: BLE001
            results = [
                SpiderTaskResult(
                    source_pdf,
                    company_name,
                    company_name,
                    "failed",
                    f"crawler_worker_error:{exc}",
                )
            ]
        self._finish_company(results)
        return results

    def _finish_company(self, results: list[SpiderTaskResult]) -> None:
        """
        【方法功能】将一个企业任务的终态结果折算到线程安全的爬虫进度快照。
        :param results: list[SpiderTaskResult]，单企业爬虫结果列表
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-17 10:30:00
        """
        failed = any(item.status in {"failed", "timeout"} for item in results if item.company_type == "root")
        skipped = any(item.status == "skipped" for item in results)
        with self._lock:
            run_progress_known = any(item.run_id and item.run_id in self._run_progresses for item in results)
            if run_progress_known:
                self._progress = replace(
                    self._progress,
                    running=max(0, self._progress.running - 1),
                    skipped=self._progress.skipped + int(skipped),
                )
            else:
                root_counts = _status_counts(item for item in results if item.company_type == "root")
                related_counts = _status_counts(item for item in results if item.company_type == "related")
                self._progress = replace(
                    self._progress,
                    running=max(0, self._progress.running - 1),
                    completed=self._progress.completed + 1,
                    failed=self._progress.failed + int(failed),
                    skipped=self._progress.skipped + int(skipped),
                    root_success=self._progress.root_success + root_counts["success"],
                    root_failed=self._progress.root_failed + root_counts["failed"],
                    root_existing=self._progress.root_existing + root_counts["existing"],
                    related_success=self._progress.related_success + related_counts["success"],
                    related_failed=self._progress.related_failed + related_counts["failed"],
                    related_existing=self._progress.related_existing + related_counts["existing"],
                )
        self._emit_progress()

    def _on_run_progress(self, run_progress: RunProgress) -> None:
        """
        【方法功能】接收单个 runId 的关联扩展快照并更新全任务根企业、关联企业进度。
        :param run_progress: RunProgress，单个关联扩展任务快照
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-20 18:00:00
        """
        with self._lock:
            self._run_progresses[run_progress.run_id] = run_progress
            runs = list(self._run_progresses.values())
            related_items: list[SpiderTaskResult] = []
            fallback_related_total = 0
            for item in runs:
                entities = [entity for entity in item.entities if entity.company_type == "related"]
                if not entities:
                    fallback_related_total += item.related_total
                related_items.extend(entities)
            related_entities = consolidate_spider_results(related_items)
            related_counts = _status_counts(related_entities)
            related_total = len(related_entities) + fallback_related_total
            related_success = related_counts["success"]
            related_failed = related_counts["failed"]
            related_existing = related_counts["existing"]
            root_success = sum(item.root_success for item in runs)
            root_failed = sum(item.root_failed for item in runs)
            root_existing = sum(item.root_existing for item in runs)
            phases = {item.expansion_status for item in runs}
            expansion_status = (
                "FAILED" if "FAILED" in phases else "RUNNING" if "RUNNING" in phases
                else "WAITING" if "WAITING" in phases else "COMPLETED"
            )
            self._progress = replace(
                self._progress,
                discovered=self._progress.root_total + related_total,
                completed=root_success + root_failed + root_existing + related_success + related_failed + related_existing,
                failed=root_failed + related_failed,
                root_success=root_success,
                root_failed=root_failed,
                root_existing=root_existing,
                related_total=related_total,
                related_success=related_success,
                related_failed=related_failed,
                related_existing=related_existing,
                expansion_status=expansion_status,
            )
        self._emit_progress()

    def _emit_progress(self) -> None:
        """
        【方法功能】将当前爬虫状态副本安全地发送给可选的外部进度回调。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-17 10:30:00
        """
        callback = self._progress_callback
        if callback is None:
            return
        with self._lock:
            snapshot = self._progress.to_dict()
        try:
            callback(snapshot)
        except Exception:  # noqa: BLE001
            return

    def _mark_new_name(self, company_name: str) -> bool:
        """
        【方法功能】判断企业名称是否尚未在当前流水线任务中提交。
        :param company_name: str，企业名称
        :return: bool，首次出现时返回 True
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        key = normalize_text(company_name).casefold()
        with self._lock:
            if not key or key in self._seen_names:
                return False
            self._seen_names.add(key)
            return True


def deduplicate_names(company_names: Iterable[str]) -> list[str]:
    """
    【函数功能】按首次出现顺序去除空企业名称和大小写等价的重复名称。
    :param company_names: Iterable[str]，待去重企业名称集合
    :return: list[str]，规范化后名称列表
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: deduplicate_names(["企业A", "企业A"])
    """
    result: list[str] = []
    seen: set[str] = set()
    for item in company_names:
        name = normalize_text(item)
        key = name.casefold()
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(name)
    return result
