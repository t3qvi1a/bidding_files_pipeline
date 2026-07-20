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
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    """

    base_url: str
    submit_mode: str = "single"
    timeout_seconds: int = 20
    poll_interval_seconds: float = 5.0
    max_poll_seconds: float = 180.0
    retry_delays: tuple[float, ...] = (5.0, 15.0)


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

    def to_dict(self) -> dict[str, Any]:
        """
        【方法功能】转换为可写入运行摘要 JSON 的字典。
        :return: dict[str, Any]，可 JSON 序列化的爬虫结果
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
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
        }


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

    def to_dict(self) -> dict[str, int | str]:
        """
        【方法功能】转换为可经由进程队列传输的结构化进度数据。
        :return: dict[str, int | str]，爬虫进度快照
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
    if "has_data" in candidates or any(status in joined for status in SUCCESS_STATUSES):
        return "success"
    if any(status in joined for status in FAILED_STATUSES):
        return "failed"
    if any(status in joined for status in RUNNING_STATUSES):
        return "running"
    return "unknown"


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
        self.config = config
        self.verifier = verifier
        self.sleep = sleep
        self.monotonic = monotonic

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
            {"keyword": keyword},
        )
        if not is_success_response(response):
            message = response.error or truncate_message(response.text) or f"trigger_http_{response.status_code}"
            return [
                SpiderTaskResult(source_pdf, name, keyword, "failed", message, attempts=attempts)
                for name in names
            ]
        return [self._poll_company(source_pdf, name, keyword, attempts) for name in names]

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
        self._progress = CrawlProgress()
        self._waited = False

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
        failed = any(item.status in {"failed", "timeout"} for item in results)
        skipped = any(item.status == "skipped" for item in results)
        with self._lock:
            self._progress = replace(
                self._progress,
                running=max(0, self._progress.running - 1),
                completed=self._progress.completed + 1,
                failed=self._progress.failed + int(failed),
                skipped=self._progress.skipped + int(skipped),
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
