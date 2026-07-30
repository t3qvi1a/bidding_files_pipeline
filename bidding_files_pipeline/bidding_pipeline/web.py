"""
【模块功能】提供 Pipeline Web 页面、后台任务队列、实时日志状态与产物下载接口。

:Author: gexinyan
:CreateTime: 2026-07-16 16:20:00
"""

from __future__ import annotations

import json
import multiprocessing
import os
import queue
import re
import shutil
import signal
import stat
import subprocess
import threading
import time
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from .cli import build_argument_parser, build_pipeline_config
from .runner import PipelineConfig, reconcile_output, run_pipeline
from .spider import SpiderClient


CATEGORIES = (
    "tender_cover",
    "bid_evaluation_report",
    "bid_candidates",
    "award_notice",
    "bid_announcement",
    "bid_list",
    "archive_info",
)
CATEGORY_LABELS = {
    "tender_cover": "投标文件封面",
    "bid_evaluation_report": "评标报告",
    "bid_candidates": "中标候选人公示",
    "award_notice": "中标通知书",
    "bid_announcement": "中标人公告",
    "bid_list": "投标单位名单",
    "archive_info": "备案材料",
}
STAGE_PROGRESS = {1: 8, 2: 38, 3: 62, 4: 76, 5: 90}
MAX_LOG_LINES = 2000
STATE_FILENAME = "job_state.json"
CANCEL_EVENT_DRAIN_SECONDS = 5.0
ACTIVE_STATUSES = {"queued", "running", "cancelling"}
TERMINAL_STATUSES = {"completed", "failed", "cancelled", "interrupted"}


def is_parallel_completion_log(message: str) -> bool:
    """
    【函数功能】识别会与详细 PDF 进度条重复的旧版并行完成提示。
    :param message: str，待判断的 Pipeline 日志正文
    :return: bool，是旧版并行完成提示时返回 True
    :Author: gexinyan
    :CreateTime: 2026-07-17 09:20:59
    Example: is_parallel_completion_log("并行处理完成 1/10 个 PDF。")
    """
    return bool(re.fullmatch(r"并行处理完成\s+\d+/\d+\s+个\s+PDF。?", message.strip()))


def infer_log_progress(logs: list[str], default: int = 0) -> int:
    """
    【函数功能】从阶段日志和已完成 PDF 数量推算任务最近进度。
    :param logs: list[str]，Pipeline 日志列表
    :param default: int，未匹配到进度信息时使用的默认值
    :return: int，0 到 100 的推算进度
    :Author: gexinyan
    :CreateTime: 2026-07-16 18:05:00
    Example: infer_log_progress(["并行处理完成 5/10 个 PDF。"], 8)
    """
    progress = default
    for line in logs:
        stage_match = re.search(r"\[阶段\s+(\d)/5\]", line)
        if stage_match:
            progress = max(progress, STAGE_PROGRESS.get(int(stage_match.group(1)), progress))
        pdf_match = re.search(r"(?:处理完成|完成)\s+(\d+)/(\d+)(?:\s+个\s+PDF)?", line)
        if pdf_match:
            completed, total = int(pdf_match.group(1)), int(pdf_match.group(2))
            if total > 0:
                progress = max(progress, min(36, 8 + int(28 * completed / total)))
    return min(100, max(0, progress))


def _nonnegative_int(value: Any) -> int:
    """
    【函数功能】将外部进度字段安全转换为非负整数。
    :param value: Any，待转换的外部字段值
    :return: int，转换后的非负整数
    :Author: gexinyan
    :CreateTime: 2026-07-17 10:30:00
    Example: _nonnegative_int("3")
    """
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def normalize_pdf_progress(value: Any) -> dict[str, int]:
    """
    【函数功能】校验并标准化 PDF 解析进度，兼容历史状态文件的缺失字段。
    :param value: Any，原始 PDF 进度对象
    :return: dict[str, int]，包含 completed、total、percent 的安全进度对象
    :Author: gexinyan
    :CreateTime: 2026-07-17 10:30:00
    Example: normalize_pdf_progress({"completed": 1, "total": 2})
    """
    payload = value if isinstance(value, dict) else {}
    total = _nonnegative_int(payload.get("total", 0))
    completed = min(_nonnegative_int(payload.get("completed", 0)), total)
    return {
        "completed": completed,
        "total": total,
        "percent": int(completed * 100 / total) if total else 0,
    }


def normalize_spider_progress(value: Any) -> dict[str, Any]:
    """
    【函数功能】校验并标准化企业爬虫动态进度，兼容历史状态文件的缺失字段。
    :param value: Any，原始爬虫进度对象
    :return: dict[str, Any]，包含根企业和关联企业嵌套统计的安全进度对象
    :Author: gexinyan
    :CreateTime: 2026-07-17 10:30:00
    Example: normalize_spider_progress({"discovered": 2, "completed": 1})
    """
    payload = value if isinstance(value, dict) else {}
    discovered = _nonnegative_int(payload.get("discovered", 0))
    completed = min(_nonnegative_int(payload.get("completed", 0)), discovered)
    running = min(_nonnegative_int(payload.get("running", 0)), max(0, discovered - completed))
    queued = min(
        _nonnegative_int(payload.get("queued", 0)),
        max(0, discovered - completed - running),
    )
    def normalize_group(name: str) -> dict[str, int]:
        """
        【函数功能】标准化根企业或关联企业的分类状态计数。
        :param name: str，进度对象中的分组键
        :return: dict[str, int]，总数、成功、失败和已有数据数量
        :Author: gexinyan
        :CreateTime: 2026-07-20 18:00:00
        """
        raw_group = payload.get(name)
        group: dict[str, Any] = raw_group if isinstance(raw_group, dict) else {}
        total = _nonnegative_int(group.get("total", 0))
        success = min(_nonnegative_int(group.get("success", 0)), total)
        failed = min(_nonnegative_int(group.get("failed", 0)), total - success)
        existing = min(_nonnegative_int(group.get("existing", 0)), total - success - failed)
        pending = min(
            _nonnegative_int(group.get("pending", 0)),
            total - success - failed - existing,
        )
        return {
            "total": total,
            "success": success,
            "failed": failed,
            "existing": existing,
            "pending": pending,
        }
    return {
        "discovered": discovered,
        "queued": queued,
        "running": running,
        "completed": completed,
        "failed": min(_nonnegative_int(payload.get("failed", 0)), completed),
        "skipped": min(_nonnegative_int(payload.get("skipped", 0)), completed),
        "phase": str(payload.get("phase", "waiting_for_companies")),
        "root": normalize_group("root"),
        "related": normalize_group("related"),
        "expansionStatus": str(payload.get("expansionStatus", "WAITING")),
    }


@dataclass(slots=True)
class JobState:
    """
    【类功能】保存一个 Web Pipeline 任务的运行状态、日志与下载产物。
    :Attributes:
        job_id: str，任务唯一标识
        input_dir: Path，本次任务输入目录
        output_dir: Path，本次任务输出目录
        status: str，queued/running/cancelling/completed/failed/cancelled/interrupted
        stage: str，当前阶段说明
        progress: int，0 到 100 的进度值
        pdf_progress: dict[str, int]，PDF 解析实时进度
        spider_progress: dict[str, Any]，企业爬虫实时进度
        logs: list[str]，最近运行日志
        artifacts: dict[str, Path]，可下载产物路径
        error: str，失败摘要
        created_at: str，任务创建时间
        completed_at: str，任务完成时间
        source_mode: str，upload/local 输入来源
        category_mode: str，all/include/exclude 类别模式
        categories: tuple[str, ...]，选择的 OCR 类别
        force_ocr: bool，是否忽略 OCR 缓存
        skip_existing_company_info: bool，是否复用数据库已有有效工商信息
        fast_company_timeout: bool，是否在 20 秒后结束慢速外部获取任务
        input_summary: str，可安全展示的输入摘要
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    """

    job_id: str
    input_dir: Path
    output_dir: Path
    status: str = "queued"
    stage: str = "等待执行"
    progress: int = 0
    pdf_progress: dict[str, int] = field(default_factory=lambda: normalize_pdf_progress({}))
    spider_progress: dict[str, Any] = field(default_factory=lambda: normalize_spider_progress({}))
    logs: list[str] = field(default_factory=list)
    artifacts: dict[str, Path] = field(default_factory=dict)
    error: str = ""
    created_at: str = field(default_factory=lambda: datetime.now().astimezone().isoformat())
    completed_at: str = ""
    source_mode: str = ""
    category_mode: str = "all"
    categories: tuple[str, ...] = field(default_factory=tuple)
    force_ocr: bool = False
    skip_existing_company_info: bool = False
    fast_company_timeout: bool = False
    input_summary: str = ""
    spider_runs: list[dict[str, Any]] = field(default_factory=list)
    remote_cancellation: dict[str, Any] = field(default_factory=dict)

    def retry_unavailable_reason(self) -> str:
        """
        【方法功能】判断任务能否使用已保存输入重新执行，并返回不可重试原因。
        :return: str，空字符串表示可以重试，否则为用户可读原因
        :Author: gexinyan
        :CreateTime: 2026-07-17 08:54:27
        """
        if self.status not in TERMINAL_STATUSES:
            return "任务尚未结束，不能重复执行。"
        if self.source_mode not in {"upload", "local"}:
            return "历史任务未保存原始输入与运行参数，请重新选择 ZIP 或服务器路径。"
        if self.category_mode not in {"all", "include", "exclude"}:
            return "任务保存的文件类别模式无效，请重新配置任务。"
        if set(self.categories).difference(CATEGORIES):
            return "任务保存的文件类别已经失效，请重新配置任务。"
        if self.category_mode != "all" and not self.categories:
            return "任务未保存完整的文件类别，请重新配置任务。"
        if not self.input_dir.is_dir():
            return "任务的原始输入目录已不存在，请重新选择输入。"
        return ""

    def to_dict(self) -> dict[str, Any]:
        """
        【方法功能】生成不暴露服务器绝对路径的前端任务状态对象。
        :return: dict[str, Any]，可序列化任务状态
        :Author: gexinyan
        :CreateTime: 2026-07-16 16:20:00
        """
        retry_reason = self.retry_unavailable_reason()
        return {
            "jobId": self.job_id,
            "status": self.status,
            "stage": self.stage,
            "progress": self.progress,
            "pdfProgress": self.pdf_progress,
            "spiderProgress": self.spider_progress,
            "logs": list(self.logs),
            "error": self.error,
            "createdAt": self.created_at,
            "completedAt": self.completed_at,
            "artifacts": sorted(self.artifacts),
            "canRetry": not retry_reason,
            "retryReason": retry_reason,
            "sourceMode": self.source_mode or "legacy",
            "inputSummary": self.input_summary or "未保存原始输入",
            "categoryMode": self.category_mode,
            "categories": list(self.categories),
            "forceOcr": self.force_ocr,
            "skipExistingCompanyInfo": self.skip_existing_company_info,
            "fastCompanyTimeout": self.fast_company_timeout,
            "remoteCancellation": self.remote_cancellation_summary(),
        }

    def to_storage_dict(self) -> dict[str, Any]:
        """
        【方法功能】生成可持久化到服务器磁盘的任务状态，不重复保存日志正文。
        :return: dict[str, Any]，包含输入输出路径和产物路径的任务状态
        :Author: gexinyan
        :CreateTime: 2026-07-16 17:40:00
        """
        return {
            "jobId": self.job_id,
            "inputDir": str(self.input_dir),
            "outputDir": str(self.output_dir),
            "status": self.status,
            "stage": self.stage,
            "progress": self.progress,
            "pdfProgress": self.pdf_progress,
            "spiderProgress": self.spider_progress,
            "artifacts": {name: str(path) for name, path in self.artifacts.items()},
            "error": self.error,
            "createdAt": self.created_at,
            "completedAt": self.completed_at,
            "sourceMode": self.source_mode,
            "categoryMode": self.category_mode,
            "categories": list(self.categories),
            "forceOcr": self.force_ocr,
            "skipExistingCompanyInfo": self.skip_existing_company_info,
            "fastCompanyTimeout": self.fast_company_timeout,
            "inputSummary": self.input_summary,
            "spiderRuns": self.spider_runs,
            "remoteCancellation": self.remote_cancellation,
        }

    @classmethod
    def from_storage_dict(cls, payload: dict[str, Any], logs: list[str]) -> "JobState":
        """
        【方法功能】从磁盘状态和日志文件恢复一个 Web 任务对象。
        :param payload: dict[str, Any]，job_state.json 内容
        :param logs: list[str]，pipeline.log 中恢复的日志
        :return: JobState，恢复后的任务状态
        :Author: gexinyan
        :CreateTime: 2026-07-16 17:40:00
        """
        return cls(
            job_id=str(payload["jobId"]),
            input_dir=Path(str(payload["inputDir"])),
            output_dir=Path(str(payload["outputDir"])),
            status=str(payload.get("status", "failed")),
            stage=str(payload.get("stage", "状态未知")),
            progress=int(payload.get("progress", 0)),
            pdf_progress=normalize_pdf_progress(payload.get("pdfProgress")),
            spider_progress=normalize_spider_progress(payload.get("spiderProgress")),
            logs=logs[-MAX_LOG_LINES:],
            artifacts={
                str(name): Path(str(path))
                for name, path in dict(payload.get("artifacts", {})).items()
            },
            error=str(payload.get("error", "")),
            created_at=str(payload.get("createdAt", "")),
            completed_at=str(payload.get("completedAt", "")),
            source_mode=str(payload.get("sourceMode", "")),
            category_mode=str(payload.get("categoryMode", "all")),
            categories=tuple(str(value) for value in payload.get("categories", [])),
            force_ocr=bool(payload.get("forceOcr", False)),
            skip_existing_company_info=bool(payload.get("skipExistingCompanyInfo", False)),
            fast_company_timeout=bool(payload.get("fastCompanyTimeout", False)),
            input_summary=str(payload.get("inputSummary", "")),
            spider_runs=[dict(item) for item in payload.get("spiderRuns", []) if isinstance(item, dict)],
            remote_cancellation=(
                dict(payload.get("remoteCancellation", {}))
                if isinstance(payload.get("remoteCancellation"), dict)
                else {}
            ),
        )

    def remote_cancellation_summary(self) -> dict[str, Any]:
        """汇总当前任务已知爬虫运行的远程取消状态。

        :return: dict[str, Any]，包含已提交、已取消、已终态、待确认和失败数量
        :Author: gexinyan
        :CreateTime: 2026-07-21 16:10:00
        Example: job.remote_cancellation_summary()
        """
        statuses = [str(item.get("cancelStatus", "")) for item in self.spider_runs]
        return {
            "state": str(self.remote_cancellation.get("state", "not_requested")),
            "submittedCount": len(self.spider_runs),
            "cancelledCount": statuses.count("cancelled"),
            "terminalCount": statuses.count("terminal"),
            "pendingCount": statuses.count("requested"),
            "failedCount": statuses.count("failed"),
            "requestedAt": str(self.remote_cancellation.get("requestedAt", "")),
            "completedAt": str(self.remote_cancellation.get("completedAt", "")),
        }


def execute_pipeline_process(config: PipelineConfig, event_queue: Any, cancel_event: Any) -> None:
    """
    【函数功能】在独立进程组中执行 Pipeline，并把日志和完成状态发送给 Web 主进程。
    :param config: PipelineConfig，完整流水线配置
    :param event_queue: Any，跨进程事件队列
    :return: None
    :Author: gexinyan
    :CreateTime: 2026-07-16 17:40:00
    Example: execute_pipeline_process(config, queue)
    """
    if hasattr(os, "setsid"):
        os.setsid()
    try:
        outcome = run_pipeline(
            config,
            progress_callback=lambda message: event_queue.put(
                {"type": "log", "message": str(message)}
            ),
            structured_progress_callback=lambda event_type, payload: event_queue.put(
                {"type": str(event_type), "progress": dict(payload)}
            ),
            spider_run_submitted_callback=lambda payload: event_queue.put(
                {"type": "spider_run_submitted", "run": dict(payload)}
            ),
            cancel_requested=cancel_event.is_set,
        )
        event_queue.put(
            {
                "type": "completed",
                "runId": outcome.run_id,
                "exitCode": outcome.exit_code,
            }
        )
    except BaseException as exc:  # noqa: BLE001
        event_queue.put({"type": "failed", "error": str(exc)})


class JobManager:
    """
    【类功能】以单工作线程管理 Pipeline 任务，避免服务器同时运行多批 OCR。
    :Attributes:
        work_root: Path，Web 任务工作根目录
        jobs: dict[str, JobState]，任务状态集合
        lock: threading.RLock，任务状态并发锁
        executor: ThreadPoolExecutor，单任务后台执行器
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    """

    def __init__(self, work_root: Path) -> None:
        """
        【方法功能】初始化工作目录和单任务执行队列。
        :param work_root: Path，Web 任务根目录
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 16:20:00
        """
        self.work_root = work_root.resolve()
        self.work_root.mkdir(parents=True, exist_ok=True)
        self.jobs: dict[str, JobState] = {}
        self.lock = threading.RLock()
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="bidding-pipeline")
        self.process_context = multiprocessing.get_context("spawn")
        self.processes: dict[str, Any] = {}
        self.cancel_events: dict[str, Any] = {}
        self.cancel_requests: set[str] = set()
        self.reconciliation_jobs: set[str] = set()
        self._load_jobs()
        for restored_job in list(self.jobs.values()):
            self._start_reconciliation(restored_job)

    def _state_path(self, job: JobState) -> Path:
        """
        【方法功能】计算指定任务的持久化状态文件路径。
        :param job: JobState，目标任务
        :return: Path，job_state.json 路径
        :Author: gexinyan
        :CreateTime: 2026-07-16 17:40:00
        """
        return self.work_root / job.job_id / STATE_FILENAME

    def maintenance_enabled(self) -> bool:
        """
        【方法功能】判断 Web 工作目录是否存在阻止新任务的部署维护标记。
        :return: bool，存在 `.maintenance` 文件时返回 True
        :Author: gexinyan
        :CreateTime: 2026-07-21 14:30:00
        """
        return (self.work_root / ".maintenance").is_file()

    def _read_logs(self, output_dir: Path) -> list[str]:
        """
        【方法功能】从任务输出目录恢复最近的 Pipeline 日志。
        :param output_dir: Path，任务输出目录
        :return: list[str]，最多 MAX_LOG_LINES 行日志
        :Author: gexinyan
        :CreateTime: 2026-07-16 17:40:00
        """
        log_path = output_dir / "pipeline.log"
        if not log_path.is_file():
            return []
        return [
            line
            for line in log_path.read_text(encoding="utf-8", errors="replace").splitlines()
            if not is_parallel_completion_log(line.split("] ", 1)[-1])
        ][-MAX_LOG_LINES:]

    def _collect_artifacts(self, output_dir: Path) -> dict[str, Path]:
        """
        【方法功能】收集任务目录中当前已经生成的可下载产物。
        :param output_dir: Path，任务输出目录
        :return: dict[str, Path]，存在的 CSV、JSON、PDF 和日志路径
        :Author: gexinyan
        :CreateTime: 2026-07-16 17:40:00
        """
        candidates = {
            "csv": output_dir / "final.csv",
            "risk_json": output_dir / "risk_records.json",
            "risk_report": output_dir / "risk_report.pdf",
            "log": output_dir / "pipeline.log",
        }
        return {name: path for name, path in candidates.items() if path.is_file()}

    def _load_jobs(self) -> None:
        """
        【方法功能】启动服务时从 job_state.json 或旧版输出目录恢复历史任务。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 17:40:00
        """
        for job_dir in sorted(self.work_root.iterdir()):
            if not job_dir.is_dir() or job_dir.name == "uploads":
                continue
            state_path = job_dir / STATE_FILENAME
            try:
                job: JobState | None
                if state_path.is_file():
                    payload = json.loads(state_path.read_text(encoding="utf-8"))
                    output_dir = Path(str(payload.get("outputDir", job_dir / "output")))
                    job = JobState.from_storage_dict(payload, self._read_logs(output_dir))
                else:
                    job = self._load_legacy_job(job_dir)
                if job is None:
                    continue
                job.artifacts = self._collect_artifacts(job.output_dir)
                job.progress = infer_log_progress(job.logs, job.progress)
                if job.status in ACTIVE_STATUSES:
                    job.status = "interrupted"
                    job.stage = "服务重启，任务已中断"
                    job.error = "任务执行期间 Web 服务被重启，原任务进程已结束。"
                    job.completed_at = datetime.now().astimezone().isoformat()
                self.jobs[job.job_id] = job
                self._persist_job(job)
            except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
                continue

    def _load_legacy_job(self, job_dir: Path) -> JobState | None:
        """
        【方法功能】从旧版 web_runs 目录恢复没有 job_state.json 的历史任务。
        :param job_dir: Path，旧版任务根目录
        :return: JobState | None，可识别时返回任务，否则返回 None
        :Author: gexinyan
        :CreateTime: 2026-07-16 17:40:00
        """
        output_dir = job_dir / "output"
        manifest_path = output_dir / "run_manifest.json"
        log_path = output_dir / "pipeline.log"
        if not manifest_path.is_file() and not log_path.is_file():
            return None
        status = "interrupted"
        stage = "历史任务未完整结束"
        error = "旧版任务未找到完成清单。"
        completed_at = ""
        if manifest_path.is_file():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            error = str(manifest.get("error", ""))
            status = "failed" if error else "completed"
            stage = "执行失败" if error else "全部流程执行完成"
            completed_at = datetime.fromtimestamp(
                manifest_path.stat().st_mtime
            ).astimezone().isoformat()
        logs = self._read_logs(output_dir)
        return JobState(
            job_id=job_dir.name,
            input_dir=Path("."),
            output_dir=output_dir,
            status=status,
            stage=stage,
            progress=100 if status == "completed" else infer_log_progress(logs),
            logs=logs,
            artifacts=self._collect_artifacts(output_dir),
            error=error,
            created_at=datetime.fromtimestamp(job_dir.stat().st_ctime).astimezone().isoformat(),
            completed_at=completed_at,
        )

    def _persist_job(self, job: JobState) -> None:
        """
        【方法功能】原子写入任务状态，确保刷新或服务重启后仍可恢复记录。
        :param job: JobState，待持久化任务
        :return: None
        :raises OSError: 状态目录或文件无法写入时抛出
        :Author: gexinyan
        :CreateTime: 2026-07-16 17:40:00
        """
        state_path = self._state_path(job)
        state_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = state_path.with_suffix(".tmp")
        temp_path.write_text(
            json.dumps(job.to_storage_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temp_path.replace(state_path)

    def create_job(
        self,
        input_dir: Path,
        category_mode: str,
        categories: tuple[str, ...],
        force_ocr: bool,
        skip_existing_company_info: bool = False,
        fast_company_timeout: bool = False,
        source_mode: str = "",
        input_summary: str = "",
    ) -> JobState:
        """
        【方法功能】创建任务状态并提交后台执行队列。
        :param input_dir: Path，已校验的 PDF 输入目录
        :param category_mode: str，all/include/exclude 类别模式
        :param categories: tuple[str, ...]，选择的 OCR 类别
        :param force_ocr: bool，是否忽略 OCR 缓存
        :param skip_existing_company_info: bool，是否复用数据库已有有效工商信息
        :param fast_company_timeout: bool，是否在 20 秒后结束慢速外部获取任务
        :param source_mode: str，upload/local 输入来源
        :param input_summary: str，可安全展示的输入摘要
        :return: JobState，已进入队列的任务状态
        :Author: gexinyan
        :CreateTime: 2026-07-16 16:20:00
        """
        if self.maintenance_enabled():
            raise ValueError("系统处于爬虫部署维护模式，暂不接受新任务")
        job_id = uuid.uuid4().hex
        output_dir = self.work_root / job_id / "output"
        job = JobState(
            job_id=job_id,
            input_dir=input_dir.resolve(),
            output_dir=output_dir,
            source_mode=source_mode,
            category_mode=category_mode,
            categories=categories,
            force_ocr=force_ocr,
            skip_existing_company_info=skip_existing_company_info,
            fast_company_timeout=fast_company_timeout,
            input_summary=input_summary,
        )
        with self.lock:
            self.jobs[job_id] = job
            self._persist_job(job)
        self.executor.submit(
            self._run_job,
            job,
            category_mode,
            categories,
            force_ocr,
            skip_existing_company_info,
            fast_company_timeout,
        )
        return job

    def retry_job(self, job_id: str, allowed_roots: tuple[Path, ...]) -> JobState:
        """
        【方法功能】校验历史任务的输入与配置，并创建一个全新的重试任务。
        :param job_id: str，原任务唯一标识
        :param allowed_roots: tuple[Path, ...]，服务器本地路径允许根目录
        :return: JobState，使用原配置创建的新任务
        :raises KeyError: 原任务不存在时抛出
        :raises ValueError: 原任务不可重试或输入越界时抛出
        :Author: gexinyan
        :CreateTime: 2026-07-17 08:54:27
        """
        with self.lock:
            source_job = self.jobs[job_id]
            unavailable_reason = source_job.retry_unavailable_reason()
            if unavailable_reason:
                raise ValueError(unavailable_reason)
            input_dir = source_job.input_dir.resolve()
            source_mode = source_job.source_mode
            category_mode = source_job.category_mode
            categories = source_job.categories
            force_ocr = source_job.force_ocr
            skip_existing_company_info = source_job.skip_existing_company_info
            fast_company_timeout = source_job.fast_company_timeout
            input_summary = source_job.input_summary
        if source_mode == "upload":
            upload_root = (self.work_root / "uploads").resolve()
            if not input_dir.is_relative_to(upload_root):
                raise ValueError("任务保存的上传目录不安全，请重新上传 ZIP 压缩包。")
        else:
            input_dir = validate_local_input(str(input_dir), allowed_roots)
        if not any(input_dir.rglob("*.pdf")):
            raise ValueError("任务的原始输入目录中已没有 PDF 文件，请重新选择输入。")
        return self.create_job(
            input_dir,
            category_mode,
            categories,
            force_ocr,
            skip_existing_company_info,
            fast_company_timeout,
            source_mode=source_mode,
            input_summary=input_summary,
        )

    def get_job(self, job_id: str) -> JobState:
        """
        【方法功能】按任务标识读取任务状态。
        :param job_id: str，任务唯一标识
        :return: JobState，任务状态
        :raises KeyError: 任务不存在时抛出
        :Author: gexinyan
        :CreateTime: 2026-07-16 16:20:00
        """
        with self.lock:
            return self.jobs[job_id]

    def get_latest_job(self) -> JobState:
        """
        【方法功能】优先返回正在执行的最新任务，否则返回创建时间最新的历史任务。
        :return: JobState，最新任务
        :raises KeyError: 当前没有任何任务时抛出
        :Author: gexinyan
        :CreateTime: 2026-07-16 17:40:00
        """
        with self.lock:
            if not self.jobs:
                raise KeyError("当前没有任务")
            active_jobs = [job for job in self.jobs.values() if job.status in ACTIVE_STATUSES]
            candidates = active_jobs or list(self.jobs.values())
            return max(candidates, key=lambda job: job.created_at)

    def cancel_job(self, job_id: str) -> JobState:
        """
        【方法功能】请求中止排队或运行中的任务，并持久化中止状态。
        :param job_id: str，任务唯一标识
        :return: JobState，更新后的任务状态
        :raises KeyError: 任务不存在时抛出
        :raises ValueError: 任务已经结束时抛出
        :Author: gexinyan
        :CreateTime: 2026-07-16 17:40:00
        """
        with self.lock:
            job = self.jobs[job_id]
            if job.status in TERMINAL_STATUSES:
                raise ValueError("任务已经结束，无法中止")
            self.cancel_requests.add(job_id)
            if job.status == "queued":
                job.status = "cancelled"
                job.stage = "任务已中止"
                job.completed_at = datetime.now().astimezone().isoformat()
            else:
                cancel_event = self.cancel_events.get(job_id)
                if cancel_event is not None:
                    cancel_event.set()
                job.status = "cancelling"
                job.stage = f"正在停止本地解析与 {len(job.spider_runs)} 个已提交爬虫运行"
                job.remote_cancellation = {
                    **job.remote_cancellation,
                    "state": "cancelling",
                    "requestedAt": datetime.now().astimezone().isoformat(),
                }
            self._persist_job(job)
        self._add_log(job, "收到用户中止请求，正在停止当前解析进程。")
        return job

    def _add_log(self, job: JobState, message: str) -> None:
        """
        【方法功能】追加带时间戳日志、更新阶段进度并持久化日志文件。
        :param job: JobState，目标任务
        :param message: str，Pipeline 日志文本
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 16:20:00
        """
        text = str(message).strip()
        if not text:
            return
        if is_parallel_completion_log(text):
            return
        timestamp = datetime.now().strftime("%H:%M:%S")
        line = f"[{timestamp}] {text}"
        stage_match = re.search(r"\[阶段\s+(\d)/5\]", text)
        with self.lock:
            job.logs.append(line)
            if len(job.logs) > MAX_LOG_LINES:
                del job.logs[:-MAX_LOG_LINES]
            if stage_match:
                stage_number = int(stage_match.group(1))
                job.progress = max(job.progress, STAGE_PROGRESS.get(stage_number, job.progress))
                job.stage = re.sub(r"^\[阶段\s+\d/5\]\s*", "", text)
            job.progress = infer_log_progress([text], job.progress)
            self._persist_job(job)
        job.output_dir.mkdir(parents=True, exist_ok=True)
        with (job.output_dir / "pipeline.log").open("a", encoding="utf-8") as stream:
            stream.write(line + "\n")

    def _update_structured_progress(self, job: JobState, event_type: str, value: Any) -> None:
        """
        【方法功能】接收子进程结构化进度事件并持久化双进度条需要的任务状态。
        :param job: JobState，正在执行的任务
        :param event_type: str，pdf_progress 或 spider_progress
        :param value: Any，来自子进程的原始进度对象
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-17 10:30:00
        """
        with self.lock:
            if job.status != "running":
                return
            if event_type == "pdf_progress":
                job.pdf_progress = normalize_pdf_progress(value)
            elif event_type == "spider_progress":
                job.spider_progress = normalize_spider_progress(value)
                if job.spider_progress.get("phase") == "existing_data_only":
                    job.stage = "已使用数据库已有工商信息，跳过获取"
            else:
                return
            self._persist_job(job)

    def _record_spider_run(self, job: JobState, value: Any) -> None:
        """持久化子进程已提交到爬虫服务的 runId，供取消操作逐个终止。

        :param job: JobState，当前 Web Pipeline 任务
        :param value: Any，子进程上报的 runId、企业和来源 PDF 信息
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-21 16:10:00
        Example: manager._record_spider_run(job, {"runId": "run-1"})
        """
        payload = value if isinstance(value, dict) else {}
        run_id = str(payload.get("runId", "")).strip()
        if not run_id:
            return
        with self.lock:
            existing = next((item for item in job.spider_runs if item.get("runId") == run_id), None)
            if existing is None:
                job.spider_runs.append(
                    {
                        "runId": run_id,
                        "sourcePdf": str(payload.get("sourcePdf", "")),
                        "companyNames": [str(name) for name in payload.get("companyNames", []) if str(name)],
                        "submittedAt": str(payload.get("submittedAt", "")),
                        "cancelStatus": "",
                        "expansionStatus": "",
                        "cancelMessage": "",
                    }
                )
                self._persist_job(job)
        self._add_log(job, f"已登记爬虫运行：{run_id}")

    def _cancel_remote_spider_runs(self, job: JobState, config: PipelineConfig) -> None:
        """调用爬虫服务取消当前任务登记的全部未终态 runId 并持久化审计结果。

        :param job: JobState，正在取消的 Web Pipeline 任务
        :param config: PipelineConfig，用于读取当前爬虫服务地址与超时配置
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-21 16:10:00
        Example: manager._cancel_remote_spider_runs(job, config)
        """
        client = SpiderClient(config.spider)
        with self.lock:
            run_ids = [
                str(item.get("runId", ""))
                for item in job.spider_runs
                if item.get("runId") and item.get("cancelStatus") not in {"cancelled", "terminal"}
            ]
        for run_id in dict.fromkeys(run_ids):
            result = client.cancel_run(run_id)
            with self.lock:
                record = next((item for item in job.spider_runs if item.get("runId") == run_id), None)
                if record is None:
                    continue
                record["cancelStatus"] = str(result.get("cancelStatus", "failed"))
                record["expansionStatus"] = str(result.get("expansionStatus", ""))
                record["cancelMessage"] = str(result.get("message", ""))
                record["cancelAttempts"] = _nonnegative_int(result.get("attempts", 0))
                record["cancelledAt"] = datetime.now().astimezone().isoformat()
                self._persist_job(job)
            self._add_log(
                job,
                f"爬虫运行 {run_id} 取消结果：{result.get('cancelStatus', 'failed')}"
                + (f"（{result.get('expansionStatus')}）" if result.get("expansionStatus") else ""),
            )
        with self.lock:
            summary = job.remote_cancellation_summary()
            job.remote_cancellation = {
                **job.remote_cancellation,
                "state": "completed" if not summary["pendingCount"] and not summary["failedCount"] else "pending",
                "completedAt": datetime.now().astimezone().isoformat(),
            }
            self._persist_job(job)

    def _drain_cancellation_events(self, job: JobState, event_queue: Any) -> None:
        """在强制结束子进程前短暂接收在途提交返回的 runId 事件。

        :param job: JobState，正在取消的 Web Pipeline 任务
        :param event_queue: Any，Pipeline 子进程事件队列
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-21 16:10:00
        Example: manager._drain_cancellation_events(job, event_queue)
        """
        deadline = time.monotonic() + CANCEL_EVENT_DRAIN_SECONDS
        while time.monotonic() < deadline:
            try:
                event = event_queue.get(timeout=min(0.25, max(0.01, deadline - time.monotonic())))
            except queue.Empty:
                continue
            if str(event.get("type", "")) == "spider_run_submitted":
                self._record_spider_run(job, event.get("run"))

    def _run_job(
        self,
        job: JobState,
        category_mode: str,
        categories: tuple[str, ...],
        force_ocr: bool,
        skip_existing_company_info: bool,
        fast_company_timeout: bool,
    ) -> None:
        """
        【方法功能】构建 Pipeline 配置、执行任务并登记最终产物。
        :param job: JobState，待运行任务
        :param category_mode: str，类别筛选模式
        :param categories: tuple[str, ...]，选择的 OCR 类别
        :param force_ocr: bool，是否忽略 OCR 缓存
        :param skip_existing_company_info: bool，是否复用数据库已有有效工商信息
        :param fast_company_timeout: bool，是否在 20 秒后结束慢速外部获取任务
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 16:20:00
        """
        with self.lock:
            if job.job_id in self.cancel_requests or job.status == "cancelled":
                self.cancel_requests.discard(job.job_id)
                return
            job.status = "running"
            job.stage = "正在初始化"
            job.progress = 2
            self._persist_job(job)
        event_queue: Any = None
        process: Any = None
        cancel_event: Any = None
        try:
            config = build_web_pipeline_config(
                job.input_dir,
                job.output_dir,
                category_mode,
                categories,
                force_ocr,
                skip_existing_company_info,
                fast_company_timeout,
            )
            event_queue = self.process_context.Queue()
            cancel_event = self.process_context.Event()
            process = self.process_context.Process(
                target=execute_pipeline_process,
                args=(config, event_queue, cancel_event),
                name=f"bidding-pipeline-{job.job_id[:8]}",
            )
            process.start()
            with self.lock:
                self.processes[job.job_id] = process
                self.cancel_events[job.job_id] = cancel_event
            while True:
                if job.job_id in self.cancel_requests:
                    cancel_event.set()
                    self._drain_cancellation_events(job, event_queue)
                    self._cancel_remote_spider_runs(job, config)
                    self._terminate_process(process)
                    self._mark_cancelled(job)
                    return
                try:
                    event = event_queue.get(timeout=0.25)
                except queue.Empty:
                    if process.is_alive():
                        continue
                    process.join(timeout=1)
                    try:
                        event = event_queue.get(timeout=0.5)
                    except queue.Empty:
                        self._fail_job(job, f"Pipeline 子进程异常退出，退出码：{process.exitcode}")
                        return
                event_type = str(event.get("type", ""))
                if event_type == "log":
                    self._add_log(job, str(event.get("message", "")))
                    continue
                if event_type == "spider_run_submitted":
                    self._record_spider_run(job, event.get("run"))
                    continue
                if event_type in {"pdf_progress", "spider_progress"}:
                    self._update_structured_progress(job, event_type, event.get("progress"))
                    continue
                if event_type == "completed":
                    process.join(timeout=2)
                    self._complete_job(
                        job,
                        str(event.get("runId", "")),
                        int(event.get("exitCode", 1)),
                    )
                    return
                if event_type == "failed":
                    process.join(timeout=2)
                    self._fail_job(job, str(event.get("error", "未知错误")))
                    return
        except Exception as exc:  # noqa: BLE001
            self._fail_job(job, str(exc))
        finally:
            with self.lock:
                self.processes.pop(job.job_id, None)
                self.cancel_events.pop(job.job_id, None)
                self.cancel_requests.discard(job.job_id)
            if event_queue is not None:
                event_queue.close()

    def _terminate_process(self, process: Any) -> None:
        """
        【方法功能】终止 Pipeline 独立进程及其 OCR 子进程，避免留下孤儿进程。
        :param process: Any，multiprocessing.Process 对象
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 17:40:00
        """
        if process is None or not process.is_alive():
            return
        if os.name == "posix":
            try:
                get_process_group = getattr(os, "getpgid")
                kill_process_group = getattr(os, "killpg")
                process_group = get_process_group(process.pid)
                if process_group == process.pid:
                    kill_process_group(process_group, signal.SIGTERM)
                else:
                    process.terminate()
            except ProcessLookupError:
                return
        else:
            subprocess.run(
                ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                check=False,
                capture_output=True,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        process.join(timeout=5)
        if process.is_alive():
            if os.name == "posix":
                try:
                    kill_process_group = getattr(os, "killpg")
                    kill_process_group(process.pid, getattr(signal, "SIGKILL", signal.SIGTERM))
                except ProcessLookupError:
                    pass
            else:
                process.kill()
            process.join(timeout=2)

    def _mark_cancelled(self, job: JobState) -> None:
        """
        【方法功能】将已停止进程的任务标记为已中止并保留部分产物。
        :param job: JobState，已中止任务
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 17:40:00
        """
        with self.lock:
            job.artifacts = self._collect_artifacts(job.output_dir)
            job.status = "cancelled"
            summary = job.remote_cancellation_summary()
            job.stage = (
                "任务已中止"
                f"（远程爬虫：已取消 {summary['cancelledCount']}，已终态 {summary['terminalCount']}，"
                f"待确认 {summary['pendingCount']}，失败 {summary['failedCount']}）"
            )
            job.completed_at = datetime.now().astimezone().isoformat()
            self._persist_job(job)
        self._add_log(job, "当前 Pipeline 任务已由用户中止。")

    def _fail_job(self, job: JobState, error: str) -> None:
        """
        【方法功能】记录 Pipeline 失败状态、错误信息和已经生成的部分产物。
        :param job: JobState，失败任务
        :param error: str，失败原因
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 17:40:00
        """
        self._add_log(job, f"[ERROR] {error}")
        with self.lock:
            job.artifacts = self._collect_artifacts(job.output_dir)
            job.status = "failed"
            job.stage = "执行失败"
            job.error = error
            job.completed_at = datetime.now().astimezone().isoformat()
            self._persist_job(job)

    def _complete_job(self, job: JobState, run_id: str, exit_code: int) -> None:
        """
        【方法功能】登记成功任务的 CSV、风险 JSON、PDF 与日志产物。
        :param job: JobState，已执行任务
        :param run_id: str，Pipeline 运行标识
        :param exit_code: int，Pipeline 退出码
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 17:40:00
        """
        with self.lock:
            job.artifacts = self._collect_artifacts(job.output_dir)
            job.status = "completed"
            job.stage = "全部流程执行完成" if exit_code == 0 else "流程完成，存在爬虫警告"
            job.progress = 100
            job.completed_at = datetime.now().astimezone().isoformat()
            self._persist_job(job)
        self._add_log(job, f"任务完成，流水线运行标识：{run_id}")
        self._start_reconciliation(job)

    def _start_reconciliation(self, job: JobState) -> None:
        """
        【方法功能】为含待对账爬虫结果的任务启动可跨 Web 重启恢复的后台复查线程。
        :param job: JobState，可能包含临时爬虫结果的任务
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-21 14:30:00
        """
        crawl_path = job.output_dir / "crawl_results.json"
        if not crawl_path.is_file():
            return
        try:
            payload = json.loads(crawl_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        if payload.get("crawlFinality") != "provisional" or int(payload.get("pendingCount", 0)) <= 0:
            return
        with self.lock:
            if job.job_id in self.reconciliation_jobs:
                return
            self.reconciliation_jobs.add(job.job_id)
        worker = threading.Thread(
            target=self._reconcile_until_final,
            args=(job.job_id,),
            name=f"spider-reconcile-{job.job_id[:8]}",
            daemon=True,
        )
        worker.start()

    def _reconcile_until_final(self, job_id: str) -> None:
        """
        【方法功能】周期复查待对账企业，并在全部终态后刷新任务产物和页面状态。
        :param job_id: str，Web 任务标识
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-21 14:30:00
        """
        try:
            while True:
                with self.lock:
                    job = self.jobs.get(job_id)
                if job is None or job.status in {"cancelled", "failed"}:
                    return
                interval = 30.0
                manifest_path = job.output_dir / "run_manifest.json"
                try:
                    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                    interval = max(
                        1.0,
                        float(
                            manifest.get("config", {})
                            .get("spider", {})
                            .get("reconcileIntervalSeconds", interval)
                        ),
                    )
                except (OSError, ValueError, TypeError, json.JSONDecodeError):
                    pass
                try:
                    audit = reconcile_output(job.output_dir)
                    pending = int(audit.get("pendingCount", 0))
                    with self.lock:
                        job.artifacts = self._collect_artifacts(job.output_dir)
                        job.stage = "爬虫结果待对账" if pending else "全部流程及爬虫对账完成"
                        self._persist_job(job)
                    if pending == 0:
                        self._add_log(job, "企业爬虫二次对账完成，风险 JSON、Markdown 和 PDF 已刷新。")
                        return
                except Exception as exc:  # noqa: BLE001
                    self._add_log(job, f"企业爬虫二次对账暂未完成：{exc}")
                time.sleep(interval)
        finally:
            with self.lock:
                self.reconciliation_jobs.discard(job_id)


def configured_allowed_roots() -> tuple[Path, ...]:
    """
    【函数功能】读取服务器本地路径允许根目录，未配置时使用工作区投标文件目录。
    :return: tuple[Path, ...]，去重后的允许根目录
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    Example: configured_allowed_roots()
    """
    configured = os.getenv("BIDDING_ALLOWED_INPUT_ROOTS", "")
    if configured:
        candidates = [Path(value).expanduser() for value in configured.split(os.pathsep) if value.strip()]
    else:
        workspace = Path(__file__).resolve().parents[2]
        candidates = [workspace / "biding_files", workspace / "bidding_files"]
    return tuple(dict.fromkeys(path.resolve() for path in candidates if path.exists()))


def validate_local_input(path_value: str, allowed_roots: tuple[Path, ...]) -> Path:
    """
    【函数功能】校验服务器本地输入目录位于配置允许根目录内。
    :param path_value: str，用户输入的服务器路径
    :param allowed_roots: tuple[Path, ...]，允许访问的根目录
    :return: Path，解析后的有效目录
    :raises ValueError: 路径不存在、不是目录或越过允许根目录时抛出
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    Example: validate_local_input("/data/bids", (Path("/data"),))
    """
    path = Path(path_value).expanduser().resolve()
    if not path.is_dir():
        raise ValueError(f"服务器输入目录不存在或不是目录：{path}")
    if not any(path == root or path.is_relative_to(root) for root in allowed_roots):
        raise ValueError("服务器输入目录不在 BIDDING_ALLOWED_INPUT_ROOTS 允许范围内")
    return path


def extract_zip_safely(archive_path: Path, destination: Path) -> Path:
    """
    【函数功能】拒绝路径穿越与符号链接后安全解压 ZIP 文件。
    :param archive_path: Path，ZIP 压缩包路径
    :param destination: Path，解压目标目录
    :return: Path，解压后的输入根目录
    :raises ValueError: 文件不是 ZIP 或成员路径不安全时抛出
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    Example: extract_zip_safely(Path("bids.zip"), Path("input"))
    """
    if not zipfile.is_zipfile(archive_path):
        raise ValueError("仅支持标准 ZIP 压缩包")
    destination = destination.resolve()
    destination.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive_path) as archive:
        max_extracted_bytes = int(
            os.getenv("BIDDING_MAX_EXTRACTED_BYTES", str(10 * 1024 * 1024 * 1024))
        )
        extracted_bytes = sum(info.file_size for info in archive.infolist() if not info.is_dir())
        if extracted_bytes > max_extracted_bytes:
            raise ValueError(f"压缩包解压后超过限制：{max_extracted_bytes} 字节")
        for info in archive.infolist():
            member_path = (destination / info.filename).resolve()
            mode = info.external_attr >> 16
            if not member_path.is_relative_to(destination) or stat.S_ISLNK(mode):
                raise ValueError(f"压缩包包含不安全路径：{info.filename}")
            if info.is_dir():
                member_path.mkdir(parents=True, exist_ok=True)
                continue
            member_path.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(info) as source, member_path.open("wb") as target:
                shutil.copyfileobj(source, target)
    if not any(destination.rglob("*.pdf")):
        raise ValueError("压缩包中未找到 PDF 文件")
    return destination


async def save_uploaded_archive(upload: UploadFile, job_root: Path) -> Path:
    """
    【函数功能】分块保存上传 ZIP，执行大小限制并安全解压。
    :param upload: UploadFile，前端上传文件
    :param job_root: Path，本次上传临时工作目录
    :return: Path，解压后的 PDF 输入目录
    :raises ValueError: 文件过大、扩展名错误或压缩包不安全时抛出
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    Example: await save_uploaded_archive(upload, Path("uploads/job"))
    """
    filename = upload.filename or ""
    if Path(filename).suffix.casefold() != ".zip":
        raise ValueError("上传文件必须是 .zip 压缩包")
    max_bytes = int(os.getenv("BIDDING_MAX_UPLOAD_BYTES", str(2 * 1024 * 1024 * 1024)))
    job_root.mkdir(parents=True, exist_ok=True)
    archive_path = job_root / "input.zip"
    total = 0
    with archive_path.open("wb") as stream:
        while chunk := await upload.read(1024 * 1024):
            total += len(chunk)
            if total > max_bytes:
                stream.close()
                archive_path.unlink(missing_ok=True)
                raise ValueError(f"上传文件超过限制：{max_bytes} 字节")
            stream.write(chunk)
    await upload.close()
    return extract_zip_safely(archive_path, job_root / "input")


def build_web_pipeline_config(
    input_dir: Path,
    output_dir: Path,
    category_mode: str,
    categories: tuple[str, ...],
    force_ocr: bool,
    skip_existing_company_info: bool = False,
    fast_company_timeout: bool = False,
) -> Any:
    """
    【函数功能】复用 CLI 参数规则构建 Web 任务 PipelineConfig。
    :param input_dir: Path，PDF 输入目录
    :param output_dir: Path，任务输出目录
    :param category_mode: str，all/include/exclude 类别模式
    :param categories: tuple[str, ...]，选择类别
    :param force_ocr: bool，是否忽略 OCR 缓存
    :param skip_existing_company_info: bool，是否复用数据库已有有效工商信息
    :param fast_company_timeout: bool，是否在 20 秒后结束慢速外部获取任务
    :return: PipelineConfig，完整流水线配置
    :raises ValueError: 类别模式或类别值不合法时抛出
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    Example: build_web_pipeline_config(Path("input"), Path("output"), "all", (), False)
    """
    if category_mode not in {"all", "include", "exclude"}:
        raise ValueError("文件类别模式不合法")
    unknown = set(categories).difference(CATEGORIES)
    if unknown:
        raise ValueError(f"存在不支持的文件类别：{', '.join(sorted(unknown))}")
    if category_mode != "all" and not categories:
        raise ValueError("include/exclude 模式至少选择一个文件类别")
    arguments = ["run", "--input", str(input_dir), "--output", str(output_dir)]
    if category_mode in {"include", "exclude"}:
        arguments.extend([f"--{category_mode}", ",".join(categories)])
    if force_ocr:
        arguments.append("--force")
    args = build_argument_parser().parse_args(arguments)
    return replace(
        build_pipeline_config(args),
        skip_existing_company_info=skip_existing_company_info,
        fast_company_timeout=fast_company_timeout,
    )


def create_app(work_root: Path | None = None) -> FastAPI:
    """
    【函数功能】创建带静态前端、任务接口和产物下载接口的 FastAPI 应用。
    :param work_root: Path | None，可选 Web 任务根目录
    :return: FastAPI，已配置应用实例
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    Example: create_app(Path("web_runs"))
    """
    package_root = Path(__file__).resolve().parent
    root = work_root or Path(os.getenv("BIDDING_WEB_WORK_ROOT", package_root.parent / "web_runs"))
    manager = JobManager(root)
    allowed_roots = configured_allowed_roots()
    app = FastAPI(title="投标文件解析与风险分析平台", version="1.0.0")
    app.state.job_manager = manager
    app.state.allowed_roots = allowed_roots
    app.mount("/static", StaticFiles(directory=package_root / "static"), name="static")

    @app.get("/", response_class=HTMLResponse)
    def index() -> HTMLResponse:
        """
        【函数功能】返回 Pipeline Web 主页面。
        :return: HTMLResponse，主页面内容
        :Author: gexinyan
        :CreateTime: 2026-07-16 16:20:00
        """
        return HTMLResponse((package_root / "templates" / "index.html").read_text(encoding="utf-8"))

    @app.get("/api/config")
    def get_config() -> dict[str, Any]:
        """
        【函数功能】返回前端可展示的类别与服务器路径配置。
        :return: dict[str, Any]，非敏感 Web 配置
        :Author: gexinyan
        :CreateTime: 2026-07-16 16:20:00
        """
        return {
            "categories": [{"value": item, "label": CATEGORY_LABELS[item]} for item in CATEGORIES],
            "allowedInputRoots": [str(path) for path in allowed_roots],
            "maintenanceMode": manager.maintenance_enabled(),
        }

    @app.post("/api/jobs", status_code=202)
    async def create_job(
        archive: UploadFile | None = File(default=None),
        local_path: str = Form(default=""),
        category_mode: str = Form(default="all"),
        categories: str = Form(default=""),
        force_ocr: bool = Form(default=False),
        skip_existing_company_info: bool = Form(default=False),
        fast_company_timeout: bool = Form(default=False),
    ) -> dict[str, Any]:
        """
        【函数功能】校验上传或本地路径并创建后台 Pipeline 任务。
        :param archive: UploadFile | None，可选 ZIP 压缩包
        :param local_path: str，可选服务器本地目录
        :param category_mode: str，all/include/exclude 类别模式
        :param categories: str，逗号分隔类别
        :param force_ocr: bool，是否忽略 OCR 缓存
        :param skip_existing_company_info: bool，是否复用数据库已有有效工商信息
        :param fast_company_timeout: bool，是否在 20 秒后结束慢速外部获取任务
        :return: dict[str, Any]，已创建任务状态
        :raises HTTPException: 输入不合法时返回 400
        :Author: gexinyan
        :CreateTime: 2026-07-16 16:20:00
        """
        has_upload = bool(archive and archive.filename)
        has_local_path = bool(local_path.strip())
        if has_upload == has_local_path:
            raise HTTPException(status_code=400, detail="必须且只能选择 ZIP 上传或服务器本地目录之一")
        category_values = tuple(dict.fromkeys(value.strip() for value in categories.split(",") if value.strip()))
        try:
            if has_upload and archive is not None:
                upload_root = manager.work_root / "uploads" / uuid.uuid4().hex
                input_dir = await save_uploaded_archive(archive, upload_root)
                source_mode = "upload"
                input_summary = Path(archive.filename or "input.zip").name
            else:
                input_dir = validate_local_input(local_path, allowed_roots)
                source_mode = "local"
                input_summary = f"服务器目录：{input_dir.name}"
            job = manager.create_job(
                input_dir,
                category_mode,
                category_values,
                force_ocr,
                skip_existing_company_info,
                fast_company_timeout,
                source_mode=source_mode,
                input_summary=input_summary,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return job.to_dict()

    @app.get("/api/jobs/latest")
    def get_latest_job() -> dict[str, Any]:
        """
        【函数功能】返回最新活动任务或最近历史任务，供网页刷新后自动恢复。
        :return: dict[str, Any]，最新任务状态
        :raises HTTPException: 当前没有任务时返回 404
        :Author: gexinyan
        :CreateTime: 2026-07-16 17:40:00
        """
        try:
            return manager.get_latest_job().to_dict()
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="当前没有可恢复的任务") from exc

    @app.get("/api/jobs/{job_id}")
    def get_job(job_id: str) -> dict[str, Any]:
        """
        【函数功能】返回指定任务的实时进度、阶段与日志。
        :param job_id: str，任务唯一标识
        :return: dict[str, Any]，任务状态
        :raises HTTPException: 任务不存在时返回 404
        :Author: gexinyan
        :CreateTime: 2026-07-16 16:20:00
        """
        try:
            return manager.get_job(job_id).to_dict()
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="任务不存在") from exc

    @app.post("/api/jobs/{job_id}/cancel")
    def cancel_job(job_id: str) -> dict[str, Any]:
        """
        【函数功能】请求中止指定 Web Pipeline 任务及其 OCR 子进程。
        :param job_id: str，任务唯一标识
        :return: dict[str, Any]，中止请求后的任务状态
        :raises HTTPException: 任务不存在或已经结束时返回 404/409
        :Author: gexinyan
        :CreateTime: 2026-07-16 17:40:00
        """
        try:
            return manager.cancel_job(job_id).to_dict()
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="任务不存在") from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post("/api/jobs/{job_id}/retry", status_code=202)
    def retry_job(job_id: str) -> dict[str, Any]:
        """
        【函数功能】使用历史任务保存的输入和运行参数创建一个全新任务。
        :param job_id: str，原任务唯一标识
        :return: dict[str, Any]，新创建的任务状态
        :raises HTTPException: 原任务不存在或不可重试时返回 404/409
        :Author: gexinyan
        :CreateTime: 2026-07-17 08:54:27
        """
        try:
            return manager.retry_job(job_id, allowed_roots).to_dict()
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="任务不存在") from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.get("/api/jobs/{job_id}/artifacts/{artifact_name}")
    def download_artifact(job_id: str, artifact_name: str) -> FileResponse:
        """
        【函数功能】下载任务生成的解析 CSV、风险报告、JSON 或日志。
        :param job_id: str，任务唯一标识
        :param artifact_name: str，产物键名
        :return: FileResponse，文件下载响应
        :raises HTTPException: 任务或产物不存在时返回 404
        :Author: gexinyan
        :CreateTime: 2026-07-16 16:20:00
        """
        try:
            job = manager.get_job(job_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="任务不存在") from exc
        artifact = job.artifacts.get(artifact_name)
        if artifact is None or not artifact.is_file():
            raise HTTPException(status_code=404, detail="产物尚未生成或不存在")
        download_names = {
            "csv": "投标文件解析结果.csv",
            "risk_json": "招投标关联风险记录.json",
            "risk_report": "招投标关联风险分析报告.pdf",
            "log": "pipeline运行日志.log",
        }
        return FileResponse(artifact, filename=download_names.get(artifact_name, artifact.name))

    return app


def run_web_server(host: str, port: int) -> None:
    """
    【函数功能】使用 Uvicorn 启动局域网可访问的 Web 服务。
    :param host: str，监听地址，服务器部署应使用 0.0.0.0
    :param port: int，监听端口
    :return: None
    :raises Exception: 服务初始化或监听失败时由 Uvicorn 抛出
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    Example: run_web_server("0.0.0.0", 8096)
    """
    import uvicorn

    uvicorn.run(create_app(), host=host, port=port, log_level="info")
