"""
【模块功能】编排 OCR、企业爬虫、最终 CSV 入库和运行审计。

:Author: gexinyan
:CreateTime: 2026-07-16 10:00:00
"""

from __future__ import annotations

import importlib
import json
import sys
import uuid
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable

from .database import DatabaseConfig, PersistenceSummary, ResultDatabaseWriter, SpiderDataVerifier
from .records import read_final_records
from .reporting import ReportSummary, generate_report
from .risk_analysis import RiskAnalysisSummary, analyze_risks
from .spider import CrawlDispatcher, SpiderClient, SpiderConfig, SpiderTaskResult


ProgressCallback = Callable[[str], None]
StructuredProgressCallback = Callable[[str, dict[str, Any]], None]


@dataclass(frozen=True, slots=True)
class PipelineConfig:
    """
    【类功能】保存一次招投标 PDF 流水线运行所需的全部非交互配置。
    :Attributes:
        input_dir: Path，PDF 输入根目录
        output_dir: Path，OCR 与运行摘要输出目录
        ocr_source: Path，biding_files_ocr_strategies 源码目录
        workers: int，OCR 多进程 worker 数量
        dpi: int，普通 OCR 分辨率
        archive_scan_dpi: int，备案材料粗检分辨率
        ocr_threshold: float，低置信度复核阈值
        force_ocr: bool，是否忽略 OCR 缓存
        category_filter: str | None，单类别筛选
        include_categories: tuple[str, ...] | None，包含类别筛选
        exclude_categories: tuple[str, ...] | None，排除类别筛选
        database: DatabaseConfig，最终结果表连接配置
        spider_result_database: str，爬虫企业信息实际落库数据库名
        spider: SpiderConfig，爬虫 HTTP 配置
        report_template: Path | None，风险报告 Markdown 模板路径
        report_renderer: Path | None，现有 Markdown 转 PDF 脚本路径
        skip_spider: bool，是否跳过外部爬虫
        skip_risk_analysis: bool，是否跳过风险分析和报告生成
        dry_run: bool，是否跳过爬虫和数据库写入
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    """

    input_dir: Path
    output_dir: Path
    ocr_source: Path
    workers: int
    dpi: int
    archive_scan_dpi: int
    ocr_threshold: float
    force_ocr: bool
    category_filter: str | None
    include_categories: tuple[str, ...] | None
    exclude_categories: tuple[str, ...] | None
    database: DatabaseConfig
    spider_result_database: str
    spider: SpiderConfig
    report_template: Path | None = None
    report_renderer: Path | None = None
    skip_spider: bool = False
    skip_risk_analysis: bool = False
    dry_run: bool = False


@dataclass(frozen=True, slots=True)
class OcrApi:
    """
    【类功能】保存从外部 OCR 策略源码动态加载的必要接口。
    :Attributes:
        processing_config_type: Any，ProcessingConfig 类型
        process_pdf_tree: Callable[..., Any]，多进程 PDF 解析入口
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    """

    processing_config_type: Any
    process_pdf_tree: Callable[..., Any]


@dataclass(frozen=True, slots=True)
class RunOutcome:
    """
    【类功能】描述一次 Pipeline 运行完成后的 OCR、爬虫、入库、风险分析和报告汇总。
    :Attributes:
        run_id: str，本次运行唯一标识
        ocr_summary: dict[str, Any]，OCR 汇总原始数据
        final_record_count: int，final.csv 中的记录数
        persistence: PersistenceSummary | None，最终入库汇总；dry-run 时为空
        spider_results: tuple[SpiderTaskResult, ...]，企业爬虫结果集合
        risk_analysis: RiskAnalysisSummary | None，风险分析汇总
        report: ReportSummary | None，风险报告汇总
        dry_run: bool，是否 dry-run
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    """

    run_id: str
    ocr_summary: dict[str, Any]
    final_record_count: int
    persistence: PersistenceSummary | None
    spider_results: tuple[SpiderTaskResult, ...]
    risk_analysis: RiskAnalysisSummary | None
    report: ReportSummary | None
    dry_run: bool

    @property
    def failed_spider_count(self) -> int:
        """
        【方法功能】统计失败、超时或无可见结果的企业爬虫任务数。
        :return: int，非成功且非跳过的爬虫任务数
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        return sum(result.status not in {"success", "existing", "skipped"} for result in self.spider_results)

    @property
    def failed_ocr_count(self) -> int:
        """
        【方法功能】从 OCR 汇总中读取失败 PDF 数量。
        :return: int，OCR 失败 PDF 数
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        return int(self.ocr_summary.get("失败文件数", 0))

    @property
    def exit_code(self) -> int:
        """
        【方法功能】根据 OCR 与爬虫结果生成命令行退出码。
        :return: int，0 为成功，1 为 OCR 失败，2 为仅爬虫部分失败
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        if self.failed_ocr_count > 0:
            return 1
        if self.failed_spider_count > 0:
            return 2
        return 0


def load_ocr_api(source_dir: Path) -> OcrApi:
    """
    【函数功能】从指定策略源码目录加载 ProcessingConfig 与 process_pdf_tree。
    :param source_dir: Path，biding_files_ocr_strategies 根目录
    :return: OcrApi，可调用的 OCR 策略接口
    :raises FileNotFoundError: 未找到 bidding_ocr 包时抛出
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: load_ocr_api(Path("../biding_files_ocr_strategies"))
    """
    resolved = source_dir.resolve()
    package_init = resolved / "bidding_ocr" / "__init__.py"
    if not package_init.is_file():
        raise FileNotFoundError(f"未找到 OCR 策略包 bidding_ocr：{package_init}")
    source_text = str(resolved)
    if source_text not in sys.path:
        sys.path.insert(0, source_text)
    importlib.invalidate_caches()
    package = importlib.import_module("bidding_ocr")
    return OcrApi(
        processing_config_type=getattr(package, "ProcessingConfig"),
        process_pdf_tree=getattr(package, "process_pdf_tree"),
    )


def write_json(path: Path, payload: dict[str, Any]) -> None:
    """
    【函数功能】以 UTF-8 JSON 格式写入运行审计文件。
    :param path: Path，JSON 输出路径
    :param payload: dict[str, Any]，待写入内容
    :return: None
    :raises OSError: 文件无法写入时抛出
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: write_json(Path("run_manifest.json"), {"ok": True})
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def pipeline_config_to_dict(config: PipelineConfig) -> dict[str, Any]:
    """
    【函数功能】生成不包含密码的 Pipeline 配置审计快照。
    :param config: PipelineConfig，本次运行配置
    :return: dict[str, Any]，可安全写入日志的配置字典
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: pipeline_config_to_dict(config)
    """
    return {
        "inputDir": str(config.input_dir),
        "outputDir": str(config.output_dir),
        "ocrSource": str(config.ocr_source),
        "workers": config.workers,
        "dpi": config.dpi,
        "archiveScanDpi": config.archive_scan_dpi,
        "ocrThreshold": config.ocr_threshold,
        "forceOcr": config.force_ocr,
        "category": config.category_filter,
        "include": list(config.include_categories or ()),
        "exclude": list(config.exclude_categories or ()),
        "database": {
            "host": config.database.host,
            "port": config.database.port,
            "database": config.database.database,
            "username": config.database.username,
            "schema": config.database.schema,
            "table": config.database.table,
        },
        "spiderResultDatabase": config.spider_result_database,
        "spider": {
            "baseUrl": config.spider.base_url,
            "submitMode": config.spider.submit_mode,
            "timeoutSeconds": config.spider.timeout_seconds,
            "pollIntervalSeconds": config.spider.poll_interval_seconds,
            "maxPollSeconds": config.spider.max_poll_seconds,
            "retryDelays": list(config.spider.retry_delays),
            "fetchDeepInfo": config.spider.fetch_deep_info,
            "fetchBiddingDetail": config.spider.fetch_bidding_detail,
            "relationExpansionDepth": config.spider.relation_expansion_depth,
        },
        "reportRenderer": str(config.report_renderer) if config.report_renderer else None,
        "reportTemplate": str(config.report_template) if config.report_template else None,
        "skipSpider": config.skip_spider,
        "skipRiskAnalysis": config.skip_risk_analysis,
        "dryRun": config.dry_run,
    }


def build_spider_statistics(spider_results: list[SpiderTaskResult]) -> dict[str, dict[str, int]]:
    """
    【函数功能】按根企业、关联企业和整体生成爬虫状态统计。
    :param spider_results: list[SpiderTaskResult]，已按企业标识去重的爬虫结果
    :return: dict[str, dict[str, int]]，三组总数、成功、失败和已有数据统计
    :Author: gexinyan
    :CreateTime: 2026-07-21 09:30:00
    Example: build_spider_statistics([])
    """
    failure_statuses = {"failed", "timeout", "empty_result"}

    def summarize(items: list[SpiderTaskResult]) -> dict[str, int]:
        """
        【函数功能】汇总单组企业的展示状态计数。
        :param items: list[SpiderTaskResult]，同一企业类型的结果
        :return: dict[str, int]，总数、成功、失败和已有数据数量
        :Author: gexinyan
        :CreateTime: 2026-07-21 09:30:00
        """
        return {
            "total": len(items),
            "success": sum(item.status == "success" for item in items),
            "failed": sum(item.status in failure_statuses for item in items),
            "existing": sum(item.status == "existing" for item in items),
        }

    roots = [item for item in spider_results if item.company_type == "root"]
    related = [item for item in spider_results if item.company_type == "related"]
    return {"root": summarize(roots), "related": summarize(related), "overall": summarize(spider_results)}


def build_crawl_audit(run_id: str, spider_results: list[SpiderTaskResult]) -> dict[str, Any]:
    """
    【函数功能】生成包含服务 runId、扩展状态、分层统计和企业明细的爬虫审计结果。
    :param run_id: str，Pipeline 运行标识
    :param spider_results: list[SpiderTaskResult]，企业爬虫结果
    :return: dict[str, Any]，crawl_results.json 完整内容
    :Author: gexinyan
    :CreateTime: 2026-07-21 09:30:00
    Example: build_crawl_audit("run", [])
    """
    service_run_ids = {item.run_id for item in spider_results if item.run_id}
    expansion_statuses: dict[str, str] = {
        item.run_id: item.expansion_status
        for item in spider_results
        if item.run_id and item.expansion_status
    }
    for item in spider_results:
        for audit in item.audit_results:
            audit_run_id = str(audit.get("runId") or "")
            audit_expansion_status = str(audit.get("expansionStatus") or "")
            if audit_run_id:
                service_run_ids.add(audit_run_id)
                if audit_expansion_status:
                    expansion_statuses[audit_run_id] = audit_expansion_status
    return {
        "runId": run_id,
        "serviceRunIds": sorted(service_run_ids),
        "expansionStatuses": expansion_statuses,
        "statistics": build_spider_statistics(spider_results),
        "results": [item.to_dict() for item in spider_results],
    }


def build_manifest(
    run_id: str,
    config: PipelineConfig,
    ocr_summary: dict[str, Any] | None,
    final_record_count: int,
    spider_results: list[SpiderTaskResult],
    persistence: PersistenceSummary | None,
    risk_analysis: RiskAnalysisSummary | None,
    report: ReportSummary | None,
    error: str = "",
) -> dict[str, Any]:
    """
    【函数功能】组装运行清单，统一记录 OCR、爬虫、入库和异常状态。
    :param run_id: str，本次运行标识
    :param config: PipelineConfig，本次运行配置
    :param ocr_summary: dict[str, Any] | None，OCR 汇总数据
    :param final_record_count: int，final.csv 记录数
    :param spider_results: list[SpiderTaskResult]，爬虫结果
    :param persistence: PersistenceSummary | None，入库结果
    :param risk_analysis: RiskAnalysisSummary | None，风险分析结果
    :param report: ReportSummary | None，报告生成结果
    :param error: str，致命错误摘要
    :return: dict[str, Any]，运行清单内容
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: build_manifest("run", config, {}, 0, [], None, None, None)
    """
    status_counts: dict[str, int] = {}
    company_type_counts: dict[str, dict[str, int]] = {"root": {}, "related": {}}
    for result in spider_results:
        status_counts[result.status] = status_counts.get(result.status, 0) + 1
        type_counts = company_type_counts.setdefault(result.company_type, {})
        type_counts[result.status] = type_counts.get(result.status, 0) + 1
    crawl_audit = build_crawl_audit(run_id, spider_results)
    return {
        "runId": run_id,
        "config": pipeline_config_to_dict(config),
        "ocrSummary": ocr_summary,
        "finalRecordCount": final_record_count,
        "spiderStatusCounts": status_counts,
        "spiderCompanyTypeStatusCounts": company_type_counts,
        "spiderStatistics": build_spider_statistics(spider_results),
        "spiderRunIds": crawl_audit["serviceRunIds"],
        "spiderExpansionStatuses": crawl_audit["expansionStatuses"],
        "spiderResultCount": len(spider_results),
        "persistence": (
            {
                "recordCount": persistence.record_count,
                "schema": persistence.schema,
                "table": persistence.table,
            }
            if persistence
            else None
        ),
        "riskAnalysis": (
            {
                "riskCount": risk_analysis.risk_count,
                "projectCount": risk_analysis.project_count,
                "companyCount": risk_analysis.company_count,
                "unmatchedCompanyCount": risk_analysis.unmatched_company_count,
                "jsonPath": str(risk_analysis.json_path),
            }
            if risk_analysis
            else None
        ),
        "report": (
            {
                "riskCount": report.risk_count,
                "markdownPath": str(report.markdown_path),
                "pdfPath": str(report.pdf_path),
            }
            if report
            else None
        ),
        "error": error,
    }


def run_pipeline(
    config: PipelineConfig,
    progress_callback: ProgressCallback = print,
    structured_progress_callback: StructuredProgressCallback | None = None,
) -> RunOutcome:
    """
    【函数功能】执行 OCR 与企业爬虫并行阶段，再顺序完成入库、风险分析和报告生成。
    :param config: PipelineConfig，完整运行配置
    :param progress_callback: ProgressCallback，终端进度输出函数
    :param structured_progress_callback: StructuredProgressCallback|None，PDF 与爬虫结构化进度接收函数
    :return: RunOutcome，流水线执行汇总
    :raises Exception: OCR、CSV 读取或最终入库失败时抛出
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: run_pipeline(config)
    """
    run_id = str(uuid.uuid4())
    output_dir = config.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    ocr_summary: dict[str, Any] | None = None
    final_record_count = 0
    spider_results: list[SpiderTaskResult] = []
    persistence: PersistenceSummary | None = None
    risk_analysis: RiskAnalysisSummary | None = None
    report: ReportSummary | None = None
    def emit_structured_progress(event_type: str, payload: dict[str, Any]) -> None:
        """
        【函数功能】隔离结构化进度回调异常，避免显示层故障中断流水线。
        :param event_type: str，pdf_progress 或 spider_progress
        :param payload: dict[str, Any]，可序列化的进度快照
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-17 10:30:00
        """
        if structured_progress_callback is None:
            return
        try:
            structured_progress_callback(event_type, payload)
        except Exception:  # noqa: BLE001
            return

    dispatcher = _build_dispatcher(
        config,
        progress_callback=lambda snapshot: emit_structured_progress("spider_progress", snapshot),
    )
    try:
        progress_callback("[阶段 1/5] 开始解析 PDF 文件，企业爬虫将在每份 PDF 解析完成后立即启动。")
        api = load_ocr_api(config.ocr_source)
        processing_config = api.processing_config_type(
            dpi=config.dpi,
            archive_scan_dpi=config.archive_scan_dpi,
            ocr_confidence_threshold=config.ocr_threshold,
            force_ocr=config.force_ocr,
        )
        def dispatch_completed_document(document: Any, warnings: list[str]) -> None:
            """
            【函数功能】将已完成 PDF 中提取到的企业即时提交给后台单线程爬虫。
            :param document: Any，OCR 已解析文档
            :param warnings: list[str]，文档解析警告
            :return: None
            :Author: gexinyan
            :CreateTime: 2026-07-17 10:30:00
            """
            dispatcher.on_pdf_completed(document, warnings)

        summary = api.process_pdf_tree(
            config.input_dir.resolve(),
            output_dir,
            processing_config,
            category_filter=config.category_filter,
            progress_callback=progress_callback,
            include_categories=config.include_categories,
            exclude_categories=config.exclude_categories,
            workers=config.workers,
            pdf_completed_callback=dispatch_completed_document,
            pdf_progress_callback=lambda snapshot: emit_structured_progress("pdf_progress", snapshot),
        )
        ocr_summary = summary.to_dict()
        progress_callback("[阶段 2/5] PDF 解析完成，正在等待企业爬虫任务收尾。")
        spider_results = dispatcher.wait()
        write_json(output_dir / "crawl_results.json", build_crawl_audit(run_id, spider_results))
        final_records = read_final_records(output_dir / "final.csv")
        final_record_count = len(final_records)
        if config.dry_run:
            progress_callback(f"Dry-run 完成：解析到 {final_record_count} 条最终记录，未调用爬虫或数据库。")
        else:
            progress_callback("[阶段 3/5] 企业爬虫完成，开始写入 PDF 解析结果。")
            persistence = ResultDatabaseWriter(config.database).persist(final_records, run_id)
            progress_callback(
                f"最终结果已写入 {persistence.schema}.{persistence.table}：{persistence.record_count} 条。"
            )
            if config.skip_risk_analysis:
                progress_callback("[阶段 4/5] 已按配置跳过风险分析。")
                progress_callback("[阶段 5/5] 已按配置跳过风险报告生成。")
            else:
                progress_callback("[阶段 4/5] 解析结果入库完成，开始查询 openGauss 并分析关联风险。")
                risk_analysis = analyze_risks(
                    config.database,
                    config.spider_result_database,
                    run_id,
                    output_dir / "risk_records.json",
                )
                progress_callback(
                    f"风险分析完成：覆盖 {risk_analysis.project_count} 个标段，发现 {risk_analysis.risk_count} 组风险。"
                )
                progress_callback("[阶段 5/5] 开始生成 Markdown 和 PDF 风险报告。")
                renderer = config.report_renderer or default_report_renderer()
                template = config.report_template or default_report_template()
                report = generate_report(
                    risk_analysis.json_path,
                    output_dir / "risk_report.md",
                    output_dir / "risk_report.pdf",
                    template,
                    renderer,
                )
                progress_callback(f"风险报告已生成：{report.pdf_path}")
        if ocr_summary is None:
            raise RuntimeError("OCR 未返回运行汇总")
        outcome = RunOutcome(
            run_id,
            ocr_summary,
            final_record_count,
            persistence,
            tuple(spider_results),
            risk_analysis,
            report,
            config.dry_run,
        )
        write_json(
            output_dir / "run_manifest.json",
            build_manifest(
                run_id,
                config,
                ocr_summary,
                final_record_count,
                spider_results,
                persistence,
                risk_analysis,
                report,
            ),
        )
        return outcome
    except Exception as exc:
        try:
            spider_results = dispatcher.wait()
            write_json(output_dir / "crawl_results.json", build_crawl_audit(run_id, spider_results))
            write_json(
                output_dir / "run_manifest.json",
                build_manifest(
                    run_id,
                    config,
                    ocr_summary,
                    final_record_count,
                    spider_results,
                    persistence,
                    risk_analysis,
                    report,
                    error=str(exc),
                ),
            )
        except Exception:  # noqa: BLE001
            pass
        raise


def default_report_renderer() -> Path:
    """
    【函数功能】定位工作区相邻风险报告项目中的现有 Markdown 转 PDF 脚本。
    :return: Path，默认报告渲染脚本路径
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    Example: default_report_renderer()
    """
    return Path(__file__).resolve().parents[2] / "biding_files_risk_reports" / "expanded_risk_report_md_to_pdf.py"


def default_report_template() -> Path:
    """
    【函数功能】定位基于现有风险报告样式创建的 Pipeline Markdown 模板。
    :return: Path，默认风险报告模板路径
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    Example: default_report_template()
    """
    return (
        Path(__file__).resolve().parents[2]
        / "biding_files_risk_reports"
        / "expand_risk_reports"
        / "pipeline_risk_reports_template.md"
    )


def _build_dispatcher(
    config: PipelineConfig,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> CrawlDispatcher:
    """
    【函数功能】根据 dry-run、跳过开关和数据库配置创建单线程爬虫调度器。
    :param config: PipelineConfig，运行配置
    :param progress_callback: Callable[[dict[str, Any]], None]|None，爬虫实时进度接收函数
    :return: CrawlDispatcher，已配置的 OCR 完成事件接收器
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: _build_dispatcher(config)
    """
    enabled = not config.skip_spider and not config.dry_run
    if not enabled:
        return CrawlDispatcher(None, enabled=False, progress_callback=progress_callback)
    verifier_config = replace(config.database, database=config.spider_result_database, table="spider_data_company")
    verifier = SpiderDataVerifier(verifier_config).verify
    return CrawlDispatcher(
        SpiderClient(config.spider, verifier=verifier),
        enabled=True,
        progress_callback=progress_callback,
    )
