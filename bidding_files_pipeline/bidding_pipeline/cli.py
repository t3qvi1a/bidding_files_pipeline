"""
【模块功能】提供招投标 PDF Pipeline 的命令行参数解析与进程退出码控制。

:Author: gexinyan
:CreateTime: 2026-07-16 10:00:00
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

from .database import DatabaseConfig
from .runner import (
    PipelineConfig,
    RunOutcome,
    default_report_renderer,
    default_report_template,
    run_pipeline,
)
from .spider import SpiderConfig


DEFAULT_DB_HOST = "192.168.1.210"
DEFAULT_DB_PORT = 15400
DEFAULT_DB_NAME = "big_data"
DEFAULT_DB_USERNAME = "jwmath"
DEFAULT_DB_SCHEMA = "dwd"
DEFAULT_DB_TABLE = "dwd_bid_extraction_results"
DEFAULT_SPIDER_BASE_URL = "http://192.168.1.166:9081"
DEFAULT_WEB_HOST = "0.0.0.0"
DEFAULT_WEB_PORT = 8096


def load_private_env(path: Path) -> None:
    """
    【函数功能】加载当前项目私有 .env 文件中尚未设置的简单 KEY=VALUE 环境变量。
    :param path: Path，私有 .env 文件路径
    :return: None
    :raises OSError: .env 文件无法读取时抛出
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: load_private_env(Path(".env"))
    """
    if not path.is_file():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ.setdefault(key, value)


def load_default_env_files(project_root: Path, current_dir: Path | None = None) -> None:
    """
    【函数功能】按当前工作目录优先、项目根目录兜底的顺序加载私有 .env 文件。
    :param project_root: Path，Pipeline 项目根目录
    :param current_dir: Path | None，可选当前工作目录，默认读取进程工作目录
    :return: None
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: load_default_env_files(Path("."))
    """
    working_dir = current_dir or Path.cwd()
    load_private_env(working_dir / ".env")
    project_env = project_root / ".env"
    if project_env != working_dir / ".env":
        load_private_env(project_env)


def default_ocr_source() -> Path:
    """
    【函数功能】计算与当前 Pipeline 项目相邻的默认 OCR 策略源码路径。
    :return: Path，默认 biding_files_ocr_strategies 路径
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: default_ocr_source()
    """
    workspace = Path(__file__).resolve().parents[2]
    candidates = (
        workspace / "biding_files_ocr_strategies",
        workspace / "bidding_files_ocr_strategies",
    )
    return next((path for path in candidates if path.is_dir()), candidates[0])


def default_worker_count() -> int:
    """
    【函数功能】计算保留一个 CPU 核后的默认 OCR 多进程数量。
    :return: int，至少为 1 且最多为 4 的 worker 数
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: default_worker_count()
    """
    return max(1, min(4, (os.cpu_count() or 2) - 1))


def positive_int(value: str) -> int:
    """
    【函数功能】解析并校验大于零的命令行整数。
    :param value: str，参数原始文本
    :return: int，校验后的正整数
    :raises argparse.ArgumentTypeError: 参数不是正整数时抛出
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: positive_int("4")
    """
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("必须传入整数") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("必须大于或等于 1")
    return parsed


def non_negative_float(value: str) -> float:
    """
    【函数功能】解析并校验非负浮点数命令行参数。
    :param value: str，参数原始文本
    :return: float，校验后的非负数
    :raises argparse.ArgumentTypeError: 参数非法时抛出
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: non_negative_float("5")
    """
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("必须传入数字") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("不能小于 0")
    return parsed


def parse_category_list(value: str) -> tuple[str, ...]:
    """
    【函数功能】解析英文或中文逗号分隔的 OCR 类别列表并去重。
    :param value: str，类别列表文本
    :return: tuple[str, ...]，按首次出现顺序去重后的类别元组
    :raises argparse.ArgumentTypeError: 参数为空时抛出
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: parse_category_list("award_notice,bid_candidates")
    """
    categories = tuple(
        dict.fromkeys(item.strip() for item in value.replace("，", ",").split(",") if item.strip())
    )
    if not categories:
        raise argparse.ArgumentTypeError("类别列表不能为空")
    return categories


def normalize_host(value: str) -> str:
    """
    【函数功能】兼容用户输入的 URL 或 Markdown 链接形式数据库主机。
    :param value: str，原始主机文本
    :return: str，规范化后的主机名或 IP
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: normalize_host("[192.168.1.210](http://192.168.1.210)")
    """
    text = value.strip()
    if text.startswith("[") and "](" in text and text.endswith(")"):
        text = text.split("](", 1)[1][:-1]
    parsed = urlparse(text)
    return parsed.hostname or text.strip("[]")


def build_argument_parser() -> argparse.ArgumentParser:
    """
    【函数功能】构建 Pipeline 的完整命令行参数解析器。
    :return: argparse.ArgumentParser，已配置 run 子命令的解析器
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: build_argument_parser()
    """
    parser = argparse.ArgumentParser(description="招投标 PDF OCR、企业爬取和 openGauss 入库流水线")
    subparsers = parser.add_subparsers(dest="command", required=True)
    run_parser = subparsers.add_parser("run", help="执行一次 PDF 解析流水线")
    run_parser.add_argument("--input", required=True, help="PDF 输入根目录")
    run_parser.add_argument("--output", default="output", help="OCR 与运行摘要输出目录")
    run_parser.add_argument("--ocr-source", default=str(default_ocr_source()), help="OCR 策略源码目录")
    run_parser.add_argument("--workers", type=positive_int, default=default_worker_count(), help="OCR 多进程数")
    run_parser.add_argument("--dpi", type=positive_int, default=300, help="普通 OCR 分辨率")
    run_parser.add_argument("--archive-scan-dpi", type=positive_int, default=150, help="备案材料粗检分辨率")
    run_parser.add_argument("--ocr-threshold", type=float, default=0.80, help="OCR 复核置信度阈值")
    run_parser.add_argument("--force", action="store_true", help="忽略 OCR 缓存并重新识别")
    category_group = run_parser.add_mutually_exclusive_group()
    category_group.add_argument("--category", default=None, help="仅处理一个 OCR 类别")
    category_group.add_argument("--include", type=parse_category_list, help="仅处理逗号分隔的类别")
    category_group.add_argument("--exclude", type=parse_category_list, help="排除逗号分隔的类别")
    run_parser.add_argument("--skip-spider", action="store_true", help="仅执行 OCR 和最终结果入库，不调用企业爬虫")
    run_parser.add_argument("--dry-run", action="store_true", help="仅执行 OCR，不调用爬虫且不写入数据库")
    run_parser.add_argument(
        "--spider-submit-mode",
        choices=("single", "batch"),
        default=os.getenv("SPIDER_SUBMIT_MODE", "single"),
        help="single 逐企业提交；batch 按单 PDF 用英文逗号拼接提交",
    )
    run_parser.add_argument("--spider-base-url", default=os.getenv("SPIDER_BASE_URL", DEFAULT_SPIDER_BASE_URL))
    run_parser.add_argument("--spider-timeout-seconds", type=positive_int, default=20)
    run_parser.add_argument("--spider-poll-interval-seconds", type=non_negative_float, default=5.0)
    run_parser.add_argument("--spider-max-poll-seconds", type=positive_int, default=180)
    run_parser.add_argument("--database", default=os.getenv("GENERAL_DB_NAME", DEFAULT_DB_NAME))
    run_parser.add_argument("--db-host", default=os.getenv("GENERAL_DB_HOST", DEFAULT_DB_HOST))
    run_parser.add_argument("--db-port", type=positive_int, default=int(os.getenv("GENERAL_DB_PORT", str(DEFAULT_DB_PORT))))
    run_parser.add_argument("--db-user", default=os.getenv("GENERAL_DB_USERNAME", DEFAULT_DB_USERNAME))
    run_parser.add_argument("--db-schema", default=os.getenv("GENERAL_DB_SCHEMA", DEFAULT_DB_SCHEMA))
    run_parser.add_argument("--db-table", default=os.getenv("BID_EXTRACTION_RESULTS_TABLE", DEFAULT_DB_TABLE))
    run_parser.add_argument(
        "--spider-result-database",
        default=os.getenv("SPIDER_RESULT_DB_NAME", DEFAULT_DB_NAME),
        help="爬虫 spider_data_company 实际落库的数据库名",
    )
    run_parser.add_argument(
        "--report-template",
        default=os.getenv("BIDDING_REPORT_TEMPLATE", str(default_report_template())),
        help="基于现有风险报告样式创建的 Markdown 模板路径",
    )
    run_parser.add_argument(
        "--report-renderer",
        default=os.getenv("BIDDING_REPORT_RENDERER", str(default_report_renderer())),
        help="现有 Markdown 转 PDF 渲染脚本路径",
    )
    run_parser.add_argument(
        "--skip-risk-analysis",
        action="store_true",
        help="完成 OCR、爬虫和入库后跳过风险分析及报告生成",
    )
    serve_parser = subparsers.add_parser("serve", help="启动局域网可访问的 Pipeline Web 服务")
    serve_parser.add_argument("--host", default=os.getenv("BIDDING_WEB_HOST", DEFAULT_WEB_HOST))
    serve_parser.add_argument(
        "--port",
        type=positive_int,
        default=int(os.getenv("BIDDING_WEB_PORT", str(DEFAULT_WEB_PORT))),
    )
    return parser


def build_pipeline_config(args: argparse.Namespace) -> PipelineConfig:
    """
    【函数功能】将命令行参数和环境变量转换为类型完整的运行配置。
    :param args: argparse.Namespace，run 子命令解析结果
    :return: PipelineConfig，可直接传给执行器的配置
    :raises ValueError: 非 dry-run 且缺失数据库密码时抛出
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: build_pipeline_config(args)
    """
    password = os.getenv("GENERAL_DB_PASSWORD", "")
    if not args.dry_run and not password:
        raise ValueError("缺少 GENERAL_DB_PASSWORD；请在运行环境或私有 .env 中设置")
    database = DatabaseConfig(
        host=normalize_host(args.db_host),
        port=args.db_port,
        database=args.database,
        username=args.db_user,
        password=password,
        schema=args.db_schema,
        table=args.db_table,
    )
    return PipelineConfig(
        input_dir=Path(args.input),
        output_dir=Path(args.output),
        ocr_source=Path(args.ocr_source),
        workers=args.workers,
        dpi=args.dpi,
        archive_scan_dpi=args.archive_scan_dpi,
        ocr_threshold=args.ocr_threshold,
        force_ocr=args.force,
        category_filter=args.category,
        include_categories=args.include,
        exclude_categories=args.exclude,
        database=database,
        spider_result_database=args.spider_result_database,
        spider=SpiderConfig(
            base_url=args.spider_base_url,
            submit_mode=args.spider_submit_mode,
            timeout_seconds=args.spider_timeout_seconds,
            poll_interval_seconds=args.spider_poll_interval_seconds,
            max_poll_seconds=args.spider_max_poll_seconds,
        ),
        report_template=Path(args.report_template),
        report_renderer=Path(args.report_renderer),
        skip_spider=args.skip_spider,
        skip_risk_analysis=args.skip_risk_analysis,
        dry_run=args.dry_run,
    )


def outcome_to_dict(outcome: RunOutcome) -> dict[str, object]:
    """
    【函数功能】将运行结果转换为终端可输出的简要 JSON。
    :param outcome: RunOutcome，流水线结果
    :return: dict[str, object]，不包含敏感配置的简要结果
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: outcome_to_dict(outcome)
    """
    return {
        "runId": outcome.run_id,
        "finalRecordCount": outcome.final_record_count,
        "failedOcrCount": outcome.failed_ocr_count,
        "failedSpiderCount": outcome.failed_spider_count,
        "riskCount": outcome.risk_analysis.risk_count if outcome.risk_analysis else None,
        "riskJson": str(outcome.risk_analysis.json_path) if outcome.risk_analysis else None,
        "riskReport": str(outcome.report.pdf_path) if outcome.report else None,
        "dryRun": outcome.dry_run,
        "exitCode": outcome.exit_code,
    }


def main(argv: list[str] | None = None) -> int:
    """
    【函数功能】解析命令行参数、运行 Pipeline 并返回约定退出码。
    :param argv: list[str] | None，可选测试参数；为空时读取系统参数
    :return: int，0 成功，1 OCR/致命错误，2 爬虫部分失败
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: main(["run", "--input", "pdf_files", "--dry-run"])
    """
    load_default_env_files(Path(__file__).resolve().parents[1])
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    if args.command == "serve":
        from .web import run_web_server

        run_web_server(args.host, args.port)
        return 0
    try:
        outcome = run_pipeline(build_pipeline_config(args))
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1
    print(json.dumps(outcome_to_dict(outcome), ensure_ascii=False, indent=2))
    return outcome.exit_code
