"""
【模块功能】验证 dry-run 编排、审计文件输出和 OCR 回调到爬虫调度的衔接。

:Author: gexinyan
:CreateTime: 2026-07-16 10:00:00
"""

from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from bidding_pipeline.database import DatabaseConfig, PersistenceSummary
from bidding_pipeline.records import CSV_COLUMN_MAPPING, ExtractionResult
from bidding_pipeline.reporting import ReportSummary
from bidding_pipeline.risk_analysis import RiskAnalysisSummary
from bidding_pipeline.runner import OcrApi, PipelineConfig, run_pipeline
from bidding_pipeline.spider import SpiderConfig


class RunnerTests(unittest.TestCase):
    """
    【类功能】覆盖不触网、不入库的 Pipeline 编排路径。
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    """

    def test_dry_run_writes_audit_files_and_skips_external_services(self) -> None:
        """
        【方法功能】验证 dry-run 仍输出 OCR、爬虫跳过和运行清单，但不调用数据库。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        class FakeProcessingConfig:
            """
            【类功能】保存 OCR 配置关键字参数的测试替身。
            :Author: gexinyan
            :CreateTime: 2026-07-16 10:00:00
            """

            def __init__(self, **_: object) -> None:
                """
                【方法功能】接收 OCR 配置参数。
                :param _: object，任意 OCR 配置参数
                :return: None
                :Author: gexinyan
                :CreateTime: 2026-07-16 10:00:00
                """

        def fake_process_pdf_tree(_: Path, output: Path, __: Any, **kwargs: Any) -> Any:
            """
            【函数功能】写入最小 final.csv 并触发一次 OCR 完成回调。
            :param _: Path，输入目录占位
            :param output: Path，输出目录
            :param __: Any，OCR 配置占位
            :param kwargs: Any，OCR 额外参数
            :return: Any，带 to_dict 的 OCR 汇总对象
            :Author: gexinyan
            :CreateTime: 2026-07-16 10:00:00
            """
            output.mkdir(parents=True, exist_ok=True)
            row = {column: "" for column in CSV_COLUMN_MAPPING}
            row.update({"项目编号": "P-1", "标段编号": "L-1", "公司名称": "企业A", "置信度": "0.9"})
            with (output / "final.csv").open("w", encoding="utf-8-sig", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=list(CSV_COLUMN_MAPPING))
                writer.writeheader()
                writer.writerow(row)
            document = SimpleNamespace(pdf_path=Path("a.pdf"), records=[ExtractionResult(company_name="企业A")])
            kwargs["pdf_completed_callback"](document, [])
            return SimpleNamespace(to_dict=lambda: {"失败文件数": 0, "文件总数": 1})

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = PipelineConfig(
                input_dir=root / "input",
                output_dir=root / "output",
                ocr_source=root / "ocr",
                workers=1,
                dpi=300,
                archive_scan_dpi=150,
                ocr_threshold=0.8,
                force_ocr=False,
                category_filter=None,
                include_categories=None,
                exclude_categories=None,
                database=DatabaseConfig("host", 15400, "db", "user", ""),
                spider_result_database="db",
                spider=SpiderConfig("http://spider"),
                dry_run=True,
            )
            api = OcrApi(FakeProcessingConfig, fake_process_pdf_tree)
            with patch("bidding_pipeline.runner.load_ocr_api", return_value=api):
                outcome = run_pipeline(config, progress_callback=lambda _: None)

            manifest = json.loads((root / "output" / "run_manifest.json").read_text(encoding="utf-8"))
            crawl = json.loads((root / "output" / "crawl_results.json").read_text(encoding="utf-8"))
            self.assertTrue(outcome.dry_run)
            self.assertEqual(outcome.final_record_count, 1)
            self.assertEqual(manifest["finalRecordCount"], 1)
            self.assertEqual(crawl["results"][0]["status"], "skipped")
            self.assertEqual(crawl["statistics"]["root"]["total"], 1)
            self.assertEqual(crawl["statistics"]["related"]["total"], 0)
            self.assertEqual(manifest["spiderStatistics"], crawl["statistics"])

    def test_pipeline_dispatches_spider_before_ocr_finishes_then_runs_follow_up_stages(self) -> None:
        """
        【方法功能】验证 PDF 回调即时调度爬虫，并在 OCR 与爬虫完成后才进入后续阶段。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 16:20:00
        """
        events: list[str] = []
        structured_events: list[tuple[str, dict[str, Any]]] = []

        class FakeProcessingConfig:
            """
            【类功能】接收 OCR 配置的正式流程测试替身。
            :Author: gexinyan
            :CreateTime: 2026-07-16 16:20:00
            """

            def __init__(self, **_: object) -> None:
                """
                【方法功能】接收任意 OCR 配置参数。
                :param _: object，OCR 配置参数
                :return: None
                :Author: gexinyan
                :CreateTime: 2026-07-16 16:20:00
                """

        class FakeDispatcher:
            """
            【类功能】记录爬虫调度与等待发生顺序的测试替身。
            :Author: gexinyan
            :CreateTime: 2026-07-16 16:20:00
            """

            def on_pdf_completed(self, _: Any, __: list[str]) -> None:
                """
                【方法功能】记录单 PDF 爬虫调度事件。
                :param _: Any，解析文档
                :param __: list[str]，解析警告
                :return: None
                :Author: gexinyan
                :CreateTime: 2026-07-16 16:20:00
                """
                events.append("spider-dispatch")

            def wait(self) -> list[Any]:
                """
                【方法功能】记录等待爬虫完成事件。
                :return: list[Any]，空爬虫结果
                :Author: gexinyan
                :CreateTime: 2026-07-16 16:20:00
                """
                events.append("spider-wait")
                return []

        def fake_process_pdf_tree(_: Path, output: Path, __: Any, **kwargs: Any) -> Any:
            """
            【函数功能】生成最小 OCR 结果并记录 OCR 完成回调时序。
            :param _: Path，输入目录占位
            :param output: Path，输出目录
            :param __: Any，OCR 配置占位
            :param kwargs: Any，OCR 回调参数
            :return: Any，带 to_dict 的 OCR 汇总
            :Author: gexinyan
            :CreateTime: 2026-07-16 16:20:00
            """
            events.append("ocr-start")
            output.mkdir(parents=True, exist_ok=True)
            row = {column: "" for column in CSV_COLUMN_MAPPING}
            row.update({"项目编号": "P-1", "标段编号": "L-1", "公司名称": "企业A", "置信度": "0.9"})
            with (output / "final.csv").open("w", encoding="utf-8-sig", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=list(CSV_COLUMN_MAPPING))
                writer.writeheader()
                writer.writerow(row)
            kwargs["pdf_completed_callback"](
                SimpleNamespace(pdf_path=Path("a.pdf"), records=[ExtractionResult(company_name="企业A")]),
                [],
            )
            kwargs["pdf_progress_callback"]({"completed": 1, "total": 1, "percent": 100})
            events.append("ocr-finish")
            return SimpleNamespace(to_dict=lambda: {"失败文件数": 0, "文件总数": 1})

        def fake_persist(_: Any, records: list[Any], __: str) -> PersistenceSummary:
            """
            【函数功能】记录入库事件并返回最小汇总。
            :param _: Any，数据库写入器实例
            :param records: list[Any]，解析记录
            :param __: str，运行标识
            :return: PersistenceSummary，入库汇总
            :Author: gexinyan
            :CreateTime: 2026-07-16 16:20:00
            """
            events.append("persist")
            return PersistenceSummary(len(records), "dwd", "results")

        def fake_analyze(_: Any, __: str, ___: str, path: Path) -> RiskAnalysisSummary:
            """
            【函数功能】记录风险分析事件并返回最小汇总。
            :param _: Any，数据库配置
            :param __: str，爬虫数据库名
            :param ___: str，运行标识
            :param path: Path，JSON 输出路径
            :return: RiskAnalysisSummary，风险分析汇总
            :Author: gexinyan
            :CreateTime: 2026-07-16 16:20:00
            """
            events.append("risk")
            return RiskAnalysisSummary(0, 1, 1, 0, path)

        def fake_report(_: Path, md: Path, pdf: Path, __: Path, ___: Path) -> ReportSummary:
            """
            【函数功能】记录报告生成事件并返回最小汇总。
            :param _: Path，风险 JSON 路径
            :param md: Path，Markdown 路径
            :param pdf: Path，PDF 路径
            :param __: Path，模板路径
            :param ___: Path，渲染脚本路径
            :return: ReportSummary，报告汇总
            :Author: gexinyan
            :CreateTime: 2026-07-16 16:20:00
            """
            events.append("report")
            return ReportSummary(md, pdf, 0)

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = PipelineConfig(
                input_dir=root / "input",
                output_dir=root / "output",
                ocr_source=root / "ocr",
                workers=1,
                dpi=300,
                archive_scan_dpi=150,
                ocr_threshold=0.8,
                force_ocr=False,
                category_filter=None,
                include_categories=None,
                exclude_categories=None,
                database=DatabaseConfig("host", 15400, "db", "user", "password"),
                spider_result_database="db",
                spider=SpiderConfig("http://spider"),
            )
            api = OcrApi(FakeProcessingConfig, fake_process_pdf_tree)
            with (
                patch("bidding_pipeline.runner.load_ocr_api", return_value=api),
                patch("bidding_pipeline.runner._build_dispatcher", return_value=FakeDispatcher()),
                patch("bidding_pipeline.runner.ResultDatabaseWriter.persist", new=fake_persist),
                patch("bidding_pipeline.runner.analyze_risks", new=fake_analyze),
                patch("bidding_pipeline.runner.generate_report", new=fake_report),
            ):
                run_pipeline(
                    config,
                    progress_callback=lambda _: None,
                    structured_progress_callback=lambda event_type, payload: structured_events.append(
                        (event_type, payload)
                    ),
                )

        self.assertEqual(
            events,
            ["ocr-start", "spider-dispatch", "ocr-finish", "spider-wait", "persist", "risk", "report"],
        )
        self.assertEqual(
            structured_events,
            [("pdf_progress", {"completed": 1, "total": 1, "percent": 100})],
        )
