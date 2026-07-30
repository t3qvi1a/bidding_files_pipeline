"""
【模块功能】验证爬虫提交模式、状态轮询、重试和全任务企业去重。

:Author: gexinyan
:CreateTime: 2026-07-16 10:00:00
"""

from __future__ import annotations

import unittest
import threading
import time
from typing import Any
from unittest.mock import patch

from bidding_pipeline.database import VerificationResult
from bidding_pipeline.records import ExtractionResult
from bidding_pipeline.spider import (
    CrawlDispatcher,
    HttpResult,
    RunProgress,
    SpiderClient,
    SpiderConfig,
    SpiderTaskResult,
    consolidate_spider_results,
    clean_company_name,
)


class SpiderTests(unittest.TestCase):
    """
    【类功能】覆盖企业爬虫客户端的无网络行为测试。
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    """

    def test_single_mode_submits_each_company_and_verifies_result(self) -> None:
        """
        【方法功能】验证默认逐企业提交，成功状态后回查企业信息。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        client = SpiderClient(
            SpiderConfig("http://spider", poll_interval_seconds=0),
            verifier=lambda _: VerificationResult(1, {"company_name": "企业A"}),
        )
        success = HttpResult(200, '{"data":{"companies":[{"queryStatus":"SUCCESS"}]}}', {"data": {"companies": [{"queryStatus": "SUCCESS"}]}})
        with patch.object(client, "_request", side_effect=[HttpResult(200, "ok", None), success, HttpResult(200, "ok", None), success]) as request_mock:
            results = client.crawl_document("a.pdf", ["企业A", "企业B"])

        self.assertEqual([item.status for item in results], ["success", "success"])
        expected_a = {
            "companyNames": ["企业A"],
            "fetchDeepInfo": False,
            "fetchBiddingDetail": False,
            "relationExpansionDepth": 1,
        }
        expected_b = {**expected_a, "companyNames": ["企业B"]}
        self.assertEqual(request_mock.call_args_list[0].args[2], expected_a)
        self.assertEqual(request_mock.call_args_list[2].args[2], expected_b)
        self.assertEqual(results[0].database_rows, 1)

    def test_batch_mode_submits_company_names_array_once(self) -> None:
        """
        【方法功能】验证 batch 模式按单 PDF 将企业名称数组一次提交。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        client = SpiderClient(SpiderConfig("http://spider", submit_mode="batch", poll_interval_seconds=0))
        success = HttpResult(200, '{"queryStatus":"SUCCESS"}', {"queryStatus": "SUCCESS"})
        with patch.object(client, "_request", side_effect=[HttpResult(200, "ok", None), success, success]) as request_mock:
            results = client.crawl_document("a.pdf", ["企业A", "企业B"])

        self.assertEqual(len(results), 2)
        self.assertEqual(
            request_mock.call_args_list[0].args[2],
            {
                "companyNames": ["企业A", "企业B"],
                "fetchDeepInfo": False,
                "fetchBiddingDetail": False,
                "relationExpansionDepth": 1,
            },
        )
        self.assertEqual([item.request_keyword for item in results], ["企业A,企业B", "企业A,企业B"])

    def test_new_spider_options_are_sent(self) -> None:
        """
        【函数功能】验证深度信息、招投标详情和关系扩展层数会进入新接口请求体。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-20 10:00:00
        """
        client = SpiderClient(
            SpiderConfig(
                "http://spider",
                fetch_deep_info=True,
                fetch_bidding_detail=True,
                relation_expansion_depth=2,
                poll_interval_seconds=0,
            )
        )
        success = HttpResult(200, '{"queryStatus":"SUCCESS"}', {"queryStatus": "SUCCESS"})
        with patch.object(
            client,
            "_request",
            side_effect=[HttpResult(200, "ok", None), success],
        ) as request_mock:
            client.crawl_document("a.pdf", ["企业A"])

        self.assertEqual(
            request_mock.call_args_list[0].args[2],
            {
                "companyNames": ["企业A"],
                "fetchDeepInfo": True,
                "fetchBiddingDetail": True,
                "relationExpansionDepth": 2,
            },
        )

    def test_run_id_polling_tracks_related_company_and_existing_status(self) -> None:
        """
        【方法功能】验证 runId 轮询会读取关联队列且已有数据优先于服务端失败状态。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-20 18:00:00
        """
        snapshots: list[RunProgress] = []
        client = SpiderClient(
            SpiderConfig("http://spider", poll_interval_seconds=0),
            run_progress_callback=snapshots.append,
        )
        submit = HttpResult(200, "", {"data": {"runId": "run-1", "expansionStatus": "WAITING"}})
        run_status = HttpResult(
            200,
            "",
            {
                "data": {
                    "runId": "run-1",
                    "status": "COMPLETED",
                    "rootStatus": "FAILED",
                    "roots": [{"companyName": "企业A", "status": "FAILED", "hasData": True}],
                    "totalNodes": 2,
                    "databaseReuseCount": 1,
                    "crawlSuccessCount": 1,
                    "failedCount": 0,
                }
            },
        )
        queue = HttpResult(
            200,
            "",
            {
                "data": {
                    "total": 1,
                    "pageNum": 1,
                    "pageSize": 100,
                    "items": [{"companyId": "related-1", "companyName": "关联企业B", "depth": 1, "status": "SUCCESS", "hasData": False}],
                }
            },
        )
        with patch.object(client, "_request", side_effect=[submit, run_status, queue]):
            results = client.crawl_document("a.pdf", ["企业A"])

        self.assertEqual([(item.company_name, item.status, item.company_type) for item in results], [("企业A", "existing", "root"), ("关联企业B", "success", "related")])
        self.assertEqual(snapshots[-1].related_total, 1)
        self.assertEqual(snapshots[-1].expansion_status, "COMPLETED")

    def test_completed_expansion_waits_until_all_entities_are_terminal(self) -> None:
        """
        【方法功能】验证扩展状态完成但关联企业仍在运行时不会提前结束轮询。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-21 09:30:00
        """
        client = SpiderClient(SpiderConfig("http://spider", poll_interval_seconds=0))
        submit = HttpResult(200, "", {"data": {"runId": "run-1", "expansionStatus": "WAITING"}})
        run_status = HttpResult(
            200,
            "",
            {"data": {"expansionStatus": "COMPLETED", "roots": [{"companyName": "企业A", "queryStatus": "SUCCESS"}], "totalNodes": 2}},
        )
        running_queue = HttpResult(
            200,
            "",
            {"data": {"total": 1, "items": [{"companyId": "related-1", "companyName": "关联企业B", "depth": 1, "queryStatus": "RUNNING"}]}},
        )
        completed_queue = HttpResult(
            200,
            "",
            {"data": {"total": 1, "items": [{"companyId": "related-1", "companyName": "关联企业B", "depth": 1, "queryStatus": "SUCCESS"}]}},
        )
        with patch.object(client, "_request", side_effect=[submit, run_status, running_queue, run_status, completed_queue]) as request_mock:
            results = client.crawl_document("a.pdf", ["企业A"])

        self.assertEqual(request_mock.call_count, 5)
        self.assertEqual([item.status for item in results], ["success", "success"])

    def test_timeout_preserves_terminal_related_results_and_audit_status(self) -> None:
        """
        【方法功能】验证扩展超时只结束未完成企业，并保留已成功关联企业及原始扩展状态。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-21 09:30:00
        """
        client = SpiderClient(SpiderConfig("http://spider"))
        progress = RunProgress(
            "run-1",
            1,
            0,
            0,
            0,
            1,
            1,
            0,
            0,
            "RUNNING",
            (
                SpiderTaskResult("a.pdf", "企业A", "企业A", "running", "", run_id="run-1", raw_status="RUNNING"),
                SpiderTaskResult("a.pdf", "关联企业B", "企业A", "success", "", run_id="run-1", company_type="related", raw_status="SUCCESS"),
            ),
        )

        results = client._finalize_run_results(progress, 1, 3, "timeout", "relation_expansion_timeout")

        self.assertEqual([item.status for item in results], ["timeout", "success"])
        self.assertEqual(results[0].expansion_status, "FAILED")
        self.assertEqual(results[0].to_dict()["auditResults"][0]["expansionStatus"], "RUNNING")

    def test_related_deduplication_uses_company_id_and_merges_root_sources(self) -> None:
        """
        【方法功能】验证跨根企业发现的关联企业按稳定 ID 去重并保留全部来源和审计记录。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-21 09:30:00
        """
        results = consolidate_spider_results(
            [
                SpiderTaskResult("a.pdf", "关联企业B", "根企业A", "success", "", run_id="run-a", company_type="related", raw_status="SUCCESS", related_sources=("根企业A",), company_id="company-1"),
                SpiderTaskResult("b.pdf", "关联企业乙", "根企业B", "existing", "", run_id="run-b", company_type="related", raw_status="FAILED", has_data=True, related_sources=("根企业B",), company_id="company-1"),
            ]
        )

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, "existing")
        self.assertEqual(results[0].related_sources, ("根企业A", "根企业B"))
        self.assertEqual(len(results[0].to_dict()["auditResults"]), 2)

    def test_root_rows_are_matched_by_company_name_instead_of_response_order(self) -> None:
        """
        【方法功能】验证服务端重排根企业数组时仍按规范化企业名称匹配状态。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-21 09:30:00
        """
        client = SpiderClient(SpiderConfig("http://spider"))
        progress = client._build_run_progress(
            "a.pdf",
            ["企业A", "企业B"],
            "企业A,企业B",
            "run-1",
            {
                "expansionStatus": "RUNNING",
                "roots": [
                    {"companyName": "企业B", "queryStatus": "FAILED"},
                    {"companyName": "企业A", "queryStatus": "SUCCESS"},
                ],
            },
            [],
        )

        self.assertEqual([(item.company_name, item.status) for item in progress.entities], [("企业A", "success"), ("企业B", "failed")])

    def test_real_queue_fields_preserve_reuse_status_ids_errors_and_sources(self) -> None:
        """
        【方法功能】验证生产队列双状态、entId、rootCompany 和 errorSummary 字段可完整解析。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-21 09:30:00
        """
        client = SpiderClient(SpiderConfig("http://spider"))
        progress = client._build_run_progress(
            "a.pdf",
            ["根企业A"],
            "根企业A",
            "run-1",
            {
                "status": "FAILED",
                "rootStatus": "FAILED",
                "roots": [{"companyName": "根企业A", "status": "FAILED"}],
                "totalNodes": 2,
            },
            [
                {
                    "companyName": "根企业A（规范名）",
                    "entId": "root-a.html",
                    "rootCompany": "根企业A",
                    "depth": 0,
                    "source": "CURRENT_RUN_REUSE",
                    "traversalStatus": "EXPANDED",
                    "collectionStatus": "COMPLETED",
                },
                {
                    "companyName": "关联企业B",
                    "entId": "related-b.html",
                    "rootCompany": "根企业A",
                    "parentCompany": "根企业A（规范名）",
                    "depth": 1,
                    "source": "DATABASE_REUSE",
                    "traversalStatus": "BLOCKED",
                    "collectionStatus": "FAILED",
                    "errorSummary": "额度已耗尽",
                },
            ],
        )

        root, related = progress.entities
        self.assertEqual((root.status, root.company_id, root.has_data), ("existing", "root-a.html", True))
        self.assertEqual((related.status, related.company_id, related.has_data), ("existing", "related-b.html", True))
        self.assertEqual(related.related_sources, ("根企业A", "根企业A（规范名）"))
        self.assertEqual(related.message, "额度已耗尽")
        self.assertIn("collectionStatus=FAILED", related.raw_status)

    def test_retries_server_error_before_success(self) -> None:
        """
        【方法功能】验证 5xx 响应按配置延迟重试且不重试成功响应。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        sleeps: list[float] = []
        client = SpiderClient(SpiderConfig("http://spider"), sleep=sleeps.append)
        with patch.object(client, "_request", side_effect=[HttpResult(503, "bad", None), HttpResult(200, "ok", None)]):
            result, attempts = client._request_with_retry("http://spider/test", "GET", None)

        self.assertEqual(result.status_code, 200)
        self.assertEqual(attempts, 2)
        self.assertEqual(sleeps, [5.0])

    def test_dispatcher_deduplicates_company_names_across_pdf_callbacks(self) -> None:
        """
        【方法功能】验证 OCR 多个 PDF 回调仅为同名企业提交一次爬虫任务。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        class Document:
            """
            【类功能】提供 PDF 路径和 OCR 记录的测试文档。
            :Author: gexinyan
            :CreateTime: 2026-07-16 10:00:00
            """

            def __init__(self, path: str, names: list[str]) -> None:
                """
                【方法功能】创建测试 PDF 文档。
                :param path: str，PDF 路径
                :param names: list[str]，企业名称列表
                :return: None
                :Author: gexinyan
                :CreateTime: 2026-07-16 10:00:00
                """
                self.pdf_path = path
                self.records = [ExtractionResult(company_name=name) for name in names]

        snapshots: list[dict[str, Any]] = []
        active_count = 0
        maximum_active_count = 0
        state_lock = threading.Lock()

        def crawl_one_company(source_pdf: str, names: list[str]) -> list[SpiderTaskResult]:
            """
            【函数功能】模拟单企业爬虫，并记录同时执行的任务数量。
            :param source_pdf: str，来源 PDF 路径
            :param names: list[str]，本次提交的唯一企业名称
            :return: list[SpiderTaskResult]，成功爬取结果
            :Author: gexinyan
            :CreateTime: 2026-07-17 10:30:00
            """
            nonlocal active_count, maximum_active_count
            with state_lock:
                active_count += 1
                maximum_active_count = max(maximum_active_count, active_count)
            time.sleep(0.01)
            with state_lock:
                active_count -= 1
            status = "timeout" if names[0] == "企业C" else "success"
            return [SpiderTaskResult(source_pdf, names[0], names[0], status, "ok")]

        client = SpiderClient(SpiderConfig("http://spider"))
        with patch.object(client, "crawl_document", side_effect=crawl_one_company) as crawl_mock:
            dispatcher = CrawlDispatcher(client, progress_callback=snapshots.append)
            dispatcher.on_pdf_completed(Document("first.pdf", ["企业A", "企业B"]), [])
            dispatcher.on_pdf_completed(Document("second.pdf", ["企业A", "企业C"]), [])
            results = dispatcher.wait()

        self.assertEqual(crawl_mock.call_count, 3)
        self.assertEqual([call.args[1] for call in crawl_mock.call_args_list], [["企业A"], ["企业B"], ["企业C"]])
        self.assertEqual(maximum_active_count, 1)
        self.assertEqual([item.company_name for item in results], ["企业A", "企业B", "企业C"])
        self.assertEqual([item.status for item in results], ["success", "success", "timeout"])
        self.assertEqual(snapshots[-1]["phase"], "completed")
        self.assertEqual(snapshots[-1]["discovered"], 3)
        self.assertEqual(snapshots[-1]["completed"], 3)
        self.assertEqual(snapshots[-1]["failed"], 0)
        self.assertEqual(snapshots[-1]["root"]["pending"], 1)

    def test_batch_precheck_skips_all_existing_companies_without_external_request(self) -> None:
        """
        【方法功能】验证全部企业已有有效工商详情时不创建外部请求并输出全命中进度。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-30 16:50:00
        """
        class Document:
            """
            【类功能】提供批量工商信息预检需要的模拟解析文档。
            :Author: gexinyan
            :CreateTime: 2026-07-30 16:50:00
            """

            pdf_path = "all-existing.pdf"
            records = [ExtractionResult(company_name="企业A"), ExtractionResult(company_name="企业B")]

        verifier_calls: list[list[str]] = []

        def verify_many(names: Any) -> dict[str, VerificationResult]:
            """
            【函数功能】记录批量核验输入并返回全部企业已有详情。
            :param names: Any，待核验企业名称迭代器
            :return: dict[str, VerificationResult]，全部命中的核验结果
            :Author: gexinyan
            :CreateTime: 2026-07-30 16:50:00
            """
            values = list(names)
            verifier_calls.append(values)
            return {name: VerificationResult(1, {"credit_code": f"code-{name}"}) for name in values}

        snapshots: list[dict[str, Any]] = []
        client = SpiderClient(SpiderConfig("http://spider"))
        with patch.object(client, "crawl_document") as crawl_mock:
            dispatcher = CrawlDispatcher(
                client,
                progress_callback=snapshots.append,
                batch_verifier=verify_many,
            )
            dispatcher.on_pdf_completed(Document(), [])
            results = dispatcher.wait()

        self.assertEqual(verifier_calls, [["企业A", "企业B"]])
        crawl_mock.assert_not_called()
        self.assertEqual([item.status for item in results], ["existing", "existing"])
        self.assertEqual(snapshots[-1]["phase"], "existing_data_only")
        self.assertEqual(snapshots[-1]["root"]["existing"], 2)
        self.assertEqual(snapshots[-1]["expansionStatus"], "NOT_REQUIRED")

    def test_batch_precheck_only_requests_missing_companies(self) -> None:
        """
        【方法功能】验证部分命中时仅缺少有效工商详情的企业进入原有外部获取流程。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-30 16:50:00
        """
        class Document:
            """
            【类功能】提供部分命中预检需要的模拟解析文档。
            :Author: gexinyan
            :CreateTime: 2026-07-30 16:50:00
            """

            pdf_path = "partial.pdf"
            records = [ExtractionResult(company_name="企业A"), ExtractionResult(company_name="企业B")]

        client = SpiderClient(SpiderConfig("http://spider"))
        external_result = [SpiderTaskResult("partial.pdf", "企业B", "企业B", "success", "ok")]
        with patch.object(client, "crawl_document", return_value=external_result) as crawl_mock:
            dispatcher = CrawlDispatcher(
                client,
                batch_verifier=lambda _: {
                    "企业A": VerificationResult(1, {"phone_number": "13800000000"}),
                    "企业B": VerificationResult(1, {}),
                },
            )
            dispatcher.on_pdf_completed(Document(), [])
            results = dispatcher.wait()

        crawl_mock.assert_called_once_with("partial.pdf", ["企业B"])
        self.assertEqual({item.company_name: item.status for item in results}, {"企业A": "existing", "企业B": "success"})

    def test_batch_precheck_database_error_fails_dispatcher(self) -> None:
        """
        【方法功能】验证批量数据库查询异常不会静默退化为外部全量请求。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-30 16:50:00
        """
        class Document:
            """
            【类功能】提供数据库异常路径需要的模拟解析文档。
            :Author: gexinyan
            :CreateTime: 2026-07-30 16:50:00
            """

            pdf_path = "error.pdf"
            records = [ExtractionResult(company_name="企业A")]

        def fail_verification(_: Any) -> dict[str, VerificationResult]:
            """
            【函数功能】模拟企业详情批量查询失败。
            :param _: Any，未使用的企业名称迭代器
            :return: dict[str, VerificationResult]，本路径不会返回
            :raises RuntimeError: 始终抛出模拟数据库异常
            :Author: gexinyan
            :CreateTime: 2026-07-30 16:50:00
            """
            raise RuntimeError("database unavailable")

        client = SpiderClient(SpiderConfig("http://spider"))
        dispatcher = CrawlDispatcher(client, batch_verifier=fail_verification)
        dispatcher.on_pdf_completed(Document(), [])
        with self.assertRaisesRegex(RuntimeError, "database unavailable"):
            dispatcher.wait()

    def test_company_name_cleanup_preserves_original_and_removes_rate_prefix(self) -> None:
        """
        【方法功能】验证爬虫提交名称清除费率表格残留且普通名称保持不变。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-21 14:30:00
        """
        self.assertEqual(clean_company_name("费率)江阴市水利工程公司"), ("江阴市水利工程公司", "rate_parenthesis"))
        self.assertEqual(
            clean_company_name("投标报价(元)山东水利建设集团有限公司"),
            ("山东水利建设集团有限公司", "bid_price_unit"),
        )
        self.assertEqual(clean_company_name("企业A"), ("企业A", ""))

    def test_fast_company_timeout_marks_root_failed_and_requests_remote_cancel(self) -> None:
        """
        【方法功能】验证快速模式达到单企业轮询上限后停止等待并异步请求取消远程运行。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-30 17:30:00
        """
        current = [0.0]

        def monotonic() -> float:
            """
            【函数功能】返回可由模拟等待推进的单调时间。
            :return: float，当前模拟时间
            :Author: gexinyan
            :CreateTime: 2026-07-30 17:30:00
            """
            return current[0]

        def sleep(seconds: float) -> None:
            """
            【函数功能】推进模拟轮询时间而不进行实际等待。
            :param seconds: float，待推进的秒数
            :return: None
            :Author: gexinyan
            :CreateTime: 2026-07-30 17:30:00
            """
            current[0] += seconds

        client = SpiderClient(
            SpiderConfig(
                "http://spider",
                poll_interval_seconds=5,
                max_poll_seconds=20,
                retry_delays=(),
                fail_on_max_poll_timeout=True,
            ),
            sleep=sleep,
            monotonic=monotonic,
        )
        running = HttpResult(
            200,
            "",
            {"data": {"status": "RUNNING", "expansionStatus": "RUNNING", "roots": [{"companyName": "企业A", "status": "RUNNING"}]}},
        )
        empty_queue = HttpResult(200, "", {"data": {"total": 0, "items": []}})
        with (
            patch.object(client, "_request", side_effect=[item for _ in range(4) for item in (running, empty_queue)]),
            patch.object(client, "_request_run_cancel_async") as cancel_mock,
        ):
            results = client._poll_run("a.pdf", ["企业A"], "企业A", "run-1", 1)

        self.assertEqual(results[0].status, "failed")
        self.assertIn("company_task_timeout_20_seconds", results[0].message)
        cancel_mock.assert_called_once_with("run-1")

    def test_static_run_becomes_pending_instead_of_failed(self) -> None:
        """
        【方法功能】验证无进度运行超过停滞窗口后进入待对账且不计为失败。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-21 14:30:00
        """
        current = [0.0]

        def monotonic() -> float:
            """
            【函数功能】返回可由测试等待函数推进的单调时间。
            :return: float，当前测试时间
            :Author: gexinyan
            :CreateTime: 2026-07-21 14:30:00
            """
            return current[0]

        def sleep(seconds: float) -> None:
            """
            【函数功能】推进测试单调时间而不执行真实等待。
            :param seconds: float，推进秒数
            :return: None
            :Author: gexinyan
            :CreateTime: 2026-07-21 14:30:00
            """
            current[0] += seconds

        client = SpiderClient(
            SpiderConfig(
                "http://spider",
                poll_interval_seconds=1,
                max_poll_seconds=0,
                stall_timeout_seconds=2,
                retry_delays=(),
                retryable_run_attempts=0,
            ),
            sleep=sleep,
            monotonic=monotonic,
        )
        run_status = HttpResult(
            200,
            "",
            {"data": {"status": "WAITING", "rootStatus": "WAITING", "roots": [{"companyName": "企业A", "status": "WAITING"}]}},
        )
        empty_queue = HttpResult(200, "", {"data": {"total": 0, "items": []}})
        with patch.object(client, "_request", side_effect=[run_status, empty_queue] * 3):
            results = client._poll_run("a.pdf", ["企业A"], "企业A", "run-1", 1, "2026-07-21T14:30:00")

        self.assertEqual([item.status for item in results], ["stale_waiting"])
        self.assertEqual(results[0].pending_reason, "relation_expansion_stalled")

    def test_progress_heartbeat_can_run_longer_than_stall_timeout(self) -> None:
        """
        【方法功能】验证持续变化的服务端心跳允许总耗时超过停滞窗口并最终成功。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-21 14:30:00
        """
        current = [0.0]

        def monotonic() -> float:
            """
            【函数功能】返回当前测试单调时间。
            :return: float，当前测试时间
            :Author: gexinyan
            :CreateTime: 2026-07-21 14:30:00
            """
            return current[0]

        def sleep(seconds: float) -> None:
            """
            【函数功能】推进测试时间以模拟长时间运行。
            :param seconds: float，推进秒数
            :return: None
            :Author: gexinyan
            :CreateTime: 2026-07-21 14:30:00
            """
            current[0] += seconds

        client = SpiderClient(
            SpiderConfig(
                "http://spider",
                poll_interval_seconds=1,
                max_poll_seconds=0,
                stall_timeout_seconds=2,
                retry_delays=(),
                retryable_run_attempts=0,
            ),
            sleep=sleep,
            monotonic=monotonic,
        )
        states = [
            HttpResult(200, "", {"data": {"status": "WAITING", "updateTime": "t1", "roots": [{"companyName": "企业A", "status": "WAITING"}]}}),
            HttpResult(200, "", {"data": {"status": "RUNNING", "updateTime": "t2", "roots": [{"companyName": "企业A", "status": "RUNNING"}]}}),
            HttpResult(200, "", {"data": {"status": "RUNNING", "updateTime": "t3", "roots": [{"companyName": "企业A", "status": "RUNNING"}]}}),
            HttpResult(200, "", {"data": {"status": "COMPLETED", "updateTime": "t4", "roots": [{"companyName": "企业A", "status": "COMPLETED"}]}}),
        ]
        empty_queue = HttpResult(200, "", {"data": {"total": 0, "items": []}})
        responses = [item for state in states for item in (state, empty_queue)]
        with patch.object(client, "_request", side_effect=responses):
            results = client._poll_run("a.pdf", ["企业A"], "企业A", "run-1", 1)

        self.assertEqual([item.status for item in results], ["success"])
        self.assertGreater(current[0], 2)

    def test_reconcile_pending_run_promotes_completed_root_to_success(self) -> None:
        """
        【方法功能】验证非阻塞二次对账可把已完成运行从 pending 更新为 success。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-21 14:30:00
        """
        client = SpiderClient(SpiderConfig("http://spider", retry_delays=()))
        pending = SpiderTaskResult(
            "a.pdf",
            "企业A",
            "企业A",
            "pending_reconciliation",
            "stalled",
            run_id="run-1",
            submitted_company_name="企业A",
        )
        completed = HttpResult(
            200,
            "",
            {"data": {"status": "COMPLETED", "roots": [{"companyName": "企业A", "status": "COMPLETED"}]}},
        )
        empty_queue = HttpResult(200, "", {"data": {"total": 0, "items": []}})
        with patch.object(client, "_request", side_effect=[completed, empty_queue]):
            results = client.reconcile_result(pending)

        self.assertEqual([item.status for item in results], ["success"])
        self.assertEqual(results[0].run_id, "run-1")

    def test_run_id_is_reported_before_cancel_stops_polling(self) -> None:
        """
        【方法功能】验证取得 runId 后即上报审计信息，取消信号不会丢失已创建的远程运行。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-21 16:10:00
        """
        submitted: list[dict[str, Any]] = []
        cancelled = [False]

        def record(payload: dict[str, Any]) -> None:
            """
            【函数功能】记录 runId 并模拟用户恰在提交成功后点击取消。
            :param payload: dict[str, Any]，已提交爬虫运行信息
            :return: None
            :Author: gexinyan
            :CreateTime: 2026-07-21 16:10:00
            """
            submitted.append(payload)
            cancelled[0] = True

        client = SpiderClient(
            SpiderConfig("http://spider", retry_delays=()),
            run_submitted_callback=record,
            cancel_requested=lambda: cancelled[0],
        )
        response = HttpResult(200, "", {"data": {"runId": "run-1"}})
        with patch.object(client, "_request", return_value=response):
            results = client._submit_keyword("a.pdf", ["企业A"], "企业A")

        self.assertEqual(results[0].status, "skipped")
        self.assertEqual(submitted[0]["runId"], "run-1")
        self.assertEqual(results[0].run_id, "run-1")

    def test_cancel_run_calls_remote_cancel_and_reads_status(self) -> None:
        """
        【方法功能】验证远程取消使用 runId 取消接口并将已取消状态写入审计结果。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-21 16:10:00
        """
        client = SpiderClient(SpiderConfig("http://spider", retry_delays=()))
        accepted = HttpResult(200, "", {"data": {"status": "CANCELLING"}})
        cancelled = HttpResult(200, "", {"data": {"expansionStatus": "CANCELLED"}})
        with patch.object(client, "_request", side_effect=[accepted, cancelled]) as request_mock:
            result = client.cancel_run("run-1")

        self.assertEqual(result["cancelStatus"], "cancelled")
        self.assertEqual(request_mock.call_args_list[0].args[:2], ("http://spider/spider/crawl/runs/run-1/cancel", "POST"))
        self.assertEqual(request_mock.call_args_list[1].args[:2], ("http://spider/spider/crawl/runs/run-1", "GET"))
