"""
【模块功能】验证爬虫提交模式、状态轮询、重试和全任务企业去重。

:Author: gexinyan
:CreateTime: 2026-07-16 10:00:00
"""

from __future__ import annotations

import unittest
import threading
import time
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
        snapshots = []
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

        snapshots: list[dict[str, int | str]] = []
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
        self.assertEqual(snapshots[-1]["failed"], 1)
