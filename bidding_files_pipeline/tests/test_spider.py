"""
【模块功能】验证爬虫提交模式、状态轮询、重试和全任务企业去重。

:Author: gexinyan
:CreateTime: 2026-07-16 10:00:00
"""

from __future__ import annotations

import unittest
from unittest.mock import patch

from bidding_pipeline.database import VerificationResult
from bidding_pipeline.records import ExtractionResult
from bidding_pipeline.spider import CrawlDispatcher, HttpResult, SpiderClient, SpiderConfig


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
        self.assertEqual(request_mock.call_args_list[0].args[2], {"keyword": "企业A"})
        self.assertEqual(request_mock.call_args_list[2].args[2], {"keyword": "企业B"})
        self.assertEqual(results[0].database_rows, 1)

    def test_batch_mode_submits_comma_joined_keyword_once(self) -> None:
        """
        【方法功能】验证 batch 模式按单 PDF 将企业名称用英文逗号拼接后提交。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        client = SpiderClient(SpiderConfig("http://spider", submit_mode="batch", poll_interval_seconds=0))
        success = HttpResult(200, '{"queryStatus":"SUCCESS"}', {"queryStatus": "SUCCESS"})
        with patch.object(client, "_request", side_effect=[HttpResult(200, "ok", None), success, success]) as request_mock:
            results = client.crawl_document("a.pdf", ["企业A", "企业B"])

        self.assertEqual(len(results), 2)
        self.assertEqual(request_mock.call_args_list[0].args[2], {"keyword": "企业A,企业B"})
        self.assertEqual([item.request_keyword for item in results], ["企业A,企业B", "企业A,企业B"])

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

        client = SpiderClient(SpiderConfig("http://spider"))
        with patch.object(client, "crawl_document", return_value=[]) as crawl_mock:
            dispatcher = CrawlDispatcher(client)
            dispatcher.on_pdf_completed(Document("first.pdf", ["企业A", "企业B"]), [])
            dispatcher.on_pdf_completed(Document("second.pdf", ["企业A", "企业C"]), [])
            dispatcher.wait()

        self.assertEqual(crawl_mock.call_count, 2)
        self.assertEqual(crawl_mock.call_args_list[0].args[1], ["企业A", "企业B"])
        self.assertEqual(crawl_mock.call_args_list[1].args[1], ["企业C"])
