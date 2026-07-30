"""
【模块功能】验证 Web 层服务器路径约束和 ZIP 安全解压。

:Author: gexinyan
:CreateTime: 2026-07-16 16:20:00
"""

from __future__ import annotations

import json
import multiprocessing
import tempfile
import threading
import time
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from fastapi.testclient import TestClient

from bidding_pipeline.web import (
    JobManager,
    JobState,
    create_app,
    extract_zip_safely,
    infer_log_progress,
    validate_local_input,
)
from bidding_pipeline.spider import SpiderConfig


class WebSecurityTests(unittest.TestCase):
    """
    【类功能】覆盖服务器路径越界和 ZIP 路径穿越防护。
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    """

    def test_validate_local_input_rejects_path_outside_allowed_root(self) -> None:
        """
        【方法功能】验证不在允许根目录下的服务器路径会被拒绝。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 16:20:00
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            allowed = root / "allowed"
            outside = root / "outside"
            allowed.mkdir()
            outside.mkdir()
            with self.assertRaises(ValueError):
                validate_local_input(str(outside), (allowed.resolve(),))

    def test_extract_zip_safely_rejects_parent_traversal(self) -> None:
        """
        【方法功能】验证包含父目录穿越成员的 ZIP 压缩包会被拒绝。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 16:20:00
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            archive_path = root / "unsafe.zip"
            with zipfile.ZipFile(archive_path, "w") as archive:
                archive.writestr("../outside.pdf", b"pdf")
            with self.assertRaises(ValueError):
                extract_zip_safely(archive_path, root / "input")

    def test_extract_zip_safely_rejects_extracted_size_over_limit(self) -> None:
        """
        【方法功能】验证解压后总大小超过配置上限的 ZIP 会被拒绝。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 17:15:00
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            archive_path = root / "oversized.zip"
            with zipfile.ZipFile(archive_path, "w") as archive:
                archive.writestr("document.pdf", b"1234")
            with patch.dict("os.environ", {"BIDDING_MAX_EXTRACTED_BYTES": "3"}):
                with self.assertRaises(ValueError):
                    extract_zip_safely(archive_path, root / "input")

    def test_index_contains_root_related_and_overall_spider_progress_bars(self) -> None:
        """
        【方法功能】验证首页同时提供根企业、关联企业和企业整体三条进度条。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-21 09:30:00
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            app = create_app(Path(temp_dir))
            with TestClient(app) as client:
                response = client.get("/")

        self.assertEqual(response.status_code, 200)
        self.assertIn('id="spider-progress-bar"', response.text)
        self.assertIn('id="root-progress-bar"', response.text)
        self.assertIn('id="related-progress-bar"', response.text)


class WebJobStateTests(unittest.TestCase):
    """
    【类功能】覆盖 Web 任务状态持久化、刷新恢复和进程中止能力。
    :Author: gexinyan
    :CreateTime: 2026-07-16 17:40:00
    """

    def test_maintenance_marker_rejects_new_jobs(self) -> None:
        """
        【方法功能】验证部署维护标记存在时 Web 管理器拒绝创建新任务。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-21 14:30:00
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            manager = JobManager(root)
            (root / ".maintenance").write_text("deploying", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "维护模式"):
                manager.create_job(root / "input", "all", (), False)
            manager.executor.shutdown(wait=False, cancel_futures=True)

    def test_running_job_is_persisted_and_recovered_as_interrupted_after_restart(self) -> None:
        """
        【方法功能】验证运行任务写入磁盘后可恢复，服务重启时明确标记为已中断。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 17:40:00
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            manager = JobManager(root)
            job = JobState(
                job_id="job-running",
                input_dir=root / "input",
                output_dir=root / "job-running" / "output",
                status="running",
                stage="正在解析 PDF",
                progress=35,
            )
            manager.jobs[job.job_id] = job
            manager._add_log(job, "正在解析第 1 个 PDF。")
            manager.executor.shutdown(wait=False, cancel_futures=True)

            restored_manager = JobManager(root)
            restored = restored_manager.get_job(job.job_id)
            self.assertEqual(restored.status, "interrupted")
            self.assertIn("正在解析第 1 个 PDF", restored.logs[-1])
            self.assertTrue((root / job.job_id / "job_state.json").is_file())
            restored_manager.executor.shutdown(wait=False, cancel_futures=True)

    def test_structured_pdf_and_spider_progress_are_persisted_and_restored(self) -> None:
        """
        【方法功能】验证双进度条状态会写入任务文件，并兼容旧版状态文件的缺失字段。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-17 10:30:00
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            manager = JobManager(root)
            job = JobState(
                job_id="dual-progress",
                input_dir=root / "input",
                output_dir=root / "dual-progress" / "output",
                status="running",
            )
            manager.jobs[job.job_id] = job
            manager._update_structured_progress(
                job,
                "pdf_progress",
                {"completed": 3, "total": 5, "percent": 60},
            )
            manager._update_structured_progress(
                job,
                "spider_progress",
                {
                    "discovered": 4,
                    "queued": 1,
                    "running": 1,
                    "completed": 2,
                    "failed": 1,
                    "skipped": 0,
                    "phase": "crawling",
                    "root": {"total": 3, "success": 1, "failed": 0, "existing": 0, "pending": 1},
                    "related": {"total": 1, "success": 0, "failed": 0, "existing": 0, "pending": 0},
                },
            )
            response = job.to_dict()
            self.assertEqual(response["pdfProgress"], {"completed": 3, "total": 5, "percent": 60})
            self.assertEqual(response["spiderProgress"]["discovered"], 4)
            self.assertEqual(response["spiderProgress"]["failed"], 1)
            self.assertEqual(response["spiderProgress"]["root"]["pending"], 1)
            manager.executor.shutdown(wait=False, cancel_futures=True)

            restored_manager = JobManager(root)
            restored = restored_manager.get_job(job.job_id)
            self.assertEqual(restored.pdf_progress["completed"], 3)
            self.assertEqual(restored.spider_progress["phase"], "crawling")
            restored_manager.executor.shutdown(wait=False, cancel_futures=True)

        legacy = JobState.from_storage_dict(
            {"jobId": "legacy", "inputDir": "input", "outputDir": "output"},
            [],
        )
        self.assertEqual(legacy.pdf_progress, {"completed": 0, "total": 0, "percent": 0})
        self.assertEqual(legacy.spider_progress["discovered"], 0)

    def test_infer_log_progress_uses_completed_pdf_ratio(self) -> None:
        """
        【方法功能】验证 OCR 日志中的已完成 PDF 比例会转换为阶段内进度。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 18:05:00
        """
        progress = infer_log_progress(
            ["[阶段 1/5] 开始解析 PDF 文件。", "[###############-----] 完成 43/54 | 类别 award_notice"]
        )
        self.assertEqual(progress, 30)

    def test_parallel_completion_log_is_not_stored_in_web_job_log(self) -> None:
        """
        【方法功能】验证 Web 层忽略旧版并行完成提示，仅保留详细 PDF 进度条。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-17 09:20:59
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            manager = JobManager(root)
            job = JobState("progress-log", root / "input", root / "progress-log" / "output")
            manager._add_log(job, "并行处理完成 1/2 个 PDF。")
            manager._add_log(job, "[#-------------------] 完成 1/2 | 类别 award_notice")
            self.assertEqual(len(job.logs), 1)
            self.assertIn("完成 1/2", job.logs[0])
            self.assertEqual(job.progress, 22)
            manager.executor.shutdown(wait=False, cancel_futures=True)

    def test_restored_log_hides_legacy_parallel_completion_messages(self) -> None:
        """
        【方法功能】验证历史 pipeline.log 中的旧版并行提示不会再次显示到页面。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-17 09:20:59
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            output_dir = root / "output"
            output_dir.mkdir()
            (output_dir / "pipeline.log").write_text(
                "[09:00:00] 并行处理完成 1/2 个 PDF。\n"
                "[09:00:01] [#-------------------] 完成 1/2 | 类别 award_notice\n",
                encoding="utf-8",
            )
            manager = JobManager(root)
            logs = manager._read_logs(output_dir)
            self.assertEqual(len(logs), 1)
            self.assertIn("完成 1/2", logs[0])
            manager.executor.shutdown(wait=False, cancel_futures=True)

    def test_retry_configuration_is_persisted_and_recovered(self) -> None:
        """
        【方法功能】验证任务输入来源与运行参数在服务重启后仍可用于重新执行。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-17 08:54:27
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "input"
            input_dir.mkdir()
            (input_dir / "document.pdf").write_bytes(b"pdf")
            manager = JobManager(root / "runs")
            job = JobState(
                job_id="retry-config",
                input_dir=input_dir,
                output_dir=root / "runs" / "retry-config" / "output",
                status="cancelled",
                source_mode="local",
                category_mode="include",
                categories=("award_notice",),
                force_ocr=True,
                input_summary="服务器目录：input",
            )
            manager.jobs[job.job_id] = job
            manager._persist_job(job)
            manager.executor.shutdown(wait=False, cancel_futures=True)

            restored_manager = JobManager(root / "runs")
            restored = restored_manager.get_job(job.job_id)
            response = restored.to_dict()
            self.assertEqual(restored.source_mode, "local")
            self.assertEqual(restored.categories, ("award_notice",))
            self.assertTrue(restored.force_ocr)
            self.assertTrue(response["canRetry"])
            self.assertEqual(response["inputSummary"], "服务器目录：input")
            restored_manager.executor.shutdown(wait=False, cancel_futures=True)

    def test_retry_job_creates_new_job_with_saved_configuration(self) -> None:
        """
        【方法功能】验证可重试任务会复用已保存输入与配置并生成新的任务标识。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-17 08:54:27
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "input"
            input_dir.mkdir()
            (input_dir / "document.pdf").write_bytes(b"pdf")
            manager = JobManager(root / "runs")
            source_job = JobState(
                job_id="source-job",
                input_dir=input_dir,
                output_dir=root / "runs" / "source-job" / "output",
                status="interrupted",
                source_mode="local",
                category_mode="exclude",
                categories=("archive_info",),
                input_summary="服务器目录：input",
            )
            manager.jobs[source_job.job_id] = source_job
            manager._persist_job(source_job)
            with patch.object(manager.executor, "submit") as submit:
                retried = manager.retry_job(source_job.job_id, (root.resolve(),))
            self.assertNotEqual(retried.job_id, source_job.job_id)
            self.assertEqual(retried.input_dir, input_dir.resolve())
            self.assertEqual(retried.category_mode, "exclude")
            self.assertEqual(retried.categories, ("archive_info",))
            self.assertEqual(retried.source_mode, "local")
            submit.assert_called_once()
            manager.executor.shutdown(wait=False, cancel_futures=True)

    def test_completed_legacy_job_is_available_as_latest_job(self) -> None:
        """
        【方法功能】验证旧版完成目录无需状态文件也能在刷新后恢复为最近任务。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 17:40:00
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            output = root / "legacy-job" / "output"
            output.mkdir(parents=True)
            (output / "pipeline.log").write_text("[17:00:00] 任务完成\n", encoding="utf-8")
            (output / "final.csv").write_text("公司名称\n企业A\n", encoding="utf-8")
            (output / "run_manifest.json").write_text(
                json.dumps({"runId": "run-1", "error": ""}, ensure_ascii=False),
                encoding="utf-8",
            )
            manager = JobManager(root)
            latest = manager.get_latest_job()
            self.assertEqual(latest.job_id, "legacy-job")
            self.assertEqual(latest.status, "completed")
            self.assertIn("csv", latest.artifacts)
            manager.executor.shutdown(wait=False, cancel_futures=True)

    def test_cancel_queued_job_persists_cancelled_status(self) -> None:
        """
        【方法功能】验证排队任务可立即中止并将 cancelled 状态持久化。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 17:40:00
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            manager = JobManager(root)
            job = JobState("job-queued", root / "input", root / "job-queued" / "output")
            manager.jobs[job.job_id] = job
            manager._persist_job(job)
            cancelled = manager.cancel_job(job.job_id)
            payload = json.loads(
                (root / job.job_id / "job_state.json").read_text(encoding="utf-8")
            )
            self.assertEqual(cancelled.status, "cancelled")
            self.assertEqual(payload["status"], "cancelled")
            manager.executor.shutdown(wait=False, cancel_futures=True)

    def test_terminate_process_stops_independent_worker(self) -> None:
        """
        【方法功能】验证中止实现能够结束独立任务进程。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 17:40:00
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = JobManager(Path(temp_dir))
            context = multiprocessing.get_context("spawn")
            process = context.Process(target=time.sleep, args=(30,))
            process.start()
            manager._terminate_process(process)
            self.assertFalse(process.is_alive())
            manager.executor.shutdown(wait=False, cancel_futures=True)

    def test_latest_and_cancel_api_return_persisted_job_state(self) -> None:
        """
        【方法功能】验证最新任务接口和中止接口返回可供刷新恢复的持久化状态。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 17:40:00
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            app = create_app(root)
            manager: JobManager = app.state.job_manager
            job = JobState("api-job", root / "input", root / "api-job" / "output")
            manager.jobs[job.job_id] = job
            manager._persist_job(job)
            with TestClient(app) as client:
                latest_response = client.get("/api/jobs/latest")
                cancel_response = client.post(f"/api/jobs/{job.job_id}/cancel")
                restored_response = client.get(f"/api/jobs/{job.job_id}")
            self.assertEqual(latest_response.status_code, 200)
            self.assertEqual(latest_response.json()["jobId"], job.job_id)
            self.assertIn("pdfProgress", latest_response.json())
            self.assertIn("spiderProgress", latest_response.json())
            self.assertEqual(cancel_response.status_code, 200)
            self.assertEqual(restored_response.json()["status"], "cancelled")
            self.assertFalse(restored_response.json()["canRetry"])
            manager.executor.shutdown(wait=False, cancel_futures=True)

    def test_retry_api_rejects_legacy_job_without_saved_input(self) -> None:
        """
        【方法功能】验证旧版任务缺少原始输入配置时由重试接口返回明确冲突提示。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-17 08:54:27
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            app = create_app(root)
            manager: JobManager = app.state.job_manager
            legacy_job = JobState(
                "legacy-job",
                Path("."),
                root / "legacy-job" / "output",
                status="interrupted",
            )
            manager.jobs[legacy_job.job_id] = legacy_job
            manager._persist_job(legacy_job)
            with TestClient(app) as client:
                response = client.post(f"/api/jobs/{legacy_job.job_id}/retry")
            self.assertEqual(response.status_code, 409)
            self.assertIn("未保存原始输入", response.json()["detail"])
            manager.executor.shutdown(wait=False, cancel_futures=True)

    def test_retry_api_creates_new_job_from_saved_local_input(self) -> None:
        """
        【方法功能】验证重试接口对合法服务器输入返回新的排队任务。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-17 08:54:27
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "input"
            input_dir.mkdir()
            (input_dir / "document.pdf").write_bytes(b"pdf")
            with patch.dict("os.environ", {"BIDDING_ALLOWED_INPUT_ROOTS": str(root)}):
                app = create_app(root / "runs")
            manager: JobManager = app.state.job_manager
            source_job = JobState(
                job_id="api-retry-source",
                input_dir=input_dir,
                output_dir=root / "runs" / "api-retry-source" / "output",
                status="cancelled",
                source_mode="local",
                category_mode="all",
                input_summary="服务器目录：input",
            )
            manager.jobs[source_job.job_id] = source_job
            manager._persist_job(source_job)
            with patch.object(manager.executor, "submit"):
                with TestClient(app) as client:
                    response = client.post(f"/api/jobs/{source_job.job_id}/retry")
            payload = response.json()
            self.assertEqual(response.status_code, 202)
            self.assertNotEqual(payload["jobId"], source_job.job_id)
            self.assertEqual(payload["status"], "queued")
            self.assertEqual(payload["sourceMode"], "local")
            manager.executor.shutdown(wait=False, cancel_futures=True)

class RemoteCancellationTests(unittest.TestCase):
    """
    【类功能】验证 Web 任务取消时的 runId 持久化和远程爬虫终止审计。
    :Author: gexinyan
    :CreateTime: 2026-07-21 16:10:00
    """

    def test_running_job_cancel_sets_shared_signal_and_persists_remote_state(self) -> None:
        """
        【方法功能】验证运行中任务收到取消请求后通知子进程停止继续提交企业。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-21 16:10:00
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = JobManager(Path(temp_dir))
            job = JobState("running-job", Path(temp_dir) / "input", Path(temp_dir) / "output", status="running")
            cancel_event = threading.Event()
            manager.jobs[job.job_id] = job
            manager.cancel_events[job.job_id] = cancel_event

            cancelled = manager.cancel_job(job.job_id)

            self.assertTrue(cancel_event.is_set())
            self.assertEqual(cancelled.status, "cancelling")
            self.assertEqual(cancelled.remote_cancellation["state"], "cancelling")
            manager.executor.shutdown(wait=False, cancel_futures=True)

    def test_remote_cancellation_updates_each_persisted_run(self) -> None:
        """
        【方法功能】验证取消操作逐个调用爬虫 runId 接口并记录远程终态。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-21 16:10:00
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            manager = JobManager(root)
            job = JobState("remote-job", root / "input", root / "output", status="cancelling")
            manager.jobs[job.job_id] = job
            manager._record_spider_run(
                job,
                {"runId": "run-1", "sourcePdf": "a.pdf", "companyNames": ["企业A"], "submittedAt": "now"},
            )
            config: Any = SimpleNamespace(spider=SpiderConfig("http://spider", retry_delays=()))
            with patch("bidding_pipeline.web.SpiderClient") as client_class:
                client_class.return_value.cancel_run.return_value = {
                    "runId": "run-1",
                    "cancelStatus": "cancelled",
                    "expansionStatus": "CANCELLED",
                    "message": "",
                    "attempts": 1,
                }
                manager._cancel_remote_spider_runs(job, config)

            self.assertEqual(client_class.return_value.cancel_run.call_args.args, ("run-1",))
            self.assertEqual(job.spider_runs[0]["cancelStatus"], "cancelled")
            self.assertEqual(job.remote_cancellation["state"], "completed")
            persisted = json.loads((root / job.job_id / "job_state.json").read_text(encoding="utf-8"))
            self.assertEqual(persisted["spiderRuns"][0]["runId"], "run-1")
            manager.executor.shutdown(wait=False, cancel_futures=True)


if __name__ == "__main__":
    unittest.main()
