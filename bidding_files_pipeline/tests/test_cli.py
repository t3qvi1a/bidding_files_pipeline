"""
【模块功能】验证命令行私有环境变量加载和运行配置安全约束。

:Author: gexinyan
:CreateTime: 2026-07-16 10:00:00
"""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bidding_pipeline.cli import load_default_env_files, load_private_env


class CliTests(unittest.TestCase):
    """
    【类功能】覆盖 CLI 环境变量加载的最小安全行为。
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    """

    def test_load_private_env_keeps_explicit_environment_value(self) -> None:
        """
        【方法功能】验证 .env 只补充缺失环境变量，不覆盖外部显式密码。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text("GENERAL_DB_PASSWORD=file-secret\nSPIDER_RESULT_DB_NAME=big_data_demo\n", encoding="utf-8")
            with patch.dict(os.environ, {"GENERAL_DB_PASSWORD": "process-secret"}, clear=True):
                load_private_env(env_path)
                self.assertEqual(os.environ["GENERAL_DB_PASSWORD"], "process-secret")
                self.assertEqual(os.environ["SPIDER_RESULT_DB_NAME"], "big_data_demo")

    def test_load_default_env_files_uses_current_directory_before_project_root(self) -> None:
        """
        【方法功能】验证从非项目目录启动时仍加载项目根目录 .env，且当前目录优先。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            current_dir = root / "current"
            project_dir = root / "project"
            current_dir.mkdir()
            project_dir.mkdir()
            (current_dir / ".env").write_text("SPIDER_RESULT_DB_NAME=current\n", encoding="utf-8")
            (project_dir / ".env").write_text(
                "GENERAL_DB_PASSWORD=project-secret\nSPIDER_RESULT_DB_NAME=project\n",
                encoding="utf-8",
            )
            with patch.dict(os.environ, {}, clear=True):
                load_default_env_files(project_dir, current_dir)
                self.assertEqual(os.environ["GENERAL_DB_PASSWORD"], "project-secret")
                self.assertEqual(os.environ["SPIDER_RESULT_DB_NAME"], "current")
