"""
【模块功能】验证解析结果表 DDL 与幂等 Upsert 参数映射。

:Author: gexinyan
:CreateTime: 2026-07-16 10:00:00
"""

from __future__ import annotations

import unittest
from typing import Any
from unittest.mock import patch

from bidding_pipeline.database import DatabaseConfig, ResultDatabaseWriter
from bidding_pipeline.records import ExtractionResult


class FakeCursor:
    """
    【类功能】记录 SQL 调用的轻量数据库游标替身。
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    """

    def __init__(self) -> None:
        """
        【方法功能】初始化 SQL 和批量参数记录容器。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        self.sql: list[str] = []
        self.batch: list[tuple[Any, ...]] = []
        self.rowcount = 0

    def __enter__(self) -> "FakeCursor":
        """
        【方法功能】进入模拟游标上下文。
        :return: FakeCursor，当前游标
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        return self

    def __exit__(self, *_: object) -> None:
        """
        【方法功能】退出模拟游标上下文。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """

    def execute(self, sql: str, _: object = None) -> None:
        """
        【方法功能】记录单条 SQL。
        :param sql: str，SQL 文本
        :param _: object，SQL 参数占位
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        self.sql.append(sql)
        self.rowcount = 0

    def executemany(self, sql: str, values: list[tuple[Any, ...]]) -> None:
        """
        【方法功能】记录批量 SQL 与参数。
        :param sql: str，批量 SQL 文本
        :param values: list[tuple[Any, ...]]，批量参数
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        self.sql.append(sql)
        self.batch = values
        self.rowcount = len(values)

    def fetchall(self) -> list[tuple[object, ...]]:
        """
        【方法功能】模拟不存在旧字段的查询结果。
        :return: list[tuple[object, ...]]，空字段列表
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        return []

    def fetchone(self) -> tuple[bool]:
        """
        【方法功能】模拟索引不存在的查询结果。
        :return: tuple[bool]，表示索引不存在
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        return (False,)


class FakeConnection:
    """
    【类功能】提供事务上下文与游标的模拟数据库连接。
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    """

    def __init__(self, cursor: FakeCursor) -> None:
        """
        【方法功能】保存模拟游标。
        :param cursor: FakeCursor，模拟游标
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        self._cursor = cursor
        self.closed = False

    def __enter__(self) -> "FakeConnection":
        """
        【方法功能】进入模拟事务上下文。
        :return: FakeConnection，当前连接
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        return self

    def __exit__(self, *_: object) -> None:
        """
        【方法功能】退出模拟事务上下文。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """

    def cursor(self) -> FakeCursor:
        """
        【方法功能】返回配置好的模拟游标。
        :return: FakeCursor，模拟游标
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        return self._cursor

    def close(self) -> None:
        """
        【方法功能】记录连接关闭状态。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        self.closed = True


class DatabaseWriterTests(unittest.TestCase):
    """
    【类功能】覆盖目标表初始化和 Upsert 行参数生成。
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    """

    def test_persist_creates_schema_and_uses_open_gauss_update_insert(self) -> None:
        """
        【方法功能】验证首次入库会建表、建索引并携带业务键执行 Upsert。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        cursor = FakeCursor()
        connection = FakeConnection(cursor)
        config = DatabaseConfig("host", 15400, "big_data", "user", "secret")
        record = ExtractionResult(project_code="P-1", lot_code="L-1", company_name="企业A")

        with patch("bidding_pipeline.database.open_connection", return_value=connection):
            summary = ResultDatabaseWriter(config).persist([record], "run-id")

        statements = "\n".join(cursor.sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS", statements)
        self.assertIn("CREATE UNIQUE INDEX", statements)
        self.assertIn("UPDATE", statements)
        self.assertIn("INSERT INTO", statements)
        self.assertEqual(cursor.sql[-1].lstrip().startswith("INSERT INTO"), True)
        self.assertEqual(summary.record_count, 1)
        self.assertTrue(connection.closed)
