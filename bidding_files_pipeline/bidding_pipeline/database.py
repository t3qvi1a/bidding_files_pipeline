"""
【模块功能】管理 openGauss 解析结果入库及爬虫企业信息回查。

:Author: gexinyan
:CreateTime: 2026-07-16 10:00:00
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Iterable, Sequence

from .records import ExtractionResult, normalize_text


RESULT_COLUMNS = (
    "business_key",
    "run_id",
    "project_name",
    "project_code",
    "lot_code",
    "lot_name",
    "company_name",
    "award_status",
    "rank",
    "category",
    "source_path",
    "source_pages",
    "extraction_method",
    "evidence",
    "confidence",
    "review_status",
    "generated_at",
)
BUSINESS_FIELDS = (
    "company_name",
    "search_value",
    "phone_number",
    "email",
    "credit_code",
    "legal_person",
)


@dataclass(frozen=True, slots=True)
class DatabaseConfig:
    """
    【类功能】保存 openGauss/PostgreSQL 兼容数据库连接配置。
    :Attributes:
        host: str，数据库主机
        port: int，数据库端口
        database: str，数据库名
        username: str，登录用户名
        password: str，登录密码，仅从运行时环境读取
        schema: str，目标 schema
        table: str，目标表名
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    """

    host: str
    port: int
    database: str
    username: str
    password: str
    schema: str = "dwd"
    table: str = "dwd_bid_extraction_results"


@dataclass(frozen=True, slots=True)
class PersistenceSummary:
    """
    【类功能】描述一次最终 OCR 结果入库操作的汇总数据。
    :Attributes:
        record_count: int，参与入库的记录数
        schema: str，写入 schema
        table: str，写入表名
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    """

    record_count: int
    schema: str
    table: str


@dataclass(frozen=True, slots=True)
class VerificationResult:
    """
    【类功能】描述爬虫企业信息在数据库中的回查结果。
    :Attributes:
        row_count: int，匹配到的企业记录数
        effective_fields: dict[str, str]，首条记录的非空业务字段
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    """

    row_count: int
    effective_fields: dict[str, str]


def quote_ident(identifier: str) -> str:
    """
    【函数功能】对 SQL 标识符进行双引号转义，防止动态表名注入。
    :param identifier: str，schema、表名或列名
    :return: str，安全引用后的 SQL 标识符
    :raises ValueError: 标识符为空时抛出
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: quote_ident("dwd")
    """
    value = normalize_text(identifier)
    if not value:
        raise ValueError("数据库标识符不能为空")
    return '"' + value.replace('"', '""') + '"'


def qualified_name(schema: str, table: str) -> str:
    """
    【函数功能】生成安全引用的 schema.table 名称。
    :param schema: str，schema 名称
    :param table: str，表名称
    :return: str，安全的完整表名
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: qualified_name("dwd", "dwd_bid_extraction_results")
    """
    return f"{quote_ident(schema)}.{quote_ident(table)}"


def load_database_driver() -> tuple[Any, str]:
    """
    【函数功能】加载优先可用的 psycopg 或 psycopg2 数据库驱动。
    :return: tuple[Any, str]，驱动模块及其名称
    :raises RuntimeError: 未安装兼容驱动时抛出
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: load_database_driver()
    """
    try:
        import psycopg

        return psycopg, "psycopg"
    except ImportError:
        pass
    try:
        import psycopg2

        return psycopg2, "psycopg2"
    except ImportError as exc:
        raise RuntimeError("缺少数据库驱动，请安装 psycopg2-binary 或 psycopg") from exc


def open_connection(config: DatabaseConfig) -> Any:
    """
    【函数功能】创建 UTF-8 openGauss/PostgreSQL 数据库连接。
    :param config: DatabaseConfig，数据库连接配置
    :return: Any，数据库连接对象
    :raises ValueError: 密码为空时抛出
    :raises Exception: 建立连接失败时由底层驱动抛出
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: open_connection(config)
    """
    if not config.password:
        raise ValueError("缺少 GENERAL_DB_PASSWORD，拒绝连接数据库")
    driver, _ = load_database_driver()
    connection = driver.connect(
        host=config.host,
        port=config.port,
        dbname=config.database,
        user=config.username,
        password=config.password,
        connect_timeout=10,
    )
    if hasattr(connection, "set_client_encoding"):
        connection.set_client_encoding("UTF8")
    else:
        with connection.cursor() as cursor:
            cursor.execute("SET client_encoding TO 'UTF8'")
    return connection


def table_columns(cursor: Any, schema: str, table: str) -> set[str]:
    """
    【函数功能】读取目标表已存在的字段集合。
    :param cursor: Any，数据库游标
    :param schema: str，schema 名称
    :param table: str，表名称
    :return: set[str]，字段名集合
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: table_columns(cursor, "dwd", "spider_data_company")
    """
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        """,
        (schema, table),
    )
    return {str(row[0]) for row in cursor.fetchall()}


def not_null_columns(cursor: Any, schema: str, table: str) -> set[str]:
    """
    【函数功能】读取目标表当前仍带 NOT NULL 约束的字段集合。
    :param cursor: Any，数据库游标
    :param schema: str，schema 名称
    :param table: str，表名称
    :return: set[str]，仍为 NOT NULL 的字段名集合
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: not_null_columns(cursor, "dwd", "dwd_bid_extraction_results")
    """
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s AND is_nullable = 'NO'
        """,
        (schema, table),
    )
    return {str(row[0]) for row in cursor.fetchall()}


class ResultDatabaseWriter:
    """
    【类功能】自动建表并将 OCR 最终结果以幂等方式写入 openGauss。
    :Attributes:
        config: DatabaseConfig，解析结果目标表连接配置
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    """

    def __init__(self, config: DatabaseConfig) -> None:
        """
        【方法功能】初始化解析结果数据库写入器。
        :param config: DatabaseConfig，目标库配置
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        self.config = config

    def ensure_schema_and_table(self, cursor: Any) -> None:
        """
        【方法功能】创建解析结果 schema、表、补充字段和唯一索引。
        :param cursor: Any，数据库游标
        :return: None
        :raises Exception: DDL 执行失败时由数据库驱动抛出
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        schema = quote_ident(self.config.schema)
        target = qualified_name(self.config.schema, self.config.table)
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {target} (
                record_id BIGSERIAL PRIMARY KEY,
                business_key CHAR(64) NOT NULL,
                run_id VARCHAR(36) NOT NULL,
                project_name TEXT,
                project_code TEXT,
                lot_code TEXT,
                lot_name TEXT,
                company_name TEXT,
                award_status TEXT,
                rank TEXT,
                category TEXT,
                source_path TEXT,
                source_pages TEXT,
                extraction_method TEXT,
                evidence TEXT,
                confidence NUMERIC(8, 4) NOT NULL DEFAULT 0,
                review_status TEXT,
                generated_at TEXT,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        required_columns = {
            "business_key": "CHAR(64)",
            "run_id": "VARCHAR(36)",
            "project_name": "TEXT",
            "project_code": "TEXT",
            "lot_code": "TEXT",
            "lot_name": "TEXT",
            "company_name": "TEXT",
            "award_status": "TEXT",
            "rank": "TEXT",
            "category": "TEXT",
            "source_path": "TEXT",
            "source_pages": "TEXT",
            "extraction_method": "TEXT",
            "evidence": "TEXT",
            "confidence": "NUMERIC(8, 4) NOT NULL DEFAULT 0",
            "review_status": "TEXT",
            "generated_at": "TEXT",
            "created_at": "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
            "updated_at": "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
        }
        existing = table_columns(cursor, self.config.schema, self.config.table)
        for column, definition in required_columns.items():
            if column not in existing:
                cursor.execute(f"ALTER TABLE {target} ADD COLUMN {quote_ident(column)} {definition}")
        nullable_columns = {
            "project_name",
            "project_code",
            "lot_code",
            "lot_name",
            "company_name",
            "award_status",
            "rank",
            "category",
            "source_path",
            "source_pages",
            "extraction_method",
            "evidence",
            "review_status",
            "generated_at",
        }
        current_not_null = not_null_columns(cursor, self.config.schema, self.config.table)
        for column in nullable_columns.intersection(current_not_null):
            cursor.execute(f"ALTER TABLE {target} ALTER COLUMN {quote_ident(column)} DROP NOT NULL")
        index_name = quote_ident(f"ux_{self.config.table}_business_key")
        cursor.execute(
            "SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = %s AND indexname = %s)",
            (self.config.schema, f"ux_{self.config.table}_business_key"),
        )
        index_exists = bool(cursor.fetchone()[0])
        if not index_exists:
            cursor.execute(f"CREATE UNIQUE INDEX {index_name} ON {target} (business_key)")

    def persist(self, records: Sequence[ExtractionResult], run_id: str) -> PersistenceSummary:
        """
        【方法功能】创建目标表并使用业务键 Upsert 写入最终 OCR 结果。
        :param records: Sequence[ExtractionResult]，待写入的最终解析结果
        :param run_id: str，本次流水线运行标识
        :return: PersistenceSummary，入库汇总
        :raises Exception: 建表、写入或提交失败时由底层驱动抛出
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        connection = open_connection(self.config)
        try:
            with connection:
                with connection.cursor() as cursor:
                    self.ensure_schema_and_table(cursor)
                    self._upsert_records(cursor, records, run_id)
            return PersistenceSummary(len(records), self.config.schema, self.config.table)
        finally:
            connection.close()

    def _upsert_records(self, cursor: Any, records: Sequence[ExtractionResult], run_id: str) -> None:
        """
        【方法功能】在 openGauss 兼容事务中按业务键先更新、未命中再插入。
        :param cursor: Any，数据库游标
        :param records: Sequence[ExtractionResult]，待写入记录
        :param run_id: str，本次运行标识
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        if not records:
            return
        target = qualified_name(self.config.schema, self.config.table)
        columns_sql = ", ".join(quote_ident(column) for column in RESULT_COLUMNS)
        placeholders = ", ".join(["%s"] * len(RESULT_COLUMNS))
        update_columns = [column for column in RESULT_COLUMNS if column != "business_key"]
        update_assignments = ", ".join(f"{quote_ident(column)} = %s" for column in update_columns)
        update_sql = (
            f"UPDATE {target} SET {update_assignments}, "
            f"{quote_ident('updated_at')} = CURRENT_TIMESTAMP "
            f"WHERE {quote_ident('business_key')} = %s"
        )
        insert_sql = f"INSERT INTO {target} ({columns_sql}) VALUES ({placeholders})"
        for record in records:
            values = record.to_db_values(run_id)
            update_values = values[1:] + (values[0],)
            cursor.execute(update_sql, update_values)
            if getattr(cursor, "rowcount", 0) == 0:
                cursor.execute(insert_sql, values)


class SpiderDataVerifier:
    """
    【类功能】回查爬虫服务落库的企业基础信息，确认企业数据是否可见。
    :Attributes:
        config: DatabaseConfig，爬虫结果库连接配置
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    """

    def __init__(self, config: DatabaseConfig) -> None:
        """
        【方法功能】初始化爬虫企业信息回查器。
        :param config: DatabaseConfig，爬虫结果表连接配置
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        self.config = replace(config, table="spider_data_company")

    def verify(self, company_name: str) -> VerificationResult:
        """
        【方法功能】按企业名称或搜索值查询爬虫企业信息表。
        :param company_name: str，待回查的企业名称
        :return: VerificationResult，匹配行数与有效字段样例
        :raises RuntimeError: 爬虫结果表缺少可查询字段时抛出
        :raises Exception: 数据库查询失败时由底层驱动抛出
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        connection = open_connection(self.config)
        try:
            with connection.cursor() as cursor:
                available = table_columns(cursor, self.config.schema, self.config.table)
                selected = [field for field in BUSINESS_FIELDS if field in available]
                predicates: list[str] = []
                params: list[str] = []
                if "company_name" in available:
                    predicates.append(f"{quote_ident('company_name')} = %s")
                    params.append(company_name)
                if "search_value" in available:
                    predicates.append(f"{quote_ident('search_value')} = %s")
                    params.append(company_name)
                if not selected or not predicates:
                    raise RuntimeError("爬虫结果表缺少 company_name/search_value 等必要字段")
                fields_sql = ", ".join(quote_ident(field) for field in selected)
                target = qualified_name(self.config.schema, self.config.table)
                cursor.execute(
                    f"SELECT {fields_sql} FROM {target} WHERE {' OR '.join(predicates)}",
                    tuple(params),
                )
                rows = cursor.fetchall()
                effective_fields = {
                    field: normalize_text(value)
                    for field, value in zip(selected, rows[0] if rows else ())
                    if normalize_text(value)
                }
                return VerificationResult(len(rows), effective_fields)
        finally:
            connection.close()
