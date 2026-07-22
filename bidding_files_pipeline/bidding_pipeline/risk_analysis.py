"""
【模块功能】查询 openGauss 投标与企业数据，识别同一标段企业间的共享信息风险。

:Author: gexinyan
:CreateTime: 2026-07-16 16:20:00
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import datetime
from itertools import combinations
from pathlib import Path
from typing import Any, Iterable, Sequence

from .database import DatabaseConfig, open_connection, qualified_name
from .records import hash_values, normalize_text


RISK_LABELS = {
    "shared_phone": "联系电话相同",
    "shared_email": "邮箱相同",
    "shared_shareholder": "股东名称相同",
    "shared_senior_staff": "高级职员名称相同",
    "shared_company_identity": "企业身份重合",
}
COMPARISON_LABELS = {
    "root_root": "根公司与根公司信息重合",
    "root_related": "根公司与另一根公司的关联公司信息重合",
    "related_related": "不同根公司的关联公司信息重合",
}
RELATION_TABLE = "spider_data_person_enterprise_relation"
EMPTY_MARKERS = {"", "-", "--", "/", "无", "暂无", "未知", "未披露", "null", "none"}
EMAIL_PATTERN = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class RiskAnalysisSummary:
    """
    【类功能】描述一次 openGauss 风险分析的产出与覆盖范围。
    :Attributes:
        risk_count: int，风险组数量
        project_count: int，参与分析的标段数量
        company_count: int，参与分析的不同企业数量
        unmatched_company_count: int，未匹配到任何爬虫数据的企业数量
        json_path: Path，风险 JSON 文件路径
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    """

    risk_count: int
    project_count: int
    company_count: int
    unmatched_company_count: int
    json_path: Path


def normalize_company_name(value: Any) -> str:
    """
    【函数功能】规范企业名称，用于投标结果与爬虫数据的稳定关联。
    :param value: Any，企业名称原始值
    :return: str，移除全部空白并转小写后的名称
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    Example: normalize_company_name(" 企业 A ")
    """
    return re.sub(r"\s+", "", normalize_text(value)).casefold()


def normalize_person_name(value: Any) -> str:
    """
    【函数功能】规范股东或高级职员名称并过滤无效占位值。
    :param value: Any，人员或机构名称原始值
    :return: str，可用于跨企业比较的名称；无效值返回空字符串
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    Example: normalize_person_name(" 张 三 ")
    """
    normalized = re.sub(r"\s+", "", normalize_text(value)).casefold()
    return "" if normalized in EMPTY_MARKERS else normalized


def extract_phone_values(value: Any) -> tuple[tuple[str, str], ...]:
    """
    【函数功能】从一个联系电话字段拆分并规范多个手机或座机号码。
    :param value: Any，联系电话原始值
    :return: tuple[tuple[str, str], ...]，由“规范值、展示值”组成的去重元组
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    Example: extract_phone_values("13800000000 / 0510-12345678")
    """
    raw_text = normalize_text(value)
    values: dict[str, str] = {}
    for part in re.split(r"[,，;；/、|\n]+", raw_text):
        digits = re.sub(r"\D", "", part)
        if digits.startswith("86") and len(digits) == 13:
            digits = digits[2:]
        if 7 <= len(digits) <= 12 and digits not in EMPTY_MARKERS:
            values.setdefault(digits, part.strip() or digits)
    return tuple(values.items())


def extract_email_values(value: Any) -> tuple[tuple[str, str], ...]:
    """
    【函数功能】从邮箱字段提取并规范一个或多个邮箱地址。
    :param value: Any，邮箱原始值
    :return: tuple[tuple[str, str], ...]，由“规范值、展示值”组成的去重元组
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    Example: extract_email_values("A@example.com; b@example.com")
    """
    values: dict[str, str] = {}
    for match in EMAIL_PATTERN.findall(normalize_text(value)):
        normalized = match.casefold()
        values.setdefault(normalized, match)
    return tuple(values.items())


def fetch_rows(cursor: Any, statement: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
    """
    【函数功能】执行参数化只读 SQL 并将游标结果转换为字段字典。
    :param cursor: Any，数据库游标
    :param statement: str，只读 SQL 语句
    :param params: Sequence[Any]，SQL 参数序列
    :return: list[dict[str, Any]]，查询结果字典列表
    :raises Exception: SQL 执行失败时由数据库驱动抛出
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    Example: fetch_rows(cursor, "SELECT 1 AS value")
    """
    cursor.execute(statement, tuple(params))
    columns = [str(item[0]) for item in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def fetch_bidder_rows(config: DatabaseConfig, run_id: str) -> list[dict[str, Any]]:
    """
    【函数功能】读取本次流水线入库的有效投标企业及项目标段信息。
    :param config: DatabaseConfig，解析结果数据库配置
    :param run_id: str，本次流水线运行标识
    :return: list[dict[str, Any]]，投标企业记录
    :raises Exception: 数据库连接或查询失败时抛出
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    Example: fetch_bidder_rows(config, "run-id")
    """
    target = qualified_name(config.schema, config.table)
    connection = open_connection(config)
    try:
        with connection.cursor() as cursor:
            return fetch_rows(
                cursor,
                f"""
                SELECT record_id, project_name, project_code, lot_code, lot_name,
                       company_name, award_status, source_path, category
                FROM {target}
                WHERE run_id = %s
                  AND company_name IS NOT NULL
                  AND LENGTH(TRIM(company_name)) > 0
                ORDER BY project_code, lot_code, company_name
                """,
                (run_id,),
            )
    finally:
        connection.close()


def fetch_historical_project_rows(
    config: DatabaseConfig,
    company_names: Sequence[str],
) -> list[dict[str, Any]]:
    """
    【函数功能】读取风险企业参与过的历史项目，并补齐这些项目全部投标企业和中标状态。
    :param config: DatabaseConfig，解析结果数据库配置
    :param company_names: Sequence[str]，风险涉及企业名称
    :return: list[dict[str, Any]]，共同项目的完整投标记录
    :raises Exception: 数据库连接或查询失败时由驱动抛出
    :Author: gexinyan
    :CreateTime: 2026-07-17 09:20:59
    Example: fetch_historical_project_rows(config, ("企业A", "企业B"))
    """
    names = tuple(dict.fromkeys(normalize_text(name) for name in company_names if normalize_text(name)))
    if not names:
        return []
    target = qualified_name(config.schema, config.table)
    name_predicate, name_params = _in_predicate("TRIM(company_name)", names)
    connection = open_connection(config)
    try:
        with connection.cursor() as cursor:
            related_rows = fetch_rows(
                cursor,
                f"""
                SELECT record_id, project_name, project_code, lot_code, lot_name,
                       company_name, award_status, source_path, category
                FROM {target}
                WHERE {name_predicate}
                """,
                name_params,
            )
            related_keys = {project_key(row)[0] for row in related_rows}
            if not related_keys:
                return []
            lot_codes = tuple(dict.fromkeys(normalize_text(row.get("lot_code")) for row in related_rows if normalize_text(row.get("lot_code"))))
            project_codes = tuple(dict.fromkeys(normalize_text(row.get("project_code")) for row in related_rows if normalize_text(row.get("project_code"))))
            project_names = tuple(
                dict.fromkeys(
                    normalize_text(row.get("project_name"))
                    for row in related_rows
                    if not normalize_text(row.get("lot_code"))
                    and not normalize_text(row.get("project_code"))
                    and normalize_text(row.get("project_name"))
                )
            )
            clauses: list[str] = []
            params: tuple[Any, ...] = ()
            for column, values in (
                ("lot_code", lot_codes),
                ("project_code", project_codes),
                ("project_name", project_names),
            ):
                if values:
                    predicate, predicate_params = _in_predicate(column, values)
                    clauses.append(predicate)
                    params += predicate_params
            if not clauses:
                return []
            candidate_rows = fetch_rows(
                cursor,
                f"""
                SELECT record_id, project_name, project_code, lot_code, lot_name,
                       company_name, award_status, source_path, category
                FROM {target}
                WHERE company_name IS NOT NULL
                  AND LENGTH(TRIM(company_name)) > 0
                  AND ({' OR '.join(clauses)})
                ORDER BY project_code, lot_code, company_name
                """,
                params,
            )
            return [row for row in candidate_rows if project_key(row)[0] in related_keys]
    finally:
        connection.close()


def _in_predicate(column: str, values: Sequence[Any]) -> tuple[str, tuple[Any, ...]]:
    """
    【函数功能】为受控列名生成参数化 SQL IN 条件。
    :param column: str，已由调用方限定的字段表达式
    :param values: Sequence[Any]，待匹配参数
    :return: tuple[str, tuple[Any, ...]]，SQL 条件及参数元组
    :raises ValueError: 参数集合为空时抛出
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    Example: _in_predicate("search_value", ("企业A",))
    """
    if not values:
        raise ValueError("IN 查询参数不能为空")
    return f"{column} IN ({', '.join(['%s'] * len(values))})", tuple(values)


def fetch_relation_rows(
    config: DatabaseConfig,
    root_company_names: Sequence[str],
) -> list[dict[str, Any]]:
    """
    【函数功能】查询当前投标根公司的有效企业关系记录。
    :param config: DatabaseConfig，爬虫结果数据库配置
    :param root_company_names: Sequence[str]，本次运行的投标根公司名称
    :return: list[dict[str, Any]]，带根公司、关联公司及关系路径的记录
    :raises Exception: 数据库连接或查询失败时由驱动抛出
    :Author: gexinyan
    :CreateTime: 2026-07-21 11:30:00
    Example: fetch_relation_rows(config, ("企业A", "企业B"))
    """
    names = tuple(
        dict.fromkeys(normalize_text(name) for name in root_company_names if normalize_text(name))
    )
    if not names:
        return []
    name_predicate, name_params = _in_predicate("TRIM(search_value)", names)
    target = qualified_name(config.schema, RELATION_TABLE)
    connection = open_connection(config)
    try:
        with connection.cursor() as cursor:
            return fetch_rows(
                cursor,
                f"""
                SELECT record_id, company_id, search_value, company_name,
                       source_type, relation_type, person_name, position,
                       holding_ratio, related_ent_id
                FROM {target}
                WHERE {name_predicate}
                  AND COALESCE(delete_flag, 'NOT_DELETE') <> 'DELETE'
                ORDER BY search_value, company_name, source_type, relation_type
                """,
                name_params,
            )
    finally:
        connection.close()


def fetch_spider_rows(
    config: DatabaseConfig,
    bidder_names: Sequence[str],
) -> dict[str, list[dict[str, Any]]]:
    """
    【函数功能】按投标企业名称读取企业联系方式、股东和高级职员爬虫数据。
    :param config: DatabaseConfig，爬虫结果数据库配置
    :param bidder_names: Sequence[str]，去重后的投标企业名称
    :return: dict[str, list[dict[str, Any]]]，按 company/shareholder/senior_staff 分类的记录
    :raises Exception: 数据库连接或查询失败时抛出
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    Example: fetch_spider_rows(config, ("企业A", "企业B"))
    """
    names = tuple(dict.fromkeys(normalize_text(name) for name in bidder_names if normalize_text(name)))
    if not names:
        return {"company": [], "shareholder": [], "senior_staff": []}
    name_predicate, name_params = _in_predicate("TRIM(search_value)", names)
    company_name_predicate, company_name_params = _in_predicate("TRIM(company_name)", names)
    schema = config.schema
    company_table = qualified_name(schema, "spider_data_company")
    shareholder_table = qualified_name(schema, "spider_data_shareholder")
    staff_table = qualified_name(schema, "spider_data_senior_staff")
    connection = open_connection(config)
    try:
        with connection.cursor() as cursor:
            companies = fetch_rows(
                cursor,
                f"""
                SELECT record_id, search_value, company_name, phone_number, email
                FROM {company_table}
                WHERE ({name_predicate} OR {company_name_predicate})
                  AND COALESCE(delete_flag, 'NOT_DELETE') <> 'DELETE'
                """,
                name_params + company_name_params,
            )
            record_ids = tuple(
                dict.fromkeys(row["record_id"] for row in companies if row.get("record_id") is not None)
            )
            shareholder_where = name_predicate
            shareholder_params: tuple[Any, ...] = name_params
            staff_where = name_predicate
            staff_params: tuple[Any, ...] = name_params
            if record_ids:
                record_predicate, record_params = _in_predicate("record_id", record_ids)
                shareholder_where = f"({shareholder_where}) OR ({record_predicate})"
                shareholder_params += record_params
                staff_where = f"({staff_where}) OR ({record_predicate})"
                staff_params += record_params
            shareholders = fetch_rows(
                cursor,
                f"""
                SELECT record_id, search_value, shareholder_name, subscribed_ratio
                FROM {shareholder_table}
                WHERE ({shareholder_where})
                  AND COALESCE(delete_flag, 'NOT_DELETE') <> 'DELETE'
                """,
                shareholder_params,
            )
            staff = fetch_rows(
                cursor,
                f"""
                SELECT record_id, search_value, staff_name, position
                FROM {staff_table}
                WHERE ({staff_where})
                  AND COALESCE(delete_flag, 'NOT_DELETE') <> 'DELETE'
                """,
                staff_params,
            )
            return {"company": companies, "shareholder": shareholders, "senior_staff": staff}
    finally:
        connection.close()


def project_key(row: dict[str, Any]) -> tuple[str, str]:
    """
    【函数功能】组合项目与标段标识构建防跨项目碰撞的稳定分组键。
    :param row: dict[str, Any]，投标结果记录
    :return: tuple[str, str]，稳定项目键和键来源
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    Example: project_key({"project_code": "P-1", "lot_code": "L-1"})
    """
    lot_code = normalize_text(row.get("lot_code"))
    lot_name = normalize_text(row.get("lot_name"))
    project_code = normalize_text(row.get("project_code"))
    project_name = normalize_text(row.get("project_name"))
    project_source, project_value = (
        ("project_code", project_code) if project_code else ("project_name", project_name)
    )
    lot_source, lot_value = ("lot_code", lot_code) if lot_code else ("lot_name", lot_name)
    if project_value and lot_value:
        key_source = f"{project_source}_{lot_source}"
        fingerprint = hash_values((key_source, project_value, lot_value))
        return f"project_lot:{fingerprint}", key_source
    if project_value:
        key_source = f"{project_source}_fallback"
        fingerprint = hash_values((key_source, project_value))
        return f"project:{fingerprint}", key_source
    record_id = normalize_text(row.get("record_id"))
    unresolved_values = (
        ("record_id", record_id)
        if record_id
        else (
            "unresolved_record",
            row.get("source_path"),
            row.get("category"),
            row.get("company_name"),
            lot_code,
            lot_name,
        )
    )
    return f"unresolved:{hash_values(unresolved_values)}", "unresolved_record"


def build_projects(bidder_rows: Sequence[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """
    【函数功能】将投标结果按标段键归并并去重参与企业。
    :param bidder_rows: Sequence[dict[str, Any]]，投标企业记录
    :return: dict[str, dict[str, Any]]，按项目键索引的项目数据
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    Example: build_projects([{"lot_code": "L-1", "company_name": "企业A"}])
    """
    projects: dict[str, dict[str, Any]] = {}
    for row in bidder_rows:
        key, key_source = project_key(row)
        item = projects.setdefault(
            key,
            {
                "projectKey": key,
                "projectKeySource": key_source,
                "projectName": normalize_text(row.get("project_name")),
                "projectCode": normalize_text(row.get("project_code")),
                "lotCode": normalize_text(row.get("lot_code")),
                "lotName": normalize_text(row.get("lot_name")),
                "companies": {},
                "awardCompanies": {},
            },
        )
        company_name = normalize_text(row.get("company_name"))
        normalized_name = normalize_company_name(company_name)
        if normalized_name:
            item["companies"].setdefault(normalized_name, company_name)
            if normalize_text(row.get("award_status")) == "是":
                item["awardCompanies"].setdefault(normalized_name, company_name)
    return projects


def build_root_networks(
    relation_rows: Sequence[dict[str, Any]],
    root_company_names: Iterable[str],
) -> dict[str, dict[str, Any]]:
    """
    【函数功能】构建以投标根公司为所有者的去重企业关系网络。
    :param relation_rows: Sequence[dict[str, Any]]，企业关系表查询结果
    :param root_company_names: Iterable[str]，本次运行的根公司名称
    :return: dict[str, dict[str, Any]]，按规范化根公司名称索引的关系网络
    :Author: gexinyan
    :CreateTime: 2026-07-21 11:30:00
    Example: build_root_networks([], ("企业A",))
    """
    aliases = {
        normalize_company_name(name): normalize_text(name)
        for name in root_company_names
        if normalize_company_name(name)
    }
    networks: dict[str, dict[str, Any]] = {}
    for root_key, root_name in aliases.items():
        networks[root_key] = {
            "rootCompanyName": root_name,
            "entities": {
                root_key: {
                    "companyKey": root_key,
                    "companyName": root_name,
                    "entityRole": "root",
                    "rootCompanyKey": root_key,
                    "rootCompanyName": root_name,
                    "relations": [],
                }
            },
        }
    relation_signatures: dict[tuple[str, str], set[tuple[str, ...]]] = defaultdict(set)
    for row in relation_rows:
        root_key = normalize_company_name(row.get("search_value"))
        related_key = normalize_company_name(row.get("company_name"))
        if root_key not in networks or not related_key or related_key == root_key:
            continue
        root_name = networks[root_key]["rootCompanyName"]
        related_name = normalize_text(row.get("company_name"))
        entity = networks[root_key]["entities"].setdefault(
            related_key,
            {
                "companyKey": related_key,
                "companyName": related_name,
                "entityRole": "related",
                "rootCompanyKey": root_key,
                "rootCompanyName": root_name,
                "relations": [],
            },
        )
        relation = {
            "sourceTable": RELATION_TABLE,
            "sourceType": normalize_text(row.get("source_type")),
            "relationType": normalize_text(row.get("relation_type")),
            "personName": normalize_text(row.get("person_name")),
            "position": normalize_text(row.get("position")),
            "holdingRatio": normalize_text(row.get("holding_ratio")),
            "relatedEntityId": normalize_text(row.get("related_ent_id")),
        }
        signature = tuple(str(relation[key]) for key in sorted(relation))
        signature_key = (root_key, related_key)
        if signature in relation_signatures[signature_key]:
            continue
        relation_signatures[signature_key].add(signature)
        entity["relations"].append(relation)
    return networks


def build_company_evidence(
    spider_rows: dict[str, list[dict[str, Any]]],
    bidder_names: Iterable[str],
) -> tuple[dict[str, dict[str, list[dict[str, Any]]]], set[str]]:
    """
    【函数功能】把三张爬虫表的数据归并到规范化投标企业名称下。
    :param spider_rows: dict[str, list[dict[str, Any]]]，爬虫查询结果
    :param bidder_names: Iterable[str]，投标企业名称集合
    :return: tuple[dict[str, dict[str, list[dict[str, Any]]]], set[str]]，企业证据索引和已匹配企业集合
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    Example: build_company_evidence({"company": []}, ["企业A"])
    """
    alias_map = {normalize_company_name(name): normalize_text(name) for name in bidder_names}
    evidence: dict[str, dict[str, list[dict[str, Any]]]] = {
        key: {risk_type: [] for risk_type in RISK_LABELS} for key in alias_map
    }
    record_to_companies: dict[Any, set[str]] = defaultdict(set)
    matched: set[str] = set()
    for row in spider_rows.get("company", []):
        company_key = normalize_company_name(row.get("company_name"))
        search_key = normalize_company_name(row.get("search_value"))
        if company_key in alias_map:
            candidates = {company_key}
        elif search_key in alias_map:
            candidates = {search_key}
        else:
            candidates = set()
        for company_key in candidates:
            matched.add(company_key)
            if row.get("record_id") is not None:
                record_to_companies[row["record_id"]].add(company_key)
            for normalized, display in extract_phone_values(row.get("phone_number")):
                evidence[company_key]["shared_phone"].append(
                    {"normalizedValue": normalized, "displayValue": display, "sourceTable": "spider_data_company"}
                )
            for normalized, display in extract_email_values(row.get("email")):
                evidence[company_key]["shared_email"].append(
                    {"normalizedValue": normalized, "displayValue": display, "sourceTable": "spider_data_company"}
                )
    detail_specs = (
        ("shareholder", "shared_shareholder", "shareholder_name", "subscribed_ratio", "spider_data_shareholder"),
        ("senior_staff", "shared_senior_staff", "staff_name", "position", "spider_data_senior_staff"),
    )
    for row_key, risk_type, value_field, detail_field, source_table in detail_specs:
        for row in spider_rows.get(row_key, []):
            record_candidates = record_to_companies.get(row.get("record_id"), set())
            candidates = set(record_candidates)
            if not candidates:
                candidates = {
                    normalize_company_name(row.get("search_value"))
                }.intersection(alias_map)
            normalized = normalize_person_name(row.get(value_field))
            if not normalized:
                continue
            for company_key in candidates:
                matched.add(company_key)
                evidence[company_key][risk_type].append(
                    {
                        "normalizedValue": normalized,
                        "displayValue": normalize_text(row.get(value_field)),
                        "detail": normalize_text(row.get(detail_field)),
                        "sourceTable": source_table,
                    }
                )
    return evidence, matched


def detect_risks(
    run_id: str,
    projects: dict[str, dict[str, Any]],
    company_evidence: dict[str, dict[str, list[dict[str, Any]]]],
    root_networks: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """
    【函数功能】识别同一标段不同根公司网络间的三类共享信息与企业身份风险。
    :param run_id: str，本次流水线运行标识
    :param projects: dict[str, dict[str, Any]]，项目与参与企业索引
    :param company_evidence: dict[str, dict[str, list[dict[str, Any]]]]，企业爬虫证据
    :param root_networks: dict[str, dict[str, Any]] | None，根公司及关联企业网络
    :return: list[dict[str, Any]]，稳定排序后的风险记录
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    Example: detect_risks("run", projects, evidence)
    """
    grouped_risks: dict[tuple[Any, ...], dict[str, Any]] = {}
    networks = root_networks or {}

    def endpoint_view(entity: dict[str, Any], evidences: Sequence[dict[str, Any]]) -> dict[str, Any]:
        """
        【函数功能】生成保留角色、根公司归属、关系路径和风险证据的比较端点。
        :param entity: dict[str, Any]，关系网络实体
        :param evidences: Sequence[dict[str, Any]]，命中共享值的证据
        :return: dict[str, Any]，可写入风险 JSON 的企业端点
        :Author: gexinyan
        :CreateTime: 2026-07-21 11:30:00
        """
        return {
            "companyName": entity["companyName"],
            "entityRole": entity["entityRole"],
            "rootCompanyName": entity["rootCompanyName"],
            "relations": list(entity.get("relations", [])),
            "evidences": list(evidences),
        }

    def add_risk(
        comparison_type: str,
        risk_type: str,
        normalized_value: str,
        display_value: str,
        endpoints: Sequence[tuple[dict[str, Any], Sequence[dict[str, Any]]]],
        project: dict[str, Any],
        rule: str,
    ) -> None:
        """
        【函数功能】按根公司对、比较类型、共享值和实体对聚合风险触发项目。
        :param comparison_type: str，root_root/root_related/related_related
        :param risk_type: str，共享信息或企业身份风险类型
        :param normalized_value: str，规范化共享值
        :param display_value: str，报告展示值
        :param endpoints: Sequence，两个带证据的比较实体
        :param project: dict[str, Any]，触发风险的项目
        :param rule: str，风险判断规则
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-21 11:30:00
        """
        ordered = sorted(
            endpoints,
            key=lambda item: (
                item[0]["rootCompanyKey"],
                item[0]["entityRole"],
                item[0]["companyKey"],
            ),
        )
        root_keys = tuple(sorted({item[0]["rootCompanyKey"] for item in ordered}))
        endpoint_keys = tuple(
            (item[0]["rootCompanyKey"], item[0]["entityRole"], item[0]["companyKey"])
            for item in ordered
        )
        grouping_key = (
            comparison_type,
            risk_type,
            normalized_value,
            root_keys,
            endpoint_keys,
        )
        companies = [endpoint_view(entity, evidences) for entity, evidences in ordered]
        root_names = [
            next(item[0]["rootCompanyName"] for item in ordered if item[0]["rootCompanyKey"] == key)
            for key in root_keys
        ]
        group = grouped_risks.setdefault(
            grouping_key,
            {
                "riskType": risk_type,
                "riskLabel": RISK_LABELS[risk_type],
                "comparisonType": comparison_type,
                "comparisonLabel": COMPARISON_LABELS[comparison_type],
                "riskLevel": "中",
                "matchValue": display_value,
                "normalizedValue": normalized_value,
                "companyCount": len(companies),
                "distinctCompanyCount": len({item[0]["companyKey"] for item in ordered}),
                "rootCompanies": root_names,
                "companies": companies,
                "triggerProjects": [],
                "rule": rule,
            },
        )
        group["triggerProjects"].append(project_public_view(project))

    def structural_evidence(entity: dict[str, Any]) -> list[dict[str, Any]]:
        """
        【函数功能】为同一企业跨根公司网络出现生成单条关系表结构证据。
        :param entity: dict[str, Any]，跨网络重复出现的企业实体
        :return: list[dict[str, Any]]，结构风险证据
        :Author: gexinyan
        :CreateTime: 2026-07-21 11:30:00
        """
        return [
            {
                "normalizedValue": entity["companyKey"],
                "displayValue": entity["companyName"],
                "sourceTable": RELATION_TABLE,
                "detail": "同一企业出现在不同投标根公司的关系网络中",
            }
        ]

    for project in projects.values():
        if len(project["companies"]) < 2:
            continue
        project_networks: dict[str, dict[str, dict[str, Any]]] = {}
        for root_key, root_name in project["companies"].items():
            network = networks.get(root_key)
            if network is not None:
                project_networks[root_key] = network["entities"]
                continue
            project_networks[root_key] = {
                root_key: {
                    "companyKey": root_key,
                    "companyName": root_name,
                    "entityRole": "root",
                    "rootCompanyKey": root_key,
                    "rootCompanyName": root_name,
                    "relations": [],
                }
            }

        for first_root, second_root in combinations(sorted(project_networks), 2):
            first_entities = project_networks[first_root]
            second_entities = project_networks[second_root]
            if first_root in second_entities:
                first_entity = first_entities[first_root]
                related_entity = second_entities[first_root]
                evidence = structural_evidence(related_entity)
                add_risk(
                    "root_related",
                    "shared_company_identity",
                    first_root,
                    first_entity["companyName"],
                    ((first_entity, evidence), (related_entity, evidence)),
                    project,
                    "同一投标企业同时是另一投标根公司的关联企业",
                )
            if second_root in first_entities:
                root_entity = second_entities[second_root]
                related_entity = first_entities[second_root]
                evidence = structural_evidence(related_entity)
                add_risk(
                    "root_related",
                    "shared_company_identity",
                    second_root,
                    root_entity["companyName"],
                    ((related_entity, evidence), (root_entity, evidence)),
                    project,
                    "同一投标企业同时是另一投标根公司的关联企业",
                )
            shared_related = {
                key
                for key in set(first_entities).intersection(second_entities)
                if first_entities[key]["entityRole"] == "related"
                and second_entities[key]["entityRole"] == "related"
            }
            for entity_key in sorted(shared_related):
                first_entity = first_entities[entity_key]
                second_entity = second_entities[entity_key]
                first_evidence = structural_evidence(first_entity)
                second_evidence = structural_evidence(second_entity)
                add_risk(
                    "related_related",
                    "shared_company_identity",
                    entity_key,
                    first_entity["companyName"],
                    ((first_entity, first_evidence), (second_entity, second_evidence)),
                    project,
                    "同一关联企业同时出现在两家不同投标根公司的关系网络中",
                )

        inverted: dict[
            tuple[str, str],
            list[tuple[dict[str, Any], list[dict[str, Any]]]],
        ] = defaultdict(list)
        for entities in project_networks.values():
            for entity in entities.values():
                for risk_type, rows in company_evidence.get(entity["companyKey"], {}).items():
                    rows_by_value: dict[str, list[dict[str, Any]]] = defaultdict(list)
                    for row in rows:
                        rows_by_value[row["normalizedValue"]].append(row)
                    for normalized_value, matching_rows in rows_by_value.items():
                        inverted[(risk_type, normalized_value)].append((entity, matching_rows))
        for (risk_type, normalized_value), occurrences in inverted.items():
            for first, second in combinations(occurrences, 2):
                first_entity, first_rows = first
                second_entity, second_rows = second
                if first_entity["rootCompanyKey"] == second_entity["rootCompanyKey"]:
                    continue
                if first_entity["companyKey"] == second_entity["companyKey"]:
                    continue
                roles = {first_entity["entityRole"], second_entity["entityRole"]}
                if roles == {"root"}:
                    comparison_type = "root_root"
                elif roles == {"related"}:
                    comparison_type = "related_related"
                else:
                    comparison_type = "root_related"
                display_value = normalize_text(
                    first_rows[0].get("displayValue") or second_rows[0].get("displayValue")
                ) or normalized_value
                add_risk(
                    comparison_type,
                    risk_type,
                    normalized_value,
                    display_value,
                    ((first_entity, first_rows), (second_entity, second_rows)),
                    project,
                    f"同一标段内{COMPARISON_LABELS[comparison_type]}，共享同一规范化信息",
                )
    risks = []
    for grouping_key, risk in grouped_risks.items():
        trigger_projects = sorted(
            {item["projectKey"]: item for item in risk["triggerProjects"]}.values(),
            key=lambda item: item["projectKey"],
        )
        fingerprint = "|".join((run_id, *(str(item) for item in grouping_key)))
        risks.append(
            {
                **risk,
                "riskId": hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()[:20],
                "project": trigger_projects[0],
                "triggerProjects": trigger_projects,
                "commonProjects": [],
            }
        )
    return sorted(
        risks,
        key=lambda item: (
            item["project"]["projectKey"],
            item["comparisonType"],
            item["riskType"],
            item["normalizedValue"],
            tuple(item["rootCompanies"]),
        ),
    )


def project_public_view(project: dict[str, Any]) -> dict[str, Any]:
    """
    【函数功能】提取项目的可序列化基础字段，避免内部企业索引进入风险记录。
    :param project: dict[str, Any]，内部项目索引对象
    :return: dict[str, Any]，项目基础字段
    :Author: gexinyan
    :CreateTime: 2026-07-17 09:20:59
    Example: project_public_view({"projectKey": "lot:L-1", "companies": {}})
    """
    return {
        key: project.get(key, "")
        for key in ("projectKey", "projectKeySource", "projectName", "projectCode", "lotCode", "lotName")
    }


def build_common_projects(risk: dict[str, Any], projects: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """
    【函数功能】查询风险企业组合全部参与过的项目，并附带全部投标企业和中标企业。
    :param risk: dict[str, Any]，已聚合的风险记录
    :param projects: dict[str, dict[str, Any]]，历史项目及投标企业索引
    :return: list[dict[str, Any]]，可直接写入风险 JSON 的共同项目列表
    :Author: gexinyan
    :CreateTime: 2026-07-17 09:20:59
    Example: build_common_projects({"companies": []}, {})
    """
    root_names = risk.get("rootCompanies") or [
        company.get("companyName") for company in risk.get("companies", [])
    ]
    risk_company_keys = {
        normalize_company_name(name) for name in root_names if normalize_company_name(name)
    }
    common_projects = []
    for project in projects.values():
        if not risk_company_keys.issubset(project["companies"]):
            continue
        common_projects.append(
            {
                **project_public_view(project),
                "bidders": sorted(project["companies"].values()),
                "awardedCompanies": sorted(project["awardCompanies"].values()),
                "riskCompanies": sorted(project["companies"][key] for key in risk_company_keys),
            }
        )
    return sorted(common_projects, key=lambda item: item["projectKey"])


def enrich_risks_with_common_projects(
    risks: Sequence[dict[str, Any]],
    projects: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    【函数功能】为本次触发的每组风险补充全库历史共同参投项目。
    :param risks: Sequence[dict[str, Any]]，本次运行触发的风险记录
    :param projects: dict[str, dict[str, Any]]，历史项目及投标企业索引
    :return: list[dict[str, Any]]，带 commonProjects 的风险记录
    :Author: gexinyan
    :CreateTime: 2026-07-17 09:20:59
    Example: enrich_risks_with_common_projects([], {})
    """
    enriched = []
    for risk in risks:
        enriched.append({**risk, "commonProjects": build_common_projects(risk, projects)})
    return enriched


def write_risk_json(
    output_path: Path,
    run_id: str,
    database_config: DatabaseConfig,
    spider_database: str,
    projects: dict[str, dict[str, Any]],
    risks: Sequence[dict[str, Any]],
    matched_companies: set[str],
    root_networks: dict[str, dict[str, Any]] | None = None,
) -> RiskAnalysisSummary:
    """
    【函数功能】写出完整风险 JSON，并返回可供流水线审计的汇总。
    :param output_path: Path，JSON 输出路径
    :param run_id: str，本次流水线运行标识
    :param database_config: DatabaseConfig，解析结果数据库配置
    :param spider_database: str，爬虫结果数据库名
    :param projects: dict[str, dict[str, Any]]，项目分组数据
    :param risks: Sequence[dict[str, Any]]，风险记录
    :param matched_companies: set[str]，匹配到爬虫数据的规范化企业名
    :param root_networks: dict[str, dict[str, Any]] | None，根公司与关联企业网络
    :return: RiskAnalysisSummary，风险分析汇总
    :raises OSError: JSON 无法写入时抛出
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    Example: write_risk_json(Path("risk.json"), "run", config, "big_data", {}, [], set())
    """
    root_company_names = {
        key: name for project in projects.values() for key, name in project["companies"].items()
    }
    networks = root_networks or {}
    related_company_names: dict[str, str] = {}
    relation_count = 0
    for root_key in root_company_names:
        network = networks.get(root_key, {})
        for entity_key, entity in network.get("entities", {}).items():
            if entity.get("entityRole") != "related":
                continue
            related_company_names.setdefault(entity_key, entity["companyName"])
            relation_count += len(entity.get("relations", []))
    risk_type_counts = {risk_type: 0 for risk_type in RISK_LABELS}
    comparison_type_counts = {comparison_type: 0 for comparison_type in COMPARISON_LABELS}
    for risk in risks:
        risk_type_counts[risk["riskType"]] += 1
        comparison_type_counts[risk["comparisonType"]] += 1
    project_items = []
    for project in projects.values():
        project_items.append(
            {
                **project_public_view(project),
                "companies": sorted(project["companies"].values()),
                "awardedCompanies": sorted(project["awardCompanies"].values()),
                "riskCount": sum(
                    project["projectKey"] in {
                        item["projectKey"]
                        for item in risk.get("triggerProjects", [risk.get("project", {})])
                    }
                    for risk in risks
                ),
            }
        )
    unmatched_root_keys = set(root_company_names).difference(matched_companies)
    unmatched_related_keys = set(related_company_names).difference(matched_companies)
    matched_root_count = len(set(root_company_names).intersection(matched_companies))
    matched_related_count = len(set(related_company_names).intersection(matched_companies))
    unmatched_company_count = len(unmatched_root_keys)
    payload = {
        "schemaVersion": "2.0",
        "generatedAt": datetime.now().astimezone().isoformat(),
        "runId": run_id,
        "source": {
            "host": database_config.host,
            "port": database_config.port,
            "resultDatabase": database_config.database,
            "spiderDatabase": spider_database,
            "schema": database_config.schema,
            "resultTable": database_config.table,
            "spiderTables": [
                "spider_data_company",
                "spider_data_shareholder",
                "spider_data_senior_staff",
                RELATION_TABLE,
            ],
        },
        "summary": {
            "projectCount": len(projects),
            "companyCount": len(root_company_names),
            "matchedCompanyCount": matched_root_count,
            "unmatchedCompanyCount": unmatched_company_count,
            "rootCompanyCount": len(root_company_names),
            "relatedCompanyCount": len(related_company_names),
            "matchedRootCompanyCount": matched_root_count,
            "unmatchedRootCompanyCount": unmatched_company_count,
            "matchedRelatedCompanyCount": matched_related_count,
            "unmatchedRelatedCompanyCount": len(unmatched_related_keys),
            "relationCount": relation_count,
            "riskCount": len(risks),
            "riskTypeCounts": risk_type_counts,
            "comparisonTypeCounts": comparison_type_counts,
        },
        "unmatchedCompanies": sorted(
            root_company_names[key] for key in unmatched_root_keys
        ),
        "unmatchedRelatedCompanies": sorted(
            related_company_names[key] for key in unmatched_related_keys
        ),
        "projects": sorted(project_items, key=lambda item: item["projectKey"]),
        "risks": list(risks),
        "notes": [
            "项目分组优先组合项目编号与标段编号；编号缺失时使用项目名称或标段名称，"
            "项目身份缺失时按记录隔离。",
            "仅比较同一标段内不同投标根公司所属的企业网络，不比较单个根公司网络内部。",
            "企业关系表仅用于确定关联拓扑，联系方式和人员证据来自企业详情表。",
            "关系表没有运行标识，关联网络反映 openGauss 当前可见的有效关系数据。",
            "风险记录仅表示数据关联线索，不构成串标违法事实认定。",
        ],
    }
    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return RiskAnalysisSummary(
        risk_count=len(risks),
        project_count=len(projects),
        company_count=len(root_company_names),
        unmatched_company_count=unmatched_company_count,
        json_path=output_path,
    )


def analyze_risks(
    database_config: DatabaseConfig,
    spider_database: str,
    run_id: str,
    output_path: Path,
) -> RiskAnalysisSummary:
    """
    【函数功能】执行投标结果、爬虫数据查询、关联分析与风险 JSON 输出全流程。
    :param database_config: DatabaseConfig，解析结果数据库配置
    :param spider_database: str，爬虫结果数据库名
    :param run_id: str，本次流水线运行标识
    :param output_path: Path，风险 JSON 输出路径
    :return: RiskAnalysisSummary，风险分析汇总
    :raises Exception: 数据库查询、分析或文件写入失败时抛出
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    Example: analyze_risks(config, "big_data", "run-id", Path("risk.json"))
    """
    bidder_rows = fetch_bidder_rows(database_config, run_id)
    projects = build_projects(bidder_rows)
    bidder_names = tuple(
        dict.fromkeys(project["companies"][key] for project in projects.values() for key in project["companies"])
    )
    spider_config = replace(database_config, database=spider_database)
    relation_rows = fetch_relation_rows(spider_config, bidder_names)
    root_networks = build_root_networks(relation_rows, bidder_names)
    related_names = [
        entity["companyName"]
        for network in root_networks.values()
        for entity in network["entities"].values()
        if entity["entityRole"] == "related"
    ]
    entity_names = tuple(dict.fromkeys((*bidder_names, *related_names)))
    spider_rows = fetch_spider_rows(spider_config, entity_names)
    evidence, matched_companies = build_company_evidence(spider_rows, entity_names)
    risks = detect_risks(run_id, projects, evidence, root_networks)
    historical_projects = build_projects(fetch_historical_project_rows(database_config, bidder_names))
    for project_key_value, current_project in projects.items():
        historical_project = historical_projects.setdefault(project_key_value, current_project)
        if historical_project is not current_project:
            historical_project["companies"].update(current_project["companies"])
            historical_project["awardCompanies"].update(current_project["awardCompanies"])
    risks = enrich_risks_with_common_projects(risks, historical_projects)
    return write_risk_json(
        output_path,
        run_id,
        database_config,
        spider_database,
        projects,
        risks,
        matched_companies,
        root_networks,
    )
