"""
【模块功能】将 Pipeline 风险 JSON 适配为新版 Markdown 风险报告并渲染 PDF。

:Author: gexinyan
:CreateTime: 2026-07-30 16:25:19
"""

from __future__ import annotations

import itertools
import json
import re
import subprocess
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from pypdf import PdfReader


VISIBLE_RISK_TYPES = {
    "shared_phone",
    "shared_email",
    "shared_shareholder",
    "shared_senior_staff",
}
RISK_TYPE_ORDER = (
    "shared_mobile",
    "shared_landline",
    "shared_email",
    "shared_shareholder",
    "shared_senior_staff",
)
RISK_TYPE_LABELS = {
    "shared_mobile": "手机号相同",
    "shared_landline": "固定电话相同",
    "shared_email": "邮箱相同",
    "shared_shareholder": "股东名称相同",
    "shared_senior_staff": "高级职员名称相同",
}
EVIDENCE_SOURCE_LABELS = {
    "spider_data_company": "企业工商详情",
    "spider_data_shareholder": "企业股东信息",
    "spider_data_senior_staff": "企业高级职员信息",
    "spider_data_person_enterprise_relation": "企业关联关系",
}
RISK_COMPANY_COLOR = "#8B0000"
WINNING_COMPANY_COLOR = "#1D4ED8"
RISK_WINNING_COMPANY_COLOR = "#15803D"
PROJECT_NAME_REDACTIONS = ("惠山区",)


@dataclass(frozen=True, slots=True)
class ReportSummary:
    """
    【类功能】描述新版风险报告 Markdown 与 PDF 产物。
    :Attributes:
        markdown_path: Path，Markdown 报告路径
        pdf_path: Path，PDF 报告路径
        risk_count: int，报告实际展示的风险线索数量
    :Author: gexinyan
    :CreateTime: 2026-07-30 16:25:19
    """

    markdown_path: Path
    pdf_path: Path
    risk_count: int


def normalize_text(value: Any, default: str = "") -> str:
    """
    【函数功能】将任意输入规范化为去除首尾空白的文本。
    :param value: Any，待规范化值
    :param default: str，空值默认文本
    :return: str，规范化文本
    :Author: gexinyan
    :CreateTime: 2026-07-30 16:25:19
    Example: normalize_text(" 企业A ")
    """
    if value is None:
        return default
    result = str(value).strip()
    return result or default


def markdown_cell(value: Any) -> str:
    """
    【函数功能】转义 Markdown 表格单元格中的控制字符。
    :param value: Any，原始单元格值
    :return: str，可安全写入 Markdown 表格的文本
    :Author: gexinyan
    :CreateTime: 2026-07-30 16:25:19
    Example: markdown_cell("A|B")
    """
    return normalize_text(value, "未披露").replace("|", "\\|").replace("\n", "<br/>")


def validate_payload(payload: Any) -> dict[str, Any]:
    """
    【函数功能】校验报告生成依赖的 Pipeline 风险 JSON 基础结构。
    :param payload: Any，反序列化后的风险 JSON
    :return: dict[str, Any]，通过校验的风险对象
    :raises ValueError: JSON 根结构、summary、projects 或 risks 无效时触发
    :Author: gexinyan
    :CreateTime: 2026-07-30 16:25:19
    Example: validate_payload({"summary": {}, "projects": [], "risks": []})
    """
    if not isinstance(payload, dict):
        raise ValueError("风险 JSON 根节点必须是对象")
    if not isinstance(payload.get("summary"), dict):
        raise ValueError("风险 JSON 缺少 summary 对象")
    for field_name in ("projects", "risks"):
        if not isinstance(payload.get(field_name), list):
            raise ValueError(f"风险 JSON 缺少 {field_name} 列表")
        if not all(isinstance(item, dict) for item in payload[field_name]):
            raise ValueError(f"风险 JSON 的 {field_name} 必须仅包含对象")
    return payload


def adapted_risk_type(risk: dict[str, Any]) -> str | None:
    """
    【函数功能】将 Pipeline 风险类型适配为新版报告分类并过滤企业身份重合风险。
    :param risk: dict[str, Any]，Pipeline 风险记录
    :return: str | None，新版报告风险类型；不展示时返回 None
    :Author: gexinyan
    :CreateTime: 2026-07-30 16:25:19
    Example: adapted_risk_type({"riskType": "shared_phone", "matchValue": "13800138000"})
    """
    risk_type = normalize_text(risk.get("riskType"))
    if risk_type not in VISIBLE_RISK_TYPES:
        return None
    if risk_type != "shared_phone":
        return risk_type
    digits = re.sub(r"\D", "", normalize_text(risk.get("normalizedValue") or risk.get("matchValue")))
    return "shared_mobile" if re.fullmatch(r"1\d{10}", digits) else "shared_landline"


def adapted_risk_label(risk: dict[str, Any]) -> str:
    """
    【函数功能】生成适合新版报告展示的风险类别名称。
    :param risk: dict[str, Any]，Pipeline 风险记录
    :return: str，风险类别名称
    :Author: gexinyan
    :CreateTime: 2026-07-30 16:25:19
    Example: adapted_risk_label({"riskType": "shared_email"})
    """
    report_type = adapted_risk_type(risk)
    return RISK_TYPE_LABELS.get(report_type or "", markdown_cell(risk.get("riskLabel")))


def format_company_name(company_name: str, risk_companies: set[str], winners: set[str]) -> str:
    """
    【函数功能】按风险企业和中标企业身份为企业名称添加颜色与加粗标记。
    :param company_name: str，企业名称
    :param risk_companies: set[str]，当前风险涉及的投标根公司
    :param winners: set[str]，当前项目中标企业
    :return: str，带安全高亮标记的企业名称
    :Author: gexinyan
    :CreateTime: 2026-07-30 16:25:19
    Example: format_company_name("企业A", {"企业A"}, {"企业A"})
    """
    safe_name = markdown_cell(company_name)
    is_risk = company_name in risk_companies
    is_winner = company_name in winners
    if is_risk and is_winner:
        color = RISK_WINNING_COMPANY_COLOR
    elif is_risk:
        color = RISK_COMPANY_COLOR
    elif is_winner:
        color = WINNING_COMPANY_COLOR
    else:
        return safe_name
    return f'<span style="color:{color}">**{safe_name}**</span>'


def relation_summary(company: dict[str, Any]) -> str:
    """
    【函数功能】将 Pipeline 企业端点的根公司归属和关系路径转换为通俗说明。
    :param company: dict[str, Any]，风险企业端点
    :return: str，企业角色与关联关系说明
    :Author: gexinyan
    :CreateTime: 2026-07-30 16:25:19
    Example: relation_summary({"entityRole": "root", "companyName": "企业A"})
    """
    company_name = normalize_text(company.get("companyName"), "未披露企业")
    if company.get("entityRole", "root") == "root":
        return f"**{company_name}**是参与投标企业。"
    root_company = normalize_text(company.get("rootCompanyName"), "未识别投标根公司")
    relation_details = relation_evidence_descriptions(company.get("relations", []))
    detail_text = "；".join(relation_details) or "两家公司的企业关系记录显示存在关联，具体关系详情未披露"
    return (
        f"**{company_name}**未参与投标；**{company_name}**与参与投标企业"
        f"**{root_company}**存在关联关系，关系依据为：“{detail_text}”。"
    )


def relation_evidence_descriptions(relations: Any) -> list[str]:
    """
    【函数功能】将企业关系记录归并为适合风险报告展示的中文关联依据。
    :param relations: Any，企业端点中的原始关联关系列表
    :return: list[str]，去重后的关联依据说明
    :Author: gexinyan
    :CreateTime: 2026-07-30 18:10:00
    Example: relation_evidence_descriptions([{"sourceType": "SHAREHOLDER", "personName": "张三"}])
    """
    grouped_types: dict[str, set[str]] = defaultdict(set)
    for relation in relations if isinstance(relations, list) else []:
        if not isinstance(relation, dict):
            continue
        raw_types = " ".join(
            normalize_text(relation.get(field_name)).upper()
            for field_name in ("sourceType", "relationType")
        )
        relationship_types = set()
        if "SHAREHOLDER" in raw_types or "股东" in raw_types:
            relationship_types.add("股东")
        if "SENIOR_STAFF" in raw_types or "高级职员" in raw_types or "高管" in raw_types:
            relationship_types.add("高级职员")
        person_name = normalize_text(relation.get("personName"), "未披露")
        if relationship_types:
            grouped_types[person_name].update(relationship_types)

    descriptions = []
    for person_name, relationship_types in sorted(grouped_types.items()):
        type_text = "和".join(
            item for item in ("股东", "高级职员") if item in relationship_types
        )
        if person_name == "未披露":
            descriptions.append(f"两家公司的{type_text}信息存在重合，相关人员名称未披露")
        else:
            descriptions.append(f"两家公司的{type_text}相同，名称为**{person_name}**")
    return descriptions


def evidence_rows(risk: dict[str, Any]) -> list[dict[str, str]]:
    """
    【函数功能】使用风险端点内已有证据生成新版报告原始数据核验表。
    :param risk: dict[str, Any]，可展示风险记录
    :return: list[dict[str, str]]，逐企业证据核验行
    :Author: gexinyan
    :CreateTime: 2026-07-30 16:25:19
    Example: evidence_rows({"matchValue": "A", "companies": []})
    """
    matched_value = normalize_text(risk.get("matchValue"), "未披露")
    rows = []
    for company in risk.get("companies", []):
        if not isinstance(company, dict):
            continue
        raw_values = []
        source_names = []
        for evidence in company.get("evidences", []):
            if not isinstance(evidence, dict):
                continue
            display_value = normalize_text(evidence.get("displayValue") or evidence.get("normalizedValue"))
            detail = normalize_text(evidence.get("detail"))
            if display_value:
                raw_values.append(f"{display_value}（{detail}）" if detail else display_value)
            source_table = normalize_text(evidence.get("sourceTable"))
            if source_table:
                source_names.append(EVIDENCE_SOURCE_LABELS.get(source_table, source_table))
        raw_text = "；".join(dict.fromkeys(raw_values)) or matched_value
        if matched_value and matched_value != "未披露":
            raw_text = re.sub(re.escape(matched_value), f"**{matched_value}**", raw_text)
        rows.append(
            {
                "企业名称": markdown_cell(company.get("companyName")),
                "企业关联关系": markdown_cell(relation_summary(company)),
                "数据来源": markdown_cell("、".join(dict.fromkeys(source_names)) or "风险分析结果"),
                "原始数据": markdown_cell(raw_text),
                "本条重合信息": markdown_cell(f"**{matched_value}**"),
            }
        )
    return rows


def project_display_name(project: dict[str, Any]) -> str:
    """
    【函数功能】生成包含必要标段信息的项目展示名称。
    :param project: dict[str, Any]，项目记录
    :return: str，项目或标段展示名称
    :Author: gexinyan
    :CreateTime: 2026-07-30 16:25:19
    Example: project_display_name({"projectName": "项目A", "lotCode": "L1"})
    """
    project_name = report_project_name(project.get("projectName") or project.get("lotName"))
    lot_code = normalize_text(project.get("lotCode"))
    return f"{project_name}（标段：{lot_code}）" if lot_code else project_name


def report_project_name(value: Any) -> str:
    """
    【函数功能】仅在风险报告展示阶段移除项目名称中的隐私区域词。
    :param value: Any，数据库或风险 JSON 中的原始项目名称
    :return: str，已脱敏且可用于报告展示的项目名称
    :Author: gexinyan
    :CreateTime: 2026-07-30 18:30:00
    Example: report_project_name("惠山区项目A")
    """
    project_name = normalize_text(value, "未披露项目")
    for redacted_text in PROJECT_NAME_REDACTIONS:
        project_name = project_name.replace(redacted_text, "")
    return normalize_text(project_name, "未披露项目")


def current_bid_records(projects: Sequence[dict[str, Any]]) -> list[dict[str, str]]:
    """
    【函数功能】从本次运行 projects 重建投标企业及中标状态记录。
    :param projects: Sequence[dict[str, Any]]，当前 run_id 的项目列表
    :return: list[dict[str, str]]，仅属于当前任务的投标记录
    :Author: gexinyan
    :CreateTime: 2026-07-30 16:25:19
    Example: current_bid_records([{"companies": ["企业A"], "awardedCompanies": []}])
    """
    records = []
    for index, project in enumerate(projects, start=1):
        project_id = normalize_text(project.get("projectKey"), f"project-{index}")
        winners = {normalize_text(item) for item in project.get("awardedCompanies", []) if normalize_text(item)}
        for company_name in project.get("companies", []):
            normalized_name = normalize_text(company_name)
            if not normalized_name:
                continue
            records.append(
                {
                    "project_id": project_id,
                    "project_name": project_display_name(project),
                    "company_name": normalized_name,
                    "bid_status": "中标" if normalized_name in winners else "未中标",
                }
            )
    return records


def build_bid_indexes(
    records: Sequence[dict[str, str]],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, str]]]:
    """
    【函数功能】构建当前任务的项目企业索引和企业项目索引。
    :param records: Sequence[dict[str, str]]，当前任务投标记录
    :return: tuple，项目索引和企业投标状态索引
    :Author: gexinyan
    :CreateTime: 2026-07-30 16:25:19
    Example: build_bid_indexes([])
    """
    projects: dict[str, dict[str, Any]] = {}
    company_projects: dict[str, dict[str, str]] = defaultdict(dict)
    for record in records:
        project = projects.setdefault(
            record["project_id"],
            {"project_name": record["project_name"], "companies": {}},
        )
        project["companies"][record["company_name"]] = record["bid_status"]
        company_projects[record["company_name"]][record["project_id"]] = record["bid_status"]
    return projects, dict(company_projects)


def project_detail_rows(
    project_ids: set[str],
    project_index: dict[str, dict[str, Any]],
    focus_companies: Sequence[str],
) -> list[dict[str, Any]]:
    """
    【函数功能】生成当前任务投标行为统计的项目明细表。
    :param project_ids: set[str]，需要展示的项目标识
    :param project_index: dict[str, dict[str, Any]]，当前任务项目索引
    :param focus_companies: Sequence[str]，当前统计条目关注企业
    :return: list[dict[str, Any]]，项目明细行
    :Author: gexinyan
    :CreateTime: 2026-07-30 16:25:19
    Example: project_detail_rows(set(), {}, [])
    """
    focus = set(focus_companies)
    rows = []
    for project_id in sorted(project_ids, key=lambda value: project_index[value]["project_name"]):
        project = project_index[project_id]
        participants = project["companies"]
        winners = {name for name, status in participants.items() if status == "中标"}
        result_parts = []
        for company_name in sorted(focus):
            if company_name not in participants:
                continue
            highlighted = format_company_name(company_name, focus, winners)
            status = participants[company_name]
            status_text = (
                f'<span style="color:{RISK_WINNING_COMPANY_COLOR}">**中标**</span>'
                if status == "中标"
                else "未中标"
            )
            result_parts.append(f"{highlighted}：{status_text}")
        rows.append(
            {
                "项目名称": markdown_cell(project["project_name"]),
                "所有参与企业": "、".join(
                    format_company_name(name, focus, winners) for name in sorted(participants)
                ),
                "投标结果": "<br/>".join(result_parts) or "未识别",
                "项目中标企业": "、".join(
                    format_company_name(name, focus, winners) for name in sorted(winners)
                ) or "未识别中标企业",
            }
        )
    return rows


def maximal_common_bidder_groups(
    company_projects: dict[str, dict[str, str]], minimum_projects: int = 4
) -> list[dict[str, Any]]:
    """
    【函数功能】识别共同参与指定数量项目的最大企业组合，避免重复输出子组合。
    :param company_projects: dict[str, dict[str, str]]，企业项目状态索引
    :param minimum_projects: int，最少共同参与项目数
    :return: list[dict[str, Any]]，最大企业组合及共同项目
    :Author: gexinyan
    :CreateTime: 2026-07-30 16:25:19
    Example: maximal_common_bidder_groups({}, 4)
    """
    eligible = {
        company: set(projects)
        for company, projects in company_projects.items()
        if len(projects) >= minimum_projects
    }
    grouped_by_projects: dict[frozenset[str], set[str]] = defaultdict(set)
    for company, projects in eligible.items():
        grouped_by_projects[frozenset(projects)].add(company)
    candidates: dict[frozenset[str], set[str]] = {}
    project_sets = list(grouped_by_projects)
    for project_set in project_sets:
        companies = {
            company
            for candidate_set, names in grouped_by_projects.items()
            if project_set.issubset(candidate_set)
            for company in names
        }
        if len(companies) >= 2:
            candidates[frozenset(companies)] = set(project_set)
    maximal = []
    for companies, projects in candidates.items():
        if any(companies < other for other in candidates):
            continue
        maximal.append({"企业组合": sorted(companies), "共同项目": projects})
    return sorted(maximal, key=lambda item: (-len(item["共同项目"]), tuple(item["企业组合"])))


def bid_statistics(records: Sequence[dict[str, str]]) -> dict[str, Any]:
    """
    【函数功能】基于当前 run_id 重建久投不中、高中标率和共同参投企业统计。
    :param records: Sequence[dict[str, str]]，当前任务投标记录
    :return: dict[str, Any]，新版模板投标行为统计上下文
    :Author: gexinyan
    :CreateTime: 2026-07-30 16:25:19
    Example: bid_statistics([])
    """
    project_index, company_projects = build_bid_indexes(records)
    statistics = []
    for company_name, projects in company_projects.items():
        wins = sum(status == "中标" for status in projects.values())
        bids = len(projects)
        statistics.append(
            {
                "企业名称": company_name,
                "投标次数": bids,
                "中标次数": wins,
                "未中标次数": bids - wins,
                "中标率数值": wins / bids * 100 if bids else 0,
            }
        )

    def company_entries(items: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        【函数功能】将企业统计转换为带项目明细的模板条目。
        :param items: Sequence[dict[str, Any]]，企业统计对象
        :return: list[dict[str, Any]]，模板企业条目
        :Author: gexinyan
        :CreateTime: 2026-07-30 16:25:19
        Example: company_entries([])
        """
        result = []
        for index, item in enumerate(items, start=1):
            company_name = item["企业名称"]
            result.append(
                {
                    "条目序号": index,
                    "企业名称": markdown_cell(company_name),
                    "投标次数": item["投标次数"],
                    "中标次数": item["中标次数"],
                    "未中标次数": item["未中标次数"],
                    "中标率": f'{item["中标率数值"]:.2f}%',
                    "项目明细行": project_detail_rows(
                        set(company_projects[company_name]), project_index, [company_name]
                    ),
                }
            )
        return result

    long_term_losers = sorted(
        (item for item in statistics if item["投标次数"] >= 4 and item["中标次数"] == 0),
        key=lambda item: (-item["投标次数"], item["企业名称"]),
    )
    high_win_rate = sorted(
        (
            item
            for item in statistics
            if item["投标次数"] >= 3 and item["中标率数值"] >= 33.33
        ),
        key=lambda item: (-item["中标率数值"], -item["投标次数"], item["企业名称"]),
    )
    suspected_groups = []
    for index, group in enumerate(maximal_common_bidder_groups(company_projects), start=1):
        suspected_groups.append(
            {
                "条目序号": index,
                "企业组合": markdown_cell("、".join(group["企业组合"])),
                "共同项目数": len(group["共同项目"]),
                "项目明细行": project_detail_rows(
                    set(group["共同项目"]), project_index, group["企业组合"]
                ),
            }
        )
    return {
        "存在久投不中企业": bool(long_term_losers),
        "无久投不中企业": not long_term_losers,
        "久投不中企业条目": company_entries(long_term_losers),
        "存在高中标率企业": bool(high_win_rate),
        "无高中标率企业": not high_win_rate,
        "高中标率企业条目": company_entries(high_win_rate),
        "存在疑似陪标企业": bool(suspected_groups),
        "无疑似陪标企业": not suspected_groups,
        "疑似陪标企业条目": suspected_groups,
    }


def historical_project_rows(risk: dict[str, Any]) -> list[dict[str, str]]:
    """
    【函数功能】仅使用风险 JSON 已有 commonProjects 生成历史共同参投证据表。
    :param risk: dict[str, Any]，可展示风险记录
    :return: list[dict[str, str]]，历史共同参投项目行
    :Author: gexinyan
    :CreateTime: 2026-07-30 16:25:19
    Example: historical_project_rows({"commonProjects": []})
    """
    rows = []
    risk_companies = {normalize_text(name) for name in risk.get("rootCompanies", []) if normalize_text(name)}
    for project in risk.get("commonProjects", []):
        if not isinstance(project, dict):
            continue
        bidders = {normalize_text(name) for name in project.get("bidders", []) if normalize_text(name)}
        winners = {
            normalize_text(name) for name in project.get("awardedCompanies", []) if normalize_text(name)
        }
        risk_bidders = sorted(bidders & risk_companies)
        rows.append(
            {
                "共同参与项目": markdown_cell(project_display_name(project)),
                "所有参与企业": "<br/>".join(
                    format_company_name(name, risk_companies, winners) for name in sorted(bidders)
                ) or "未识别投标企业",
                "投标结果": "<br/>".join(
                    f"{format_company_name(name, risk_companies, winners)}："
                    f"{'中标' if name in winners else '未中标'}"
                    for name in risk_bidders
                ) or "未识别涉及风险的投标企业",
                "项目中标企业": "、".join(
                    format_company_name(name, risk_companies, winners) for name in sorted(winners)
                ) or "未识别中标企业",
            }
        )
    return rows


def risk_description(risk: dict[str, Any]) -> str:
    """
    【函数功能】生成包含企业、风险类别和重合对象的通俗风险说明。
    :param risk: dict[str, Any]，可展示风险记录
    :return: str，风险说明
    :Author: gexinyan
    :CreateTime: 2026-07-30 16:25:19
    Example: risk_description({"companies": [], "matchValue": "A"})
    """
    company_names = [
        normalize_text(company.get("companyName"))
        for company in risk.get("companies", [])
        if isinstance(company, dict) and normalize_text(company.get("companyName"))
    ]
    names = "、".join(f"**{name}**" for name in company_names) or "相关企业"
    return (
        f"{names}出现{adapted_risk_label(risk)}，重合信息为"
        f"**{normalize_text(risk.get('matchValue'), '未披露')}**，建议结合原始材料进一步核验。"
    )


def two_column_name_rows(company_names: Sequence[str]) -> list[dict[str, str | int]]:
    """
    【函数功能】将企业名称按两列编号排布为报告模板行数据。
    :param company_names: Sequence[str]，已排序的参与投标企业名称
    :return: list[dict[str, str | int]]，包含左右序号和名称的双栏行数据
    :Author: gexinyan
    :CreateTime: 2026-07-30 18:10:00
    Example: two_column_name_rows(["企业A", "企业B", "企业C"])
    """
    rows = []
    for start_index in range(0, len(company_names), 2):
        left_name = company_names[start_index]
        right_index = start_index + 1
        right_name = company_names[right_index] if right_index < len(company_names) else ""
        rows.append(
            {
                "左序号": start_index + 1,
                "左名称": markdown_cell(left_name),
                "右序号": right_index + 1 if right_name else "",
                "右名称": markdown_cell(right_name) if right_name else "",
            }
        )
    return rows


def cross_network_statistics(visible_risks: Sequence[dict[str, Any]]) -> tuple[int, str]:
    """
    【函数功能】统计可展示风险涉及的投标企业组合及其历史共同参投补充证据。
    :param visible_risks: Sequence[dict[str, Any]]，已过滤企业身份重合后的风险记录
    :return: tuple[int, str]，企业组合数量和可直接填入结论的共同项目说明
    :Author: gexinyan
    :CreateTime: 2026-07-30 18:10:00
    Example: cross_network_statistics([])
    """
    company_pairs = set()
    common_project_names = set()
    for risk in visible_risks:
        root_companies = sorted(
            {
                normalize_text(company_name)
                for company_name in risk.get("rootCompanies", [])
                if normalize_text(company_name)
            }
        )
        if len(root_companies) >= 2:
            company_pairs.update(itertools.combinations(root_companies, 2))
        for project in risk.get("commonProjects", []):
            if isinstance(project, dict):
                common_project_names.add(project_display_name(project))
    if common_project_names:
        common_project_analysis = (
            f"风险 JSON 已提供 {len(common_project_names)} 个历史共同参投项目作为补充证据，"
        )
    else:
        common_project_analysis = "风险 JSON 未提供历史共同参投项目的补充证据，"
    return len(company_pairs), common_project_analysis


def build_template_context(payload: dict[str, Any]) -> dict[str, Any]:
    """
    【函数功能】将 Pipeline schemaVersion 2.0 风险 JSON 转换为新版报告模板上下文。
    :param payload: dict[str, Any]，已校验的 Pipeline 风险 JSON
    :return: dict[str, Any]，新版 Markdown 模板上下文
    :Author: gexinyan
    :CreateTime: 2026-07-30 16:25:19
    Example: build_template_context({"summary": {}, "projects": [], "risks": []})
    """
    payload = validate_payload(payload)
    summary = payload["summary"]
    projects = payload["projects"]
    visible_risks = [risk for risk in payload["risks"] if adapted_risk_type(risk)]
    risk_counts = Counter(adapted_risk_type(risk) for risk in visible_risks)
    records = current_bid_records(projects)
    company_names = sorted({record["company_name"] for record in records})
    cross_network_pair_count, common_project_analysis = cross_network_statistics(visible_risks)
    risk_items = []
    for index, risk in enumerate(visible_risks, start=1):
        relationships = [
            relation_summary(company)
            for company in risk.get("companies", [])
            if isinstance(company, dict)
        ]
        common_rows = historical_project_rows(risk)
        risk_items.append(
            {
                "风险序号": index,
                "风险类别": adapted_risk_label(risk),
                "重合对象": markdown_cell(risk.get("matchValue")),
                "风险企业": markdown_cell(
                    "、".join(
                        normalize_text(company.get("companyName"))
                        for company in risk.get("companies", [])
                        if isinstance(company, dict) and normalize_text(company.get("companyName"))
                    )
                ),
                "企业关系": markdown_cell("<br/>".join(relationships) or "未披露"),
                "风险说明": markdown_cell(risk_description(risk)),
                "原始数据核验行": evidence_rows(risk),
                "存在共同项目": bool(common_rows),
                "无共同项目": not common_rows,
                "共同项目行": common_rows,
            }
        )
    project_rows = []
    for index, project in enumerate(projects, start=1):
        winners = {normalize_text(name) for name in project.get("awardedCompanies", []) if normalize_text(name)}
        project_rows.append(
            {
                "序号": index,
                "项目名称": markdown_cell(project_display_name(project)),
                "参与企业": markdown_cell("、".join(project.get("companies", []))),
                "中标企业": markdown_cell("、".join(sorted(winners)) or "未识别中标企业"),
            }
        )
    context = {
        "报告标题": "招投标风险分析报告",
        "生成时间": markdown_cell(payload.get("generatedAt")),
        "投标项目数": len(projects),
        "投标企业数": int(summary.get("rootCompanyCount", len(company_names))),
        "投标记录数": len(records),
        "企业详情覆盖数": int(summary.get("matchedRootCompanyCount", 0)),
        "风险线索总数": len(visible_risks),
        "跨网络参与投标企业对数量": cross_network_pair_count,
        "共同项目分析": common_project_analysis,
        "爬虫数据状态": (
            "临时结果（待对账）"
            if summary.get("crawlFinality") == "provisional"
            else "最终结果"
        ),
        "爬虫覆盖提示": markdown_cell(summary.get("crawlCoverageNotice", "当前已完成风险分析。")),
        "项目行": project_rows,
        "参与投标企业行": two_column_name_rows(company_names),
        "风险类型统计行": [
            {"风险类别": RISK_TYPE_LABELS[risk_type], "风险线索数": risk_counts.get(risk_type, 0)}
            for risk_type in RISK_TYPE_ORDER
        ],
        "存在风险": bool(visible_risks),
        "无风险": not visible_risks,
        "风险问题条目": risk_items,
    }
    context.update(bid_statistics(records))
    return context


def render_template(template_text: str, context: dict[str, Any]) -> str:
    """
    【函数功能】渲染支持变量、布尔块和列表块的 Markdown 模板。
    :param template_text: str，模板文件正文
    :param context: dict[str, Any]，模板上下文
    :return: str，渲染后的 Markdown 正文
    :Author: gexinyan
    :CreateTime: 2026-07-30 16:25:19
    Example: render_template("{{名称}}", {"名称": "项目A"})
    """
    rendered = render_variables(render_blocks(template_text, context), context)
    return rendered.strip() + "\n"


def render_blocks(template_text: str, context: dict[str, Any]) -> str:
    """
    【函数功能】递归展开模板中的列表块和条件块。
    :param template_text: str，待展开模板正文
    :param context: dict[str, Any]，当前模板上下文
    :return: str，块结构展开后的文本
    :Author: gexinyan
    :CreateTime: 2026-07-30 16:25:19
    Example: render_blocks("{{#行}}{{名称}}{{/行}}", {"行": [{"名称": "A"}]})
    """
    pattern = re.compile(r"{{#\s*([^{}]+?)\s*}}([\s\S]*?){{/\s*\1\s*}}")

    def replace_block(match: re.Match[str]) -> str:
        """
        【函数功能】按上下文替换一个模板条件块或列表块。
        :param match: re.Match[str]，模板块匹配对象
        :return: str，模板块渲染结果
        :Author: gexinyan
        :CreateTime: 2026-07-30 16:25:19
        Example: replace_block(match)
        """
        name, body = match.group(1).strip(), match.group(2)
        value = context.get(name)
        if isinstance(value, list):
            rendered_items = []
            for item in value:
                child = dict(context)
                if isinstance(item, dict):
                    child.update(item)
                else:
                    child["."] = item
                rendered_items.append(render_template(body.strip("\n"), child).strip("\n"))
            separator = "\n" if body.lstrip().startswith("|") else "\n\n"
            return separator.join(rendered_items) + ("\n" if rendered_items else "")
        return render_template(body, context) if value else ""

    current = template_text
    previous = None
    while previous != current:
        previous, current = current, pattern.sub(replace_block, current)
    return current


def render_variables(template_text: str, context: dict[str, Any]) -> str:
    """
    【函数功能】替换模板普通变量，并保留未定义占位符供最终校验。
    :param template_text: str，块结构已展开的模板正文
    :param context: dict[str, Any]，模板上下文
    :return: str，变量替换后的模板正文
    :Author: gexinyan
    :CreateTime: 2026-07-30 16:25:19
    Example: render_variables("{{名称}}", {"名称": "A"})
    """
    pattern = re.compile(r"{{([^#/{][^{}]*)}}")

    def replace_variable(match: re.Match[str]) -> str:
        """
        【函数功能】替换已定义模板变量并保留未定义占位符供生成阶段校验。
        :param match: re.Match[str]，普通变量匹配对象
        :return: str，变量值或原始占位符
        :Author: gexinyan
        :CreateTime: 2026-07-30 16:25:19
        Example: replace_variable(match)
        """
        name = match.group(1).strip()
        return str(context[name]) if name in context else match.group(0)

    return pattern.sub(replace_variable, template_text)


def generate_report(
    risk_json_path: Path,
    markdown_path: Path,
    pdf_path: Path,
    template_path: Path,
    renderer_script: Path,
) -> ReportSummary:
    """
    【函数功能】读取 Pipeline 风险 JSON、生成新版 Markdown 并渲染可下载 PDF。
    :param risk_json_path: Path，风险 JSON 输入路径
    :param markdown_path: Path，Markdown 输出路径
    :param pdf_path: Path，PDF 输出路径
    :param template_path: Path，新版 Markdown 模板路径
    :param renderer_script: Path，新版 Markdown 转 PDF 脚本路径
    :return: ReportSummary，报告生成汇总
    :raises FileNotFoundError: 输入、模板或渲染脚本不存在时触发
    :raises ValueError: 风险 JSON 或模板占位符无效时触发
    :raises RuntimeError: PDF 渲染失败、为空或不可读取时触发
    :Author: gexinyan
    :CreateTime: 2026-07-30 16:25:19
    Example: generate_report(Path("risk.json"), Path("report.md"), Path("report.pdf"), Path("template.md"), Path("renderer.py"))
    """
    risk_json_path = risk_json_path.resolve()
    template_path = template_path.resolve()
    renderer_script = renderer_script.resolve()
    for path, label in (
        (risk_json_path, "风险 JSON"),
        (template_path, "风险报告 Markdown 模板"),
        (renderer_script, "风险报告渲染脚本"),
    ):
        if not path.is_file():
            raise FileNotFoundError(f"{label}不存在：{path}")
    payload = validate_payload(json.loads(risk_json_path.read_text(encoding="utf-8")))
    context = build_template_context(payload)
    rendered = render_template(template_path.read_text(encoding="utf-8"), context)
    unresolved = sorted(set(re.findall(r"{{[^{}]+}}", rendered)))
    if unresolved:
        raise ValueError(f"风险报告模板存在未替换占位符：{'、'.join(unresolved)}")
    markdown_path = markdown_path.resolve()
    pdf_path = pdf_path.resolve()
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(rendered, encoding="utf-8")
    completed = subprocess.run(
        [
            sys.executable,
            str(renderer_script),
            "--input-md",
            str(markdown_path),
            "--output-pdf",
            str(pdf_path),
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or "未知渲染错误"
        raise RuntimeError(f"风险报告 PDF 渲染失败：{detail}")
    if not pdf_path.is_file() or pdf_path.stat().st_size == 0:
        raise RuntimeError(f"风险报告 PDF 未生成或为空：{pdf_path}")
    try:
        reader = PdfReader(str(pdf_path))
        if not reader.pages:
            raise ValueError("PDF 不包含页面")
    except Exception as error:
        raise RuntimeError(f"风险报告 PDF 无法读取：{pdf_path}，{error}") from error
    return ReportSummary(markdown_path, pdf_path, int(context["风险线索总数"]))
