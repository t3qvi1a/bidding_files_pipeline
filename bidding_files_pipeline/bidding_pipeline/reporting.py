"""
【模块功能】将风险 JSON 扩充为 Markdown 报告，并调用现有脚本渲染 PDF。

:Author: gexinyan
:CreateTime: 2026-07-16 16:20:00
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class ReportSummary:
    """
    【类功能】描述风险报告 Markdown 与 PDF 产物。
    :Attributes:
        markdown_path: Path，Markdown 报告路径
        pdf_path: Path，PDF 报告路径
        risk_count: int，报告包含的风险组数量
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    """

    markdown_path: Path
    pdf_path: Path
    risk_count: int


def markdown_cell(value: Any) -> str:
    """
    【函数功能】转义 Markdown 表格单元格中的控制字符。
    :param value: Any，原始单元格值
    :return: str，可安全写入 Markdown 表格的文本
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    Example: markdown_cell("A|B")
    """
    return str(value if value not in (None, "") else "未披露").replace("|", "\\|").replace("\n", "<br>")


def shared_information_label(risk: dict[str, Any]) -> str:
    """
    【函数功能】为共享风险值生成适合报告展示的信息类型名称。
    :param risk: dict[str, Any]，风险记录
    :return: str，手机号、固定电话、邮箱、股东名称或高级职员名称
    :Author: gexinyan
    :CreateTime: 2026-07-17 09:20:59
    Example: shared_information_label({"riskType": "shared_phone", "normalizedValue": "13800000000"})
    """
    risk_type = str(risk.get("riskType", ""))
    if risk_type == "shared_phone":
        digits = re.sub(r"\D", "", str(risk.get("normalizedValue", risk.get("matchValue", ""))))
        return "手机号" if re.fullmatch(r"1\d{10}", digits) else "固定电话"
    return {
        "shared_email": "邮箱",
        "shared_shareholder": "股东名称",
        "shared_senior_staff": "高级职员名称",
    }.get(risk_type, str(risk.get("riskLabel", "共享信息")))


def report_common_projects(risk: dict[str, Any]) -> list[dict[str, Any]]:
    """
    【函数功能】将风险共同项目转换为报告所需格式，并兼容旧版风险 JSON。
    :param risk: dict[str, Any]，风险记录
    :return: list[dict[str, Any]]，包含项目、投标企业和中标企业的报告项目数据
    :Author: gexinyan
    :CreateTime: 2026-07-17 09:20:59
    Example: report_common_projects({"commonProjects": []})
    """
    projects = list(risk.get("commonProjects", []))
    if not projects and risk.get("project"):
        projects = [{**dict(risk["project"]), "bidders": [item.get("companyName") for item in risk.get("companies", [])]}]
    rows = []
    for project in projects:
        bidders = [str(item) for item in project.get("bidders", []) if item]
        awarded_companies = [str(item) for item in project.get("awardedCompanies", []) if item]
        rows.append(
            {
                "项目名称": markdown_cell(project.get("projectName")),
                "项目编号": markdown_cell(project.get("projectCode")),
                "标段编号": markdown_cell(project.get("lotCode")),
                "标段名称": markdown_cell(project.get("lotName")),
                "投标企业": markdown_cell("、".join(bidders) or "未识别投标企业"),
                "中标企业": markdown_cell("、".join(awarded_companies) or "未识别中标企业"),
            }
        )
    return rows


def build_template_context(payload: dict[str, Any]) -> dict[str, Any]:
    """
    【函数功能】将风险 JSON 转换为报告模板所需的中文变量和循环数据。
    :param payload: dict[str, Any]，风险分析 JSON 对象
    :return: dict[str, Any]，模板渲染上下文
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    Example: build_template_context({"summary": {}, "projects": [], "risks": []})
    """
    summary = payload.get("summary", {})
    risks = payload.get("risks", [])
    projects = payload.get("projects", [])
    risk_counts = summary.get("riskTypeCounts", {})
    risk_project_keys = {
        project.get("projectKey")
        for risk in risks
        for project in risk.get("triggerProjects", [risk.get("project", {})])
    }
    project_names = [
        str(project.get("projectName"))
        for project in projects
        if project.get("projectName")
    ]
    distinct_project_names = list(dict.fromkeys(project_names))
    project_title = distinct_project_names[0] if len(distinct_project_names) == 1 else "本批次招投标项目"
    project_rows = [
        {
            "项目名称": markdown_cell(project.get("projectName")),
            "项目编号": markdown_cell(project.get("projectCode")),
            "标段编号": markdown_cell(project.get("lotCode")),
            "投标企业": markdown_cell("、".join(project.get("companies", []))),
            "风险线索数": project.get("riskCount", 0),
        }
        for project in projects
    ]
    risk_sections = []
    for index, risk in enumerate(risks, start=1):
        project = risk.get("project", {})
        common_projects = report_common_projects(risk)
        company_rows = []
        for company in risk.get("companies", []):
            evidences = company.get("evidences", [])
            company_rows.append(
                {
                    "企业名称": markdown_cell(company.get("companyName")),
                    "数据来源": markdown_cell(
                        "、".join(sorted({item.get("sourceTable", "") for item in evidences if item.get("sourceTable")}))
                    ),
                    "补充信息": markdown_cell(
                        "、".join(str(item.get("detail")) for item in evidences if item.get("detail")) or "无"
                    ),
                }
            )
        risk_sections.append(
            {
                "风险序号": index,
                "风险类型": markdown_cell(risk.get("riskLabel")),
                "风险编号": markdown_cell(risk.get("riskId")),
                "项目名称": markdown_cell(project.get("projectName")),
                "项目编号": markdown_cell(project.get("projectCode")),
                "标段编号": markdown_cell(project.get("lotCode")),
                "共享信息": markdown_cell(risk.get("matchValue")),
                "共享信息类型": markdown_cell(shared_information_label(risk)),
                "涉及企业数": risk.get("companyCount", 0),
                "风险等级": markdown_cell(risk.get("riskLevel")),
                "判断规则": markdown_cell(risk.get("rule")),
                "企业证据行": company_rows,
                "共同项目行": common_projects,
                "项目投标章节": common_projects,
            }
        )
    unmatched = payload.get("unmatchedCompanies", [])

    def involved_project_count(risk_type: str) -> int:
        """
        【函数功能】统计指定风险类型涉及的不同标段数量。
        :param risk_type: str，风险类型键
        :return: int，去重标段数量
        :Author: gexinyan
        :CreateTime: 2026-07-16 16:20:00
        """
        return len(
            {
                project.get("projectKey")
                for risk in risks
                for project in risk.get("triggerProjects", [risk.get("project", {})])
                if risk.get("riskType") == risk_type
            }
        )

    project_count = int(summary.get("projectCount", 0))
    involved_count = len(risk_project_keys.difference({None}))
    return {
        "项目名称": markdown_cell(project_title),
        "生成时间": markdown_cell(payload.get("generatedAt")),
        "运行标识": markdown_cell(payload.get("runId")),
        "项目总数": project_count,
        "投标企业总数": summary.get("companyCount", 0),
        "匹配企业总数": summary.get("matchedCompanyCount", 0),
        "未匹配企业总数": summary.get("unmatchedCompanyCount", 0),
        "风险线索总数": summary.get("riskCount", 0),
        "涉及风险项目数": involved_count,
        "风险项目占比": f"{(involved_count / project_count * 100) if project_count else 0:.2f}",
        "共享联系电话风险数量": risk_counts.get("shared_phone", 0),
        "共享联系电话涉及项目数": involved_project_count("shared_phone"),
        "共享邮箱风险数量": risk_counts.get("shared_email", 0),
        "共享邮箱涉及项目数": involved_project_count("shared_email"),
        "共享股东风险数量": risk_counts.get("shared_shareholder", 0),
        "共享股东涉及项目数": involved_project_count("shared_shareholder"),
        "共享高级职员风险数量": risk_counts.get("shared_senior_staff", 0),
        "共享高级职员涉及项目数": involved_project_count("shared_senior_staff"),
        "项目行": project_rows,
        "存在风险": bool(risks),
        "无风险": not risks,
        "风险章节": risk_sections,
        "存在未匹配企业": bool(unmatched),
        "无未匹配企业": not unmatched,
        "未匹配企业行": [{"企业名称": markdown_cell(name)} for name in unmatched],
    }


def render_template(template_text: str, context: dict[str, Any]) -> str:
    """
    【函数功能】渲染支持变量、布尔块和列表块的 Markdown 文件模板。
    :param template_text: str，模板文件正文
    :param context: dict[str, Any]，模板上下文
    :return: str，渲染后的 Markdown 正文
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    Example: render_template("{{名称}}", {"名称": "项目A"})
    """
    return render_variables(render_blocks(template_text, context), context).strip() + "\n"


def render_blocks(template_text: str, context: dict[str, Any]) -> str:
    """
    【函数功能】递归展开模板中的列表块和条件块。
    :param template_text: str，待展开模板正文
    :param context: dict[str, Any]，当前模板上下文
    :return: str，块结构展开后的文本
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    Example: render_blocks("{{#行}}{{名称}}{{/行}}", {"行": [{"名称": "A"}]})
    """
    pattern = re.compile(r"{{#([^{}]+)}}([\s\S]*?){{/\1}}")

    def replace_block(match: re.Match[str]) -> str:
        """
        【函数功能】按上下文值替换一个模板条件或循环块。
        :param match: re.Match[str]，模板块匹配对象
        :return: str，块渲染结果
        :Author: gexinyan
        :CreateTime: 2026-07-16 16:20:00
        """
        name, body = match.group(1).strip(), match.group(2)
        value = context.get(name)
        if isinstance(value, list):
            rendered = []
            for item in value:
                child = dict(context)
                if isinstance(item, dict):
                    child.update(item)
                else:
                    child["."] = item
                rendered.append(render_template(body, child))
            return "".join(rendered)
        return render_template(body, context) if value else ""

    current = template_text
    while pattern.search(current):
        current = pattern.sub(replace_block, current)
    return current


def render_variables(template_text: str, context: dict[str, Any]) -> str:
    """
    【函数功能】替换模板中的普通变量，未提供变量渲染为空字符串。
    :param template_text: str，块结构已展开的模板正文
    :param context: dict[str, Any]，模板上下文
    :return: str，变量替换后的文本
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    Example: render_variables("{{名称}}", {"名称": "A"})
    """
    pattern = re.compile(r"{{([^#/{][^{}]*)}}")
    return pattern.sub(lambda match: str(context.get(match.group(1).strip(), "")), template_text)


def generate_report(
    risk_json_path: Path,
    markdown_path: Path,
    pdf_path: Path,
    template_path: Path,
    renderer_script: Path,
) -> ReportSummary:
    """
    【函数功能】读取风险 JSON、生成 Markdown，并调用现有脚本渲染 PDF。
    :param risk_json_path: Path，风险 JSON 输入路径
    :param markdown_path: Path，Markdown 输出路径
    :param pdf_path: Path，PDF 输出路径
    :param template_path: Path，基于现有报告样式创建的 Markdown 模板路径
    :param renderer_script: Path，现有 Markdown 转 PDF 脚本路径
    :return: ReportSummary，报告生成汇总
    :raises FileNotFoundError: 输入 JSON 或渲染脚本不存在时抛出
    :raises RuntimeError: PDF 渲染子进程失败或未生成产物时抛出
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    Example: generate_report(Path("risk.json"), Path("report.md"), Path("report.pdf"), Path("template.md"), Path("renderer.py"))
    """
    risk_json_path = risk_json_path.resolve()
    template_path = template_path.resolve()
    renderer_script = renderer_script.resolve()
    if not risk_json_path.is_file():
        raise FileNotFoundError(f"风险 JSON 不存在：{risk_json_path}")
    if not renderer_script.is_file():
        raise FileNotFoundError(f"风险报告渲染脚本不存在：{renderer_script}")
    if not template_path.is_file():
        raise FileNotFoundError(f"风险报告 Markdown 模板不存在：{template_path}")
    payload = json.loads(risk_json_path.read_text(encoding="utf-8"))
    markdown_path = markdown_path.resolve()
    pdf_path = pdf_path.resolve()
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    template_text = template_path.read_text(encoding="utf-8")
    markdown_path.write_text(render_template(template_text, build_template_context(payload)), encoding="utf-8")
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
        raise RuntimeError(f"风险报告 PDF 未生成：{pdf_path}")
    return ReportSummary(markdown_path, pdf_path, int(payload.get("summary", {}).get("riskCount", 0)))
