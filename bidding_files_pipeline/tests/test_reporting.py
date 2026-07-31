"""新版 Pipeline 风险报告适配、模板渲染与 PDF 生成测试。"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from pypdf import PdfReader

from bidding_pipeline.reporting import (
    adapted_risk_type,
    build_template_context,
    generate_report,
    render_template,
)


def repository_root() -> Path:
    """
    【函数功能】定位包含 Pipeline 与风险报告目录的代码仓库根目录。
    :return: Path，仓库根目录
    :Author: gexinyan
    :CreateTime: 2026-07-30 16:25:19
    Example: repository_root()
    """
    return Path(__file__).resolve().parents[2]


def risk_record(
    risk_type: str,
    match_value: str,
    risk_id: str,
    common_projects: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    """
    【函数功能】构造包含关联关系和原始证据的 Pipeline 风险测试记录。
    :param risk_type: str，风险类型
    :param match_value: str，风险重合值
    :param risk_id: str，风险编号
    :param common_projects: list[dict[str, object]] | None，历史共同项目
    :return: dict[str, object]，风险测试记录
    :Author: gexinyan
    :CreateTime: 2026-07-30 16:25:19
    Example: risk_record("shared_phone", "13800138000", "risk-1")
    """
    source_table = {
        "shared_phone": "spider_data_company",
        "shared_email": "spider_data_company",
        "shared_shareholder": "spider_data_shareholder",
        "shared_senior_staff": "spider_data_senior_staff",
        "shared_company_identity": "spider_data_person_enterprise_relation",
    }[risk_type]
    return {
        "riskId": risk_id,
        "riskType": risk_type,
        "riskLabel": "原始风险名称",
        "comparisonType": "root_related",
        "comparisonLabel": "根公司与关联公司信息重合",
        "rootCompanies": ["企业A", "企业B"],
        "riskLevel": "中",
        "matchValue": match_value,
        "normalizedValue": "".join(character for character in match_value if character.isalnum()),
        "companyCount": 2,
        "distinctCompanyCount": 2,
        "rule": "同标段共享信息",
        "companies": [
            {
                "companyName": "企业A",
                "entityRole": "root",
                "rootCompanyName": "企业A",
                "relations": [],
                "evidences": [
                    {
                        "displayValue": match_value,
                        "normalizedValue": match_value,
                        "sourceTable": source_table,
                    }
                ],
            },
            {
                "companyName": "关联企业C",
                "entityRole": "related",
                "rootCompanyName": "企业B",
                "relations": [
                    {
                        "sourceType": "SHAREHOLDER",
                        "relationType": "DIRECT_INVESTMENT",
                        "personName": "张三",
                    }
                ],
                "evidences": [
                    {
                        "displayValue": match_value,
                        "normalizedValue": match_value,
                        "sourceTable": source_table,
                        "detail": "原始详情",
                    }
                ],
            },
        ],
        "commonProjects": common_projects or [],
    }


def sample_payload(risks: list[dict[str, object]] | None = None) -> dict[str, object]:
    """
    【函数功能】构造当前任务项目、汇总和风险数据完整测试对象。
    :param risks: list[dict[str, object]] | None，自定义风险列表
    :return: dict[str, object]，Pipeline 风险 JSON 测试对象
    :Author: gexinyan
    :CreateTime: 2026-07-30 16:25:19
    Example: sample_payload([])
    """
    return {
        "schemaVersion": "2.0",
        "generatedAt": "2026-07-30T16:25:19+08:00",
        "runId": "run-report-adapter",
        "summary": {
            "projectCount": 1,
            "companyCount": 2,
            "rootCompanyCount": 2,
            "matchedRootCompanyCount": 2,
            "relatedCompanyCount": 1,
            "relationCount": 1,
            "riskCount": len(risks or []),
            "crawlFinality": "final",
            "crawlCoverageNotice": "爬虫企业均已取得最终状态。",
        },
        "projects": [
            {
                "projectKey": "lot:L-1",
                "projectName": "测试项目",
                "projectCode": "P-1",
                "lotCode": "L-1",
                "companies": ["企业A", "企业B"],
                "awardedCompanies": ["企业B"],
                "riskCount": len(risks or []),
            }
        ],
        "risks": risks or [],
    }


class ReportingTests(unittest.TestCase):
    """
    【类功能】覆盖新版报告数据适配、当前任务统计、模板和 PDF 生成。
    :Author: gexinyan
    :CreateTime: 2026-07-30 16:25:19
    """

    def test_visible_risk_mapping_splits_phone_and_hides_company_identity(self) -> None:
        """
        【方法功能】验证五类可展示风险映射、电话拆分及企业身份风险隐藏。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-30 16:25:19
        """
        risks = [
            risk_record("shared_phone", "13800138000", "mobile"),
            risk_record("shared_phone", "0510-83293586", "landline"),
            risk_record("shared_email", "risk@example.com", "email"),
            risk_record("shared_shareholder", "共同股东", "shareholder"),
            risk_record("shared_senior_staff", "共同高管", "staff"),
            risk_record("shared_company_identity", "关联企业C", "identity"),
        ]

        context = build_template_context(sample_payload(risks))

        self.assertEqual("shared_mobile", adapted_risk_type(risks[0]))
        self.assertEqual("shared_landline", adapted_risk_type(risks[1]))
        self.assertIsNone(adapted_risk_type(risks[-1]))
        self.assertEqual(5, context["风险线索总数"])
        self.assertEqual(
            [1, 1, 1, 1, 1],
            [item["风险线索数"] for item in context["风险类型统计行"]],
        )
        self.assertNotIn("关联企业C", [item["重合对象"] for item in context["风险问题条目"]])

    def test_report_title_is_fixed_for_all_task_scopes(self) -> None:
        """
        【方法功能】验证新版报告始终使用固定的招投标风险分析标题。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-30 16:25:19
        """
        payload = sample_payload([])
        self.assertEqual(
            "招投标风险分析报告",
            build_template_context(payload)["报告标题"],
        )
        payload["projects"].append(
            {
                "projectKey": "lot:L-2",
                "projectName": "第二项目",
                "companies": ["企业C"],
                "awardedCompanies": [],
            }
        )
        self.assertEqual(
            "招投标风险分析报告",
            build_template_context(payload)["报告标题"],
        )

    def test_bid_statistics_use_current_projects_only(self) -> None:
        """
        【方法功能】验证投标行为统计仅使用当前任务项目且历史项目只作为风险证据。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-30 16:25:19
        """
        historical = [
            {
                "projectKey": "history:H-1",
                "projectName": "历史项目",
                "bidders": ["企业A", "企业B"],
                "awardedCompanies": ["企业A"],
            }
        ]
        payload = sample_payload([risk_record("shared_email", "risk@example.com", "email", historical)])
        payload["projects"] = [
            {
                "projectKey": f"current:{index}",
                "projectName": f"当前项目{index}",
                "companies": ["企业A", "企业B"],
                "awardedCompanies": ["企业B"],
            }
            for index in range(1, 5)
        ]

        context = build_template_context(payload)

        self.assertEqual(8, context["投标记录数"])
        self.assertEqual(["企业A"], [item["企业名称"] for item in context["久投不中企业条目"]])
        self.assertEqual("历史项目", context["风险问题条目"][0]["共同项目行"][0]["共同参与项目"])
        self.assertNotIn(
            "历史项目",
            [row["项目名称"] for item in context["久投不中企业条目"] for row in item["项目明细行"]],
        )

    def test_report_project_names_remove_huishan_without_mutating_payload(self) -> None:
        """
        【方法功能】验证报告展示脱敏项目名称且不修改当前任务和历史项目原始数据。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-30 18:30:00
        """
        historical = [
            {
                "projectName": "惠山区历史共同项目",
                "bidders": ["企业A", "企业B"],
                "awardedCompanies": ["企业B"],
            }
        ]
        payload = sample_payload(
            [risk_record("shared_email", "risk@example.com", "privacy", historical)]
        )
        payload["projects"][0]["projectName"] = "惠山区当前任务项目"

        context = build_template_context(payload)
        current_project_name = context["项目行"][0]["项目名称"]
        historical_project_name = context["风险问题条目"][0]["共同项目行"][0]["共同参与项目"]

        self.assertEqual("惠山区当前任务项目", payload["projects"][0]["projectName"])
        self.assertEqual("惠山区历史共同项目", historical[0]["projectName"])
        self.assertNotIn("惠山区", current_project_name)
        self.assertNotIn("惠山区", historical_project_name)
        self.assertIn("当前任务项目", current_project_name)
        self.assertIn("历史共同项目", historical_project_name)

    def test_historical_bid_results_only_list_risk_companies(self) -> None:
        """
        【方法功能】验证历史共同参投结果仅展示风险企业及其红色或绿色标注。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-31 10:00:00
        """
        historical = [
            {
                "projectName": "历史共同项目",
                "bidders": ["企业A", "企业B", "非风险企业D"],
                "awardedCompanies": ["企业B", "非风险企业D"],
            }
        ]
        context = build_template_context(
            sample_payload([risk_record("shared_email", "risk@example.com", "history", historical)])
        )
        row = context["风险问题条目"][0]["共同项目行"][0]

        self.assertIn("企业A", row["投标结果"])
        self.assertIn("企业B", row["投标结果"])
        self.assertNotIn("非风险企业D", row["投标结果"])
        self.assertIn('color:#8B0000', row["投标结果"])
        self.assertIn('color:#15803D', row["投标结果"])
        self.assertNotIn('color:#1D4ED8', row["投标结果"])
        self.assertIn("非风险企业D", row["所有参与企业"])
        self.assertIn("非风险企业D", row["项目中标企业"])

    def test_template_renders_relationship_evidence_history_and_no_placeholders(self) -> None:
        """
        【方法功能】验证新版模板展示关联关系、原始证据、历史项目且无残留占位符。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-30 16:25:19
        """
        historical = [
            {
                "projectName": "历史共同项目",
                "bidders": ["企业A", "企业B", "企业D"],
                "awardedCompanies": ["企业B"],
            }
        ]
        payload = sample_payload(
            [risk_record("shared_phone", "0510-83293586", "landline", historical)]
        )
        template_path = (
            repository_root()
            / "bidding_files_risk_reports"
            / "pipeline_relationship_risk_report_template.md"
        )

        rendered = render_template(
            template_path.read_text(encoding="utf-8"),
            build_template_context(payload),
        )

        self.assertIn("招投标风险分析报告", rendered)
        self.assertIn("固定电话相同", rendered)
        self.assertIn("两家公司的股东相同，名称为**张三**", rendered)
        self.assertIn("企业工商详情", rendered)
        self.assertIn("历史共同项目", rendered)
        self.assertIn("<br/>", rendered)
        self.assertIn('color:#8B0000', rendered)
        self.assertIn('color:#1D4ED8', rendered)
        self.assertIn("高标农田招投标项目八大风险点", rendered)
        self.assertIn("建议采用分层核验方式推进后续处置", rendered)
        self.assertNotIn("运行标识", rendered)
        self.assertNotIn("发现的关联企业", rendered)
        self.assertNotIn("有效企业关系记录", rendered)
        self.assertNotIn("本报告展示风险线索", rendered)
        self.assertNotIn("{{", rendered)

    def test_template_uses_two_column_companies_and_removes_legacy_placeholders(self) -> None:
        """
        【方法功能】验证参与投标企业双栏排版及已废弃占位符均不进入新版模板。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-30 18:10:00
        """
        payload = sample_payload([])
        payload["projects"][0]["companies"] = ["企业A", "企业B", "企业C"]
        context = build_template_context(payload)
        self.assertEqual(
            [
                {"左序号": 1, "左名称": "企业A", "右序号": 2, "右名称": "企业B"},
                {"左序号": 3, "左名称": "企业C", "右序号": "", "右名称": ""},
            ],
            context["参与投标企业行"],
        )
        template = (
            repository_root()
            / "bidding_files_risk_reports"
            / "pipeline_relationship_risk_report_template.md"
        ).read_text(encoding="utf-8")
        removed_placeholders = (
            "企业身份重合风险数量", "共享联系电话涉及项目数", "共享联系电话风险数量",
            "共享股东涉及项目数", "共享股东风险数量", "共享邮箱涉及项目数",
            "共享邮箱风险数量", "共享高级职员涉及项目数", "共享高级职员风险数量",
            "关系记录总数", "关联公司与关联公司风险数量", "关联公司总数",
            "匹配关联公司总数", "待对账企业数", "未匹配关联公司总数", "未匹配根公司总数",
            "根公司与关联公司风险数量", "根公司与根公司风险数量", "根公司总数",
            "涉及风险项目数", "风险项目占比",
        )
        for placeholder in removed_placeholders:
            self.assertNotIn(f"{{{{{placeholder}}}}}", template)
        rendered = render_template(template, context)
        self.assertNotIn("{{", rendered)

    def test_relation_summary_combines_shareholder_and_senior_staff_without_raw_codes(self) -> None:
        """
        【方法功能】验证关系依据使用中文名称并合并同一人员的股东和高级职员关系。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-30 18:10:00
        """
        risk = risk_record("shared_email", "risk@example.com", "relationship")
        related_company = risk["companies"][1]
        related_company["relations"].append(
            {"sourceType": "SENIOR_STAFF", "personName": "张三"}
        )
        context = build_template_context(sample_payload([risk]))
        relation_text = context["风险问题条目"][0]["企业关系"]
        evidence_text = context["风险问题条目"][0]["原始数据核验行"][1]["企业关联关系"]
        expected = "两家公司的股东和高级职员相同，名称为**张三**"
        self.assertIn(expected, relation_text)
        self.assertIn(expected, evidence_text)
        self.assertNotIn("SHAREHOLDER", relation_text)
        self.assertNotIn("SENIOR_STAFF", relation_text)

    def test_only_company_identity_risk_renders_as_no_visible_risk(self) -> None:
        """
        【方法功能】验证仅有企业身份风险时新版报告按零条可展示风险处理。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-30 16:25:19
        """
        context = build_template_context(
            sample_payload([risk_record("shared_company_identity", "关联企业C", "identity")])
        )
        self.assertEqual(0, context["风险线索总数"])
        self.assertFalse(context["存在风险"])
        self.assertTrue(context["无风险"])
        self.assertEqual([], context["风险问题条目"])

    def test_empty_bid_statistics_render_threshold_notice_for_each_section(self) -> None:
        """
        【方法功能】验证三类投标行为统计未命中阈值时均展示明确提示。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-31 09:30:00
        """
        template_path = (
            repository_root()
            / "bidding_files_risk_reports"
            / "pipeline_relationship_risk_report_template.md"
        )
        rendered = render_template(
            template_path.read_text(encoding="utf-8"),
            build_template_context(sample_payload([])),
        )
        notice = "当前任务暂无满足统计阈值的企业/企业组合。"
        self.assertEqual(3, rendered.count(notice))
        self.assertNotIn("{{", rendered)

    def test_invalid_payload_is_rejected_before_rendering(self) -> None:
        """
        【方法功能】验证缺少必需列表的风险 JSON 在模板渲染前被拒绝。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-30 16:25:19
        """
        with self.assertRaisesRegex(ValueError, "projects"):
            build_template_context({"summary": {}, "risks": []})

    def test_generate_report_rejects_unresolved_template_placeholder(self) -> None:
        """
        【方法功能】验证未解析模板占位符阻止 PDF 生成。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-30 16:25:19
        """
        renderer_path = (
            repository_root()
            / "bidding_files_risk_reports"
            / "pipeline_relationship_report_md_to_pdf.py"
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            json_path = root / "risk.json"
            template_path = root / "template.md"
            json_path.write_text(json.dumps(sample_payload([]), ensure_ascii=False), encoding="utf-8")
            template_path.write_text("# {{报告标题}}\n{{不存在变量}}", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "未替换占位符"):
                generate_report(
                    json_path,
                    root / "report.md",
                    root / "report.pdf",
                    template_path,
                    renderer_path,
                )

    def test_generate_report_creates_readable_pdf_for_risk_and_zero_risk(self) -> None:
        """
        【方法功能】验证有风险和零风险数据均可生成标题正确的非空 PDF。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-30 16:25:19
        """
        report_root = repository_root() / "bidding_files_risk_reports"
        template_path = report_root / "pipeline_relationship_risk_report_template.md"
        renderer_path = report_root / "pipeline_relationship_report_md_to_pdf.py"
        cases = (
            ("risk", sample_payload([risk_record("shared_shareholder", "共同股东", "risk")]), 1),
            ("zero", sample_payload([]), 0),
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            for case_name, payload, expected_count in cases:
                with self.subTest(case=case_name):
                    json_path = root / f"{case_name}.json"
                    md_path = root / f"{case_name}.md"
                    pdf_path = root / f"{case_name}.pdf"
                    json_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
                    summary = generate_report(
                        json_path,
                        md_path,
                        pdf_path,
                        template_path,
                        renderer_path,
                    )
                    reader = PdfReader(str(pdf_path))
                    extracted_text = "\n".join(page.extract_text() or "" for page in reader.pages)
                    self.assertGreater(pdf_path.stat().st_size, 0)
                    self.assertGreaterEqual(len(reader.pages), 1)
                    self.assertIn("测试项目", extracted_text)
                    self.assertEqual(expected_count, summary.risk_count)


if __name__ == "__main__":
    unittest.main()
