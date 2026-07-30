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

    def test_report_title_uses_single_project_and_falls_back_for_multiple_projects(self) -> None:
        """
        【方法功能】验证单项目标题使用项目名，多项目标题使用批次通用名称。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-30 16:25:19
        """
        payload = sample_payload([])
        self.assertEqual(
            "测试项目招投标企业风险横向分析报告",
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
            "本批次招投标项目企业风险横向分析报告",
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

        self.assertIn("测试项目招投标企业风险横向分析报告", rendered)
        self.assertIn("固定电话相同", rendered)
        self.assertIn("SHAREHOLDER/DIRECT_INVESTMENT/张三", rendered)
        self.assertIn("企业工商详情", rendered)
        self.assertIn("历史共同项目", rendered)
        self.assertIn('color:#8B0000', rendered)
        self.assertIn('color:#1D4ED8', rendered)
        self.assertNotIn("{{", rendered)

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
