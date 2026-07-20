"""
【模块功能】验证风险 JSON 上下文与文件化 Markdown 模板渲染。

:Author: gexinyan
:CreateTime: 2026-07-16 16:20:00
"""

from __future__ import annotations

import unittest
import json
import tempfile
from pathlib import Path

from pypdf import PdfReader

from bidding_pipeline.reporting import build_template_context, generate_report, render_template


class ReportingTests(unittest.TestCase):
    """
    【类功能】覆盖报告模板条件块、循环块和变量替换。
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    """

    def test_pipeline_template_renders_risk_and_project_rows(self) -> None:
        """
        【方法功能】验证模板使用风险 JSON 生成完整且无残留占位符的 Markdown。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 16:20:00
        """
        payload = {
            "generatedAt": "2026-07-16T16:20:00+08:00",
            "runId": "run-1",
            "summary": {
                "projectCount": 1,
                "companyCount": 2,
                "matchedCompanyCount": 2,
                "unmatchedCompanyCount": 0,
                "riskCount": 1,
                "riskTypeCounts": {"shared_phone": 1},
            },
            "unmatchedCompanies": [],
            "projects": [{
                "projectKey": "lot:L-1",
                "projectName": "测试项目",
                "projectCode": "P-1",
                "lotCode": "L-1",
                "companies": ["企业A", "企业B"],
                "riskCount": 1,
            }],
            "risks": [{
                "riskId": "risk-1",
                "riskType": "shared_phone",
                "riskLabel": "联系电话相同",
                "riskLevel": "中",
                "matchValue": "13800000000",
                "companyCount": 2,
                "rule": "同标段共享信息",
                "project": {"projectKey": "lot:L-1", "projectName": "测试项目", "projectCode": "P-1", "lotCode": "L-1"},
                "triggerProjects": [
                    {"projectKey": "lot:L-1", "projectName": "测试项目", "projectCode": "P-1", "lotCode": "L-1"},
                ],
                "companies": [
                    {"companyName": "企业A", "evidences": [{"sourceTable": "spider_data_company"}]},
                    {"companyName": "企业B", "evidences": [{"sourceTable": "spider_data_company"}]},
                ],
                "commonProjects": [
                    {
                        "projectKey": "lot:L-1",
                        "projectName": "测试项目",
                        "projectCode": "P-1",
                        "lotCode": "L-1",
                        "bidders": ["企业A", "企业B", "企业C"],
                        "awardedCompanies": ["企业B", "企业C"],
                    },
                    {
                        "projectKey": "lot:L-2",
                        "projectName": "历史项目",
                        "projectCode": "P-2",
                        "lotCode": "L-2",
                        "bidders": ["企业A", "企业B"],
                        "awardedCompanies": [],
                    },
                ],
            }],
        }
        template_path = (
            Path(__file__).resolve().parents[2]
            / "biding_files_risk_reports"
            / "expand_risk_reports"
            / "pipeline_risk_reports_template.md"
        )
        rendered = render_template(
            template_path.read_text(encoding="utf-8"),
            build_template_context(payload),
        )
        self.assertIn("测试项目专项分析", rendered)
        self.assertIn("联系电话相同", rendered)
        self.assertIn("手机号：13800000000", rendered)
        self.assertIn("13800000000", rendered)
        self.assertIn("共同参与项目", rendered)
        self.assertIn("历史项目", rendered)
        self.assertIn("企业A、企业B、企业C", rendered)
        self.assertIn("企业B、企业C", rendered)
        self.assertIn("未识别中标企业", rendered)
        self.assertNotIn("{{", rendered)

    def test_generate_report_creates_readable_pdf_with_existing_renderer(self) -> None:
        """
        【方法功能】验证文件模板与现有渲染脚本可实际生成至少一页 PDF。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 16:20:00
        """
        payload = {
            "generatedAt": "2026-07-16T16:20:00+08:00",
            "runId": "run-pdf",
            "summary": {
                "projectCount": 1,
                "companyCount": 2,
                "matchedCompanyCount": 2,
                "unmatchedCompanyCount": 0,
                "riskCount": 1,
                "riskTypeCounts": {"shared_shareholder": 1},
            },
            "unmatchedCompanies": [],
            "projects": [{
                "projectKey": "lot:L-1",
                "projectName": "测试项目",
                "projectCode": "P-1",
                "lotCode": "L-1",
                "companies": ["企业A", "企业B"],
                "riskCount": 1,
            }],
            "risks": [{
                "riskId": "risk-pdf",
                "riskType": "shared_shareholder",
                "riskLabel": "股东名称相同",
                "riskLevel": "中",
                "matchValue": "共同股东",
                "companyCount": 2,
                "rule": "同标段共享信息",
                "project": {"projectKey": "lot:L-1", "projectName": "测试项目", "projectCode": "P-1", "lotCode": "L-1"},
                "triggerProjects": [{"projectKey": "lot:L-1", "projectName": "测试项目", "projectCode": "P-1", "lotCode": "L-1"}],
                "companies": [
                    {"companyName": "企业A", "evidences": [{"sourceTable": "spider_data_shareholder", "detail": "20%"}]},
                    {"companyName": "企业B", "evidences": [{"sourceTable": "spider_data_shareholder", "detail": "30%"}]},
                ],
                "commonProjects": [{
                    "projectKey": "lot:L-1",
                    "projectName": "测试项目",
                    "projectCode": "P-1",
                    "lotCode": "L-1",
                    "bidders": ["企业A", "企业B"],
                    "awardedCompanies": ["企业A"],
                }],
            }],
        }
        repository_root = Path(__file__).resolve().parents[2]
        template_path = repository_root / "biding_files_risk_reports" / "expand_risk_reports" / "pipeline_risk_reports_template.md"
        renderer_path = repository_root / "biding_files_risk_reports" / "expanded_risk_report_md_to_pdf.py"
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            json_path = root / "risk.json"
            md_path = root / "report.md"
            pdf_path = root / "report.pdf"
            json_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            summary = generate_report(json_path, md_path, pdf_path, template_path, renderer_path)
            self.assertGreater(pdf_path.stat().st_size, 0)
            self.assertGreaterEqual(len(PdfReader(str(pdf_path)).pages), 1)
            self.assertEqual(summary.risk_count, 1)


if __name__ == "__main__":
    unittest.main()
