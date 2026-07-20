"""
【模块功能】验证同标段共享电话、邮箱、股东和高级职员风险识别规则。

:Author: gexinyan
:CreateTime: 2026-07-16 16:20:00
"""

from __future__ import annotations

import unittest

from bidding_pipeline.risk_analysis import (
    build_company_evidence,
    build_projects,
    detect_risks,
    enrich_risks_with_common_projects,
    extract_phone_values,
)


class RiskAnalysisTests(unittest.TestCase):
    """
    【类功能】覆盖风险值规范化、企业数据关联和跨企业分组判断。
    :Author: gexinyan
    :CreateTime: 2026-07-16 16:20:00
    """

    def test_extract_phone_values_normalizes_mobile_and_landline(self) -> None:
        """
        【方法功能】验证手机国家码和座机分隔符会被规范后保留。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 16:20:00
        """
        values = dict(extract_phone_values("+86 13800000000 / 0510-12345678"))
        self.assertEqual(set(values), {"13800000000", "051012345678"})

    def test_detect_risks_requires_two_companies_in_same_lot(self) -> None:
        """
        【方法功能】验证同一标段两家企业共享四类信息时生成四组风险。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 16:20:00
        """
        bidder_rows = [
            {"project_name": "项目一", "project_code": "P-1", "lot_code": "L-1", "lot_name": "一标段", "company_name": "企业A"},
            {"project_name": "项目一", "project_code": "P-1", "lot_code": "L-1", "lot_name": "一标段", "company_name": "企业B"},
            {"project_name": "项目二", "project_code": "P-2", "lot_code": "L-2", "lot_name": "二标段", "company_name": "企业C"},
        ]
        spider_rows = {
            "company": [
                {"record_id": 1, "search_value": "企业A", "company_name": "企业A", "phone_number": "13800000000", "email": "same@example.com"},
                {"record_id": 2, "search_value": "企业B", "company_name": "企业B", "phone_number": "+86 13800000000", "email": "SAME@example.com"},
                {"record_id": 3, "search_value": "企业C", "company_name": "企业C", "phone_number": "13800000000", "email": "same@example.com"},
            ],
            "shareholder": [
                {"record_id": 1, "search_value": "企业A", "shareholder_name": "共同股东", "subscribed_ratio": "10%"},
                {"record_id": 2, "search_value": "企业B", "shareholder_name": "共同股东", "subscribed_ratio": "20%"},
            ],
            "senior_staff": [
                {"record_id": 1, "search_value": "企业A", "staff_name": "张三", "position": "董事"},
                {"record_id": 2, "search_value": "企业B", "staff_name": "张 三", "position": "监事"},
            ],
        }
        projects = build_projects(bidder_rows)
        evidence, matched = build_company_evidence(spider_rows, ("企业A", "企业B", "企业C"))
        risks = detect_risks("run-1", projects, evidence)
        self.assertEqual(
            {risk["riskType"] for risk in risks},
            {"shared_phone", "shared_email", "shared_shareholder", "shared_senior_staff"},
        )
        self.assertEqual({risk["project"]["lotCode"] for risk in risks}, {"L-1"})
        self.assertEqual(len(matched), 3)

    def test_risk_groups_merge_same_companies_and_include_historical_projects(self) -> None:
        """
        【方法功能】验证相同企业组合的风险会合并，并列出历史共同项目全部投标企业和中标企业。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-17 09:20:59
        """
        current_rows = [
            {"project_name": "当前项目一", "project_code": "P-1", "lot_code": "L-1", "company_name": "企业A", "award_status": "否"},
            {"project_name": "当前项目一", "project_code": "P-1", "lot_code": "L-1", "company_name": "企业B", "award_status": "是"},
            {"project_name": "当前项目二", "project_code": "P-2", "lot_code": "L-2", "company_name": "企业A", "award_status": "否"},
            {"project_name": "当前项目二", "project_code": "P-2", "lot_code": "L-2", "company_name": "企业B", "award_status": "否"},
            {"project_name": "当前项目三", "project_code": "P-3", "lot_code": "L-3", "company_name": "企业A", "award_status": "未知"},
            {"project_name": "当前项目三", "project_code": "P-3", "lot_code": "L-3", "company_name": "企业C", "award_status": "未知"},
        ]
        spider_rows = {
            "company": [
                {"record_id": 1, "search_value": "企业A", "company_name": "企业A", "phone_number": "13800000000"},
                {"record_id": 2, "search_value": "企业B", "company_name": "企业B", "phone_number": "13800000000"},
                {"record_id": 3, "search_value": "企业C", "company_name": "企业C", "phone_number": "13800000000"},
            ],
            "shareholder": [],
            "senior_staff": [],
        }
        history_rows = [
            *current_rows,
            {"project_name": "历史项目", "project_code": "H-1", "lot_code": "H-L-1", "company_name": "企业A", "award_status": "否"},
            {"project_name": "历史项目", "project_code": "H-1", "lot_code": "H-L-1", "company_name": "企业B", "award_status": "否"},
            {"project_name": "历史项目", "project_code": "H-1", "lot_code": "H-L-1", "company_name": "企业C", "award_status": "是"},
        ]
        current_projects = build_projects(current_rows)
        evidence, _ = build_company_evidence(spider_rows, ("企业A", "企业B", "企业C"))
        risks = enrich_risks_with_common_projects(
            detect_risks("run-1", current_projects, evidence),
            build_projects(history_rows),
        )

        self.assertEqual(len(risks), 2)
        risk_ab = next(risk for risk in risks if {item["companyName"] for item in risk["companies"]} == {"企业A", "企业B"})
        risk_ac = next(risk for risk in risks if {item["companyName"] for item in risk["companies"]} == {"企业A", "企业C"})
        self.assertEqual({item["lotCode"] for item in risk_ab["triggerProjects"]}, {"L-1", "L-2"})
        self.assertEqual({item["lotCode"] for item in risk_ab["commonProjects"]}, {"L-1", "L-2", "H-L-1"})
        historical = next(item for item in risk_ab["commonProjects"] if item["lotCode"] == "H-L-1")
        self.assertEqual(historical["bidders"], ["企业A", "企业B", "企业C"])
        self.assertEqual(historical["awardedCompanies"], ["企业C"])
        self.assertEqual({item["lotCode"] for item in risk_ac["commonProjects"]}, {"L-3", "H-L-1"})
        unknown_award = next(item for item in risk_ac["commonProjects"] if item["lotCode"] == "L-3")
        self.assertEqual(unknown_award["awardedCompanies"], [])


if __name__ == "__main__":
    unittest.main()
