"""
【模块功能】验证同标段共享电话、邮箱、股东和高级职员风险识别规则。

:Author: gexinyan
:CreateTime: 2026-07-16 16:20:00
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from bidding_pipeline.database import DatabaseConfig
from bidding_pipeline.risk_analysis import (
    build_company_evidence,
    build_projects,
    build_root_networks,
    detect_risks,
    enrich_risks_with_common_projects,
    extract_phone_values,
    project_key,
    write_risk_json,
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

    def test_same_lot_code_in_different_projects_does_not_trigger_cross_project_risk(self) -> None:
        """
        【方法功能】验证不同项目使用相同标段编号时保持隔离且不产生跨项目风险。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-21 10:57:32
        """
        bidder_rows = [
            {"project_name": "项目一", "project_code": "P-1", "lot_code": "01", "company_name": "企业A"},
            {"project_name": "项目二", "project_code": "P-2", "lot_code": "01", "company_name": "企业B"},
        ]
        projects = build_projects(bidder_rows)
        evidence, _ = build_company_evidence(
            {
                "company": [
                    {"record_id": 1, "search_value": "企业A", "company_name": "企业A", "phone_number": "13800000000"},
                    {"record_id": 2, "search_value": "企业B", "company_name": "企业B", "phone_number": "13800000000"},
                ],
                "shareholder": [],
                "senior_staff": [],
            },
            ("企业A", "企业B"),
        )
        self.assertEqual(len(projects), 2)
        self.assertEqual(
            {project["projectKeySource"] for project in projects.values()},
            {"project_code_lot_code"},
        )
        self.assertEqual(detect_risks("run-1", projects, evidence), [])

    def test_project_name_and_lot_name_form_fallback_composite_key(self) -> None:
        """
        【方法功能】验证编号缺失时使用项目名称与标段名称组合分组。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-21 10:57:32
        """
        first_key, first_source = project_key({"project_name": "项目一", "lot_name": "一标段"})
        second_key, second_source = project_key({"project_name": " 项目 一 ", "lot_name": "一 标段"})
        self.assertEqual(first_key, second_key)
        self.assertEqual(first_source, "project_name_lot_name")
        self.assertEqual(second_source, "project_name_lot_name")

    def test_missing_project_identity_uses_record_level_isolation(self) -> None:
        """
        【方法功能】验证缺少项目身份的相同标段记录不会被归并为一个风险项目。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-21 10:57:32
        """
        bidder_rows = [
            {"record_id": 1, "lot_code": "01", "company_name": "企业A", "source_path": "a.pdf"},
            {"record_id": 2, "lot_code": "01", "company_name": "企业B", "source_path": "b.pdf"},
        ]
        projects = build_projects(bidder_rows)
        self.assertEqual(len(projects), 2)
        self.assertEqual(
            {project["projectKeySource"] for project in projects.values()},
            {"unresolved_record"},
        )
        fallback_a = project_key({"lot_code": "01", "company_name": "企业A", "source_path": "a.pdf"})[0]
        fallback_b = project_key({"lot_code": "01", "company_name": "企业B", "source_path": "b.pdf"})[0]
        self.assertNotEqual(fallback_a, fallback_b)

    def test_relation_network_filters_self_relations_and_preserves_paths(self) -> None:
        """
        【方法功能】验证关系网络过滤自关联、合并实体并保留不同关系路径。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-21 11:30:00
        """
        networks = build_root_networks(
            [
                {"search_value": "企业A", "company_name": "企业A", "relation_type": "SELF"},
                {
                    "search_value": "企业A",
                    "company_name": "关联企业D",
                    "source_type": "SHAREHOLDER",
                    "relation_type": "DIRECT_INVESTMENT",
                    "person_name": "张三",
                },
                {
                    "search_value": "企业A",
                    "company_name": "关联企业D",
                    "source_type": "SENIOR_STAFF",
                    "relation_type": "LEGAL_REPRESENTATIVE",
                    "person_name": "李四",
                },
            ],
            ("企业A",),
        )
        entities = networks["企业a"]["entities"]
        self.assertEqual(set(entities), {"企业a", "关联企业d"})
        self.assertEqual(len(entities["关联企业d"]["relations"]), 2)

    def test_detects_three_root_network_comparison_types(self) -> None:
        """
        【方法功能】验证根根、根关联和关联关联三类共享信息风险均可识别。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-21 11:30:00
        """
        bidder_rows = [
            {"project_code": "P-1", "lot_code": "L-1", "company_name": "企业A"},
            {"project_code": "P-1", "lot_code": "L-1", "company_name": "企业B"},
        ]
        networks = build_root_networks(
            [
                {"search_value": "企业A", "company_name": "关联企业D", "relation_type": "DIRECT_INVESTMENT"},
                {"search_value": "企业B", "company_name": "关联企业C", "relation_type": "SENIOR_STAFF"},
            ],
            ("企业A", "企业B"),
        )
        spider_rows = {
            "company": [
                {"record_id": 1, "search_value": "企业A", "company_name": "企业A", "phone_number": "13800000001", "email": "root-related@example.com"},
                {"record_id": 2, "search_value": "企业B", "company_name": "企业B", "phone_number": "13800000001"},
                {"record_id": 3, "search_value": "关联企业C", "company_name": "关联企业C", "phone_number": "13800000002", "email": "root-related@example.com"},
                {"record_id": 4, "search_value": "关联企业D", "company_name": "关联企业D", "phone_number": "13800000002"},
            ],
            "shareholder": [],
            "senior_staff": [],
        }
        evidence, _ = build_company_evidence(
            spider_rows,
            ("企业A", "企业B", "关联企业C", "关联企业D"),
        )
        risks = detect_risks("run-1", build_projects(bidder_rows), evidence, networks)
        self.assertEqual(
            {(risk["comparisonType"], risk["riskType"]) for risk in risks},
            {
                ("root_root", "shared_phone"),
                ("root_related", "shared_email"),
                ("related_related", "shared_phone"),
            },
        )
        self.assertTrue(all(risk["rootCompanies"] == ["企业A", "企业B"] for risk in risks))
        enriched = enrich_risks_with_common_projects(risks, build_projects(bidder_rows))
        root_related = next(risk for risk in enriched if risk["comparisonType"] == "root_related")
        self.assertEqual(root_related["commonProjects"][0]["projectCode"], "P-1")

    def test_company_evidence_prefers_canonical_company_and_record_mapping(self) -> None:
        """
        【方法功能】验证关联企业详情不会因根搜索值而同时错误归属给根公司。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-21 11:30:00
        """
        evidence, matched = build_company_evidence(
            {
                "company": [
                    {
                        "record_id": 3,
                        "search_value": "企业A",
                        "company_name": "关联企业C",
                        "phone_number": "13800000000",
                    }
                ],
                "shareholder": [
                    {
                        "record_id": 3,
                        "search_value": "企业A",
                        "shareholder_name": "关联股东",
                    }
                ],
                "senior_staff": [],
            },
            ("企业A", "关联企业C"),
        )
        self.assertEqual(matched, {"关联企业c"})
        self.assertEqual(evidence["企业a"]["shared_phone"], [])
        self.assertEqual(len(evidence["关联企业c"]["shared_phone"]), 1)
        self.assertEqual(len(evidence["关联企业c"]["shared_shareholder"]), 1)

    def test_shared_company_identity_is_reported_once_without_field_self_comparison(self) -> None:
        """
        【方法功能】验证跨根网络同一企业仅生成结构风险且不逐字段自比较。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-21 11:30:00
        """
        projects = build_projects(
            [
                {"project_code": "P-1", "lot_code": "L-1", "company_name": "企业A"},
                {"project_code": "P-1", "lot_code": "L-1", "company_name": "企业B"},
            ]
        )
        networks = build_root_networks(
            [
                {"search_value": "企业A", "company_name": "共同关联企业C"},
                {"search_value": "企业B", "company_name": "共同关联企业C"},
                {"search_value": "企业B", "company_name": "企业A"},
            ],
            ("企业A", "企业B"),
        )
        shared_rows: dict[str, list[dict[str, str]]] = {
            "shared_phone": [],
            "shared_email": [],
            "shared_shareholder": [],
            "shared_senior_staff": [],
            "shared_company_identity": [],
        }
        evidence = {"共同关联企业c": shared_rows, "企业a": shared_rows}
        risks = detect_risks("run-1", projects, evidence, networks)
        identity_risks = [risk for risk in risks if risk["riskType"] == "shared_company_identity"]
        self.assertEqual(len(identity_risks), 2)
        self.assertEqual(
            {risk["comparisonType"] for risk in identity_risks},
            {"root_related", "related_related"},
        )
        self.assertFalse(any(risk["riskType"] == "shared_phone" for risk in risks))

    def test_risk_json_uses_network_schema_and_coverage_summary(self) -> None:
        """
        【方法功能】验证风险 JSON 2.0 输出根公司、关联公司及匹配覆盖率。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-21 10:57:32
        """
        config = DatabaseConfig("host", 15400, "big_data", "user", "secret")
        projects = build_projects(
            [
                {"project_code": "P-1", "lot_code": "L-1", "company_name": "企业A"},
                {"project_code": "P-1", "lot_code": "L-1", "company_name": "企业B"},
            ]
        )
        networks = build_root_networks(
            [
                {"search_value": "企业A", "company_name": "关联企业C", "relation_type": "DIRECT_INVESTMENT"},
                {"search_value": "企业B", "company_name": "关联企业D", "relation_type": "SENIOR_STAFF"},
            ],
            ("企业A", "企业B"),
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "risk_records.json"
            write_risk_json(
                output_path,
                "run-1",
                config,
                "big_data",
                projects,
                [],
                {"企业a", "关联企业c"},
                networks,
            )
            payload = json.loads(output_path.read_text(encoding="utf-8"))
        self.assertEqual(payload["schemaVersion"], "2.0")
        self.assertEqual(payload["summary"]["rootCompanyCount"], 2)
        self.assertEqual(payload["summary"]["relatedCompanyCount"], 2)
        self.assertEqual(payload["summary"]["matchedRootCompanyCount"], 1)
        self.assertEqual(payload["summary"]["matchedRelatedCompanyCount"], 1)
        self.assertEqual(payload["unmatchedCompanies"], ["企业B"])
        self.assertEqual(payload["unmatchedRelatedCompanies"], ["关联企业D"])


if __name__ == "__main__":
    unittest.main()
