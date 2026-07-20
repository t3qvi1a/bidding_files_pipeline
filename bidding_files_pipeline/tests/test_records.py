"""
【模块功能】验证 OCR 最终 CSV 读取、企业名称去重和业务键生成逻辑。

:Author: gexinyan
:CreateTime: 2026-07-16 10:00:00
"""

from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from bidding_pipeline.records import CSV_COLUMN_MAPPING, ExtractionResult, extract_company_names, read_final_records


class RecordTests(unittest.TestCase):
    """
    【类功能】覆盖最终 CSV 到数据库记录模型的基础转换行为。
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    """

    def test_read_final_records_maps_chinese_columns(self) -> None:
        """
        【方法功能】验证 UTF-8 BOM CSV 可映射为完整解析记录。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "final.csv"
            row = {column: "" for column in CSV_COLUMN_MAPPING}
            row.update(
                {
                    "项目名称": "项目A",
                    "项目编号": "P-001",
                    "标段编号": "L-01",
                    "公司名称": "企业A",
                    "置信度": "1.2",
                }
            )
            with csv_path.open("w", encoding="utf-8-sig", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=list(CSV_COLUMN_MAPPING))
                writer.writeheader()
                writer.writerow(row)

            records = read_final_records(csv_path)

            self.assertEqual(len(records), 1)
            self.assertEqual(records[0].project_code, "P-001")
            self.assertEqual(records[0].company_name, "企业A")
            self.assertEqual(records[0].confidence, 1.0)

    def test_business_key_uses_business_fields_and_review_fallback(self) -> None:
        """
        【方法功能】验证完整记录与不完整复核记录都生成稳定且不同的业务键。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        primary = ExtractionResult(project_code="P-001", lot_code="L-01", company_name="企业A")
        same_primary = ExtractionResult(project_code="p-001", lot_code="l-01", company_name="企业A")
        review = ExtractionResult(source_path="a.pdf", category="unknown", evidence="parse failed")

        self.assertEqual(primary.business_key, same_primary.business_key)
        self.assertEqual(len(review.business_key), 64)
        self.assertNotEqual(primary.business_key, review.business_key)

    def test_extract_company_names_keeps_first_order_and_removes_blanks(self) -> None:
        """
        【方法功能】验证单 PDF 企业名称按首次出现顺序去重。
        :return: None
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        records = [
            ExtractionResult(company_name="企业A"),
            ExtractionResult(company_name="  企业A  "),
            ExtractionResult(company_name=""),
            ExtractionResult(company_name="企业B"),
        ]

        self.assertEqual(extract_company_names(records), ["企业A", "企业B"])

