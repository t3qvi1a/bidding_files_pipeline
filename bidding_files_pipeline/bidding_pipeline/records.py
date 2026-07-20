"""
【模块功能】读取 OCR 最终 CSV、规范化解析记录并生成稳定业务键。

:Author: gexinyan
:CreateTime: 2026-07-16 10:00:00
"""

from __future__ import annotations

import csv
import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


CSV_COLUMN_MAPPING = {
    "项目名称": "project_name",
    "项目编号": "project_code",
    "标段编号": "lot_code",
    "标段名称": "lot_name",
    "公司名称": "company_name",
    "中标与否": "award_status",
    "投标排名": "rank",
    "文件类别": "category",
    "依据文件路径": "source_path",
    "来源页码": "source_pages",
    "提取方式": "extraction_method",
    "证据文本": "evidence",
    "置信度": "confidence",
    "复核状态": "review_status",
    "解析结果生成日期时间": "generated_at",
}


@dataclass(frozen=True, slots=True)
class ExtractionResult:
    """
    【类功能】表示一条来自 OCR 最终汇总 CSV 的可入库解析结果。
    :Attributes:
        project_name: str，项目名称
        project_code: str，项目编号
        lot_code: str，标段编号
        lot_name: str，标段名称
        company_name: str，企业名称
        award_status: str，中标状态
        rank: str，投标排名
        category: str，来源文件类别
        source_path: str，依据文件路径
        source_pages: str，来源页码
        extraction_method: str，提取方式
        evidence: str，证据文本
        confidence: float，综合置信度
        review_status: str，复核状态
        generated_at: str，OCR 生成时间
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    """

    project_name: str = ""
    project_code: str = ""
    lot_code: str = ""
    lot_name: str = ""
    company_name: str = ""
    award_status: str = ""
    rank: str = ""
    category: str = ""
    source_path: str = ""
    source_pages: str = ""
    extraction_method: str = ""
    evidence: str = ""
    confidence: float = 0.0
    review_status: str = ""
    generated_at: str = ""

    @property
    def business_key(self) -> str:
        """
        【方法功能】生成用于 openGauss 幂等写入的稳定业务键。
        :return: str，64 位 SHA-256 十六进制业务键
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        project_identifier = self.project_code or self.project_name
        lot_identifier = self.lot_code or self.lot_name
        if project_identifier and lot_identifier and self.company_name:
            return hash_values(("business", project_identifier, lot_identifier, self.company_name))
        return hash_values(
            (
                "review",
                self.source_path,
                self.category,
                self.company_name,
                self.evidence,
            )
        )

    def to_db_values(self, run_id: str) -> tuple[Any, ...]:
        """
        【方法功能】按目标表列顺序转换为数据库参数元组。
        :param run_id: str，本次流水线运行标识
        :return: tuple[Any, ...]，可直接传给数据库驱动的参数元组
        :Author: gexinyan
        :CreateTime: 2026-07-16 10:00:00
        """
        return (
            self.business_key,
            run_id,
            nullable_text(self.project_name),
            nullable_text(self.project_code),
            nullable_text(self.lot_code),
            nullable_text(self.lot_name),
            nullable_text(self.company_name),
            nullable_text(self.award_status),
            nullable_text(self.rank),
            nullable_text(self.category),
            nullable_text(self.source_path),
            nullable_text(self.source_pages),
            nullable_text(self.extraction_method),
            nullable_text(self.evidence),
            self.confidence,
            nullable_text(self.review_status),
            nullable_text(self.generated_at),
        )


def normalize_text(value: Any) -> str:
    """
    【函数功能】将任意输入转换为去除空白和空字符的文本。
    :param value: Any，原始值
    :return: str，规范化后的文本
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: normalize_text(" 企业A ")
    """
    if value is None:
        return ""
    return str(value).replace("\x00", "").strip()


def nullable_text(value: Any) -> str | None:
    """
    【函数功能】将空文本转换为数据库 NULL，兼容 openGauss 空字符串语义。
    :param value: Any，待写入数据库的文本值
    :return: str | None，非空文本或 NULL
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: nullable_text(" ")
    """
    text = normalize_text(value)
    return text or None


def normalize_key_component(value: Any) -> str:
    """
    【函数功能】将业务键组成字段标准化为可稳定比较的紧凑文本。
    :param value: Any，业务字段原始值
    :return: str，去空白并转小写后的文本
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: normalize_key_component(" 项目 A ")
    """
    return re.sub(r"\s+", "", normalize_text(value)).casefold()


def hash_values(values: Iterable[Any]) -> str:
    """
    【函数功能】对字段序列生成带边界分隔符的 SHA-256 稳定哈希。
    :param values: Iterable[Any]，待哈希的字段序列
    :return: str，64 位 SHA-256 十六进制字符串
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: hash_values(("project", "company"))
    """
    normalized = "\x1f".join(normalize_key_component(item) for item in values)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def parse_confidence(value: Any) -> float:
    """
    【函数功能】解析 OCR 置信度并将非法值降级为零。
    :param value: Any，CSV 中的置信度文本
    :return: float，范围限制在 0 至 1 的置信度
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: parse_confidence("0.9234")
    """
    try:
        parsed = float(normalize_text(value))
    except (TypeError, ValueError):
        return 0.0
    return min(max(parsed, 0.0), 1.0)


def extraction_result_from_csv_row(row: dict[str, Any]) -> ExtractionResult:
    """
    【函数功能】将 OCR 最终 CSV 的中文表头行映射为解析结果对象。
    :param row: dict[str, Any]，CSV 读取的单行数据
    :return: ExtractionResult，规范化后的解析结果
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: extraction_result_from_csv_row({"公司名称": "企业A"})
    """
    values = {
        field: normalize_text(row.get(column, ""))
        for column, field in CSV_COLUMN_MAPPING.items()
        if field != "confidence"
    }
    return ExtractionResult(
        confidence=parse_confidence(row.get("置信度", "")),
        **values,
    )


def read_final_records(path: Path) -> list[ExtractionResult]:
    """
    【函数功能】读取 OCR 生成的 final.csv 并校验必要的中文表头。
    :param path: Path，final.csv 文件路径
    :return: list[ExtractionResult]，按 CSV 原始顺序读取的结果列表
    :raises FileNotFoundError: final.csv 不存在时抛出
    :raises ValueError: CSV 缺失约定表头时抛出
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: read_final_records(Path("results/final.csv"))
    """
    if not path.is_file():
        raise FileNotFoundError(f"OCR 最终结果文件不存在：{path}")
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        headers = set(reader.fieldnames or ())
        missing = set(CSV_COLUMN_MAPPING).difference(headers)
        if missing:
            raise ValueError(f"OCR 最终结果缺少表头：{', '.join(sorted(missing))}")
        return [extraction_result_from_csv_row(row) for row in reader if row]


def extract_company_names(records: Iterable[Any]) -> list[str]:
    """
    【函数功能】从单个 PDF 的解析记录中按首次出现顺序提取并去重企业名称。
    :param records: Iterable[Any]，含 company_name 属性的 OCR 记录集合
    :return: list[str]，非空且已去重的企业名称列表
    :Author: gexinyan
    :CreateTime: 2026-07-16 10:00:00
    Example: extract_company_names([])
    """
    result: list[str] = []
    seen: set[str] = set()
    for record in records:
        name = normalize_text(getattr(record, "company_name", ""))
        key = normalize_key_component(name)
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(name)
    return result
