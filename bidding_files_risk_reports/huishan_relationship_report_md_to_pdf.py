#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
【模块功能】将扩充版风险报告 Markdown 渲染为 PDF，并保留原 ReportLab 样式。
:Author: gexinyan
:CreateTime: 2026-07-01 17:06:42
"""

from __future__ import annotations

import argparse
import html
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pypdf import PdfReader
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import LongTable, Paragraph, SimpleDocTemplate, Spacer
from reportlab.platypus.tables import TableStyle


DEFAULT_INPUT_MD = "huishan_relationship_risk_report.md"
DEFAULT_OUTPUT_PDF = "huishan_relationship_risk_report.pdf"
TITLE_REGEX = re.compile(r"^(#{1,6})\s+(.*)$")
TABLE_ROW_REGEX = re.compile(r"^\s*\|.*\|\s*$")
TABLE_SEPARATOR_CELL_REGEX = re.compile(r"^:?-{3,}:?$")
GENERATION_TIME_REGEX = re.compile(r"^生成时间[：:]\s*.+$")
HORIZONTAL_RULE_REGEX = re.compile(r"^(?:-{3,}|\*{3,}|_{3,})$")
INLINE_STRONG_REGEX = re.compile(r"(\*\*|__)(?=\S)(.+?)(?<=\S)\1")
INLINE_HIGHLIGHT_SPAN_REGEX = re.compile(
    r"<span\s+style=[\"'][^\"']*color\s*:\s*(?P<color>#(?:8B0000|15803D|1D4ED8))[^\"']*[\"']\s*>(?P<content>.*?)</span>",
    re.IGNORECASE | re.DOTALL,
)
ESCAPED_HTML_BREAK_REGEX = re.compile(r"&lt;br\s*/?&gt;", re.IGNORECASE)
UNORDERED_LIST_ITEM_REGEX = re.compile(r"^\s*[-*+]\s+(.+)$")
BLOCKQUOTE_REGEX = re.compile(r"^\s*>\s?(.*)$")
LATIN_RUN_REGEX = re.compile(r"[A-Za-z0-9][A-Za-z0-9\s.,;:!?%()+\-/'\"]*")
ESCAPED_PIPE_TOKEN = "__MARKDOWN_PIPE_TOKEN__"
HIGHLIGHT_COLORS = {
    "#8b0000": "#8B0000",
    "#15803d": "#15803D",
    "#1d4ed8": "#1D4ED8",
}
EN_FONT_NAME = "TimesNewRoman"
EN_BOLD_FONT_NAME = "TimesNewRomanBold"


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """
    【函数功能】解析 Markdown 转 PDF 脚本的命令行参数。
    :param argv: Optional[Sequence[str]] 命令行参数列表，默认读取 sys.argv。
    :return: argparse.Namespace 解析后的参数对象。
    :raises SystemExit: 参数不合法时由 argparse 抛出。
    :Author: gexinyan
    :CreateTime: 2026-07-01 17:06:42
    Example: parse_args(["--input-md", "a.md", "--output-pdf", "a.pdf"])
    """
    repo_root = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description="将扩充版风险报告 Markdown 渲染为 PDF，并保留原 ReportLab 样式。"
    )
    parser.add_argument(
        "--input-md",
        default=str(repo_root / "report_outputs" / DEFAULT_INPUT_MD),
        help="Markdown 输入文件路径，默认读取 report_outputs 下的惠山区风险报告。",
    )
    parser.add_argument(
        "--output-pdf",
        default=None,
        help="PDF 输出路径。若不指定，则默认与输入 Markdown 同目录同名。",
    )
    return parser.parse_args(argv)


def load_markdown_lines(path: Path) -> List[str]:
    """
    【函数功能】以 UTF-8 编码读取 Markdown 文件并拆分为逐行文本。
    :param path: Path Markdown 文件路径。
    :return: List[str] Markdown 行列表。
    :raises FileNotFoundError: 文件不存在时抛出。
    :raises OSError: 文件读取失败时抛出。
    :Author: gexinyan
    :CreateTime: 2026-07-01 17:06:42
    Example: load_markdown_lines(Path("report.md"))
    """
    return path.read_text(encoding="utf-8-sig").splitlines()


def find_font_path(candidates: Sequence[str]) -> str:
    """
    【函数功能】从候选字体路径中选择第一个可用字体文件。
    :param candidates: Sequence[str] 字体候选路径列表。
    :return: str 可用字体文件路径。
    :raises FileNotFoundError: 所有候选字体都不存在时抛出。
    :Author: gexinyan
    :CreateTime: 2026-07-01 17:06:42
    Example: find_font_path([r"C:\\Windows\\Fonts\\Deng.ttf"])
    """
    for candidate in candidates:
        if Path(candidate).exists():
            return candidate
    raise FileNotFoundError("未找到可用于 PDF 中文渲染的字体文件。")


def register_pdf_fonts() -> Tuple[str, str]:
    """
    【函数功能】注册 PDF 中英文字体，并返回中文常规与加粗字体名称。
    :return: Tuple[str, str] 常规字体名和加粗字体名。
    :raises FileNotFoundError: 中文字体不可用时抛出。
    :Author: gexinyan
    :CreateTime: 2026-07-01 17:06:42
    Example: register_pdf_fonts()
    """
    regular_path = find_font_path(
        [
            r"C:\Windows\Fonts\simsun.ttc",
            r"C:\Windows\Fonts\STSONG.TTF",
            r"C:\Windows\Fonts\NotoSerifSC-VF.ttf",
            r"C:\Windows\Fonts\NotoSansSC-VF.ttf",
            r"C:\Windows\Fonts\Deng.ttf",
        ]
    )
    bold_path = find_font_path(
        [
            r"C:\Windows\Fonts\STZHONGS.TTF",
            r"C:\Windows\Fonts\simsun.ttc",
            r"C:\Windows\Fonts\STSONG.TTF",
            r"C:\Windows\Fonts\NotoSerifSC-VF.ttf",
            r"C:\Windows\Fonts\msyhbd.ttc",
            r"C:\Windows\Fonts\simhei.ttf",
            r"C:\Windows\Fonts\NotoSansSC-VF.ttf",
            r"C:\Windows\Fonts\Dengb.ttf",
        ]
    )
    english_regular_path = find_font_path(
        [
            r"C:\Windows\Fonts\times.ttf",
            r"C:\Windows\Fonts\TIMES.TTF",
        ]
    )
    english_bold_path = find_font_path(
        [
            r"C:\Windows\Fonts\timesbd.ttf",
            r"C:\Windows\Fonts\TIMESBD.TTF",
        ]
    )
    pdfmetrics.registerFont(TTFont("CNFont", regular_path))
    pdfmetrics.registerFont(TTFont("CNFontBold", bold_path))
    pdfmetrics.registerFont(TTFont(EN_FONT_NAME, english_regular_path))
    pdfmetrics.registerFont(TTFont(EN_BOLD_FONT_NAME, english_bold_path))
    pdfmetrics.registerFontFamily(
        "CNFont",
        normal="CNFont",
        bold="CNFontBold",
        italic="CNFont",
        boldItalic="CNFontBold",
    )
    pdfmetrics.registerFontFamily(
        EN_FONT_NAME,
        normal=EN_FONT_NAME,
        bold=EN_BOLD_FONT_NAME,
        italic=EN_FONT_NAME,
        boldItalic=EN_BOLD_FONT_NAME,
    )
    return "CNFont", "CNFontBold"


def build_pdf_styles(font_name: str, bold_font_name: str) -> Dict[str, ParagraphStyle]:
    """
    【函数功能】构建与原报告一致的 PDF 排版样式集合。
    :param font_name: str 常规中文字体名。
    :param bold_font_name: str 加粗中文字体名。
    :return: Dict[str, ParagraphStyle] 样式字典。
    :raises 无: 本函数不主动抛出业务异常。
    :Author: gexinyan
    :CreateTime: 2026-07-01 17:06:42
    Example: build_pdf_styles("CNFont", "CNFontBold")
    """
    base = getSampleStyleSheet()
    return {
        "Title": ParagraphStyle(
            "ReportTitle",
            parent=base["Title"],
            fontName=bold_font_name,
            fontSize=20,
            leading=27,
            alignment=TA_CENTER,
            textColor=colors.HexColor("#1F2A44"),
            spaceAfter=12,
            wordWrap="CJK",
        ),
        "Subtitle": ParagraphStyle(
            "ReportSubtitle",
            parent=base["Normal"],
            fontName=font_name,
            fontSize=11,
            leading=16,
            alignment=TA_CENTER,
            textColor=colors.HexColor("#1F2A44"),
            spaceAfter=4,
            wordWrap="CJK",
        ),
        "Metadata": ParagraphStyle(
            "ReportMetadata",
            parent=base["Normal"],
            fontName=font_name,
            fontSize=8.5,
            leading=12,
            alignment=TA_CENTER,
            textColor=colors.HexColor("#6B7280"),
            spaceAfter=12,
            wordWrap="CJK",
        ),
        "Heading1": ParagraphStyle(
            "ReportHeading1",
            parent=base["Heading1"],
            fontName=bold_font_name,
            fontSize=13,
            leading=19,
            textColor=colors.HexColor("#1F2A44"),
            spaceBefore=10,
            spaceAfter=6,
            wordWrap="CJK",
        ),
        "Heading2": ParagraphStyle(
            "ReportHeading2",
            parent=base["Heading2"],
            fontName=bold_font_name,
            fontSize=11,
            leading=16,
            textColor=colors.HexColor("#243B53"),
            spaceBefore=8,
            spaceAfter=5,
            wordWrap="CJK",
        ),
        "Heading3": ParagraphStyle(
            "ReportHeading3",
            parent=base["Heading2"],
            fontName=bold_font_name,
            fontSize=10,
            leading=14,
            textColor=colors.HexColor("#334155"),
            spaceBefore=6,
            spaceAfter=4,
            wordWrap="CJK",
        ),
        "Heading4": ParagraphStyle(
            "ReportHeading4",
            parent=base["Heading2"],
            fontName=bold_font_name,
            fontSize=9.2,
            leading=13,
            textColor=colors.HexColor("#334155"),
            spaceBefore=4,
            spaceAfter=3,
            wordWrap="CJK",
        ),
        "Body": ParagraphStyle(
            "ReportBody",
            parent=base["BodyText"],
            fontName=font_name,
            fontSize=9.5,
            leading=15,
            firstLineIndent=18,
            alignment=TA_JUSTIFY,
            textColor=colors.HexColor("#111827"),
            spaceAfter=5,
            wordWrap="CJK",
        ),
        "Bullet": ParagraphStyle(
            "ReportBullet",
            parent=base["BodyText"],
            fontName=font_name,
            fontSize=9.5,
            leading=15,
            firstLineIndent=0,
            leftIndent=18,
            bulletIndent=7,
            bulletFontName=font_name,
            bulletFontSize=7.2,
            alignment=TA_JUSTIFY,
            textColor=colors.HexColor("#111827"),
            spaceAfter=4,
            wordWrap="CJK",
        ),
        "Quote": ParagraphStyle(
            "ReportQuote",
            parent=base["BodyText"],
            fontName=font_name,
            fontSize=9,
            leading=14,
            firstLineIndent=0,
            leftIndent=14,
            alignment=TA_LEFT,
            textColor=colors.HexColor("#4B5563"),
            spaceBefore=3,
            spaceAfter=6,
            wordWrap="CJK",
        ),
        "Cell": ParagraphStyle(
            "ReportCell",
            parent=base["BodyText"],
            fontName=font_name,
            fontSize=7.6,
            leading=10.5,
            alignment=TA_LEFT,
            textColor=colors.HexColor("#111827"),
            wordWrap="CJK",
        ),
        "CellNoWrap": ParagraphStyle(
            "ReportCellNoWrap",
            parent=base["BodyText"],
            fontName=font_name,
            fontSize=7.4,
            leading=10.0,
            alignment=TA_LEFT,
            textColor=colors.HexColor("#111827"),
            splitLongWords=0,
        ),
        "CellCenter": ParagraphStyle(
            "ReportCellCenter",
            parent=base["BodyText"],
            fontName=font_name,
            fontSize=7.4,
            leading=10.0,
            alignment=TA_CENTER,
            textColor=colors.HexColor("#111827"),
            wordWrap="CJK",
        ),
        "CellHeader": ParagraphStyle(
            "ReportCellHeader",
            parent=base["BodyText"],
            fontName=bold_font_name,
            fontSize=7.8,
            leading=10.5,
            alignment=TA_CENTER,
            textColor=colors.HexColor("#1F2A44"),
            wordWrap="CJK",
        ),
    }


def normalize_text(value: Any, default: str = "") -> str:
    """
    【函数功能】将任意值归一化为适合渲染和测量的字符串。
    :param value: Any 待处理值。
    :param default: str 当值为空时使用的默认字符串。
    :return: str 归一化后的字符串。
    :raises 无: 本函数不主动抛出业务异常。
    :Author: gexinyan
    :CreateTime: 2026-07-01 17:06:42
    Example: normalize_text(None, "未披露")
    """
    if value is None:
        return default
    text = str(value)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.strip()
    return text if text else default


def escape_paragraph_text(value: Any, default: str = "") -> str:
    """
    【函数功能】将文本转换为 ReportLab Paragraph 可安全渲染的内容，并处理 Markdown 行内加粗。
    :param value: Any 待渲染文本。
    :param default: str 当值为空时使用的默认文本。
    :return: str 转义并转换行内标记后的文本。
    :raises 无: 本函数不主动抛出业务异常。
    :Author: gexinyan
    :CreateTime: 2026-07-01 17:06:42
    Example: escape_paragraph_text("A&B")
    """
    return render_inline_markdown(value, default)


def render_plain_text_segment(text: str, bold: bool = False) -> str:
    """
    【函数功能】转义普通文本片段，并将英文和数字片段标记为 Times New Roman 字体。
    :param text: str 待渲染普通文本。
    :param bold: bool 是否按加粗片段选择英文字体。
    :return: str 可传入 ReportLab Paragraph 的文本片段。
    :raises 无: 本函数不主动抛出业务异常。
    :Author: gexinyan
    :CreateTime: 2026-07-07 12:04:40
    Example: render_plain_text_segment("AI Agent系统")
    """
    parts: List[str] = []
    last_index = 0
    english_font_name = EN_BOLD_FONT_NAME if bold else EN_FONT_NAME
    for match in LATIN_RUN_REGEX.finditer(text):
        parts.append(html.escape(text[last_index : match.start()]))
        parts.append(f'<font name="{english_font_name}">{html.escape(match.group(0))}</font>')
        last_index = match.end()
    parts.append(html.escape(text[last_index:]))
    return "".join(parts)


def render_inline_markdown(value: Any, default: str = "") -> str:
    """
    【函数功能】将 Markdown 行内强调和白名单企业高亮span标记转换为 ReportLab Paragraph 支持的安全标记。
    :param value: Any 待转换文本。
    :param default: str 当值为空时使用的默认文本。
    :return: str 可传入 ReportLab Paragraph 的文本标记。
    :raises 无: 本函数不主动抛出业务异常。
    :Author: gexinyan
    :CreateTime: 2026-07-09 14:30:00
    Example: render_inline_markdown("**重点**内容")
    """
    text = normalize_inline_spacing(normalize_text(value, default)).replace("<br/>", "\n").replace("<br>", "\n")
    rendered = render_highlight_span_segments(text).replace("\n", "<br/>")
    return restore_html_break_tags(rendered)


def normalize_inline_spacing(text: str) -> str:
    """
    【函数功能】规范PDF渲染文本中中文与英文、数字之间的空格。
    :param text: str，待规范的文本。
    :return: str，规范后的文本。
    :raises 无: 本函数不主动抛出业务异常。
    :Author: gexinyan
    :CreateTime: 2026-07-09 14:30:00
    Example: normalize_inline_spacing("本AI Agent覆盖325条记录")
    """
    cjk_pattern = r"[\u3400-\u4dbf\u4e00-\u9fff]"
    normalized = re.sub(rf"({cjk_pattern})([A-Za-z0-9])", r"\1 \2", text)
    normalized = re.sub(rf"([A-Za-z0-9%])({cjk_pattern})", r"\1 \2", normalized)
    return re.sub(r"[ \t]{2,}", " ", normalized)


def restore_html_break_tags(value: str) -> str:
    """
    【函数功能】将被安全转义的HTML换行标记还原为ReportLab可识别的br标记。
    :param value: str，已经完成安全转义和行内Markdown转换的文本。
    :return: str，还原换行标记后的文本。
    :raises 无: 本函数不主动抛出业务异常。
    :Author: gexinyan
    :CreateTime: 2026-07-09 14:30:00
    Example: restore_html_break_tags("A&lt;br/&gt;B")
    """
    return ESCAPED_HTML_BREAK_REGEX.sub("<br/>", value)


def render_strong_segments(text: str) -> str:
    """
    【函数功能】将单段文本中的Markdown加粗语法转换为ReportLab安全标记。
    :param text: str，待转换的非深红色span文本。
    :return: str，转换后的安全标记文本。
    :raises 无: 本函数不主动抛出业务异常。
    :Author: gexinyan
    :CreateTime: 2026-07-09 14:30:00
    Example: render_strong_segments("**重点**内容")
    """
    parts: List[str] = []
    last_index = 0
    for match in INLINE_STRONG_REGEX.finditer(text):
        parts.append(render_plain_text_segment(text[last_index : match.start()]))
        parts.append(f"<b>{render_plain_text_segment(match.group(2), bold=True)}</b>")
        last_index = match.end()
    parts.append(render_plain_text_segment(text[last_index:]))
    return "".join(parts)


def render_highlight_span_segments(text: str) -> str:
    """
    【函数功能】将白名单内的企业高亮HTML span转换为ReportLab font color标记，并保留内部加粗语法。
    :param text: str，待转换的行内Markdown文本。
    :return: str，转换后的安全标记文本。
    :raises 无: 本函数不主动抛出业务异常。
    :Author: gexinyan
    :CreateTime: 2026-07-09 14:30:00
    Example: render_highlight_span_segments('<span style="color:#15803D">**中标企业**</span>')
    """
    parts: List[str] = []
    last_index = 0
    for match in INLINE_HIGHLIGHT_SPAN_REGEX.finditer(text):
        parts.append(render_strong_segments(text[last_index : match.start()]))
        highlighted_content = render_highlight_span_segments(match.group("content"))
        color = HIGHLIGHT_COLORS[match.group("color").lower()]
        parts.append(f'<font color="{color}">{highlighted_content}</font>')
        last_index = match.end()
    parts.append(render_strong_segments(text[last_index:]))
    return "".join(parts)


def measure_text_width(text: Any, font_name: str, font_size: float) -> float:
    """
    【函数功能】测量文本在指定字体和字号下的最大显示宽度。
    :param text: Any 待测量文本。
    :param font_name: str 字体名称。
    :param font_size: float 字号。
    :return: float 宽度，单位为点。
    :raises 无: 本函数不主动抛出业务异常。
    :Author: gexinyan
    :CreateTime: 2026-07-01 17:06:42
    Example: measure_text_width("标题", "CNFont", 10)
    """
    normalized = normalize_inline_spacing(normalize_text(text, ""))
    if not normalized:
        return 0.0
    width = 0.0
    for line in normalized.split("\n"):
        width = max(width, pdfmetrics.stringWidth(line, font_name, font_size))
    return width


def split_markdown_row(line: str) -> List[str]:
    """
    【函数功能】拆分 Markdown 表格行并还原被转义的竖线字符。
    :param line: str Markdown 表格行。
    :return: List[str] 单元格列表。
    :raises 无: 本函数不主动抛出业务异常。
    :Author: gexinyan
    :CreateTime: 2026-07-01 17:06:42
    Example: split_markdown_row("| A \\| B | C |")
    """
    content = line.strip()
    if content.startswith("|"):
        content = content[1:]
    if content.endswith("|"):
        content = content[:-1]
    content = content.replace(r"\|", ESCAPED_PIPE_TOKEN)
    cells = [cell.strip().replace(ESCAPED_PIPE_TOKEN, "|") for cell in content.split("|")]
    return cells


def is_table_separator_row(line: str) -> bool:
    """
    【函数功能】判断某一行是否为 Markdown 表格分隔行。
    :param line: str 待判断文本。
    :return: bool 是否为表格分隔行。
    :raises 无: 本函数不主动抛出业务异常。
    :Author: gexinyan
    :CreateTime: 2026-07-01 17:06:42
    Example: is_table_separator_row("| --- | --- |")
    """
    if not TABLE_ROW_REGEX.match(line):
        return False
    cells = split_markdown_row(line)
    if not cells:
        return False
    return all(TABLE_SEPARATOR_CELL_REGEX.match(cell.replace(" ", "")) for cell in cells)


def is_table_start(lines: Sequence[str], index: int) -> bool:
    """
    【函数功能】判断指定位置是否为 Markdown 表格起始位置。
    :param lines: Sequence[str] 文本行列表。
    :param index: int 待判断的行索引。
    :return: bool 是否为表格起始位置。
    :raises 无: 本函数不主动抛出业务异常。
    :Author: gexinyan
    :CreateTime: 2026-07-01 17:06:42
    Example: is_table_start(["| A | B |", "| --- | --- |"], 0)
    """
    if index < 0 or index + 1 >= len(lines):
        return False
    return bool(TABLE_ROW_REGEX.match(lines[index])) and is_table_separator_row(lines[index + 1])


def is_unordered_list_item(line: str) -> bool:
    """
    【函数功能】判断指定文本行是否为 Markdown 无序列表项。
    :param line: str 待判断文本行。
    :return: bool 是否为无序列表项。
    :raises 无: 本函数不主动抛出业务异常。
    :Author: gexinyan
    :CreateTime: 2026-07-07 11:39:08
    Example: is_unordered_list_item("- 风险提示")
    """
    return bool(UNORDERED_LIST_ITEM_REGEX.match(line))


def is_blockquote_line(line: str) -> bool:
    """
    【函数功能】判断指定文本行是否为 Markdown 块引用行。
    :param line: str 待判断文本行。
    :return: bool 是否为块引用行。
    :raises 无: 本函数不主动抛出业务异常。
    :Author: gexinyan
    :CreateTime: 2026-07-07 11:39:08
    Example: is_blockquote_line("> 注：说明")
    """
    return bool(BLOCKQUOTE_REGEX.match(line))


def normalize_table_row(row: Sequence[str], target_length: int) -> List[str]:
    """
    【函数功能】将表格行调整为固定列数，避免渲染时列数不一致。
    :param row: Sequence[str] 原始行。
    :param target_length: int 目标列数。
    :return: List[str] 调整后的行。
    :raises 无: 本函数不主动抛出业务异常。
    :Author: gexinyan
    :CreateTime: 2026-07-01 17:06:42
    Example: normalize_table_row(["A", "B"], 3)
    """
    normalized = [normalize_text(cell, "") for cell in row]
    if target_length <= 0:
        return []
    if len(normalized) < target_length:
        normalized.extend([""] * (target_length - len(normalized)))
        return normalized
    if len(normalized) > target_length:
        head = normalized[: target_length - 1]
        tail = " | ".join(normalized[target_length - 1 :]).strip()
        return head + [tail]
    return normalized


def parse_table_block(lines: Sequence[str], start_index: int) -> Tuple[List[str], List[List[str]], int]:
    """
    【函数功能】解析连续的 Markdown 表格块。
    :param lines: Sequence[str] 文本行列表。
    :param start_index: int 表格起始行索引。
    :return: Tuple[List[str], List[List[str]], int] 表头、数据行和下一个待处理索引。
    :raises ValueError: 表格结构不完整时抛出。
    :Author: gexinyan
    :CreateTime: 2026-07-01 17:06:42
    Example: parse_table_block(["| A | B |", "| --- | --- |", "| 1 | 2 |"], 0)
    """
    if not is_table_start(lines, start_index):
        raise ValueError(f"Invalid markdown table start at line {start_index + 1}.")

    headers = split_markdown_row(lines[start_index])
    rows: List[List[str]] = []
    index = start_index + 2
    while index < len(lines):
        current = normalize_text(lines[index], "")
        if not current:
            break
        if not TABLE_ROW_REGEX.match(current):
            break
        if is_table_separator_row(current):
            break
        rows.append(split_markdown_row(current))
        index += 1
    normalized_rows = [normalize_table_row(row, len(headers)) for row in rows]
    return headers, normalized_rows, index


def fit_table_widths(raw_widths: Sequence[float], available_width: float) -> List[float]:
    """
    【函数功能】将表格列宽压缩或展开到可用页面宽度。
    :param raw_widths: Sequence[float] 原始列宽权重。
    :param available_width: float 页面可用宽度，单位为点。
    :return: List[float] 调整后的列宽列表。
    :raises 无: 本函数不主动抛出业务异常。
    :Author: gexinyan
    :CreateTime: 2026-07-01 17:06:42
    Example: fit_table_widths([10, 20, 30], 200)
    """
    widths = [float(width) for width in raw_widths]
    column_count = len(widths)
    if column_count == 0:
        return []
    if available_width <= 0:
        return [1.0 for _ in widths]

    minimum_width = 12 * mm
    if column_count * minimum_width >= available_width:
        equal_width = available_width / column_count
        return [equal_width for _ in widths]

    widths = [max(width, minimum_width) for width in widths]
    remaining_indices = set(range(column_count))
    final_widths = [0.0] * column_count
    remaining_width = available_width
    remaining_weight = sum(widths)

    while remaining_indices:
        if remaining_weight <= 0:
            equal_width = available_width / column_count
            return [equal_width for _ in widths]

        proposed = {
            index: remaining_width * widths[index] / remaining_weight for index in remaining_indices
        }
        underflow = [index for index, width in proposed.items() if width < minimum_width]
        if not underflow:
            for index, width in proposed.items():
                final_widths[index] = width
            break

        for index in underflow:
            original_weight = widths[index]
            final_widths[index] = minimum_width
            remaining_indices.remove(index)
            remaining_width -= minimum_width
            remaining_weight -= original_weight

        if remaining_width <= 0:
            equal_width = available_width / column_count
            return [equal_width for _ in widths]

    if not any(final_widths):
        equal_width = available_width / column_count
        return [equal_width for _ in widths]

    difference = available_width - sum(final_widths)
    final_widths[-1] += difference
    return final_widths


def find_matched_object_column(headers: Sequence[str]) -> int | None:
    """
    【函数功能】定位风险表中用于展示手机号、固定电话或邮箱的重合信息列。
    :param headers: Sequence[str]，Markdown 表头文本。
    :return: int | None，目标列下标；不存在时返回 None。
    :Author: gexinyan
    :CreateTime: 2026-07-24 15:30:00
    Example: find_matched_object_column(["风险类别", "本条重合信息"])
    """
    for index, header in enumerate(headers):
        if normalize_text(header) in {"重合对象", "重合信息", "本条重合信息"}:
            return index
    return None


def is_matched_object_detail_row(headers: Sequence[str], row: Sequence[str]) -> bool:
    """
    【函数功能】判断双列表格中的当前行是否为重合信息详情行。
    :param headers: Sequence[str]，Markdown 表头文本。
    :param row: Sequence[str]，当前表格行。
    :return: bool，当前行是重合信息详情行时返回 True。
    :Author: gexinyan
    :CreateTime: 2026-07-24 15:30:00
    Example: is_matched_object_detail_row(["字段", "内容"], ["重合信息", "13800138000"])
    """
    return len(headers) == 2 and bool(row) and normalize_text(row[0]) in {"重合对象", "重合信息"}


def build_table_col_widths(
    headers: Sequence[str],
    rows: Sequence[Sequence[str]],
    styles: Dict[str, ParagraphStyle],
    available_width: float,
) -> List[float]:
    """
    【函数功能】根据表头和表格内容自动计算列宽。
    :param headers: Sequence[str] 表头文本。
    :param rows: Sequence[Sequence[str]] 表格数据行。
    :param styles: Dict[str, ParagraphStyle] PDF 样式集合。
    :param available_width: float 页面可用宽度，单位为点。
    :return: List[float] 列宽列表。
    :raises 无: 本函数不主动抛出业务异常。
    :Author: gexinyan
    :CreateTime: 2026-07-01 17:06:42
    Example: build_table_col_widths(["A", "B"], [["1", "2"]], styles, 400)
    """
    if not headers:
        return []

    header_font = styles["CellHeader"].fontName
    header_size = styles["CellHeader"].fontSize
    cell_font = styles["Cell"].fontName
    cell_size = styles["Cell"].fontSize

    raw_widths: List[float] = []
    for column_index, header in enumerate(headers):
        max_width = measure_text_width(header, header_font, header_size)
        for row in rows:
            cell_text = row[column_index] if column_index < len(row) else ""
            max_width = max(max_width, measure_text_width(cell_text, cell_font, cell_size))
        raw_widths.append(max_width + 8.0)
    column_widths = fit_table_widths(raw_widths, available_width)
    clean_headers = [normalize_text(header) for header in headers]

    if clean_headers == [
        "标注示例",
        "代表含义",
        "详细说明",
    ]:
        return [
            available_width * 0.20,
            available_width * 0.24,
            available_width * 0.55,
        ]


    # 2.1.1 低中标率企业 + 2.1.2 高中标率企业风险表格列宽调整
    if clean_headers == [
        "序号",
        "项目名称",
        "所有参与企业",
        "该企业投标结果",
        "项目中标企业",
    ]:
        return [
            available_width * 0.07,
            available_width * 0.12,
            available_width * 0.34,
            available_width * 0.14,
            available_width * 0.32,
        ]

    # 2.1.3 风险表格列宽调整
    if clean_headers == [
        "序号",
        "项目名称",
        "所有参与企业",
        "企业组合投标结果",
        "项目中标企业",
    ]:
        return [
            available_width * 0.07,
            available_width * 0.12,
            available_width * 0.34,
            available_width * 0.34,
            available_width * 0.12,
        ]
    if clean_headers == [
        "企业名称",
        "企业关联关系",
        "原始数据",
        "本条重合信息",
    ]:
        return [
            available_width * 0.18,
            available_width * 0.26,
            available_width * 0.38,
            available_width * 0.18,
        ]
    # 历史共同参投补充证据表按报告版式指定列宽，最后 1% 留作表格外侧间距。
    if clean_headers in (
        [
            "共同参与项目",
            "所有参与企业",
            "各参与企业投标结果",
            "项目中标企业",
        ],
        [
            "共同参与项目",
            "所有参与企业",
            "各参与投标企业投标结果",
            "项目中标企业",
        ],
    ):
        return [
            available_width * 0.20,
            available_width * 0.35,
            available_width * 0.30,
            available_width * 0.14,
        ]
    if clean_headers == ["字段", "内容"] and len(column_widths) == 2:
        target_width = min(max(58.0, available_width * 0.12), available_width * 0.20)
        extra_width = min(target_width - column_widths[0], column_widths[1] - 80.0)
        if extra_width > 0:
            column_widths[0] += extra_width
            column_widths[1] -= extra_width
    matched_object_column = find_matched_object_column(headers)
    if matched_object_column is None or len(column_widths) < 2:
        return column_widths

    # 为电话号码与邮箱预留可读的单行宽度；其余列按比例让出少量空间。
    target_width = min(max(88.0, available_width * 0.18), available_width * 0.28)
    extra_width = target_width - column_widths[matched_object_column]
    donor_indexes = [index for index in range(len(column_widths)) if index != matched_object_column]
    donor_capacity = sum(max(0.0, column_widths[index] - 40.0) for index in donor_indexes)
    if extra_width > 0 and donor_capacity > 0:
        transferred_width = min(extra_width, donor_capacity)
        for index in donor_indexes:
            capacity = max(0.0, column_widths[index] - 40.0)
            column_widths[index] -= transferred_width * capacity / donor_capacity
        column_widths[matched_object_column] += transferred_width
    return column_widths


def resolve_table_render_width(headers: Sequence[str], available_width: float) -> Tuple[float, str]:
    """
    【函数功能】根据表格类型确定PDF中的表格渲染宽度和对齐方式。
    :param headers: Sequence[str]，Markdown表头文本。
    :param available_width: float，页面可用宽度，单位为点。
    :return: Tuple[float, str]，表格目标宽度和ReportLab水平对齐方式。
    :raises 无: 本函数不主动抛出业务异常。
    :Author: gexinyan
    :CreateTime: 2026-07-09 14:30:00
    Example: resolve_table_render_width(["摘要指标", "统计结果"], 400)
    """
    clean_headers = [normalize_text(header) for header in headers]
    if clean_headers == ["摘要指标", "统计结果"]:
        return available_width * 0.62, "CENTER"
    return available_width, "LEFT"


def heading_style_name(level: int) -> str:
    """
    【函数功能】根据 Markdown 标题层级选择 PDF 样式名。
    :param level: int Markdown 标题层级。
    :return: str 样式字典中的键名。
    :raises 无: 本函数不主动抛出业务异常。
    :Author: gexinyan
    :CreateTime: 2026-07-01 17:06:42
    Example: heading_style_name(3)
    """
    if level <= 1:
        return "Heading1"
    if level == 2:
        return "Heading2"
    if level == 3:
        return "Heading3"
    return "Heading4"


def append_heading(story: List[Any], text: str, styles: Dict[str, ParagraphStyle], level: int) -> None:
    """
    【函数功能】向 PDF 内容流添加标题段落。
    :param story: List[Any] PDF 内容流。
    :param text: str 标题文本。
    :param styles: Dict[str, ParagraphStyle] PDF 样式集合。
    :param level: int Markdown 标题层级。
    :return: None
    :raises 无: 本函数不主动抛出业务异常。
    :Author: gexinyan
    :CreateTime: 2026-07-01 17:06:42
    Example: append_heading(story, "一、概述", styles, 2)
    """
    style_name = heading_style_name(level)
    story.append(Paragraph(escape_paragraph_text(text), styles[style_name]))


def append_paragraph(story: List[Any], text: str, styles: Dict[str, ParagraphStyle]) -> None:
    """
    【函数功能】向 PDF 内容流添加正文段落。
    :param story: List[Any] PDF 内容流。
    :param text: str 正文文本。
    :param styles: Dict[str, ParagraphStyle] PDF 样式集合。
    :return: None
    :raises 无: 本函数不主动抛出业务异常。
    :Author: gexinyan
    :CreateTime: 2026-07-01 17:06:42
    Example: append_paragraph(story, "正文", styles)
    """
    story.append(Paragraph(escape_paragraph_text(text), styles["Body"]))


def append_unordered_list_item(story: List[Any], text: str, styles: Dict[str, ParagraphStyle]) -> None:
    """
    【函数功能】向 PDF 内容流添加 Markdown 无序列表项。
    :param story: List[Any] PDF 内容流。
    :param text: str 列表项正文文本。
    :param styles: Dict[str, ParagraphStyle] PDF 样式集合。
    :return: None
    :raises 无: 本函数不主动抛出业务异常。
    :Author: gexinyan
    :CreateTime: 2026-07-07 11:39:08
    Example: append_unordered_list_item(story, "重点风险", styles)
    """
    story.append(Paragraph(escape_paragraph_text(text), styles["Bullet"], bulletText="●"))


def append_blockquote(story: List[Any], text: str, styles: Dict[str, ParagraphStyle]) -> None:
    """
    【函数功能】向 PDF 内容流添加 Markdown 块引用说明。
    :param story: List[Any] PDF 内容流。
    :param text: str 块引用正文文本。
    :param styles: Dict[str, ParagraphStyle] PDF 样式集合。
    :return: None
    :raises 无: 本函数不主动抛出业务异常。
    :Author: gexinyan
    :CreateTime: 2026-07-07 11:39:08
    Example: append_blockquote(story, "注：说明", styles)
    """
    story.append(Paragraph(escape_paragraph_text(text), styles["Quote"]))


def append_table(
    story: List[Any],
    headers: Sequence[str],
    rows: Sequence[Sequence[str]],
    styles: Dict[str, ParagraphStyle],
    available_width: float,
) -> None:
    """
    【函数功能】向 PDF 内容流添加表格并自动补充间距。
    :param story: List[Any] PDF 内容流。
    :param headers: Sequence[str] 表头文本。
    :param rows: Sequence[Sequence[str]] 表格数据行。
    :param styles: Dict[str, ParagraphStyle] PDF 样式集合。
    :param available_width: float 页面可用宽度，单位为点。
    :return: None
    :raises 无: 本函数不主动抛出业务异常。
    :Author: gexinyan
    :CreateTime: 2026-07-01 17:06:42
    Example: append_table(story, ["列"], [["值"]], styles, 400)
    """
    table_width, horizontal_align = resolve_table_render_width(headers, available_width)
    col_widths = build_table_col_widths(headers, rows, styles, table_width)
    matched_object_column = find_matched_object_column(headers)
    serial_number_columns = {
        column_index
        for column_index, header in enumerate(headers)
        if normalize_text(header) == "序号"
    }
    table_data = [[Paragraph(escape_paragraph_text(header), styles["CellHeader"]) for header in headers]]
    for row in rows:
        table_row = []
        for column_index, cell in enumerate(row):
            use_no_wrap = column_index == matched_object_column or (
                column_index == 1 and is_matched_object_detail_row(headers, row)
            )
            content = escape_paragraph_text(cell)
            cell_style = (
                styles["CellCenter"]
                if column_index in serial_number_columns
                else styles["CellNoWrap"] if use_no_wrap else styles["Cell"]
            )
            table_row.append(Paragraph(f"<nobr>{content}</nobr>" if use_no_wrap else content, cell_style))
        table_data.append(table_row)
    table = LongTable(table_data, colWidths=col_widths, repeatRows=1, hAlign=horizontal_align, splitInRow=0)
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#E8EEF7")),
                ("BOX", (0, 0), (-1, -1), 0.4, colors.HexColor("#9AA6B2")),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#C4CCD6")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 3.5),
                ("RIGHTPADDING", (0, 0), (-1, -1), 3.5),
                ("TOPPADDING", (0, 0), (-1, -1), 3.5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3.5),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
            ]
        )
    )
    story.append(table)
    story.append(Spacer(1, 6))


def draw_page_footer(canvas: Any, doc: SimpleDocTemplate) -> None:
    """
    【函数功能】绘制 PDF 页脚页码。
    :param canvas: Any ReportLab 画布对象。
    :param doc: SimpleDocTemplate 文档对象。
    :return: None
    :raises 无: 本函数不主动抛出业务异常。
    :Author: gexinyan
    :CreateTime: 2026-07-01 17:06:42
    Example: draw_page_footer(canvas, doc)
    """
    canvas.saveState()
    footer_font_name = getattr(doc, "_footer_font_name", "Helvetica")
    canvas.setFont(footer_font_name, 8)
    canvas.setFillColor(colors.HexColor("#6B7280"))
    canvas.drawCentredString(A4[0] / 2, 12 * mm, f"第 {doc.page} 页")
    canvas.restoreState()


def extract_title_block(lines: Sequence[str]) -> Tuple[str, str, str, int]:
    """
    【函数功能】提取文档顶部标题、副标题和生成时间，并返回正文起始索引。
    :param lines: Sequence[str] Markdown 行列表。
    :return: Tuple[str, str, str, int] 标题、副标题、生成时间和正文起始索引。
    :raises 无: 本函数不主动抛出业务异常。
    :Author: gexinyan
    :CreateTime: 2026-07-07 12:04:40
    Example: extract_title_block(["# 标题", "——专项分析", "生成时间：2026-07-01"])
    """
    index = 0
    while index < len(lines) and not normalize_text(lines[index], ""):
        index += 1

    title = ""
    subtitle = ""
    metadata = ""
    body_start = index
    if index < len(lines):
        title_match = TITLE_REGEX.match(normalize_text(lines[index], ""))
        if title_match and len(title_match.group(1)) == 1:
            title = title_match.group(2).strip()
            body_start = index + 1
            next_index = body_start
            while next_index < len(lines) and not normalize_text(lines[next_index], ""):
                next_index += 1
            if next_index < len(lines):
                candidate = normalize_text(lines[next_index], "")
                if GENERATION_TIME_REGEX.match(candidate):
                    metadata = candidate
                    body_start = next_index + 1
                elif not TITLE_REGEX.match(candidate):
                    subtitle = candidate
                    body_start = next_index + 1
                    metadata_index = body_start
                    while metadata_index < len(lines) and not normalize_text(lines[metadata_index], ""):
                        metadata_index += 1
                    if metadata_index < len(lines):
                        metadata_candidate = normalize_text(lines[metadata_index], "")
                        if GENERATION_TIME_REGEX.match(metadata_candidate):
                            metadata = metadata_candidate
                            body_start = metadata_index + 1
    return title, subtitle, metadata, body_start


def collect_paragraph(lines: Sequence[str], start_index: int) -> Tuple[str, int]:
    """
    【函数功能】收集连续的正文行并合并为一个段落。
    :param lines: Sequence[str] Markdown 行列表。
    :param start_index: int 段落起始索引。
    :return: Tuple[str, int] 合并后的段落文本和下一个待处理索引。
    :raises 无: 本函数不主动抛出业务异常。
    :Author: gexinyan
    :CreateTime: 2026-07-01 17:06:42
    Example: collect_paragraph(["第一行", "第二行", "", "## 标题"], 0)
    """
    parts: List[str] = []
    index = start_index
    while index < len(lines):
        current = normalize_text(lines[index], "")
        if not current:
            break
        if TITLE_REGEX.match(current):
            break
        if is_table_start(lines, index):
            break
        if HORIZONTAL_RULE_REGEX.match(current):
            break
        if is_unordered_list_item(current):
            break
        if is_blockquote_line(current):
            break
        parts.append(current)
        index += 1
    return "".join(parts), index


def render_markdown_story(
    lines: Sequence[str],
    styles: Dict[str, ParagraphStyle],
    available_width: float,
) -> Tuple[List[Any], str]:
    """
    【函数功能】将 Markdown 行流转换为 ReportLab story。
    :param lines: Sequence[str] Markdown 行列表。
    :param styles: Dict[str, ParagraphStyle] PDF 样式集合。
    :param available_width: float 页面可用宽度，单位为点。
    :return: Tuple[List[Any], str] PDF 内容流和标题文本。
    :raises 无: 本函数不主动抛出业务异常。
    :Author: gexinyan
    :CreateTime: 2026-07-01 17:06:42
    Example: render_markdown_story(lines, styles, 400)
    """
    story: List[Any] = []
    title, subtitle, metadata, body_start = extract_title_block(lines)
    if title:
        story.append(Paragraph(escape_paragraph_text(title), styles["Title"]))
        if subtitle:
            story.append(Paragraph(escape_paragraph_text(subtitle), styles["Subtitle"]))
        if metadata:
            story.append(Paragraph(escape_paragraph_text(metadata), styles["Metadata"]))

    index = body_start
    while index < len(lines):
        current = normalize_text(lines[index], "")
        if not current:
            index += 1
            continue

        if HORIZONTAL_RULE_REGEX.match(current):
            story.append(Spacer(1, 6))
            index += 1
            continue

        if current == "<!-- RISK_ITEM_GAP -->":
            story.append(Spacer(1, 18))
            index += 1
            continue

        heading_match = TITLE_REGEX.match(current)
        if heading_match:
            heading_level = len(heading_match.group(1))
            heading_text = heading_match.group(2).strip()
            append_heading(story, heading_text, styles, heading_level)
            index += 1
            continue

        if is_table_start(lines, index):
            headers, rows, next_index = parse_table_block(lines, index)
            append_table(story, headers, rows, styles, available_width)
            index = next_index
            continue

        list_match = UNORDERED_LIST_ITEM_REGEX.match(current)
        if list_match:
            append_unordered_list_item(story, list_match.group(1).strip(), styles)
            index += 1
            continue

        blockquote_match = BLOCKQUOTE_REGEX.match(current)
        if blockquote_match:
            append_blockquote(story, blockquote_match.group(1).strip(), styles)
            index += 1
            continue

        paragraph_text, next_index = collect_paragraph(lines, index)
        if paragraph_text:
            append_paragraph(story, paragraph_text, styles)
        index = max(next_index, index + 1)

    return story, title


def validate_pdf(output_pdf: Path, input_md: Path, report_title: str) -> Dict[str, Any]:
    """
    【函数功能】校验生成后的 PDF 文件并返回摘要信息。
    :param output_pdf: Path PDF 输出路径。
    :param input_md: Path 输入 Markdown 路径。
    :param report_title: str 文档标题。
    :return: Dict[str, Any] 校验摘要。
    :raises FileNotFoundError: PDF 未生成时抛出。
    :Author: gexinyan
    :CreateTime: 2026-07-01 17:06:42
    Example: validate_pdf(Path("a.pdf"), Path("a.md"), "标题")
    """
    if not output_pdf.exists():
        raise FileNotFoundError(f"PDF 文件未生成：{output_pdf}")
    reader = PdfReader(str(output_pdf))
    return {
        "inputMarkdown": str(input_md),
        "outputPdf": str(output_pdf),
        "outputBytes": output_pdf.stat().st_size,
        "pdfPages": len(reader.pages),
        "reportTitle": report_title,
    }


def build_pdf_from_markdown(input_md: Path, output_pdf: Path) -> Dict[str, Any]:
    """
    【函数功能】将 Markdown 报告渲染为 PDF 文件。
    :param input_md: Path Markdown 输入文件路径。
    :param output_pdf: Path PDF 输出文件路径。
    :return: Dict[str, Any] 生成结果摘要。
    :raises OSError: 读取 Markdown、写入 PDF 或字体加载失败时抛出。
    :Author: gexinyan
    :CreateTime: 2026-07-01 17:06:42
    Example: build_pdf_from_markdown(Path("report.md"), Path("report.pdf"))
    """
    input_md = input_md.resolve()
    output_pdf = output_pdf.resolve()
    output_pdf.parent.mkdir(parents=True, exist_ok=True)

    lines = load_markdown_lines(input_md)
    font_name, bold_font_name = register_pdf_fonts()
    styles = build_pdf_styles(font_name, bold_font_name)
    story, title = render_markdown_story(lines, styles, A4[0] - 36 * mm)
    if not story:
        raise ValueError(f"Markdown 文件中未解析到可渲染内容：{input_md}")

    report_title = title or input_md.stem
    doc = SimpleDocTemplate(
        str(output_pdf),
        pagesize=A4,
        leftMargin=18 * mm,
        rightMargin=18 * mm,
        topMargin=18 * mm,
        bottomMargin=18 * mm,
        title=report_title,
        author="gexinyan",
    )
    doc._footer_font_name = font_name  # type: ignore[attr-defined]
    doc.build(story, onFirstPage=draw_page_footer, onLaterPages=draw_page_footer)
    return validate_pdf(output_pdf, input_md, report_title)


def resolve_output_path(input_md: Path, output_value: Optional[str]) -> Path:
    """
    【函数功能】解析 PDF 输出路径，默认与输入 Markdown 同目录同名。
    :param input_md: Path Markdown 输入文件路径。
    :param output_value: Optional[str] 命令行传入的输出路径。
    :return: Path 解析后的 PDF 输出路径。
    :raises 无: 本函数不主动抛出业务异常。
    :Author: gexinyan
    :CreateTime: 2026-07-01 17:06:42
    Example: resolve_output_path(Path("risk/report.md"), None)
    """
    if output_value is None or not normalize_text(output_value, ""):
        return input_md.with_suffix(".pdf")
    output_path = Path(output_value)
    if not output_path.is_absolute():
        output_path = input_md.parent / output_path
    return output_path


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    【函数功能】执行 Markdown 到 PDF 的完整转换流程。
    :param argv: Optional[Sequence[str]] 命令行参数列表，默认读取 sys.argv。
    :return: int 进程退出码，0 表示成功。
    :raises Exception: 上层未捕获异常会继续向外抛出。
    :Author: gexinyan
    :CreateTime: 2026-07-01 17:06:42
    Example: main()
    """
    args = parse_args(argv)
    input_md = Path(args.input_md).resolve()
    output_pdf = resolve_output_path(input_md, args.output_pdf)
    summary = build_pdf_from_markdown(input_md, output_pdf)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
