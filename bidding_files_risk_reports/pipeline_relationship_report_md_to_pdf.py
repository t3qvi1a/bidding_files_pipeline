"""
【模块功能】在服务器字体缺失时为新版风险报告渲染器提供内置中文字体回退。

:Author: gexinyan
:CreateTime: 2026-07-30 16:25:19
"""

from __future__ import annotations

import sys
from typing import Optional, Sequence, Tuple

from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont

import huishan_relationship_report_md_to_pdf as renderer


def register_pdf_fonts() -> Tuple[str, str]:
    """
    【函数功能】优先注册新版报告字体，缺失时回退到 ReportLab 内置中文字体。
    :return: Tuple[str, str]，正文与粗体字体名称
    :Author: gexinyan
    :CreateTime: 2026-07-30 16:25:19
    Example: register_pdf_fonts()
    """
    try:
        return renderer_original_register_pdf_fonts()
    except FileNotFoundError:
        font_name = "STSong-Light"
        if font_name not in pdfmetrics.getRegisteredFontNames():
            pdfmetrics.registerFont(UnicodeCIDFont(font_name))
        pdfmetrics.registerFontFamily(
            font_name,
            normal=font_name,
            bold=font_name,
            italic=font_name,
            boldItalic=font_name,
        )
        renderer.EN_FONT_NAME = font_name
        renderer.EN_BOLD_FONT_NAME = font_name
        return font_name, font_name


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    【函数功能】使用包含字体回退的新版本渲染器执行命令行入口。
    :param argv: Optional[Sequence[str]]，命令行参数
    :return: int，进程退出码
    :Author: gexinyan
    :CreateTime: 2026-07-30 16:25:19
    Example: main(["--input-md", "report.md"])
    """
    return renderer.main(argv)


renderer_original_register_pdf_fonts = renderer.register_pdf_fonts
renderer.register_pdf_fonts = register_pdf_fonts


if __name__ == "__main__":
    sys.exit(main())
