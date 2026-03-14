from pathlib import Path
import json
from datetime import datetime

from docx import Document
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn


OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

PACKET_JSON_PATH = OUTPUT_DIR / "weekly_market_packet.json"
DOCX_OUTPUT_PATH = OUTPUT_DIR / "weekly_market_packet.docx"


def set_document_defaults(doc: Document) -> None:
    section = doc.sections[0]
    section.top_margin = Inches(0.8)
    section.bottom_margin = Inches(0.8)
    section.left_margin = Inches(0.8)
    section.right_margin = Inches(0.8)

    styles = doc.styles

    normal = styles["Normal"]
    normal.font.name = "Calibri"
    normal._element.rPr.rFonts.set(qn("w:eastAsia"), "Calibri")
    normal.font.size = Pt(14)

    if "Title" in styles:
        styles["Title"].font.name = "Calibri"
        styles["Title"]._element.rPr.rFonts.set(qn("w:eastAsia"), "Calibri")
