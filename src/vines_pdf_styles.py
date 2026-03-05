"""Reusable Vines of Mendoza branding constants and PDF components for ReportLab."""

from reportlab.lib.colors import HexColor
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    Paragraph, Spacer, Table, TableStyle, PageBreak, HRFlowable,
)

# ── Page layout ──────────────────────────────────────────────────────────────

PAGE_WIDTH, PAGE_HEIGHT = letter
MARGIN_LEFT = 0.75 * inch
MARGIN_RIGHT = 0.75 * inch
MARGIN_TOP = 0.9 * inch
MARGIN_BOTTOM = 0.75 * inch
CONTENT_WIDTH = PAGE_WIDTH - MARGIN_LEFT - MARGIN_RIGHT

# ── Brand colours ────────────────────────────────────────────────────────────

DARK_BROWN = HexColor("#3C2415")
BURGUNDY = HexColor("#8B1A1A")
OLIVE_GREEN = HexColor("#6B8E23")
CREAM = HexColor("#FFF8F0")
DARK_TEXT = HexColor("#2C1810")
LIGHT_GREY = HexColor("#F5F0EB")
MID_GREY = HexColor("#A09080")
WHITE = HexColor("#FFFFFF")
WINE_LIGHT = HexColor("#D4A574")

# ── Fonts (built-in Helvetica family) ────────────────────────────────────────

FONT_REGULAR = "Helvetica"
FONT_BOLD = "Helvetica-Bold"
FONT_ITALIC = "Helvetica-Oblique"
FONT_BOLD_ITALIC = "Helvetica-BoldOblique"

# ── Paragraph styles ─────────────────────────────────────────────────────────

_base = getSampleStyleSheet()


def _style(name, **kw):
    defaults = dict(fontName=FONT_REGULAR, fontSize=10, textColor=DARK_TEXT, leading=14)
    defaults.update(kw)
    return ParagraphStyle(name, **defaults)


STYLE_TITLE = _style("VTitle", fontName=FONT_BOLD, fontSize=26, textColor=DARK_BROWN,
                      alignment=TA_CENTER, leading=32, spaceAfter=6)
STYLE_SUBTITLE = _style("VSubtitle", fontName=FONT_ITALIC, fontSize=14, textColor=BURGUNDY,
                         alignment=TA_CENTER, leading=18, spaceAfter=4)
STYLE_H1 = _style("VH1", fontName=FONT_BOLD, fontSize=18, textColor=DARK_BROWN,
                   leading=22, spaceBefore=16, spaceAfter=8)
STYLE_H2 = _style("VH2", fontName=FONT_BOLD, fontSize=14, textColor=BURGUNDY,
                   leading=18, spaceBefore=12, spaceAfter=6)
STYLE_H3 = _style("VH3", fontName=FONT_BOLD, fontSize=11, textColor=OLIVE_GREEN,
                   leading=14, spaceBefore=8, spaceAfter=4)
STYLE_BODY = _style("VBody", fontSize=9.5, leading=13, spaceAfter=4)
STYLE_BODY_SM = _style("VBodySm", fontSize=8.5, leading=11, spaceAfter=2)
STYLE_BULLET = _style("VBullet", fontSize=9.5, leading=13, leftIndent=18,
                       bulletIndent=6, spaceAfter=2)
STYLE_FOOTER = _style("VFooter", fontName=FONT_ITALIC, fontSize=7.5, textColor=MID_GREY,
                       alignment=TA_CENTER)
STYLE_HEADER = _style("VHeader", fontName=FONT_BOLD, fontSize=7.5, textColor=MID_GREY,
                       alignment=TA_RIGHT)
STYLE_COVER_DATE = _style("VCoverDate", fontName=FONT_REGULAR, fontSize=11, textColor=MID_GREY,
                           alignment=TA_CENTER, leading=14, spaceBefore=12)
STYLE_TABLE_HEADER = _style("VTblH", fontName=FONT_BOLD, fontSize=8.5, textColor=WHITE, leading=11)
STYLE_TABLE_CELL = _style("VTblC", fontSize=8.5, leading=11)
STYLE_TABLE_CELL_SM = _style("VTblCSm", fontSize=7.5, leading=10)


# ── Reusable components ──────────────────────────────────────────────────────

def header_footer(canvas, doc, title="The Vines of Mendoza"):
    """Draw branded header and footer on every page (except cover)."""
    canvas.saveState()
    # Header line
    canvas.setStrokeColor(DARK_BROWN)
    canvas.setLineWidth(0.5)
    canvas.line(MARGIN_LEFT, PAGE_HEIGHT - 0.6 * inch,
                PAGE_WIDTH - MARGIN_RIGHT, PAGE_HEIGHT - 0.6 * inch)
    canvas.setFont(FONT_BOLD, 7.5)
    canvas.setFillColor(MID_GREY)
    canvas.drawRightString(PAGE_WIDTH - MARGIN_RIGHT, PAGE_HEIGHT - 0.52 * inch, title)

    # Footer
    canvas.setStrokeColor(MID_GREY)
    canvas.setLineWidth(0.3)
    canvas.line(MARGIN_LEFT, 0.55 * inch, PAGE_WIDTH - MARGIN_RIGHT, 0.55 * inch)
    canvas.setFont(FONT_ITALIC, 7.5)
    canvas.drawString(MARGIN_LEFT, 0.38 * inch, "Confidential")
    canvas.drawRightString(PAGE_WIDTH - MARGIN_RIGHT, 0.38 * inch,
                           f"Page {doc.page}")
    canvas.restoreState()


def cover_header_footer(canvas, doc):
    """Minimal footer for cover page — no header."""
    canvas.saveState()
    canvas.setFont(FONT_ITALIC, 7.5)
    canvas.setFillColor(MID_GREY)
    canvas.drawCentredString(PAGE_WIDTH / 2, 0.4 * inch, "Confidential")
    canvas.restoreState()


def section_heading(text, level=1):
    """Return a Paragraph with the appropriate heading style."""
    style = {1: STYLE_H1, 2: STYLE_H2, 3: STYLE_H3}.get(level, STYLE_H1)
    return Paragraph(text, style)


def section_divider():
    """A thin branded horizontal rule."""
    return HRFlowable(width="100%", thickness=0.5, color=WINE_LIGHT,
                      spaceBefore=8, spaceAfter=8)


def body_text(text):
    return Paragraph(text, STYLE_BODY)


def body_text_sm(text):
    return Paragraph(text, STYLE_BODY_SM)


def bullet_text(text):
    return Paragraph(f"\u2022  {text}", STYLE_BULLET)


def branded_table(headers, rows, col_widths=None, font_size=8.5):
    """Build a styled table with branded header row."""
    cell_style = STYLE_TABLE_CELL if font_size >= 8.5 else STYLE_TABLE_CELL_SM
    header_paras = [Paragraph(h, STYLE_TABLE_HEADER) for h in headers]
    data = [header_paras]
    for row in rows:
        data.append([Paragraph(str(c), cell_style) for c in row])

    if col_widths is None:
        n = len(headers)
        col_widths = [CONTENT_WIDTH / n] * n

    t = Table(data, colWidths=col_widths, repeatRows=1)
    style_cmds = [
        ("BACKGROUND", (0, 0), (-1, 0), DARK_BROWN),
        ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
        ("FONTNAME", (0, 0), (-1, 0), FONT_BOLD),
        ("FONTSIZE", (0, 0), (-1, 0), font_size),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
        ("TOPPADDING", (0, 0), (-1, 0), 6),
        ("FONTNAME", (0, 1), (-1, -1), FONT_REGULAR),
        ("FONTSIZE", (0, 1), (-1, -1), font_size),
        ("BOTTOMPADDING", (0, 1), (-1, -1), 4),
        ("TOPPADDING", (0, 1), (-1, -1), 4),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("GRID", (0, 0), (-1, -1), 0.3, MID_GREY),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, LIGHT_GREY]),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]
    t.setStyle(TableStyle(style_cmds))
    return t


def cover_page_elements(title, subtitle, date_text, doc_type=""):
    """Return a list of flowables for a branded cover page."""
    elements = []
    elements.append(Spacer(1, 1.8 * inch))

    # Decorative top rule
    elements.append(HRFlowable(width="60%", thickness=2, color=DARK_BROWN,
                               spaceBefore=0, spaceAfter=12))

    elements.append(Paragraph("THE VINES OF MENDOZA", _style(
        "CoverBrand", fontName=FONT_BOLD, fontSize=13, textColor=MID_GREY,
        alignment=TA_CENTER, leading=16, spaceAfter=8,
        letterSpacing=3,
    )))
    elements.append(Spacer(1, 0.15 * inch))
    elements.append(Paragraph(title, STYLE_TITLE))
    elements.append(Paragraph(subtitle, STYLE_SUBTITLE))
    if doc_type:
        elements.append(Spacer(1, 0.1 * inch))
        elements.append(Paragraph(doc_type, _style(
            "CoverType", fontName=FONT_BOLD, fontSize=11, textColor=OLIVE_GREEN,
            alignment=TA_CENTER, leading=14,
        )))
    elements.append(Spacer(1, 0.2 * inch))

    # Decorative bottom rule
    elements.append(HRFlowable(width="60%", thickness=2, color=DARK_BROWN,
                               spaceBefore=8, spaceAfter=12))

    elements.append(Paragraph(date_text, STYLE_COVER_DATE))

    elements.append(Spacer(1, 0.6 * inch))
    elements.append(Paragraph("EN  |  PT-BR  |  ES", _style(
        "CoverLang", fontName=FONT_BOLD, fontSize=12, textColor=BURGUNDY,
        alignment=TA_CENTER, leading=16,
    )))

    elements.append(PageBreak())
    return elements
