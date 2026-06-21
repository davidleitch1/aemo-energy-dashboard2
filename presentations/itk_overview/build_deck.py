#!/usr/bin/env python3
"""
Build an ITK overview deck (PowerPoint + PDF) in the Flexoki Light style.

Reads `itk_overview.md` and produces:
  - itk_overview.pptx  (via python-pptx)
  - itk_overview.pdf   (via LibreOffice headless conversion of the .pptx)

The colours and typography mirror the dashboard's Flexoki theme
(src/aemo_dashboard/shared/flexoki_theme.py) so the deck matches the brand.

Usage:
    python build_deck.py
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

from pptx import Presentation
from pptx.util import Inches, Pt, Emu
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.lang import MSO_LANGUAGE_ID
from pptx.oxml.ns import qn

# ---------------------------------------------------------------------------
# Flexoki palette (mirrors src/aemo_dashboard/shared/flexoki_theme.py)
# ---------------------------------------------------------------------------

PAPER = RGBColor(0xFF, 0xFC, 0xF0)   # #FFFCF0
BLACK = RGBColor(0x10, 0x0F, 0x0F)   # #100F0F

BASE = {
    50:  RGBColor(0xF2, 0xF0, 0xE5),
    100: RGBColor(0xE6, 0xE4, 0xD9),
    150: RGBColor(0xDA, 0xD8, 0xCE),
    200: RGBColor(0xCE, 0xCD, 0xC3),
    300: RGBColor(0xB7, 0xB5, 0xAC),
    400: RGBColor(0x9F, 0x9D, 0x96),
    500: RGBColor(0x87, 0x85, 0x80),
    600: RGBColor(0x6F, 0x6E, 0x69),
    700: RGBColor(0x57, 0x56, 0x53),
    800: RGBColor(0x40, 0x3E, 0x3C),
}

ACCENT = {
    'red':     RGBColor(0xAF, 0x30, 0x29),
    'orange':  RGBColor(0xBC, 0x52, 0x15),
    'yellow':  RGBColor(0xAD, 0x83, 0x01),
    'green':   RGBColor(0x66, 0x80, 0x0B),
    'cyan':    RGBColor(0x24, 0x83, 0x7B),
    'blue':    RGBColor(0x20, 0x5E, 0xA6),
    'purple':  RGBColor(0x5E, 0x40, 0x9D),
    'magenta': RGBColor(0xA0, 0x2F, 0x6F),
}

FONT_SANS = "IBM Plex Sans"
FONT_MONO = "IBM Plex Mono"

# Colour each content section gets, in order.
SECTION_ACCENTS = ['cyan', 'blue', 'purple', 'green', 'orange', 'magenta']

# Slide geometry (16:9 widescreen)
SLIDE_W = Inches(13.333)
SLIDE_H = Inches(7.5)

CONTACT_NAME = "David Leitch"
CONTACT_ORG = "ITK Services"
CONTACT_EMAIL = "david.leitch@itkservices.com.au"
TAGLINE = "Independent research and analysis for the Australian energy market"


# ---------------------------------------------------------------------------
# Markdown parsing
# ---------------------------------------------------------------------------

def parse_markdown(md_path: Path):
    """Return (title, [(section_title, [bullets]), ...])."""
    title = "ITK"
    sections = []
    current = None
    for raw in md_path.read_text(encoding="utf-8").splitlines():
        line = raw.rstrip()
        if line.startswith("## "):
            current = (line[3:].strip(), [])
            sections.append(current)
        elif line.startswith("# "):
            title = line[2:].strip()
        elif line.startswith("- ") and current is not None:
            current[1].append(line[2:].strip())
    return title, sections


# ---------------------------------------------------------------------------
# Drawing helpers
# ---------------------------------------------------------------------------

def set_bg(slide, color):
    """Solid background fill for a slide."""
    bg = slide.background
    bg.fill.solid()
    bg.fill.fore_color.rgb = color


def add_rect(slide, x, y, w, h, color, line_color=None, line_w=None):
    shape = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, x, y, w, h)
    shape.fill.solid()
    shape.fill.fore_color.rgb = color
    if line_color is None:
        shape.line.fill.background()
    else:
        shape.line.color.rgb = line_color
        shape.line.width = line_w or Pt(1)
    shape.shadow.inherit = False
    return shape


def _set_run(run, *, font=FONT_SANS, size=18, color=BLACK, bold=False,
             italic=False, spacing=None):
    run.font.name = font
    run.font.size = Pt(size)
    run.font.bold = bold
    run.font.italic = italic
    run.font.color.rgb = color
    run.font.language_id = MSO_LANGUAGE_ID.ENGLISH_AUS
    # Ensure latin + east-asian + complex-script all use the same face so
    # LibreOffice picks IBM Plex when rendering to PDF.
    rPr = run._r.get_or_add_rPr()
    for tag in ("a:latin", "a:cs"):
        el = rPr.find(qn(tag))
        if el is None:
            el = rPr.makeelement(qn(tag), {})
            rPr.append(el)
        el.set("typeface", font)
    if spacing is not None:
        rPr.set("spc", str(int(spacing * 100)))  # spacing in points


def add_text(slide, x, y, w, h, text, *, font=FONT_SANS, size=18, color=BLACK,
             bold=False, italic=False, align=PP_ALIGN.LEFT,
             anchor=MSO_ANCHOR.TOP, spacing=None):
    box = slide.shapes.add_textbox(x, y, w, h)
    tf = box.text_frame
    tf.word_wrap = True
    tf.vertical_anchor = anchor
    tf.margin_left = 0
    tf.margin_right = 0
    tf.margin_top = 0
    tf.margin_bottom = 0
    p = tf.paragraphs[0]
    p.alignment = align
    run = p.add_run()
    run.text = text
    _set_run(run, font=font, size=size, color=color, bold=bold,
             italic=italic, spacing=spacing)
    return box


def add_footer(slide, accent):
    """Thin brand footer common to every slide."""
    y = Inches(7.02)
    add_rect(slide, 0, y, SLIDE_W, Pt(2), accent)
    add_text(
        slide, Inches(0.55), Inches(7.08), Inches(8.0), Inches(0.35),
        f"{CONTACT_ORG}  ·  {CONTACT_EMAIL}",
        font=FONT_MONO, size=9, color=BASE[600], anchor=MSO_ANCHOR.MIDDLE,
    )
    add_text(
        slide, Inches(9.0), Inches(7.08), Inches(3.78), Inches(0.35),
        "ITK", font=FONT_SANS, size=10, color=accent, bold=True,
        align=PP_ALIGN.RIGHT, anchor=MSO_ANCHOR.MIDDLE, spacing=2,
    )


# ---------------------------------------------------------------------------
# Slides
# ---------------------------------------------------------------------------

def blank_slide(prs):
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    set_bg(slide, PAPER)
    return slide


def title_slide(prs, title, sections):
    slide = blank_slide(prs)
    accent = ACCENT['cyan']

    # Left accent column
    add_rect(slide, 0, 0, Inches(0.35), SLIDE_H, accent)

    # Wordmark
    add_text(
        slide, Inches(0.9), Inches(2.15), Inches(8), Inches(1.8),
        title, font=FONT_SANS, size=120, color=BLACK, bold=True, spacing=2,
    )
    # Accent rule under wordmark
    add_rect(slide, Inches(1.0), Inches(3.95), Inches(3.2), Pt(4), accent)

    # Tagline
    add_text(
        slide, Inches(1.0), Inches(4.2), Inches(9.5), Inches(1.0),
        TAGLINE, font=FONT_SANS, size=22, color=BASE[700], italic=True,
    )

    # Section chips along the bottom
    labels = [s[0] for s in sections]
    chip_y = Inches(5.55)
    chip_h = Inches(0.5)
    x = Inches(1.0)
    gap = Inches(0.25)
    for i, label in enumerate(labels):
        col = ACCENT[SECTION_ACCENTS[i % len(SECTION_ACCENTS)]]
        w = Inches(0.55 + 0.105 * len(label))
        chip = add_rect(slide, x, chip_y, w, chip_h, PAPER,
                        line_color=col, line_w=Pt(1.25))
        tf = chip.text_frame
        tf.word_wrap = False
        tf.margin_top = 0
        tf.margin_bottom = 0
        p = tf.paragraphs[0]
        p.alignment = PP_ALIGN.CENTER
        r = p.add_run()
        r.text = label
        _set_run(r, font=FONT_SANS, size=13, color=col, bold=True)
        x = Emu(int(x) + int(w) + int(gap))

    # Presenter block
    add_text(
        slide, Inches(1.0), Inches(6.45), Inches(8), Inches(0.4),
        CONTACT_NAME, font=FONT_SANS, size=15, color=BLACK, bold=True,
    )
    add_footer(slide, accent)
    return slide


def section_slide(prs, idx, title, bullets):
    slide = blank_slide(prs)
    accent = ACCENT[SECTION_ACCENTS[idx % len(SECTION_ACCENTS)]]

    # Header band
    band_h = Inches(1.45)
    add_rect(slide, 0, 0, SLIDE_W, band_h, accent)
    # Section number, mono, faint
    add_text(
        slide, Inches(0.55), Inches(0.28), Inches(2), Inches(0.4),
        f"{idx + 1:02d}", font=FONT_MONO, size=14, color=PAPER, spacing=2,
    )
    add_text(
        slide, Inches(0.55), Inches(0.55), Inches(11), Inches(0.85),
        title, font=FONT_SANS, size=40, color=PAPER, bold=True,
        anchor=MSO_ANCHOR.MIDDLE,
    )

    # Bullets
    top = Inches(2.05)
    row_h = Inches(0.84)
    marker_size = Inches(0.16)
    text_x = Inches(1.25)
    text_w = Inches(11.4)
    for i, bullet in enumerate(bullets):
        cy = Emu(int(top) + int(row_h) * i)
        # Square marker in accent colour
        my = Emu(int(cy) + int(Inches(0.16)))
        add_rect(slide, Inches(0.6), my, marker_size, marker_size, accent)
        box = add_text(
            slide, text_x, cy, text_w, row_h, bullet,
            font=FONT_SANS, size=22, color=BLACK,
            anchor=MSO_ANCHOR.MIDDLE,
        )
        # Hairline divider under each row
        if i < len(bullets) - 1:
            div_y = Emu(int(cy) + int(row_h) - int(Pt(1)))
            add_rect(slide, text_x, div_y, text_w, Pt(0.75), BASE[150])

    add_footer(slide, accent)
    return slide


def closing_slide(prs):
    slide = blank_slide(prs)
    accent = ACCENT['cyan']
    add_rect(slide, 0, 0, Inches(0.35), SLIDE_H, accent)

    add_text(
        slide, Inches(0.9), Inches(2.3), Inches(11), Inches(1.0),
        "Let's talk", font=FONT_SANS, size=54, color=BLACK, bold=True,
    )
    add_rect(slide, Inches(1.0), Inches(3.45), Inches(2.4), Pt(4), accent)

    lines = [
        (CONTACT_NAME, FONT_SANS, 22, BLACK, True),
        (CONTACT_ORG, FONT_SANS, 18, BASE[700], False),
        (CONTACT_EMAIL, FONT_MONO, 18, accent, False),
    ]
    y = Inches(3.85)
    for text, font, size, color, bold in lines:
        add_text(slide, Inches(1.0), y, Inches(10), Inches(0.5), text,
                 font=font, size=size, color=color, bold=bold)
        y = Emu(int(y) + int(Inches(0.52)))

    add_footer(slide, accent)
    return slide


# ---------------------------------------------------------------------------
# Build + convert
# ---------------------------------------------------------------------------

def build(md_path: Path, out_pptx: Path):
    title, sections = parse_markdown(md_path)

    prs = Presentation()
    prs.slide_width = SLIDE_W
    prs.slide_height = SLIDE_H

    title_slide(prs, title, sections)
    for i, (sec_title, bullets) in enumerate(sections):
        section_slide(prs, i, sec_title, bullets)
    closing_slide(prs)

    prs.save(str(out_pptx))
    print(f"Wrote {out_pptx}  ({len(sections) + 2} slides)")


def to_pdf(pptx_path: Path):
    """Convert the .pptx to .pdf via LibreOffice headless.

    LibreOffice needs a writable, isolated user profile; we point it at a
    throwaway directory so the conversion works in clean/CI environments.
    Requires the `libreoffice-impress` package for the Impress export filter.
    """
    import os
    import tempfile

    out_dir = pptx_path.parent
    profile = Path(tempfile.mkdtemp(prefix="lo_profile_"))
    env = {**os.environ, "HOME": str(profile)}
    cmd = [
        "soffice", "--headless",
        f"-env:UserInstallation=file://{profile}",
        "--convert-to", "pdf:impress_pdf_Export",
        "--outdir", str(out_dir), str(pptx_path),
    ]
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    pdf_path = pptx_path.with_suffix(".pdf")
    if not pdf_path.exists():
        raise RuntimeError(
            "PDF conversion failed. Ensure 'libreoffice-impress' is "
            f"installed.\nstdout: {result.stdout}\nstderr: {result.stderr}"
        )
    print(f"Wrote {pdf_path}")


def main():
    here = Path(__file__).resolve().parent
    md_path = here / "itk_overview.md"
    out_pptx = here / "itk_overview.pptx"
    build(md_path, out_pptx)
    to_pdf(out_pptx)


if __name__ == "__main__":
    sys.exit(main())
