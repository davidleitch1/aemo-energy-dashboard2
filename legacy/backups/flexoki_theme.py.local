"""
Flexoki Light Theme Definition
Centralized theme colors and utilities for the AEMO Dashboard

This module provides all color definitions, style dictionaries, and utilities
for the Flexoki Light theme. Import from here instead of hardcoding colors.
"""

import os

# Theme mode (for potential rollback support)
THEME_MODE = os.getenv('AEMO_THEME', 'flexoki')  # 'flexoki' or 'dracula'

# =============================================================================
# BASE COLORS
# =============================================================================

FLEXOKI_PAPER = '#FFFCF0'
FLEXOKI_BLACK = '#100F0F'

# Background scale (50 = lightest, 950 = darkest)
FLEXOKI_BASE = {
    50: '#F2F0E5',
    100: '#E6E4D9',
    150: '#DAD8CE',
    200: '#CECDC3',
    300: '#B7B5AC',
    400: '#9F9D96',
    500: '#878580',
    600: '#6F6E69',
    700: '#575653',
    800: '#403E3C',
    850: '#343331',
    900: '#282726',
    950: '#1C1B1A',
}

# =============================================================================
# ACCENT COLORS (600 weight - primary usage)
# =============================================================================

FLEXOKI_ACCENT = {
    'red': '#AF3029',
    'orange': '#BC5215',
    'yellow': '#AD8301',
    'green': '#66800B',
    'cyan': '#24837B',
    'blue': '#205EA6',
    'purple': '#5E409D',
    'magenta': '#A02F6F',
}

# =============================================================================
# REGION COLOR MAPPING
# Note: TAS1 uses cyan (not yellow) to maintain user familiarity with
# the original Dracula theme mapping where TAS1 was cyan.
# =============================================================================

REGION_COLORS = {
    'NSW1': FLEXOKI_ACCENT['green'],    # #66800B
    'QLD1': FLEXOKI_ACCENT['orange'],   # #BC5215
    'SA1': FLEXOKI_ACCENT['magenta'],   # #A02F6F
    'TAS1': FLEXOKI_ACCENT['cyan'],     # #24837B (kept cyan per review)
    'VIC1': FLEXOKI_ACCENT['purple'],   # #5E409D
    'NEM': FLEXOKI_BLACK,               # #100F0F
}

# =============================================================================
# TEXT COLORS FOR BACKGROUNDS
# Pre-computed optimal text colors based on WCAG contrast ratios (>=4.5:1)
# =============================================================================

TEXT_ON_BACKGROUND = {
    # Light backgrounds use black text
    'paper': FLEXOKI_BLACK,
    'base_50': FLEXOKI_BLACK,
    'base_100': FLEXOKI_BLACK,
    'base_150': FLEXOKI_BLACK,
    # Dark accent backgrounds use paper (light) text
    'cyan': FLEXOKI_PAPER,
    'green': FLEXOKI_PAPER,
    'orange': FLEXOKI_PAPER,
    'purple': FLEXOKI_PAPER,
    'red': FLEXOKI_PAPER,
    'magenta': FLEXOKI_PAPER,
    'blue': FLEXOKI_PAPER,
    # Yellow is borderline - use black for better contrast
    'yellow': FLEXOKI_BLACK,
}

# =============================================================================
# INTERACTIVE STATES
# Colors for hover, active, focus, and disabled states
# =============================================================================

INTERACTIVE_STATES = {
    'hover_bg': FLEXOKI_BASE[100],      # Slightly darker on hover
    'active_bg': FLEXOKI_BASE[150],     # More contrast when clicked
    'focus_ring': FLEXOKI_ACCENT['cyan'],
    'disabled_text': FLEXOKI_BASE[400],
    'disabled_bg': FLEXOKI_BASE[50],
}

# =============================================================================
# MATPLOTLIB STYLE (replaces DRACULA_STYLE)
# =============================================================================

FLEXOKI_MATPLOTLIB_STYLE = {
    'axes.facecolor': FLEXOKI_PAPER,
    'axes.edgecolor': FLEXOKI_BASE[150],
    'axes.labelcolor': FLEXOKI_BLACK,
    'figure.facecolor': FLEXOKI_PAPER,
    'grid.color': FLEXOKI_BASE[100],
    'grid.linestyle': '-',
    'grid.linewidth': 0.5,
    'text.color': FLEXOKI_BLACK,
    'xtick.color': FLEXOKI_BLACK,
    'ytick.color': FLEXOKI_BLACK,
    'axes.prop_cycle': None,  # Set programmatically with get_axes_prop_cycle()
}


def get_axes_prop_cycle():
    """Get matplotlib color cycle for Flexoki accent colors."""
    import matplotlib.pyplot as plt
    return plt.cycler('color', [
        FLEXOKI_ACCENT['cyan'],
        FLEXOKI_ACCENT['magenta'],
        FLEXOKI_ACCENT['green'],
        FLEXOKI_ACCENT['orange'],
        FLEXOKI_ACCENT['purple'],
        FLEXOKI_ACCENT['red'],
        FLEXOKI_ACCENT['yellow'],
    ])


def apply_matplotlib_style():
    """Apply Flexoki theme to matplotlib globally."""
    import matplotlib.pyplot as plt
    for key, value in FLEXOKI_MATPLOTLIB_STYLE.items():
        if value is not None:
            plt.rcParams[key] = value
    plt.rcParams['axes.prop_cycle'] = get_axes_prop_cycle()


# =============================================================================
# PLOTLY TEMPLATE (for renewable gauge and other Plotly charts)
# =============================================================================

def get_plotly_template():
    """Get Plotly template dict for Flexoki theme."""
    return {
        'layout': {
            'paper_bgcolor': FLEXOKI_PAPER,
            'plot_bgcolor': FLEXOKI_PAPER,
            'font': {
                'color': FLEXOKI_BLACK,
                'family': 'IBM Plex Sans, -apple-system, BlinkMacSystemFont, sans-serif',
            },
            'colorway': [
                FLEXOKI_ACCENT['cyan'],
                FLEXOKI_ACCENT['magenta'],
                FLEXOKI_ACCENT['green'],
                FLEXOKI_ACCENT['orange'],
                FLEXOKI_ACCENT['purple'],
                FLEXOKI_ACCENT['red'],
                FLEXOKI_ACCENT['yellow'],
            ],
            'xaxis': {
                'gridcolor': FLEXOKI_BASE[100],
                'linecolor': FLEXOKI_BASE[150],
                'tickcolor': FLEXOKI_BLACK,
            },
            'yaxis': {
                'gridcolor': FLEXOKI_BASE[100],
                'linecolor': FLEXOKI_BASE[150],
                'tickcolor': FLEXOKI_BLACK,
            },
        }
    }


def register_plotly_template():
    """Register Flexoki template with Plotly."""
    import plotly.io as pio
    pio.templates['flexoki'] = get_plotly_template()
    pio.templates.default = 'flexoki'


# =============================================================================
# PANEL CSS TEMPLATE (for widgets: buttons, dropdowns, sliders, etc.)
# =============================================================================

FLEXOKI_PANEL_CSS = f"""
:host {{
    --panel-primary-color: {FLEXOKI_ACCENT['cyan']};
    --panel-background-color: {FLEXOKI_PAPER};
    --panel-text-color: {FLEXOKI_BLACK};
    --panel-border-color: {FLEXOKI_BASE[150]};
}}

/* Primary buttons */
.bk-btn-primary {{
    background-color: {FLEXOKI_ACCENT['cyan']} !important;
    color: {FLEXOKI_PAPER} !important;
    border-color: {FLEXOKI_ACCENT['cyan']} !important;
}}

.bk-btn-primary:hover {{
    background-color: {FLEXOKI_BASE[800]} !important;
}}

/* Default buttons */
.bk-btn-default {{
    background-color: {FLEXOKI_BASE[50]} !important;
    color: {FLEXOKI_BLACK} !important;
    border-color: {FLEXOKI_BASE[150]} !important;
}}

.bk-btn-default:hover {{
    background-color: {FLEXOKI_BASE[100]} !important;
}}

/* Input fields */
.bk-input {{
    background-color: {FLEXOKI_PAPER} !important;
    color: {FLEXOKI_BLACK} !important;
    border-color: {FLEXOKI_BASE[150]} !important;
}}

.bk-input:focus {{
    border-color: {FLEXOKI_ACCENT['cyan']} !important;
    box-shadow: 0 0 0 2px {FLEXOKI_ACCENT['cyan']}33 !important;
}}

/* Select/Dropdown */
.bk-input-group select {{
    background-color: {FLEXOKI_PAPER} !important;
    color: {FLEXOKI_BLACK} !important;
}}

/* Slider */
.noUi-connect {{
    background: {FLEXOKI_ACCENT['cyan']} !important;
}}

.noUi-handle {{
    background: {FLEXOKI_PAPER} !important;
    border-color: {FLEXOKI_ACCENT['cyan']} !important;
}}

/* Tabs */
.bk-tab {{
    background-color: {FLEXOKI_BASE[50]} !important;
    color: {FLEXOKI_BLACK} !important;
}}

.bk-tab.bk-active {{
    background-color: {FLEXOKI_PAPER} !important;
    border-bottom-color: {FLEXOKI_ACCENT['cyan']} !important;
}}
"""

# =============================================================================
# PANDAS TABLE STYLES (replaces PRICE_TABLE_STYLES)
# =============================================================================

FLEXOKI_TABLE_STYLES = [
    dict(selector="caption",
         props=[("text-align", "left"),
                ("font-size", "150%"),
                ("color", FLEXOKI_PAPER),
                ("background-color", FLEXOKI_ACCENT['cyan']),
                ("caption-side", "top"),
                ("padding", "8px")]),
    dict(selector="",
         props=[("color", FLEXOKI_BLACK),
                ("background-color", FLEXOKI_PAPER),
                ("border-bottom", f"1px solid {FLEXOKI_BASE[150]}"),
                ("font-family", "'IBM Plex Sans', -apple-system, sans-serif")]),
    dict(selector="th",
         props=[("background-color", FLEXOKI_BASE[50]),
                ("border-bottom", f"1px solid {FLEXOKI_BASE[150]}"),
                ("font-size", "14px"),
                ("color", FLEXOKI_BLACK),
                ("padding", "8px")]),
    dict(selector="tr",
         props=[("background-color", FLEXOKI_PAPER),
                ("border-bottom", f"1px solid {FLEXOKI_BASE[100]}"),
                ("color", FLEXOKI_BLACK)]),
    dict(selector="td",
         props=[("font-size", "14px"),
                ("padding", "6px 8px"),
                ("font-family", "'IBM Plex Mono', 'SF Mono', monospace")]),
    dict(selector="th.col_heading",
         props=[("color", FLEXOKI_PAPER),
                ("font-size", "110%"),
                ("background-color", FLEXOKI_ACCENT['cyan'])]),
    dict(selector="tr:last-child",
         props=[("color", FLEXOKI_BLACK),
                ("border-bottom", f"3px solid {FLEXOKI_BASE[200]}")]),
    dict(selector=".row_heading",
         props=[("background-color", FLEXOKI_BASE[50]),
                ("border-bottom", f"1px solid {FLEXOKI_BASE[150]}"),
                ("color", FLEXOKI_BLACK),
                ("font-size", "14px")]),
    dict(selector="thead th:first-child",
         props=[("background-color", FLEXOKI_ACCENT['cyan']),
                ("color", FLEXOKI_PAPER)]),
    dict(selector="tr:hover",
         props=[("background-color", FLEXOKI_BASE[50])]),
]

# =============================================================================
# TYPOGRAPHY
# =============================================================================

FONT_FAMILY_SANS = "'IBM Plex Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif"
FONT_FAMILY_MONO = "'IBM Plex Mono', 'SF Mono', Consolas, 'Liberation Mono', monospace"

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================


def hex_to_rgb(hex_color):
    """Convert hex color to RGB tuple (0-255)."""
    hex_color = hex_color.lstrip('#')
    return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))


def calculate_luminance(hex_color):
    """Calculate relative luminance for WCAG contrast."""
    hex_color = hex_color.lstrip('#')
    r, g, b = tuple(int(hex_color[i:i+2], 16) / 255 for i in (0, 2, 4))

    def adjust(c):
        return c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4

    return 0.2126 * adjust(r) + 0.7152 * adjust(g) + 0.0722 * adjust(b)


def contrast_ratio(color1, color2):
    """Calculate WCAG contrast ratio between two colors."""
    l1 = calculate_luminance(color1)
    l2 = calculate_luminance(color2)
    lighter = max(l1, l2)
    darker = min(l1, l2)
    return (lighter + 0.05) / (darker + 0.05)


def color_distance_rgb(color1, color2):
    """
    Calculate Euclidean distance between two colors in RGB space.
    Returns a value from 0 (identical) to ~441 (black to white).
    Values > 80 are considered distinguishable.
    """
    import math
    r1, g1, b1 = hex_to_rgb(color1)
    r2, g2, b2 = hex_to_rgb(color2)
    return math.sqrt((r2 - r1) ** 2 + (g2 - g1) ** 2 + (b2 - b1) ** 2)


def get_optimal_text_color(bg_color):
    """Determine optimal text color (black or paper) for given background."""
    black_contrast = contrast_ratio(bg_color, FLEXOKI_BLACK)
    paper_contrast = contrast_ratio(bg_color, FLEXOKI_PAPER)
    return FLEXOKI_BLACK if black_contrast > paper_contrast else FLEXOKI_PAPER
