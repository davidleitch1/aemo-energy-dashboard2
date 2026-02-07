"""
Market Notices Component for Today Tab
======================================
Fetches and displays price-relevant AEMO market notices.

Filters for:
- RESERVE (LOR notices)
- POWER (emergency)
- INTER-REGIONAL (interconnector issues)
- Keywords: LOR1/2/3, RERT, direction, price cap, CPT, APC, MPC

Excludes:
- RECLASSIFY (routine)
- NON-CONFORMANCE (backward-looking)
- SETTLEMENTS / Negative Settlement Residue
"""

import panel as pn
import requests
import re
from datetime import datetime, timedelta

from ..shared.logging_config import get_logger
from ..shared.flexoki_theme import (
    FLEXOKI_PAPER, FLEXOKI_BLACK, FLEXOKI_BASE, FLEXOKI_ACCENT
)

logger = get_logger(__name__)


def fetch_market_notices(limit=10):
    """
    Fetch recent market notices from NEMWeb, filtered for price-relevance.

    Args:
        limit: Maximum number of notices to return

    Returns:
        list: List of notice dicts with keys: notice_id, notice_type_id,
              notice_type_description, creation_date, reason, reason_short, reason_full
    """
    try:
        url = "https://www.nemweb.com.au/REPORTS/CURRENT/Market_Notice/"
        response = requests.get(url, timeout=30)

        pattern = r'NEMITWEB1_MKTNOTICE_(\d{8})\.R(\d+)'
        matches = re.findall(pattern, response.text)

        if not matches:
            logger.warning("No market notice files found")
            return []

        cutoff = datetime.now() - timedelta(hours=72)  # Look back 72 hours for files
        recent_files = []
        for date_str, ref_num in matches:
            try:
                file_date = datetime.strptime(date_str, '%Y%m%d')
                if file_date.date() >= cutoff.date():
                    filename = f"NEMITWEB1_MKTNOTICE_{date_str}.R{ref_num}"
                    if filename not in recent_files:
                        recent_files.append(filename)
            except ValueError:
                continue

        # Get more notices to filter from
        recent_files = sorted(recent_files, key=lambda x: int(x.split('.R')[1]))[-50:]

        # Notice types to EXCLUDE (not price-predictive)
        EXCLUDED_TYPES = ['RECLASSIFY', 'NON-CONFORMANCE', 'SETTLEMENTS']
        # Type descriptions to exclude (partial match)
        EXCLUDED_DESCRIPTIONS = ['NEGATIVE SETTLEMENT', 'RESIDUE']

        # Price-relevant notice types (always include)
        RELEVANT_TYPES = ['RESERVE', 'POWER', 'INTER-REGIONAL']

        # Keywords that indicate price relevance (LOR = Lack of Reserve)
        RELEVANT_KEYWORDS = ['LOR1', 'LOR2', 'LOR3', 'LOR 1', 'LOR 2', 'LOR 3',
                            'RERT', 'direction', 'administered price', 'price cap',
                            'CPT', 'APC', 'MPC']

        notices = []
        for filename in reversed(recent_files):
            if len(notices) >= limit:
                break
            try:
                notice_url = f"{url}{filename}"
                content = requests.get(notice_url, timeout=15).text

                notice = {'filename': filename}
                match = re.search(r'Notice ID\s*:\s*(\d+)', content)
                if match:
                    notice['notice_id'] = int(match.group(1))
                match = re.search(r'Notice Type ID\s*:\s*(\S+)', content)
                if match:
                    notice['notice_type_id'] = match.group(1)
                match = re.search(r'Notice Type Description\s*:\s*(.+?)(?:\n|$)', content)
                if match:
                    notice['notice_type_description'] = match.group(1).strip()
                match = re.search(r'Creation Date\s*:\s*(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2}:\d{2})', content)
                if match:
                    notice['creation_date'] = datetime.strptime(f"{match.group(1)} {match.group(2)}", '%d/%m/%Y %H:%M:%S')
                match = re.search(r'Reason\s*:\s*\n+(.*?)(?:\n-{20,}|\Z)', content, re.DOTALL)
                if match:
                    notice['reason'] = match.group(1).strip()
                    notice['reason_short'] = notice['reason'][:120]
                    notice['reason_full'] = notice['reason']

                # Filter for price-relevant notices
                type_id = notice.get('notice_type_id', '')
                type_desc_upper = (notice.get('notice_type_description', '') or '').upper()

                # Skip excluded types
                if type_id in EXCLUDED_TYPES:
                    continue
                # Skip excluded descriptions (e.g., Negative Settlement Residue)
                if any(excl in type_desc_upper for excl in EXCLUDED_DESCRIPTIONS):
                    continue

                reason_upper = (notice.get('reason', '') or '').upper()

                is_relevant = (
                    type_id in RELEVANT_TYPES or
                    any(kw.upper() in reason_upper for kw in RELEVANT_KEYWORDS)
                )

                # Also check if within 48 hours
                creation = notice.get('creation_date')
                cutoff_48h = datetime.now() - timedelta(hours=48)
                is_recent = creation and creation >= cutoff_48h

                if is_relevant and is_recent:
                    notices.append(notice)

            except Exception as e:
                logger.warning(f"Error fetching notice {filename}: {e}")

        logger.info(f"Found {len(notices)} price-relevant notices in last 48h")
        return notices

    except Exception as e:
        logger.error(f"Error fetching market notices: {e}")
        return []


def create_notices_panel(notices):
    """
    Create market notices display with expandable full text using HTML5 details.

    Args:
        notices: List of notice dicts from fetch_market_notices()

    Returns:
        Panel HTML pane with expandable notices
    """
    if not notices:
        return pn.pane.HTML(
            f'<div style="color: {FLEXOKI_BASE[600]}; padding: 10px;">No key market notices in last 48h</div>'
        )

    type_colors = {
        'RESERVE': FLEXOKI_ACCENT['red'],      # LOR notices - most important
        'POWER': FLEXOKI_ACCENT['magenta'],    # Emergency
        'INTER-REGIONAL': FLEXOKI_ACCENT['blue'],  # Interconnector
        'RECLASSIFY': FLEXOKI_ACCENT['purple'],
        'GENERAL': FLEXOKI_ACCENT['orange'],
    }

    html = f"""<style>
        .notice-item {{
            border-bottom: 1px solid {FLEXOKI_BASE[150]};
            font-size: 11px;
        }}
        .notice-item details {{
            padding: 8px 10px;
        }}
        .notice-item details[open] {{
            background: {FLEXOKI_BASE[50]};
        }}
        .notice-item summary {{
            cursor: pointer;
            list-style: none;
            display: flex;
            align-items: center;
            gap: 8px;
            flex-wrap: wrap;
        }}
        .notice-item summary::-webkit-details-marker {{
            display: none;
        }}
        .notice-type {{
            display: inline-block;
            padding: 2px 6px;
            border-radius: 3px;
            font-size: 9px;
            font-weight: bold;
            white-space: nowrap;
        }}
        .notice-time {{
            color: {FLEXOKI_BLACK};
            font-size: 11px;
            font-weight: bold;
            white-space: nowrap;
        }}
        .notice-expand {{
            color: {FLEXOKI_ACCENT['cyan']};
            font-size: 10px;
            margin-left: auto;
        }}
        .notice-summary {{
            color: {FLEXOKI_BASE[800]};
            font-size: 10px;
            line-height: 1.4;
            flex-basis: 100%;
            margin-top: 4px;
        }}
        .notice-full {{
            color: {FLEXOKI_BASE[800]};
            margin-top: 10px;
            padding: 10px;
            background: white;
            border-radius: 4px;
            font-size: 11px;
            line-height: 1.5;
            white-space: pre-wrap;
            word-wrap: break-word;
            border: 1px solid {FLEXOKI_BASE[150]};
        }}
    </style>"""

    for notice in notices[:6]:
        type_id = notice.get('notice_type_id', 'GENERAL')
        type_desc = notice.get('notice_type_description', 'Notice')[:20]
        color = type_colors.get(type_id, FLEXOKI_BASE[600])
        creation = notice.get('creation_date')
        time_str = creation.strftime('%d %b %H:%M') if creation else ''

        reason_short = notice.get('reason_short', '')[:100]
        reason_full = notice.get('reason_full', notice.get('reason', ''))

        # Clean up the full text for HTML
        reason_full_clean = reason_full.replace('<', '&lt;').replace('>', '&gt;')

        html += f"""<div class="notice-item">
            <details>
                <summary>
                    <span class="notice-time">{time_str}</span>
                    <span class="notice-type" style="background: {color}; color: white;">{type_desc}</span>
                    <span class="notice-expand">click to expand</span>
                    <div class="notice-summary">{reason_short}...</div>
                </summary>
                <div class="notice-full">{reason_full_clean}</div>
            </details>
        </div>"""

    return pn.pane.HTML(html, sizing_mode='stretch_width')


def create_notices_component():
    """
    Create the complete market notices component for the Today tab.

    Returns:
        Panel Column containing title and notices panel
    """
    try:
        notices = fetch_market_notices(limit=10)
        notices_panel = create_notices_panel(notices)

        # Add title
        title_html = f'<h4 style="margin: 5px 0 8px 0; color: {FLEXOKI_BLACK};">Key Market Notices</h4>'

        return pn.Column(
            pn.pane.HTML(title_html),
            notices_panel,
            sizing_mode='stretch_width'
        )
    except Exception as e:
        logger.error(f"Error creating notices component: {e}")
        return pn.pane.HTML(
            f'<div style="color: {FLEXOKI_ACCENT["red"]}; padding: 10px;">Error loading notices: {e}</div>'
        )
