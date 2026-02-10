"""
PASA Outage Monitor Tab for AEMO Energy Dashboard

Displays outage insights from High Impact Outages, MT-PASA, and ST-PASA data.
Uses Flexoki Light theme for consistent styling with the main dashboard.
"""

import logging
import os
import pickle
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import panel as pn

from .analyzer import OutageAnalyzer
from .change_detector import ChangeDetector
from aemo_dashboard.shared.flexoki_theme import (
    FLEXOKI_PAPER, FLEXOKI_BLACK, FLEXOKI_BASE, FLEXOKI_ACCENT, REGION_COLORS
)

logger = logging.getLogger(__name__)

# Production paths
DEFAULT_DATA_PATH = Path(os.getenv(
    'AEMO_DATA_PATH',
    '/Users/davidleitch/aemo_production/data'
))

# Gen info path (for fuel types)
GEN_INFO_PATH = DEFAULT_DATA_PATH / 'gen_info.pkl'

# Fuel type colors
FUEL_COLORS = {
    'Coal': FLEXOKI_BLACK,
    'Gas': FLEXOKI_ACCENT['orange'],
    'Hydro': FLEXOKI_ACCENT['blue'],
    'Wind': FLEXOKI_ACCENT['cyan'],
    'Solar': FLEXOKI_ACCENT['yellow'],
    'Battery Storage': FLEXOKI_ACCENT['magenta'],
    'Water': FLEXOKI_ACCENT['blue'],
    'Other': FLEXOKI_BASE[600],
}

# Map region format: NSW1 -> NSW for display
REGION_MAP = {'NSW1': 'NSW', 'QLD1': 'QLD', 'VIC1': 'VIC', 'SA1': 'SA', 'TAS1': 'TAS'}

# Notice category display
NOTICE_LABELS = {
    'planned': 'Planned',
    'short_notice': 'Short Notice',
    'unplanned': 'Unplanned',
    'unknown': 'Unknown',
}
NOTICE_COLORS = {
    'planned': FLEXOKI_ACCENT['green'],      # Green - expected
    'short_notice': FLEXOKI_ACCENT['yellow'],  # Yellow - caution
    'unplanned': FLEXOKI_ACCENT['red'],       # Red - alert
    'unknown': FLEXOKI_BASE[600],             # Gray - unknown
}


def load_gen_info() -> pd.DataFrame:
    """Load generator info for DUID -> fuel type mapping."""
    if GEN_INFO_PATH.exists():
        try:
            with open(GEN_INFO_PATH, 'rb') as f:
                return pickle.load(f)
        except Exception as e:
            logger.warning(f"Could not load gen_info: {e}")
    return pd.DataFrame()


def create_section_header(title: str) -> pn.pane.HTML:
    """Create a section header."""
    return pn.pane.HTML(f"""
    <h3 style="
        margin: 20px 0 10px 0;
        padding-bottom: 8px;
        border-bottom: 1px solid {FLEXOKI_BASE[300]};
        color: {FLEXOKI_BLACK};
        font-size: 16px;
        font-weight: 600;
    ">{title}</h3>
    """, sizing_mode='stretch_width')


def create_metric_card(label: str, value, color: str = None, unit: str = "") -> pn.pane.HTML:
    """Create a styled metric card."""
    color = color or FLEXOKI_BLACK
    display_value = f"{value:,.0f}" if isinstance(value, (int, float)) else str(value)
    return pn.pane.HTML(f"""
    <div style="
        background-color: {FLEXOKI_BASE[100]};
        border-radius: 8px;
        padding: 15px 20px;
        text-align: center;
        min-width: 120px;
    ">
        <div style="
            font-size: 28px;
            font-weight: 700;
            color: {color};
            line-height: 1.2;
        ">{display_value}<span style="font-size: 14px; font-weight: 400;">{unit}</span></div>
        <div style="
            font-size: 11px;
            color: {FLEXOKI_BASE[600]};
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-top: 5px;
        ">{label}</div>
    </div>
    """, sizing_mode='fixed', width=140, height=90)


def create_fuel_bar(fuel: str, mw: float, max_mw: float, unit_breakdown: list = None) -> pn.pane.HTML:
    """Create a horizontal bar for fuel type MW with individual unit segments.

    Args:
        fuel: Fuel type name
        mw: Total MW for this fuel
        max_mw: Maximum MW across all fuels (for scaling)
        unit_breakdown: List of (duid, unit_mw, region) tuples for each unit
    """
    width_pct = (mw / max_mw * 100) if max_mw > 0 else 0

    # Build stacked segments for individual units
    if unit_breakdown and len(unit_breakdown) > 0:
        # Sort by MW descending for consistent ordering
        sorted_units = sorted(unit_breakdown, key=lambda x: -x[1])
        segments_html = ""
        for duid, unit_mw, region in sorted_units:
            if unit_mw > 0:
                unit_pct = (unit_mw / mw * 100) if mw > 0 else 0
                # Map region to REGION_COLORS key format
                region_key = f"{region}1" if region and not region.endswith('1') else region
                color = REGION_COLORS.get(region_key, FLEXOKI_BASE[600])
                # Create segment with DUID label if wide enough
                min_width_for_label = 40  # px equivalent for label visibility
                show_label = (unit_pct > 8)  # Show label if segment is > 8% of bar
                label_html = f'<span style="font-size: 9px; color: white; text-shadow: 0 0 2px rgba(0,0,0,0.5); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; padding: 0 2px;">{duid}</span>' if show_label else ''
                segments_html += f"""
                <div style="
                    width: {unit_pct}%;
                    height: 100%;
                    background-color: {color};
                    display: inline-flex;
                    align-items: center;
                    justify-content: center;
                    box-sizing: border-box;
                    border-right: 1px solid rgba(255,255,255,0.3);
                " title="{duid} ({region}): {unit_mw:,.0f} MW">{label_html}</div>
                """
        bar_inner = f"""
        <div style="
            width: {width_pct}%;
            height: 100%;
            display: flex;
            border-radius: 4px;
            overflow: hidden;
            min-width: {15 if mw > 0 else 0}px;
        ">{segments_html}</div>
        """
    else:
        color = FUEL_COLORS.get(fuel, FLEXOKI_BASE[600])
        bar_inner = f"""
        <div style="
            width: {width_pct}%;
            height: 100%;
            background-color: {color};
            border-radius: 4px;
            min-width: {15 if mw > 0 else 0}px;
        "></div>
        """

    return pn.pane.HTML(f"""
    <div style="margin: 6px 0;">
        <div style="display: flex; align-items: center; gap: 10px;">
            <span style="
                width: 90px;
                font-weight: 500;
                font-size: 13px;
                color: {FLEXOKI_BLACK};
            ">{fuel}</span>
            <div style="
                flex: 1;
                height: 24px;
                background-color: {FLEXOKI_BASE[100]};
                border-radius: 4px;
                overflow: hidden;
            ">
                {bar_inner}
            </div>
            <span style="
                width: 70px;
                text-align: right;
                font-weight: 600;
                font-size: 13px;
                color: {FLEXOKI_BASE[800]};
            ">{mw:,.0f} MW</span>
        </div>
    </div>
    """, sizing_mode='stretch_width')


def get_tabulator_css() -> str:
    """Common Tabulator styling."""
    return f"""
    .tabulator {{
        background-color: {FLEXOKI_PAPER} !important;
        border: 1px solid {FLEXOKI_BASE[300]} !important;
        font-size: 13px;
    }}
    .tabulator .tabulator-header {{
        background-color: {FLEXOKI_BASE[100]} !important;
        border-bottom: 1px solid {FLEXOKI_BASE[300]} !important;
    }}
    .tabulator .tabulator-header .tabulator-col {{
        background-color: {FLEXOKI_BASE[100]} !important;
        color: {FLEXOKI_BLACK} !important;
        font-weight: 600;
    }}
    .tabulator .tabulator-row {{
        background-color: {FLEXOKI_PAPER} !important;
        color: {FLEXOKI_BASE[800]};
    }}
    .tabulator .tabulator-row:hover {{
        background-color: {FLEXOKI_BASE[100]} !important;
    }}
    .tabulator .tabulator-cell {{
        border-right: none !important;
    }}
    """


def create_generator_fuel_summary(detector: ChangeDetector, gen_info: pd.DataFrame) -> pn.Column:
    """Create summary of generator outages by fuel type, with individual unit segments colored by region."""
    # Get PASA changes only (mtpasa and stpasa are generator data, high_impact is transmission)
    mtpasa_changes = detector.get_recent_changes(hours=48, source='mtpasa')
    stpasa_changes = detector.get_recent_changes(hours=48, source='stpasa')
    changes = pd.concat([mtpasa_changes, stpasa_changes], ignore_index=True) if not mtpasa_changes.empty or not stpasa_changes.empty else pd.DataFrame()

    if changes.empty or gen_info.empty:
        return pn.Column(
            create_section_header("Generator Outages by Fuel"),
            pn.pane.HTML(f"""
            <div style="padding: 15px; text-align: center; color: {FLEXOKI_BASE[600]}; font-style: italic;">
                No generator outage data available
            </div>
            """),
            sizing_mode='stretch_width'
        )

    # Create DUID -> fuel/capacity/region lookup
    info_cols = ['Fuel', 'Capacity(MW)']
    if 'Region' in gen_info.columns:
        info_cols.append('Region')
    duid_info = gen_info.set_index('DUID')[info_cols].to_dict('index')

    # Extract MW values from descriptions, track individual units by fuel
    # Use dict to deduplicate by DUID, keeping max MW value
    duid_data = {}  # duid -> (fuel, mw, region)

    for _, row in changes.iterrows():
        duid = row['identifier']
        desc = row['description']

        # Get fuel type and region
        info = duid_info.get(duid, {})
        fuel = info.get('Fuel', 'Unknown')
        capacity = info.get('Capacity(MW)', 0)
        region = info.get('Region', 'Unknown')
        # Clean region (remove trailing '1' from NSW1, QLD1, etc.)
        if region and len(region) > 2 and region[-1].isdigit():
            region = region[:-1]

        # Parse MW from description
        mw = 0
        if 'dropping to 0' in desc:
            mw = capacity
        elif 'reduced by' in desc:
            match = re.search(r'reduced by (\d+)', desc)
            if match:
                mw = float(match.group(1))

        if mw > 0:
            # Keep max MW for each DUID (handles duplicates)
            if duid not in duid_data or mw > duid_data[duid][1]:
                duid_data[duid] = (fuel, mw, region)

    # Aggregate by fuel type
    fuel_mw = {}  # fuel -> total MW
    fuel_units = {}  # fuel -> [(duid, mw, region), ...]

    for duid, (fuel, mw, region) in duid_data.items():
        fuel_mw[fuel] = fuel_mw.get(fuel, 0) + mw
        if fuel not in fuel_units:
            fuel_units[fuel] = []
        fuel_units[fuel].append((duid, mw, region))

    if not fuel_mw:
        return pn.Column(
            create_section_header("Generator Outages by Fuel"),
            pn.pane.HTML(f"""
            <div style="padding: 15px; text-align: center; color: {FLEXOKI_BASE[600]}; font-style: italic;">
                No capacity reductions detected
            </div>
            """),
            sizing_mode='stretch_width'
        )

    # Sort by MW descending
    sorted_fuels = sorted(fuel_mw.items(), key=lambda x: -x[1])
    max_mw = max(fuel_mw.values())
    total_mw = sum(fuel_mw.values())

    bars = [create_fuel_bar(fuel, mw, max_mw, fuel_units.get(fuel, [])) for fuel, mw in sorted_fuels]

    # Create x-axis with tick marks
    tick_interval = 500 if max_mw > 1000 else 250 if max_mw > 500 else 100
    num_ticks = int(max_mw // tick_interval) + 1
    ticks_html = ""
    for i in range(num_ticks + 1):
        tick_mw = i * tick_interval
        if tick_mw > max_mw * 1.05:
            break
        tick_pct = (tick_mw / max_mw * 100) if max_mw > 0 else 0
        ticks_html += f"""
        <div style="
            position: absolute;
            left: {tick_pct}%;
            height: 6px;
            border-left: 1px solid {FLEXOKI_BASE[400]};
        "></div>
        <div style="
            position: absolute;
            left: {tick_pct}%;
            top: 8px;
            transform: translateX(-50%);
            font-size: 9px;
            color: {FLEXOKI_BASE[600]};
        ">{tick_mw:,.0f}</div>
        """

    x_axis = pn.pane.HTML(f"""
    <div style="margin: 2px 0 15px 100px; position: relative; height: 22px;">
        <div style="
            position: absolute;
            left: 0;
            right: 70px;
            top: 0;
            height: 1px;
            background-color: {FLEXOKI_BASE[300]};
        "></div>
        {ticks_html}
        <div style="
            position: absolute;
            right: 0;
            top: 8px;
            font-size: 9px;
            color: {FLEXOKI_BASE[600]};
            width: 70px;
            text-align: right;
        ">MW</div>
    </div>
    """, sizing_mode='stretch_width')

    # Create legend for regions
    legend_items = []
    for region_key, color in REGION_COLORS.items():
        if region_key == 'NEM':
            continue
        region_display = REGION_MAP.get(region_key, region_key)
        legend_items.append(f'<span style="display: inline-flex; align-items: center; margin-right: 12px;">'
                          f'<span style="width: 12px; height: 12px; background: {color}; border-radius: 2px; margin-right: 4px;"></span>'
                          f'<span style="font-size: 11px; color: {FLEXOKI_BASE[800]};">{region_display}</span></span>')

    legend = pn.pane.HTML(f"""
    <div style="margin: 8px 0 4px 100px; display: flex; flex-wrap: wrap;">
        {''.join(legend_items)}
    </div>
    """, sizing_mode='stretch_width')

    return pn.Column(
        create_section_header(f"Generator Outages by Fuel ({total_mw:,.0f} MW total)"),
        legend,
        *bars,
        x_axis,
        sizing_mode='stretch_width'
    )


def create_generator_changes_table(detector: ChangeDetector, gen_info: pd.DataFrame) -> pn.Column:
    """Create table showing recent generator availability changes with notice classification."""
    # Get PASA changes only (mtpasa and stpasa are generator data)
    mtpasa_changes = detector.get_recent_changes(hours=48, source='mtpasa')
    stpasa_changes = detector.get_recent_changes(hours=48, source='stpasa')
    df = pd.concat([mtpasa_changes, stpasa_changes], ignore_index=True) if not mtpasa_changes.empty or not stpasa_changes.empty else pd.DataFrame()

    if df.empty:
        return pn.Column(
            create_section_header("Generator Changes (Last 48h)"),
            pn.pane.HTML(f"""
            <div style="padding: 15px; text-align: center; color: {FLEXOKI_BASE[600]}; font-style: italic;">
                No recent generator changes detected
            </div>
            """),
            sizing_mode='stretch_width'
        )

    # Add fuel type from gen_info
    if not gen_info.empty:
        fuel_map = gen_info.set_index('DUID')['Fuel'].to_dict()
        df = df.copy()
        df['fuel'] = df['identifier'].map(fuel_map).fillna('Unknown')
    else:
        df = df.copy()
        df['fuel'] = 'Unknown'

    # Format notice category for display
    df['notice'] = df.get('notice_category', pd.Series(['unknown'] * len(df)))
    df['notice'] = df['notice'].fillna('unknown').map(
        lambda x: NOTICE_LABELS.get(x, 'Unknown')
    )

    # Get return dates from MT-PASA forecast
    duids = df['identifier'].unique().tolist()
    return_dates = detector.get_return_dates(duids)
    df['return_date'] = df['identifier'].map(return_dates)
    df['return'] = df['return_date'].apply(
        lambda x: x.strftime('%b %d') if pd.notna(x) else '—'
    )

    # Build display columns - include Notice and Return
    display_cols = ['identifier', 'fuel', 'description', 'notice', 'return']
    display_df = df[display_cols].copy()
    display_df.columns = ['DUID', 'Fuel', 'Description', 'Notice', 'Return']

    table = pn.widgets.Tabulator(
        display_df,
        show_index=False,
        sizing_mode='stretch_width',
        height=min(250, 50 + len(display_df) * 30),
        stylesheets=[get_tabulator_css()],
        configuration={'layout': 'fitColumns'},
    )

    # Count by notice category
    notice_counts = df['notice'].value_counts()
    unplanned_count = notice_counts.get('Unplanned', 0)
    short_notice_count = notice_counts.get('Short Notice', 0)

    # Build header with counts
    header_parts = []
    if unplanned_count > 0:
        header_parts.append(f"{unplanned_count} unplanned")
    if short_notice_count > 0:
        header_parts.append(f"{short_notice_count} short notice")
    header_suffix = f" ({', '.join(header_parts)})" if header_parts else ""

    return pn.Column(
        create_section_header(f"Generator Changes{header_suffix}"),
        table,
        sizing_mode='stretch_width'
    )


def consolidate_outages(df: pd.DataFrame) -> pd.DataFrame:
    """Consolidate consecutive outages for the same asset into date ranges."""
    if df.empty:
        return df

    df = df.copy()

    # Ensure datetime columns
    for col in ['Start', 'Finish']:
        if col in df.columns and not pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = pd.to_datetime(df[col], errors='coerce')

    if 'Network Asset' not in df.columns or 'Start' not in df.columns:
        return df

    # Group by asset and region, consolidate overlapping/consecutive dates
    consolidated = []
    for (region, asset), group in df.groupby(['Region', 'Network Asset']):
        group = group.sort_values('Start')

        current = None
        for _, row in group.iterrows():
            if current is None:
                current = {
                    'Region': region,
                    'Network Asset': asset,
                    'Start': row['Start'],
                    'Finish': row['Finish'],
                    'Status': row.get('Status', ''),
                    'Count': 1,
                }
            else:
                # Check if this row is within 2 days of current finish
                gap = (row['Start'] - current['Finish']).days if pd.notna(row['Start']) and pd.notna(current['Finish']) else 999
                if gap <= 2:
                    # Extend current range
                    current['Finish'] = max(current['Finish'], row['Finish']) if pd.notna(row['Finish']) else current['Finish']
                    current['Count'] += 1
                else:
                    # Save current and start new
                    consolidated.append(current)
                    current = {
                        'Region': region,
                        'Network Asset': asset,
                        'Start': row['Start'],
                        'Finish': row['Finish'],
                        'Status': row.get('Status', ''),
                        'Count': 1,
                    }

        if current:
            consolidated.append(current)

    result = pd.DataFrame(consolidated)

    # Add duration info to assets with multiple consolidated entries
    if 'Count' in result.columns:
        mask = result['Count'] > 1
        result.loc[mask, 'Network Asset'] = result.loc[mask].apply(
            lambda r: f"{r['Network Asset']} ({r['Count']} periods)", axis=1
        )
        result = result.drop(columns=['Count'])

    return result


def format_outage_df(df: pd.DataFrame, consolidate: bool = False) -> pd.DataFrame:
    """Format outage DataFrame for Tabulator display."""
    if df.empty:
        return pd.DataFrame(columns=['Region', 'Network Asset', 'Start', 'Finish', 'Status'])

    if consolidate:
        df = consolidate_outages(df)

    cols = ['Region', 'Network Asset', 'Start', 'Finish', 'Status']
    available = [c for c in cols if c in df.columns]
    result = df[available].copy()

    for col in ['Start', 'Finish']:
        if col in result.columns and pd.api.types.is_datetime64_any_dtype(result[col]):
            result[col] = result[col].dt.strftime('%Y-%m-%d')

    if 'Network Asset' in result.columns:
        result['Network Asset'] = result['Network Asset'].astype(str).str[:60]

    return result.head(25)


def create_outage_table(df: pd.DataFrame, title: str = "", consolidate: bool = False) -> pn.Column:
    """Create a styled outage table."""
    formatted = format_outage_df(df, consolidate=consolidate)

    if formatted.empty:
        return pn.Column(
            create_section_header(title) if title else None,
            pn.pane.HTML(f"""
            <div style="padding: 15px; text-align: center; color: {FLEXOKI_BASE[600]}; font-style: italic;">
                No outages to display
            </div>
            """),
            sizing_mode='stretch_width'
        )

    table = pn.widgets.Tabulator(
        formatted,
        show_index=False,
        sizing_mode='stretch_width',
        height=min(350, 50 + len(formatted) * 30),
        stylesheets=[get_tabulator_css()],
        configuration={'layout': 'fitColumns'},
    )

    components = []
    if title:
        components.append(create_section_header(title))
    components.append(table)

    return pn.Column(*components, sizing_mode='stretch_width')


def create_pasa_tab() -> pn.Column:
    """Create the PASA outage monitoring tab layout."""
    analyzer = OutageAnalyzer(DEFAULT_DATA_PATH)
    detector = ChangeDetector(data_path=DEFAULT_DATA_PATH)
    gen_info = load_gen_info()
    summary = analyzer.get_summary()

    # Header
    report_date_str = summary.report_date.strftime('%Y-%m-%d %H:%M') if summary.report_date else 'N/A'

    header = pn.pane.HTML(f"""
    <div style="
        background-color: {FLEXOKI_PAPER};
        padding: 15px 0;
        border-bottom: 2px solid {FLEXOKI_BASE[300]};
    ">
        <h2 style="
            margin: 0;
            color: {FLEXOKI_BLACK};
            font-size: 22px;
            font-weight: 600;
        ">NEM Outage Monitor</h2>
        <p style="
            margin: 5px 0 0 0;
            color: {FLEXOKI_BASE[600]};
            font-size: 13px;
        ">Last updated: {report_date_str}</p>
    </div>
    """, sizing_mode='stretch_width')

    # Get outage dataframes
    current_df = analyzer.get_current_outages()
    upcoming_df = analyzer.get_upcoming_outages(days=30)
    inter_df = analyzer.get_inter_regional_outages()
    unplanned_df = analyzer.get_unplanned_outages()

    # Calculate total generator MW affected (PASA sources only)
    mtpasa_changes = detector.get_recent_changes(hours=48, source='mtpasa')
    stpasa_changes = detector.get_recent_changes(hours=48, source='stpasa')
    pasa_changes = pd.concat([mtpasa_changes, stpasa_changes], ignore_index=True) if not mtpasa_changes.empty or not stpasa_changes.empty else pd.DataFrame()
    total_gen_mw = 0
    if not pasa_changes.empty and not gen_info.empty:
        duid_capacity = gen_info.set_index('DUID')['Capacity(MW)'].to_dict()
        for _, row in pasa_changes.iterrows():
            desc = row['description']
            duid = row['identifier']
            if 'dropping to 0' in desc:
                total_gen_mw += duid_capacity.get(duid, 0)
            elif 'reduced by' in desc:
                match = re.search(r'reduced by (\d+)', desc)
                if match:
                    total_gen_mw += float(match.group(1))

    # Metrics row
    metrics = pn.Row(
        create_metric_card("In Progress", summary.in_progress,
                          FLEXOKI_ACCENT['red'] if summary.in_progress > 0 else FLEXOKI_ACCENT['green']),
        create_metric_card("Unplanned", summary.unplanned,
                          FLEXOKI_ACCENT['red'] if summary.unplanned > 0 else FLEXOKI_BASE[600]),
        create_metric_card("Gen Affected", total_gen_mw, FLEXOKI_ACCENT['orange'], " MW"),
        create_metric_card("Next 7 Days", summary.upcoming_7d, FLEXOKI_ACCENT['blue']),
        create_metric_card("Inter-Regional", summary.inter_regional, FLEXOKI_ACCENT['magenta']),
        sizing_mode='stretch_width',
        margin=(20, 0),
    )

    # Generator section
    fuel_summary = create_generator_fuel_summary(detector, gen_info)
    generator_table = create_generator_changes_table(detector, gen_info)

    generator_section = pn.Column(
        fuel_summary,
        generator_table,
        sizing_mode='stretch_width',
    )

    # Active/Urgent tab content
    active_content = pn.Column(
        create_outage_table(current_df, "Transmission In Progress"),
        create_outage_table(unplanned_df, "Unplanned Outages"),
        generator_section,
        sizing_mode='stretch_width',
    )

    # Planned tab content (consolidated to reduce repetition)
    planned_content = pn.Column(
        create_outage_table(upcoming_df, "Planned Transmission (Next 30 Days)", consolidate=True),
        create_outage_table(inter_df, "Inter-Regional Planned (Consolidated)", consolidate=True),
        sizing_mode='stretch_width',
    )

    # Create sub-tabs
    tabs = pn.Tabs(
        ('Active & Generators', active_content),
        ('Planned Transmission', planned_content),
        sizing_mode='stretch_width',
        tabs_location='above',
        stylesheets=[f"""
        .bk-tab {{
            background-color: {FLEXOKI_BASE[100]} !important;
            color: {FLEXOKI_BASE[800]} !important;
            border: 1px solid {FLEXOKI_BASE[300]} !important;
            padding: 8px 16px !important;
            font-weight: 500;
        }}
        .bk-tab.bk-active {{
            background-color: {FLEXOKI_PAPER} !important;
            color: {FLEXOKI_BLACK} !important;
            border-bottom: 2px solid {FLEXOKI_ACCENT['cyan']} !important;
        }}
        """],
    )

    # Attribution
    attribution = pn.pane.HTML(f"""
    <div style="
        text-align: right;
        padding: 15px 0;
        color: {FLEXOKI_BASE[600]};
        font-size: 11px;
        font-style: italic;
        border-top: 1px solid {FLEXOKI_BASE[300]};
        margin-top: 20px;
    ">Data: AEMO High Impact Outages, MT-PASA, ST-PASA {datetime.now().strftime("%b '%y")}</div>
    """, sizing_mode='stretch_width')

    # Final layout
    layout = pn.Column(
        header,
        metrics,
        tabs,
        attribution,
        sizing_mode='stretch_width',
        styles={
            'background-color': FLEXOKI_PAPER,
            'padding': '0 20px',
        },
    )

    return layout
