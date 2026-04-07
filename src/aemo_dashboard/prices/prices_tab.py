"""Prices tab — widget layout, callbacks, and subtab wiring.

Extracted from ``gen_dash.py._create_prices_tab()``.
Receives the ``EnergyDashboard`` instance as *dashboard* so it can
access ``query_manager``, hooks, and other shared state.
"""

import logging
from datetime import datetime, time

import numpy as np
import pandas as pd
import panel as pn
import plotly.graph_objects as go

from ..shared.flexoki_theme import FLEXOKI_PAPER, FLEXOKI_BLACK, FLEXOKI_BASE, FLEXOKI_ACCENT
from ..shared.config import config
from .fuel_weighted_prices import compute_fuel_weighted_prices, build_combined_stats_table
from .price_bands import (
    compute_price_bands,
    build_band_charts,
    build_band_detail_table,
    compute_region_avg_demand,
)
from .price_chart import build_price_time_series, build_tod_chart
from .fuel_relatives import (
    query_fuel_relatives,
    apply_loess_smoothing,
    build_fuel_relatives_chart,
    build_price_index_chart,
)

logger = logging.getLogger(__name__)

GEN_INFO_FILE = config.gen_info_file


def _placeholder_fig(msg, color=None, height=400):
    """Create a Plotly figure with centered text message (placeholder/error)."""
    if color is None:
        color = FLEXOKI_BLACK
    fig = go.Figure()
    fig.update_layout(
        autosize=True, height=height,
        paper_bgcolor=FLEXOKI_PAPER, plot_bgcolor=FLEXOKI_PAPER,
        xaxis=dict(visible=False), yaxis=dict(visible=False),
        annotations=[dict(
            text=msg.replace('\n', '<br>'), xref='paper', yref='paper',
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=16, color=color), xanchor='center', yanchor='middle',
        )],
    )
    return fig


_TABULATOR_CSS = f"""
    .tabulator {{
        background-color: {FLEXOKI_PAPER};
        color: {FLEXOKI_BLACK};
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
        border: none;
    }}
    .tabulator-header {{
        background-color: {FLEXOKI_PAPER};
        color: {FLEXOKI_BLACK};
        font-weight: bold;
        border-bottom: 1px solid {FLEXOKI_BASE[300]};
    }}
    .tabulator-row {{
        background-color: {FLEXOKI_PAPER};
        color: {FLEXOKI_BLACK};
        border-bottom: 1px solid {FLEXOKI_BASE[100]};
    }}
    .tabulator-row:hover {{
        background-color: {FLEXOKI_BASE[50]};
    }}
    .tabulator-cell {{
        padding: 4px 8px;
        text-align: right;
    }}
    .tabulator-cell:first-child {{
        text-align: left;
    }}
"""


def create_prices_tab(dashboard):
    """Create the complete Prices tab with three subtabs.

    Parameters
    ----------
    dashboard : EnergyDashboard
        The parent dashboard instance providing ``query_manager``,
        ``start_date``, ``end_date``, and Bokeh hook methods.

    Returns
    -------
    pn.Row
        The assembled prices tab layout.
    """
    try:
        logger.info("Creating prices tab...")

        # ── Widgets ────────────────────────────────────────────────────
        analyze_button = pn.widgets.Button(
            name='\u25cf Analyze Prices', button_type='success',
            width=140, margin=(0, 15, 0, 0),
            stylesheets=["""
                :host(.solid) .bk-btn-success {
                    font-weight: 600; font-size: 13px;
                }
            """],
        )

        regions = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']
        region_selector = pn.widgets.CheckBoxGroup(
            name='', value=['NSW1', 'VIC1'], options=regions,
            inline=True, margin=(2, 0, 0, 0),
        )

        date_presets = pn.widgets.RadioButtonGroup(
            name='', options=['1d', '7d', '30d', '90d', '1y', 'All'],
            value='30d', button_type='default', margin=(0, 10, 0, 0),
        )

        default_end = pd.Timestamp.now().date()
        default_start = default_end - pd.Timedelta(days=30)

        start_date_picker = pn.widgets.DatePicker(
            name='From', value=default_start, width=110, margin=(0, 5, 0, 0),
        )
        end_date_picker = pn.widgets.DatePicker(
            name='To', value=default_end, width=110, margin=(0, 10, 0, 0),
        )

        aggregate_selector = pn.widgets.RadioButtonGroup(
            name='', value='30m',
            options=['5m', '30m', '1h', 'D', 'M', 'Q', 'Y'],
            button_type='default', margin=(0, 10, 0, 0),
        )

        smoothing_selector = pn.widgets.Select(
            name='Smooth', value='None',
            options=[
                'None',
                'LOESS (3h)', 'LOESS (1d)', 'LOESS (7d)',
                'LOESS (30d)', 'LOESS (90d)',
                'EWM (7d)', 'EWM (14d)', 'EWM (30d)', 'EWM (60d)',
            ],
            width=130, margin=(0, 10, 0, 0),
        )

        log_scale_checkbox = pn.widgets.Checkbox(
            name='Log', value=False, margin=(8, 0, 0, 0),
        )

        # Mapping: compact smoothing labels → original values
        _SMOOTH_MAP = {
            'None': 'None',
            'LOESS (3h)': 'LOESS (3 hours, frac=0.01)',
            'LOESS (1d)': 'LOESS (1 day, frac=0.02)',
            'LOESS (7d)': 'LOESS (7 days, frac=0.05)',
            'LOESS (30d)': 'LOESS (30 days, frac=0.1)',
            'LOESS (90d)': 'LOESS (90 days, frac=0.15)',
            'EWM (7d)': 'EWM (7 days, fast response)',
            'EWM (14d)': 'EWM (14 days, balanced)',
            'EWM (30d)': 'EWM (30 days, smooth)',
            'EWM (60d)': 'EWM (60 days, very smooth)',
        }

        # Mapping: compact freq labels → original values
        _FREQ_MAP = {
            '5m': '5 min', '30m': '30 min', '1h': '1 hour',
            'D': 'Daily', 'M': 'Monthly', 'Q': 'Quarterly', 'Y': 'Yearly',
        }

        # Mapping: compact date preset labels → original values
        _DATE_MAP = {
            '1d': '1 day', '7d': '7 days', '30d': '30 days',
            '90d': '90 days', '1y': '1 year', 'All': 'All data',
        }

        # ── Control state tracking (dirty/clean feedback) ─────────────
        def mark_controls_dirty(event=None):
            """Mark analyze button as needing re-run after settings change."""
            analyze_button.button_type = 'warning'
            analyze_button.name = 'Analyze Prices ●'

        def mark_controls_clean():
            """Mark analyze button as up-to-date after successful analysis."""
            analyze_button.button_type = 'primary'
            analyze_button.name = '✓ Analyze Prices'

        # ── Panes ──────────────────────────────────────────────────────
        price_plot_pane = pn.pane.Plotly(
            _placeholder_fig("Click 'Analyze Prices' to load data"),
            sizing_mode='stretch_width',
        )

        stats_title_pane = pn.pane.Markdown(
            "### Price Statistics",
            styles={'color': FLEXOKI_BLACK, 'background-color': FLEXOKI_PAPER, 'padding': '10px'},
        )

        stats_pane = pn.widgets.Tabulator(
            value=pd.DataFrame({'Message': ['Click "Analyze Prices" to see statistics']}),
            theme='fast', layout='fit_data_table', height=420,
            show_index=False, sizing_mode='stretch_width',
            stylesheets=[_TABULATOR_CSS + f"""
                .tabulator-col {{ min-width: auto !important; }}
                .tabulator-row:first-child .tabulator-cell {{
                    font-weight: bold;
                    color: {FLEXOKI_ACCENT['green']};
                    background-color: {FLEXOKI_BASE[50]};
                }}
                .tabulator-row:nth-child(3) {{
                    border-bottom: 1px solid #6F6E69;
                    padding-bottom: 8px;
                }}
            """],
            configuration={'columnDefaults': {'headerFilter': False, 'tooltip': True}},
        )

        fuel_prices_pane = pn.widgets.Tabulator(
            value=pd.DataFrame(), theme='fast', layout='fit_data_table',
            height=250, show_index=False, sizing_mode='stretch_width',
            stylesheets=[_TABULATOR_CSS + f"""
                .tabulator {{ margin-top: -10px; }}
                .tabulator-header {{ padding-top: 8px; }}
                .tabulator-row .tabulator-cell:first-child {{
                    font-weight: bold;
                    text-align: left;
                    color: {FLEXOKI_ACCENT['cyan']};
                }}
            """],
            configuration={
                'columnDefaults': {
                    'headerFilter': False, 'tooltip': True,
                    'headerHozAlign': 'center', 'hozAlign': 'right',
                },
                'columns': [
                    {'title': 'Fuel Type', 'field': 'Fuel Type', 'hozAlign': 'left', 'frozen': True}
                ],
            },
        )

        bands_plot_pane = pn.Column(sizing_mode='stretch_width')
        bands_plot_pane.append(pn.pane.Plotly(
            _placeholder_fig("Price bands will appear here", height=400),
            sizing_mode='stretch_width',
        ))

        fuel_relatives_plot_pane = pn.pane.Plotly(
            _placeholder_fig("Select a region and click Analyze to view fuel-weighted prices"),
            sizing_mode='stretch_width',
        )

        price_index_plot_pane = pn.pane.Plotly(
            _placeholder_fig("Price index will appear here after analysis"),
            sizing_mode='stretch_width',
        )

        high_price_events_pane = pn.widgets.Tabulator(
            pd.DataFrame(), show_index=False, sizing_mode='stretch_width',
            height=380, theme='fast',
            configuration={
                'columnDefaults': {
                    'headerSort': False, 'resizable': True, 'cellVertAlign': 'middle',
                },
                'layout': 'fitColumns', 'responsiveLayout': 'hide',
                'rowHeight': 20, 'headerHeight': 25,
            },
            stylesheets=[f"""
                .tabulator {{
                    font-size: 11px !important;
                    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
                    background-color: {FLEXOKI_PAPER}; color: {FLEXOKI_BLACK}; border: none;
                }}
                .tabulator-header {{
                    font-size: 11px !important;
                    background-color: {FLEXOKI_PAPER}; color: {FLEXOKI_BLACK};
                    font-weight: bold; border-bottom: 1px solid {FLEXOKI_BASE[300]};
                }}
                .tabulator-row {{
                    background-color: {FLEXOKI_PAPER}; color: {FLEXOKI_BLACK};
                    border-bottom: 1px solid {FLEXOKI_BASE[100]};
                }}
                .tabulator-row:hover {{ background-color: {FLEXOKI_BASE[50]}; }}
                .tabulator-cell {{ padding: 2px 4px !important; text-align: right; }}
                .tabulator-cell:first-child {{ text-align: left; }}
            """],
        )

        tod_plot_pane = pn.pane.Plotly(
            _placeholder_fig("Time of day analysis will appear here"),
            sizing_mode='stretch_width',
        )

        # ── Controls layout — compact horizontal bar ──────────────────
        row1 = pn.FlexBox(
            analyze_button,
            pn.pane.Markdown("**Region**", margin=(5, 5, 0, 0), width=50),
            region_selector,
            flex_wrap='wrap', align_items='center',
            sizing_mode='stretch_width', margin=(0, 0, 5, 0),
        )

        row2 = pn.FlexBox(
            pn.pane.Markdown("**Period**", margin=(5, 5, 0, 0), width=45),
            date_presets,
            start_date_picker, end_date_picker,
            flex_wrap='wrap', align_items='center',
            sizing_mode='stretch_width', margin=(0, 0, 5, 0),
        )

        row3 = pn.FlexBox(
            pn.pane.Markdown("**Freq**", margin=(5, 5, 0, 0), width=35),
            aggregate_selector,
            smoothing_selector,
            log_scale_checkbox,
            flex_wrap='wrap', align_items='center',
            sizing_mode='stretch_width', margin=(0, 0, 8, 0),
        )

        controls_bar = pn.Column(
            row1, row2, row3,
            sizing_mode='stretch_width',
            styles={
                'border-bottom': f'1px solid {FLEXOKI_BASE[200]}',
                'padding-bottom': '8px',
                'margin-bottom': '10px',
            },
        )

        # ── Subtab layouts ────────────────────────────────────────────
        price_analysis_content = pn.Column(
            pn.FlexBox(
                pn.Column("## Price Statistics ($)", stats_pane, min_width=350),
                pn.Column("## Time of Day Pattern", tod_plot_pane, min_width=350),
                flex_wrap='wrap', gap='20px', sizing_mode='stretch_width',
            ),
            pn.Spacer(height=20),
            pn.Column("## Price Time Series", price_plot_pane, sizing_mode='stretch_width'),
            sizing_mode='stretch_width',
        )

        price_bands_content = pn.FlexBox(
            pn.Column("## Price Band Details", high_price_events_pane, min_width=350),
            pn.Column("## Price Band Distribution", bands_plot_pane, min_width=400),
            flex_wrap='wrap', gap='20px', sizing_mode='stretch_width',
        )

        fuel_relatives_content = pn.Column(
            pn.pane.Markdown(
                "*Uses first selected region. Data covers all available history (~5.5 years).*",
                styles={'color': FLEXOKI_BASE[500], 'font-size': '12px'},
            ),
            pn.Column(
                "## Fuel-Weighted vs Flat Load Prices (90-day LOESS smoothed)",
                fuel_relatives_plot_pane,
                pn.Spacer(height=20),
                "## Price Index (Flat Load = 100)",
                price_index_plot_pane,
                sizing_mode='stretch_width',
            ),
            sizing_mode='stretch_width',
        )

        price_subtabs = pn.Tabs(
            ('Price Analysis', price_analysis_content),
            ('Price Bands', price_bands_content),
            ('Fuel Relatives', fuel_relatives_content),
            sizing_mode='stretch_both',
        )

        prices_tab = pn.Column(
            controls_bar,
            price_subtabs,
            sizing_mode='stretch_width',
        )

        # ── Date callbacks ────────────────────────────────────────────
        def update_date_range(event):
            preset = event.new
            current_end = end_date_picker.value
            if preset == '1d':
                new_start = current_end - pd.Timedelta(days=1)
            elif preset == '7d':
                new_start = current_end - pd.Timedelta(days=7)
            elif preset == '30d':
                new_start = current_end - pd.Timedelta(days=30)
            elif preset == '90d':
                new_start = current_end - pd.Timedelta(days=90)
            elif preset == '1y':
                new_start = current_end - pd.Timedelta(days=365)
            else:
                new_start = pd.Timestamp('2020-01-01').date()
            start_date_picker.value = new_start

        def update_date_display(event):
            current_end = end_date_picker.value
            current_start = start_date_picker.value
            days_diff = (current_end - current_start).days
            matches = False
            if date_presets.value == '1d' and days_diff == 1:
                matches = True
            elif date_presets.value == '7d' and days_diff == 7:
                matches = True
            elif date_presets.value == '30d' and days_diff == 30:
                matches = True
            elif date_presets.value == '90d' and days_diff == 90:
                matches = True
            elif date_presets.value == '1y' and days_diff == 365:
                matches = True
            elif date_presets.value == 'All' and current_start == pd.Timestamp('2020-01-01').date():
                matches = True
            if not matches:
                date_presets.value = None

        # ── Main analysis callback ────────────────────────────────────
        def load_and_plot_prices(event=None):
            try:
                price_plot_pane.object = _placeholder_fig('Loading price data...')

                selected_regions = region_selector.value
                if not selected_regions:
                    _show_empty("Please select at least one region", is_error=True)
                    return

                from ..shared.adapter_selector import load_price_data
                start_datetime = datetime.combine(start_date_picker.value, time.min)
                end_datetime = datetime.combine(end_date_picker.value, time.max)

                price_data = load_price_data(
                    start_date=start_datetime, end_date=end_datetime,
                    regions=selected_regions, resolution='auto',
                )
                if price_data.empty:
                    _show_empty("No data available for selected period")
                    return

                if price_data.index.name == 'SETTLEMENTDATE' and 'SETTLEMENTDATE' not in price_data.columns:
                    price_data = price_data.reset_index()

                # Log scale handling
                use_log = log_scale_checkbox.value
                if use_log and (price_data['RRP'] <= 0).any():
                    min_price = price_data['RRP'].min()
                    if min_price <= 0:
                        shift = abs(min_price) + 1
                        price_data['RRP_adjusted'] = price_data['RRP'] + shift
                        ylabel = f'Price ($/MWh) + {shift:.0f} [Log Scale]'
                        y_col = 'RRP_adjusted'
                    else:
                        y_col, ylabel = 'RRP', 'Price ($/MWh) [Log Scale]'
                else:
                    y_col, ylabel = 'RRP', 'Price ($/MWh)'

                # Resample
                freq_map = {
                    '5 min': '5min', '30 min': '30min', '1 hour': 'h',
                    'Daily': 'D', 'Monthly': 'M', 'Quarterly': 'Q', 'Yearly': 'Y',
                }
                freq = freq_map.get(_FREQ_MAP.get(aggregate_selector.value, aggregate_selector.value), '30min')
                original_30min_price_data = price_data.copy()

                if freq != '5min':
                    if 'SETTLEMENTDATE' in price_data.columns:
                        price_data = price_data.set_index('SETTLEMENTDATE')
                    agg_dict = {}
                    if y_col in price_data.columns:
                        agg_dict[y_col] = 'mean'
                    if 'RRP' in price_data.columns and y_col != 'RRP':
                        agg_dict['RRP'] = 'mean'
                    if not agg_dict:
                        agg_dict['RRP'] = 'mean'
                    price_data = price_data.groupby('REGIONID').resample(freq).agg(agg_dict).reset_index()

                original_price_data = price_data.copy()

                # Apply smoothing (for time series plot only)
                smoothing_value_full = _SMOOTH_MAP.get(smoothing_selector.value, smoothing_selector.value)
                if smoothing_value_full != 'None':
                    price_data = _apply_smoothing_to_prices(
                        price_data, smoothing_value_full,
                        aggregate_selector.value, selected_regions, y_col,
                    )

                # Date range text
                date_range_text = _get_date_range_text(
                    date_presets.value, start_date_picker.value, end_date_picker.value,
                )

                price_data = price_data.dropna(subset=[y_col])

                # ── Price time series chart ──
                plot = build_price_time_series(
                    price_data, y_col, ylabel, use_log, date_range_text,
                    start_date_picker.value, end_date_picker.value,
                    dashboard._get_attribution_hook(), dashboard._get_flexoki_background_hook(),
                )
                price_plot_pane.object = plot

                # ── Statistics ──
                stats_list = []
                for region in selected_regions:
                    rd = original_price_data[original_price_data['REGIONID'] == region]['RRP']
                    s = rd.describe()
                    stats_list.append(pd.DataFrame({
                        'Statistic': ['Mean', 'Max', 'Min'],
                        region: [f"{s['mean']:.0f}", f"{s['max']:.0f}", f"{s['min']:.0f}"],
                    }))

                if stats_list:
                    base_stats_df = stats_list[0]
                    for df in stats_list[1:]:
                        base_stats_df = base_stats_df.merge(df, on='Statistic', how='outer')
                    stat_order = ['Mean', 'Max', 'Min']
                    base_stats_df['Statistic'] = pd.Categorical(
                        base_stats_df['Statistic'], categories=stat_order, ordered=True,
                    )
                    base_stats_df = base_stats_df.sort_values('Statistic').reset_index(drop=True)
                    stats_title_pane.object = f"### Price Statistics ({date_range_text})"

                # ── Fuel-weighted prices ──
                try:
                    logger.info("Calculating fuel-weighted prices...")
                    import pickle
                    with open(GEN_INFO_FILE, 'rb') as f:
                        duid_mapping = pickle.load(f)
                    if 'Fuel' in duid_mapping.columns:
                        duid_mapping = duid_mapping.rename(columns={'Fuel': 'FUEL_TYPE', 'Region': 'REGIONID'})

                    gen_data = pd.DataFrame()
                    if hasattr(dashboard, 'generation_query_manager'):
                        gen_data = dashboard.generation_query_manager.query_generation_by_fuel(
                            start_datetime, end_datetime, selected_regions,
                        )
                    if gen_data.empty:
                        from ..shared.adapter_selector import load_generation_data
                        gen_data = load_generation_data(
                            start_date=start_datetime, end_date=end_datetime,
                            region=selected_regions[0], resolution='auto',
                        )

                    if not gen_data.empty and not original_price_data.empty:
                        fuel_prices = compute_fuel_weighted_prices(
                            gen_data, original_price_data, original_30min_price_data,
                            selected_regions, freq,
                        )
                        if fuel_prices and 'base_stats_df' in locals():
                            combined = build_combined_stats_table(
                                base_stats_df, fuel_prices, selected_regions,
                            )
                            stats_pane.value = combined
                            logger.info(f"Combined statistics:\n{combined}")
                        elif 'base_stats_df' in locals():
                            stats_pane.value = base_stats_df
                    else:
                        logger.warning("No generation data for selected period")
                        if 'base_stats_df' in locals():
                            stats_pane.value = base_stats_df
                except Exception as e:
                    logger.error(f"Error calculating fuel-weighted prices: {e}", exc_info=True)
                    if 'base_stats_df' in locals():
                        stats_pane.value = base_stats_df

                # ── Price bands ──
                _, bands_df = compute_price_bands(original_price_data, selected_regions)
                if bands_df is not None and not bands_df.empty:
                    butterfly_fig = build_band_charts(bands_df, date_range_text)

                    region_avg_demand = compute_region_avg_demand(
                        dashboard.query_manager, start_datetime, end_datetime, selected_regions,
                    )
                    detail_df = build_band_detail_table(
                        bands_df, original_price_data, selected_regions, region_avg_demand,
                    )
                    if detail_df is not None:
                        high_price_events_pane.value = detail_df
                    else:
                        high_price_events_pane.value = pd.DataFrame(
                            {'Info': ['No price band data in selected period']},
                        )

                    bands_plot_pane.clear()
                    bands_plot_pane.append(
                        pn.pane.Plotly(butterfly_fig, sizing_mode='stretch_width'),
                    )
                else:
                    bands_plot_pane.clear()
                    bands_plot_pane.append(pn.pane.Plotly(
                        _placeholder_fig('No price band data available\nPlease select regions and click "Analyze Prices"', height=400),
                        sizing_mode='stretch_width',
                    ))
                    high_price_events_pane.value = pd.DataFrame({'Info': ['No price band data available']})

                # ── Time-of-day chart ──
                tod_plot = build_tod_chart(
                    original_price_data, date_range_text,
                    dashboard._get_attribution_hook(), dashboard._get_flexoki_background_hook(),
                )
                tod_plot_pane.object = tod_plot

                mark_controls_clean()

            except Exception as e:
                logger.error(f"Error loading price data: {e}", exc_info=True)
                _show_error(str(e))

        def _show_empty(msg, is_error=False):
            color = FLEXOKI_ACCENT['red'] if is_error else FLEXOKI_BLACK
            price_plot_pane.object = _placeholder_fig(msg, color=color)
            stats_pane.value = pd.DataFrame({'Message': [msg]})
            high_price_events_pane.value = pd.DataFrame({'Message': [msg]})
            tod_plot_pane.object = _placeholder_fig(msg, color=color)

        def _show_error(msg):
            price_plot_pane.object = _placeholder_fig(f'Error: {msg}', color=FLEXOKI_ACCENT['red'])
            stats_pane.value = pd.DataFrame({'Error': [msg]})
            high_price_events_pane.value = pd.DataFrame({'Error': [msg]})
            tod_plot_pane.object = _placeholder_fig('Error loading data', color=FLEXOKI_ACCENT['red'])

        # ── Fuel relatives callback ───────────────────────────────────
        def update_fuel_relatives(event=None):
            try:
                selected_regions = region_selector.value
                if not selected_regions:
                    fuel_relatives_plot_pane.object = _placeholder_fig(
                        "Select at least one region to view fuel-weighted prices",
                    )
                    return

                # Use first selected region (fuel relatives is single-region)
                region = selected_regions[0]
                logger.info(f"Updating fuel relatives for region: {region}")

                fuel_relatives_plot_pane.object = _placeholder_fig(
                    f"Loading data for {region}...",
                    color=FLEXOKI_ACCENT['green'],
                )

                import sys
                from pathlib import Path
                sys.path.append(str(Path(__file__).parent.parent.parent))
                from data_service.shared_data_duckdb import duckdb_data_service

                daily_prices = query_fuel_relatives(duckdb_data_service.conn, region)

                if daily_prices.empty:
                    fuel_relatives_plot_pane.object = _placeholder_fig(f'No data available for {region}')
                    return

                smoothed = apply_loess_smoothing(daily_prices)

                fr_chart = build_fuel_relatives_chart(smoothed, region)
                if fr_chart is not None:
                    fuel_relatives_plot_pane.object = fr_chart
                else:
                    fuel_relatives_plot_pane.object = _placeholder_fig('Insufficient data for smoothing')
                    price_index_plot_pane.object = _placeholder_fig('Insufficient data for indexing')
                    return

                pi_chart = build_price_index_chart(smoothed, region)
                if pi_chart is not None:
                    price_index_plot_pane.object = pi_chart
                else:
                    price_index_plot_pane.object = _placeholder_fig('Flat Load data not available for indexing')

            except Exception as e:
                logger.error(f"Error in fuel relatives calculation: {e}", exc_info=True)
                fuel_relatives_plot_pane.object = _placeholder_fig(f'Error: {e}')
                price_index_plot_pane.object = _placeholder_fig(f'Error: {e}')

        # ── Wire up callbacks ─────────────────────────────────────────
        def analyze_all(event=None):
            """Run price analysis AND fuel relatives with one click."""
            load_and_plot_prices()
            update_fuel_relatives(event='analyze')

        date_presets.param.watch(update_date_range, 'value')
        start_date_picker.param.watch(update_date_display, 'value')
        end_date_picker.param.watch(update_date_display, 'value')
        analyze_button.on_click(analyze_all)

        # Mark dirty when any control changes
        for widget in [region_selector, aggregate_selector, smoothing_selector, log_scale_checkbox]:
            widget.param.watch(mark_controls_dirty, 'value')

        logger.info("Prices tab created successfully")
        return prices_tab

    except Exception as e:
        logger.error(f"Error creating prices tab: {e}", exc_info=True)
        return pn.pane.Markdown(f"**Error loading Prices tab:** {e}")


def _get_date_range_text(preset_value, start_val, end_val):
    """Convert the date preset selection to a human-readable string."""
    mapping = {
        '1d': "Last 24 hours",
        '7d': "Last 7 days",
        '30d': "Last 30 days",
        '90d': "Last 90 days",
        '1y': "Last year",
        'All': "All available data",
    }
    if preset_value in mapping:
        return mapping[preset_value]
    return f"{start_val.strftime('%Y-%m-%d')} to {end_val.strftime('%Y-%m-%d')}"


def _apply_smoothing_to_prices(price_data, smoothing_value, aggregate_value, selected_regions, y_col):
    """Apply user-selected smoothing to price time-series data.

    Reproduces the inline smoothing logic from the original
    ``load_and_plot_prices`` callback.
    """
    for region in selected_regions:
        region_mask = price_data['REGIONID'] == region

        if smoothing_value == 'Moving Avg (7 periods)':
            price_data.loc[region_mask, y_col] = (
                price_data.loc[region_mask, y_col].rolling(7, center=True).mean()
            )
        elif smoothing_value == 'Moving Avg (30 periods)':
            price_data.loc[region_mask, y_col] = (
                price_data.loc[region_mask, y_col].rolling(30, center=True).mean()
            )
        elif smoothing_value == 'Exponential (α=0.3)':
            price_data.loc[region_mask, y_col] = (
                price_data.loc[region_mask, y_col].ewm(alpha=0.3).mean()
            )
        elif smoothing_value.startswith('EWM'):
            # Parse EWM options
            if '7 days' in smoothing_value:
                span = 7
            elif '14 days' in smoothing_value:
                span = 14
            elif '30 days' in smoothing_value:
                span = 30
            elif '60 days' in smoothing_value:
                span = 60
            else:
                span = 14
            price_data.loc[region_mask, y_col] = (
                price_data.loc[region_mask, y_col].ewm(span=span).mean()
            )
        elif smoothing_value.startswith('Savitzky-Golay'):
            from scipy.signal import savgol_filter

            if '7 days' in smoothing_value:
                days = 7
            elif '30 days' in smoothing_value:
                days = 30
            elif '90 days' in smoothing_value:
                days = 90
            else:
                days = 7

            freq_label = aggregate_value
            window_size = 7
            poly_order = 3

            if freq_label == '5 min':
                periods_per_day = 288
            elif freq_label == '30 min':
                periods_per_day = 48
            elif freq_label == '1 hour':
                periods_per_day = 24
            elif freq_label == 'Daily':
                periods_per_day = 1
            elif freq_label == 'Monthly':
                window_size = min(days, 12)
                poly_order = 3
                periods_per_day = None
            elif freq_label == 'Quarterly':
                window_size = min(days // 30, 4)
                poly_order = 2
                periods_per_day = None
            elif freq_label == 'Yearly':
                window_size = 3
                poly_order = 2
                periods_per_day = None
            else:
                periods_per_day = 24

            if periods_per_day is not None:
                window_size = days * periods_per_day
                poly_order = 3
                if window_size % 2 == 0:
                    window_size += 1
                max_window = min(len(price_data.loc[region_mask]) // 2, 2001)
                window_size = min(window_size, max_window)

            try:
                region_prices = price_data.loc[region_mask, y_col].values
                if len(region_prices) >= window_size:
                    smoothed = savgol_filter(region_prices, window_size, poly_order, mode='nearest')
                    price_data.loc[region_mask, y_col] = smoothed
                else:
                    logger.warning(f"Not enough points ({len(region_prices)}) for SG window {window_size}")
            except Exception as e:
                logger.error(f"Savitzky-Golay error: {e}")

        elif smoothing_value.startswith('LOESS'):
            from statsmodels.nonparametric.smoothers_lowess import lowess

            if '3 hours' in smoothing_value:
                frac = 0.01
            elif '1 day' in smoothing_value:
                frac = 0.02
            elif '7 days' in smoothing_value:
                frac = 0.05
            elif '30 days' in smoothing_value:
                frac = 0.1
            elif '90 days' in smoothing_value:
                frac = 0.15
            else:
                frac = 0.1

            try:
                region_data = price_data.loc[region_mask].copy()
                region_prices = region_data[y_col].values
                valid_mask = ~np.isnan(region_prices)
                if valid_mask.sum() == 0:
                    continue
                valid_prices = region_prices[valid_mask]
                min_points = max(3, int(frac * len(valid_prices)) + 1)
                if len(valid_prices) >= min_points:
                    x = np.arange(len(valid_prices))
                    result = lowess(
                        valid_prices, x, frac=frac, it=0,
                        delta=0.01 * len(valid_prices) if len(valid_prices) > 100 else 0,
                    )
                    smoothed_full = np.full_like(region_prices, np.nan)
                    smoothed_full[valid_mask] = result[:, 1]
                    price_data.loc[region_mask, y_col] = smoothed_full
            except Exception as e:
                logger.error(f"LOESS error for {region}: {e}")

    return price_data
