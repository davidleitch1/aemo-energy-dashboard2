"""Prices tab — widget layout, callbacks, and subtab wiring.

Extracted from ``gen_dash.py._create_prices_tab()``.
Receives the ``EnergyDashboard`` instance as *dashboard* so it can
access ``query_manager``, hooks, and other shared state.
"""

import logging
from datetime import datetime, time

import holoviews as hv
import hvplot.pandas
import numpy as np
import pandas as pd
import panel as pn

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


def _text_background_hook(plot, element):
    """Set background for text/placeholder elements."""
    try:
        p = plot.state
        p.background_fill_color = FLEXOKI_PAPER
        p.border_fill_color = FLEXOKI_PAPER
        p.outline_line_color = FLEXOKI_BASE[150]
    except Exception:
        pass


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
        date_presets = pn.widgets.RadioBoxGroup(
            name='', options=['1 day', '7 days', '30 days', '90 days', '1 year', 'All data'],
            value='30 days', inline=False, width=100,
        )

        default_end = pd.Timestamp.now().date()
        default_start = default_end - pd.Timedelta(days=30)

        start_date_picker = pn.widgets.DatePicker(name='Start Date', value=default_start, width=120)
        end_date_picker = pn.widgets.DatePicker(name='End Date', value=default_end, width=120)

        date_display = pn.pane.Markdown(
            f"**Selected Period:** {default_start.strftime('%Y-%m-%d')} to {default_end.strftime('%Y-%m-%d')}",
            width=300,
        )

        regions = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']
        region_selector = pn.widgets.CheckBoxGroup(
            name='', value=['NSW1', 'VIC1'], options=regions,
            inline=False, align='start', margin=(0, 0, 0, 0),
        )

        aggregate_selector = pn.widgets.RadioBoxGroup(
            name='', value='30 min',
            options=['5 min', '30 min', '1 hour', 'Daily', 'Monthly', 'Quarterly', 'Yearly'],
            inline=False, width=120,
        )

        smoothing_selector = pn.widgets.Select(
            name='Smoothing', value='None',
            options=[
                'None',
                'LOESS (3 hours, frac=0.01)',
                'LOESS (1 day, frac=0.02)',
                'LOESS (7 days, frac=0.05)',
                'LOESS (30 days, frac=0.1)',
                'LOESS (90 days, frac=0.15)',
                'EWM (7 days, fast response)',
                'EWM (14 days, balanced)',
                'EWM (30 days, smooth)',
                'EWM (60 days, very smooth)',
            ],
            width=250,
        )

        log_scale_checkbox = pn.widgets.Checkbox(name='Log Scale Y-axis', value=False, width=150)

        analyze_button = pn.widgets.Button(name='Analyze Prices', button_type='primary', width=150)

        # ── Panes ──────────────────────────────────────────────────────
        price_plot_pane = pn.pane.HoloViews(height=400, sizing_mode='stretch_width')
        price_plot_pane.object = hv.Text(0.5, 0.5, "Click 'Analyze Prices' to load data").opts(
            xlim=(0, 1), ylim=(0, 1), bgcolor=FLEXOKI_PAPER, color=FLEXOKI_BLACK,
            fontsize=16, hooks=[_text_background_hook],
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

        bands_plot_pane = pn.Column(sizing_mode='stretch_width', height=550)
        _initial_bands = pn.pane.HoloViews(
            hv.Text(0.5, 0.5, "Price bands will appear here").opts(
                xlim=(0, 1), ylim=(0, 1), bgcolor=FLEXOKI_PAPER,
                color=FLEXOKI_BLACK, fontsize=14, hooks=[_text_background_hook],
            ),
            sizing_mode='stretch_width', height=550,
        )
        bands_plot_pane.clear()
        bands_plot_pane.append(_initial_bands)

        fuel_relatives_plot_pane = pn.pane.HoloViews(
            hv.Text(0.5, 0.5,
                     "Select a region and click Analyze to view fuel-weighted prices").opts(
                xlim=(0, 1), ylim=(0, 1), bgcolor=FLEXOKI_PAPER,
                color=FLEXOKI_BLACK, fontsize=14, hooks=[_text_background_hook],
            ),
            sizing_mode='stretch_both', height=400,
        )

        price_index_plot_pane = pn.pane.HoloViews(
            hv.Text(0.5, 0.5, "Price index will appear here after analysis").opts(
                xlim=(0, 1), ylim=(0, 1), bgcolor=FLEXOKI_PAPER,
                color=FLEXOKI_BLACK, fontsize=14, hooks=[_text_background_hook],
            ),
            sizing_mode='stretch_both', height=400,
        )

        high_price_events_pane = pn.widgets.Tabulator(
            pd.DataFrame(), show_index=False, sizing_mode='fixed',
            width=550, height=380, theme='fast',
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

        tod_plot_pane = pn.pane.HoloViews(height=400, sizing_mode='stretch_width')
        tod_plot_pane.object = hv.Text(0.5, 0.5, "Time of day analysis will appear here").opts(
            xlim=(0, 1), ylim=(0, 1), bgcolor=FLEXOKI_PAPER,
            color=FLEXOKI_BLACK, fontsize=14, hooks=[_text_background_hook],
        )

        # ── Controls layout ───────────────────────────────────────────
        region_group = pn.Column("### Region", region_selector, align='start', width=120)
        frequency_group = pn.Column("### Frequency", aggregate_selector, width=120)
        top_controls = pn.Row(region_group, pn.Spacer(width=10), frequency_group, align='start')

        date_controls = pn.Row(
            pn.Column("Start Date", start_date_picker, width=100),
            pn.Column("End Date", end_date_picker, width=100),
            pn.Column("Quick Select", date_presets, width=100),
            align='start',
        )

        controls_column = pn.Column(
            "## Price Analysis Controls", pn.Spacer(height=10),
            top_controls, pn.Spacer(height=15),
            "### Date Range", date_controls, date_display, pn.Spacer(height=15),
            "### Smoothing", smoothing_selector, pn.Spacer(height=10),
            log_scale_checkbox, pn.Spacer(height=15), analyze_button,
            width=350, margin=(0, 20, 0, 0), align='start',
        )

        # ── Subtab layouts ────────────────────────────────────────────
        price_analysis_content = pn.Column(
            pn.Row(
                pn.Column("## Price Statistics ($)", stats_pane, sizing_mode='stretch_width', width=550),
                pn.Spacer(width=20),
                pn.Column("## Time of Day Pattern", tod_plot_pane, sizing_mode='stretch_width', width=400),
                sizing_mode='stretch_width', height=600,
            ),
            pn.Spacer(height=20),
            pn.Column("## Price Time Series", price_plot_pane, sizing_mode='stretch_both', width_policy='max'),
            sizing_mode='stretch_both',
        )

        price_bands_content = pn.Row(
            pn.Column("## Price Band Details", high_price_events_pane, sizing_mode='stretch_both', width=500),
            pn.Spacer(width=20),
            pn.Column("## Price Band Distribution", bands_plot_pane, sizing_mode='stretch_width', height=600),
            sizing_mode='stretch_both',
        )

        fuel_relatives_region_selector = pn.widgets.RadioButtonGroup(
            name='Region', value='NSW1', options=['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1'],
            button_type='primary', button_style='outline',
        )

        fuel_relatives_content = pn.Column(
            pn.Row(pn.Column("### Select Region", fuel_relatives_region_selector, width=150)),
            pn.Spacer(height=10),
            pn.Column(
                "## Fuel-Weighted vs Flat Load Prices (90-day LOESS smoothed)",
                fuel_relatives_plot_pane,
                pn.Spacer(height=20),
                "## Price Index (Flat Load = 100)",
                price_index_plot_pane,
                sizing_mode='stretch_both',
            ),
            sizing_mode='stretch_both',
        )

        price_subtabs = pn.Tabs(
            ('Price Analysis', price_analysis_content),
            ('Price Bands', price_bands_content),
            ('Fuel Relatives', fuel_relatives_content),
            sizing_mode='stretch_both',
        )

        prices_tab = pn.Row(controls_column, price_subtabs, sizing_mode='stretch_both')

        # ── Date callbacks ────────────────────────────────────────────
        def update_date_range(event):
            preset = event.new
            current_end = end_date_picker.value
            if preset == '1 day':
                new_start = current_end - pd.Timedelta(days=1)
            elif preset == '7 days':
                new_start = current_end - pd.Timedelta(days=7)
            elif preset == '30 days':
                new_start = current_end - pd.Timedelta(days=30)
            elif preset == '90 days':
                new_start = current_end - pd.Timedelta(days=90)
            elif preset == '1 year':
                new_start = current_end - pd.Timedelta(days=365)
            else:
                new_start = pd.Timestamp('2020-01-01').date()
            start_date_picker.value = new_start

        def update_date_display(event):
            date_display.object = (
                f"**Selected Period:** {start_date_picker.value.strftime('%Y-%m-%d')} "
                f"to {end_date_picker.value.strftime('%Y-%m-%d')}"
            )
            current_end = end_date_picker.value
            current_start = start_date_picker.value
            days_diff = (current_end - current_start).days
            matches = False
            if date_presets.value == '1 day' and days_diff == 1:
                matches = True
            elif date_presets.value == '7 days' and days_diff == 7:
                matches = True
            elif date_presets.value == '30 days' and days_diff == 30:
                matches = True
            elif date_presets.value == '90 days' and days_diff == 90:
                matches = True
            elif date_presets.value == '1 year' and days_diff == 365:
                matches = True
            elif date_presets.value == 'All data' and current_start == pd.Timestamp('2020-01-01').date():
                matches = True
            if not matches:
                date_presets.value = None

        # ── Main analysis callback ────────────────────────────────────
        def load_and_plot_prices(event=None):
            try:
                price_plot_pane.object = hv.Text(0.5, 0.5, 'Loading price data...').opts(
                    xlim=(0, 1), ylim=(0, 1), bgcolor=FLEXOKI_PAPER, color=FLEXOKI_BLACK, fontsize=14,
                )

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
                freq = freq_map.get(aggregate_selector.value, '30min')
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
                if smoothing_selector.value != 'None':
                    price_data = _apply_smoothing_to_prices(
                        price_data, smoothing_selector.value,
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
                    contrib_plot, time_plot = build_band_charts(
                        bands_df, date_range_text, dashboard._get_flexoki_background_hook(),
                    )
                    contrib_plot = contrib_plot.opts(
                        padding=(0.1, 0.1),
                        hooks=[dashboard._get_attribution_hook(), dashboard._get_flexoki_background_hook()],
                    )
                    time_plot = time_plot.opts(
                        padding=(0.1, 0.1),
                        hooks=[dashboard._get_attribution_hook(), dashboard._get_flexoki_background_hook()],
                    )

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
                        pn.pane.HoloViews(contrib_plot, sizing_mode='stretch_width', height=275),
                    )
                    bands_plot_pane.append(
                        pn.pane.HoloViews(time_plot, sizing_mode='stretch_width', height=275),
                    )
                else:
                    bands_plot_pane.clear()
                    bands_plot_pane.append(pn.pane.HoloViews(
                        hv.Text(0.5, 0.5,
                                'No price band data available\nPlease select regions and click "Analyze Prices"').opts(
                            xlim=(0, 1), ylim=(0, 1), bgcolor=FLEXOKI_PAPER,
                            color=FLEXOKI_BLACK, fontsize=14,
                        ),
                        sizing_mode='stretch_width', height=550,
                    ))
                    high_price_events_pane.value = pd.DataFrame({'Info': ['No price band data available']})

                # ── Time-of-day chart ──
                tod_plot = build_tod_chart(
                    original_price_data, date_range_text,
                    dashboard._get_attribution_hook(), dashboard._get_flexoki_background_hook(),
                )
                tod_plot_pane.object = tod_plot

            except Exception as e:
                logger.error(f"Error loading price data: {e}", exc_info=True)
                _show_error(str(e))

        def _show_empty(msg, is_error=False):
            color = FLEXOKI_ACCENT['red'] if is_error else FLEXOKI_BLACK
            opts = dict(xlim=(0, 1), ylim=(0, 1), bgcolor=FLEXOKI_PAPER, color=color, fontsize=14)
            price_plot_pane.object = hv.Text(0.5, 0.5, msg).opts(**opts)
            stats_pane.value = pd.DataFrame({'Message': [msg]})
            high_price_events_pane.value = pd.DataFrame({'Message': [msg]})
            tod_plot_pane.object = hv.Text(0.5, 0.5, msg).opts(**opts)

        def _show_error(msg):
            opts = dict(xlim=(0, 1), ylim=(0, 1), bgcolor=FLEXOKI_PAPER, color=FLEXOKI_ACCENT['red'], fontsize=14)
            price_plot_pane.object = hv.Text(0.5, 0.5, f'Error: {msg}').opts(**opts)
            stats_pane.value = pd.DataFrame({'Error': [msg]})
            high_price_events_pane.value = pd.DataFrame({'Error': [msg]})
            tod_plot_pane.object = hv.Text(0.5, 0.5, 'Error loading data').opts(**opts)

        # ── Fuel relatives callback ───────────────────────────────────
        def update_fuel_relatives(event=None):
            try:
                if event is None:
                    fuel_relatives_plot_pane.object = hv.Text(
                        0.5, 0.5,
                        "Select a region to view 90-day LOESS smoothed fuel-weighted prices\n"
                        "(Uses all available data ~5.5 years, excludes biomass, includes battery discharge)",
                    ).opts(xlim=(0, 1), ylim=(0, 1), bgcolor=FLEXOKI_PAPER, color=FLEXOKI_BLACK, fontsize=14)
                    return

                region = fuel_relatives_region_selector.value
                logger.info(f"Updating fuel relatives for region: {region}")

                fuel_relatives_plot_pane.object = hv.Text(
                    0.5, 0.5,
                    f"Loading 5.5 years of data for {region}...\nThis may take 20-30 seconds",
                ).opts(xlim=(0, 1), ylim=(0, 1), bgcolor=FLEXOKI_PAPER,
                       color=FLEXOKI_ACCENT['green'], fontsize=14)

                import sys
                from pathlib import Path
                sys.path.append(str(Path(__file__).parent.parent.parent))
                from data_service.shared_data_duckdb import duckdb_data_service

                daily_prices = query_fuel_relatives(duckdb_data_service.conn, region)

                if daily_prices.empty:
                    fuel_relatives_plot_pane.object = hv.Text(
                        0.5, 0.5, f'No data available for {region}',
                    ).opts(xlim=(0, 1), ylim=(0, 1), bgcolor=FLEXOKI_PAPER, color=FLEXOKI_BLACK, fontsize=14)
                    return

                smoothed = apply_loess_smoothing(daily_prices)

                fr_chart = build_fuel_relatives_chart(
                    smoothed, region,
                    dashboard._get_attribution_hook(), dashboard._get_flexoki_background_hook(),
                )
                if fr_chart is not None:
                    fuel_relatives_plot_pane.object = fr_chart
                else:
                    fuel_relatives_plot_pane.object = hv.Text(
                        0.5, 0.5, 'Insufficient data for smoothing',
                    ).opts(xlim=(0, 1), ylim=(0, 1), bgcolor=FLEXOKI_PAPER, color=FLEXOKI_BLACK, fontsize=14)
                    price_index_plot_pane.object = hv.Text(
                        0.5, 0.5, 'Insufficient data for indexing',
                    ).opts(xlim=(0, 1), ylim=(0, 1), bgcolor=FLEXOKI_PAPER, color=FLEXOKI_BLACK, fontsize=14)
                    return

                pi_chart = build_price_index_chart(
                    smoothed, region,
                    dashboard._get_attribution_hook(), dashboard._get_flexoki_background_hook(),
                )
                if pi_chart is not None:
                    price_index_plot_pane.object = pi_chart
                else:
                    price_index_plot_pane.object = hv.Text(
                        0.5, 0.5, 'Flat Load data not available for indexing',
                    ).opts(xlim=(0, 1), ylim=(0, 1), bgcolor=FLEXOKI_PAPER, color=FLEXOKI_BLACK, fontsize=14)

            except Exception as e:
                logger.error(f"Error in fuel relatives calculation: {e}", exc_info=True)
                opts = dict(xlim=(0, 1), ylim=(0, 1), bgcolor=FLEXOKI_PAPER, color=FLEXOKI_BLACK, fontsize=14)
                fuel_relatives_plot_pane.object = hv.Text(0.5, 0.5, f'Error: {e}').opts(**opts)
                price_index_plot_pane.object = hv.Text(0.5, 0.5, f'Error: {e}').opts(**opts)

        # ── Wire up callbacks ─────────────────────────────────────────
        date_presets.param.watch(update_date_range, 'value')
        start_date_picker.param.watch(update_date_display, 'value')
        end_date_picker.param.watch(update_date_display, 'value')
        analyze_button.on_click(lambda event: load_and_plot_prices())
        fuel_relatives_region_selector.param.watch(update_fuel_relatives, 'value')

        logger.info("Prices tab created successfully")
        return prices_tab

    except Exception as e:
        logger.error(f"Error creating prices tab: {e}", exc_info=True)
        return pn.pane.Markdown(f"**Error loading Prices tab:** {e}")


def _get_date_range_text(preset_value, start_val, end_val):
    """Convert the date preset selection to a human-readable string."""
    mapping = {
        '1 day': "Last 24 hours",
        '7 days': "Last 7 days",
        '30 days': "Last 30 days",
        '90 days': "Last 90 days",
        '1 year': "Last year",
        'All data': "All available data",
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
