#!/usr/bin/env python3
"""
Curtailment Tab for AEMO Energy Dashboard
Regional and DUID-level curtailment analysis using UIGF-based data
"""

import param
import panel as pn
import pandas as pd
import hvplot.pandas
from datetime import datetime, timedelta, date
from typing import Optional, Tuple
import logging

from .curtailment_query_manager import CurtailmentQueryManager

logger = logging.getLogger(__name__)

# NEM Regions
NEM_REGIONS = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']


class CurtailmentTab(param.Parameterized):
    """Regional curtailment analysis tab for dashboard"""

    # Time range selector matching Prices tab pattern
    time_range = param.Selector(
        default='7',
        objects=['1', '7', '30', '90', '365', 'All'],
        doc="Select time range to display"
    )

    start_date = param.Date(
        default=datetime.now().date() - timedelta(days=7),
        doc="Start date for custom range"
    )

    end_date = param.Date(
        default=datetime.now().date(),
        doc="End date for custom range"
    )

    def __init__(self, **params):
        """Initialize curtailment tab with query manager"""
        super().__init__(**params)
        self.query_manager = CurtailmentQueryManager()

        # Get data coverage for date range
        stats = self.query_manager.get_statistics()
        self.data_coverage = stats.get('data_coverage', {})

        # Create widgets
        self._create_widgets()

        # Initialize date range from preset
        self._update_date_range_from_preset()

    def _create_widgets(self):
        """Create all widgets"""
        # View mode selector
        self.mode_selector = pn.widgets.RadioButtonGroup(
            name='View Mode',
            value='By Region',
            options=['By Region', 'By Fuel Type', 'NEM Total']
        )

        # Region selector
        self.region_selector = pn.widgets.Select(
            name='Region',
            value='All',
            options=['All'] + NEM_REGIONS
        )

        # Fuel selector
        self.fuel_selector = pn.widgets.Select(
            name='Fuel Type',
            value='All',
            options=['All', 'Solar', 'Wind'],
            visible=False
        )

        # Curtailment type selector for DUID analysis
        self.curtailment_type_selector = pn.widgets.RadioButtonGroup(
            name='Curtailment Type',
            value='Both',
            options=['Both', 'Economic', 'Grid'],
            button_type='default'
        )

        # Time range widget (RadioBoxGroup matching Prices tab)
        self.time_range_widget = pn.widgets.RadioBoxGroup(
            name="",
            value=self.time_range,
            options=['1', '7', '30', '90', '365', 'All'],
            inline=True,
            width=350
        )
        self.time_range_widget.link(self, value='time_range')

        # Custom date pickers
        self.start_date_picker = pn.widgets.DatePicker(
            name='Start Date',
            value=self.start_date,
            width=150
        )
        self.start_date_picker.link(self, value='start_date')

        self.end_date_picker = pn.widgets.DatePicker(
            name='End Date',
            value=self.end_date,
            width=150
        )
        self.end_date_picker.link(self, value='end_date')

        # Aggregation
        self.aggregation_selector = pn.widgets.Select(
            name='Aggregation',
            value='hourly',
            options=['5min', '30min', 'hourly', 'daily']
        )

        # Wire up mode selector
        self.mode_selector.param.watch(self._update_selectors, 'value')

    def _update_selectors(self, event):
        """Update visibility of selectors based on mode"""
        if self.mode_selector.value == 'By Region':
            self.region_selector.visible = True
            self.fuel_selector.visible = False
        elif self.mode_selector.value == 'By Fuel Type':
            self.region_selector.visible = True
            self.fuel_selector.visible = True
        else:  # NEM Total
            self.region_selector.visible = False
            self.fuel_selector.visible = False

    @param.depends('time_range', watch=True)
    def on_time_range_change(self):
        """Called when time range parameter changes"""
        logger.info(f"Time range changed to: {self.time_range}")
        self._update_date_range_from_preset()

    def _update_date_range_from_preset(self):
        """Update start_date and end_date based on time_range preset"""
        end_date = datetime.now().date()

        if self.time_range == '1':
            start_date = end_date - timedelta(days=1)
        elif self.time_range == '7':
            start_date = end_date - timedelta(days=7)
        elif self.time_range == '30':
            start_date = end_date - timedelta(days=30)
        elif self.time_range == '90':
            start_date = end_date - timedelta(days=90)
        elif self.time_range == '365':
            start_date = end_date - timedelta(days=365)
        elif self.time_range == 'All':
            # Use data coverage if available, otherwise default
            if self.data_coverage:
                earliest = self.data_coverage.get('earliest')
                if earliest:
                    start_date = earliest.date() if hasattr(earliest, 'date') else earliest
                else:
                    start_date = date(2025, 7, 1)
            else:
                start_date = date(2025, 7, 1)
        else:
            # Keep current custom dates
            return

        # Update the date parameters
        self.start_date = start_date
        self.end_date = end_date

        # Update picker widgets
        if hasattr(self, 'start_date_picker'):
            self.start_date_picker.value = start_date
        if hasattr(self, 'end_date_picker'):
            self.end_date_picker.value = end_date

    def _get_effective_date_range(self) -> Tuple[datetime, datetime]:
        """Get the effective start and end datetime for data filtering"""
        if self.time_range == 'All':
            # For all data, use earliest available
            if self.data_coverage:
                earliest = self.data_coverage.get('earliest')
                if earliest:
                    start_datetime = earliest if isinstance(earliest, datetime) else datetime.combine(earliest, datetime.min.time())
                else:
                    start_datetime = datetime(2025, 7, 1)
            else:
                start_datetime = datetime(2025, 7, 1)
            end_datetime = datetime.now()
            return start_datetime, end_datetime
        else:
            # Convert dates to datetime for filtering
            start_datetime = datetime.combine(self.start_date, datetime.min.time())

            # Cap end_datetime at current time if end_date is today or future
            now = datetime.now()
            end_date_midnight = datetime.combine(self.end_date, datetime.max.time())

            if end_date_midnight >= now:
                end_datetime = now
            else:
                end_datetime = end_date_midnight

            return start_datetime, end_datetime

    def _get_time_range_display(self) -> str:
        """Get formatted time range string for chart titles"""
        if self.time_range == '1':
            return "Last 24 Hours"
        elif self.time_range == '7':
            return "Last 7 Days"
        elif self.time_range == '30':
            return "Last 30 Days"
        elif self.time_range == '90':
            return "Last 90 Days"
        elif self.time_range == '365':
            return "Last 365 Days"
        elif self.time_range == 'All':
            return "All Available Data"
        else:
            if self.start_date and self.end_date:
                if self.start_date == self.end_date:
                    return f"{self.start_date.strftime('%Y-%m-%d')}"
                else:
                    return f"{self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')}"
            return "Custom Range"

    def create_plot(self, mode, region, fuel, aggregation, time_range):
        """Create curtailment visualization"""
        try:
            # Get effective date range
            start_dt, end_dt = self._get_effective_date_range()

            # Determine filters
            region_filter = None if region == 'All' or mode == 'NEM Total' else region
            fuel_filter = None if fuel == 'All' else fuel

            # Query data
            data = self.query_manager.query_curtailment_data(
                start_date=start_dt,
                end_date=end_dt,
                region=region_filter,
                fuel=fuel_filter,
                resolution=aggregation
            )

            if data.empty:
                return pn.pane.Markdown("No data available for selected filters")

            # Aggregate for NEM Total if needed
            if mode == 'NEM Total':
                data = data.groupby('timestamp').agg({
                    'solar_curtailment': 'sum',
                    'wind_curtailment': 'sum',
                    'total_curtailment': 'sum',
                    'solar_cleared': 'sum',
                    'wind_cleared': 'sum',
                    'solar_uigf': 'sum',
                    'wind_uigf': 'sum'
                }).reset_index()

            # Create title with time range
            time_range_str = self._get_time_range_display()
            title_parts = [f"Curtailment Analysis - {time_range_str}"]
            if mode == 'By Region' and region != 'All':
                title_parts.append(f"Region: {region}")
            elif mode == 'By Fuel Type':
                if fuel != 'All':
                    title_parts.append(f"{fuel}")
                if region != 'All':
                    title_parts.append(f"in {region}")
            elif mode == 'NEM Total':
                title_parts.append("NEM-Wide Total")

            title = " - ".join(title_parts)

            # Determine which columns to plot based on fuel filter
            if fuel_filter == 'Solar':
                # Single fuel - show generation and curtailment
                plot = data.hvplot.area(
                    x='timestamp',
                    y='cleared',
                    label='Solar Generation',
                    color='#ffb86c',  # Orange
                    alpha=0.7,
                    height=450,
                    width=1000,
                    title=title,
                    ylabel='Power (MW)',
                    legend='top_left'
                ).opts(bgcolor='#282a36', show_grid=True)

                plot = plot * data.hvplot.area(
                    x='timestamp',
                    y='curtailment',
                    label='Solar Curtailment',
                    color='#ff5555',  # Red
                    alpha=0.5
                )

                plot = plot * data.hvplot.line(
                    x='timestamp',
                    y='uigf',
                    label='Solar UIGF (Potential)',
                    color='#8be9fd',  # Cyan
                    line_dash='dashed',
                    line_width=2
                )

            elif fuel_filter == 'Wind':
                # Single fuel - show generation and curtailment
                plot = data.hvplot.area(
                    x='timestamp',
                    y='cleared',
                    label='Wind Generation',
                    color='#50fa7b',  # Green
                    alpha=0.7,
                    height=450,
                    width=1000,
                    title=title,
                    ylabel='Power (MW)',
                    legend='top_left'
                ).opts(bgcolor='#282a36', show_grid=True)

                plot = plot * data.hvplot.area(
                    x='timestamp',
                    y='curtailment',
                    label='Wind Curtailment',
                    color='#ff5555',  # Red
                    alpha=0.5
                )

                plot = plot * data.hvplot.line(
                    x='timestamp',
                    y='uigf',
                    label='Wind UIGF (Potential)',
                    color='#8be9fd',  # Cyan
                    line_dash='dashed',
                    line_width=2
                )

            else:
                # Both fuels - show stacked curtailment
                plot = data.hvplot.area(
                    x='timestamp',
                    y='solar_cleared',
                    label='Solar Generation',
                    color='#ffb86c',  # Orange
                    alpha=0.7,
                    height=450,
                    width=1000,
                    title=title,
                    ylabel='Power (MW)',
                    legend='top_left'
                ).opts(bgcolor='#282a36', show_grid=True)

                plot = plot * data.hvplot.area(
                    x='timestamp',
                    y='wind_cleared',
                    label='Wind Generation',
                    color='#50fa7b',  # Green
                    alpha=0.7
                )

                plot = plot * data.hvplot.area(
                    x='timestamp',
                    y='solar_curtailment',
                    label='Solar Curtailment',
                    color='#ff5555',  # Red
                    alpha=0.4
                )

                plot = plot * data.hvplot.area(
                    x='timestamp',
                    y='wind_curtailment',
                    label='Wind Curtailment',
                    color='#ff79c6',  # Pink
                    alpha=0.4
                )

            return plot

        except Exception as e:
            import traceback
            return pn.pane.Markdown(f"Error: {str(e)}\n\n{traceback.format_exc()}")

    def create_stats(self, mode, region, fuel, time_range):
        """Create statistics panel"""
        try:
            # Get effective date range
            start_dt, end_dt = self._get_effective_date_range()

            # Get summary based on mode
            if mode == 'By Fuel Type' or fuel != 'All':
                region_filter = None if region == 'All' else region
                summary = self.query_manager.query_fuel_summary(start_dt, end_dt, region_filter)

                if summary.empty:
                    return pn.pane.Markdown("No statistics available")

                if fuel != 'All':
                    summary = summary[summary['fuel'] == fuel]

                total_curtailment = summary['curtailment_mwh'].sum()
                total_generation = summary['generation_mwh'].sum()
                avg_rate = (total_curtailment / (total_curtailment + total_generation) * 100) if total_generation > 0 else 0
                max_curtailment = summary['max_curtailment_mw'].max()

            else:
                summary = self.query_manager.query_region_summary(start_dt, end_dt)

                if summary.empty:
                    return pn.pane.Markdown("No statistics available")

                if region != 'All':
                    summary = summary[summary['region'] == region]

                total_curtailment = summary['total_curtailment_mwh'].sum()
                total_generation = summary['total_generation_mwh'].sum()
                avg_rate = summary['total_curtailment_rate_pct'].mean()
                max_curtailment = summary['max_total_curtailment_mw'].max()

            # Build filter description
            filter_desc = []
            if mode == 'By Region' and region != 'All':
                filter_desc.append(f"Region: {region}")
            elif mode == 'By Fuel Type':
                if fuel != 'All':
                    filter_desc.append(f"Fuel: {fuel}")
                if region != 'All':
                    filter_desc.append(f"Region: {region}")
            elif mode == 'NEM Total':
                filter_desc.append("NEM-Wide")

            filter_text = ", ".join(filter_desc) if filter_desc else "All Data"

            # Convert to GWh/TWh for display
            if total_curtailment > 1000000:
                curtailment_str = f"{total_curtailment/1000000:.2f} TWh"
            elif total_curtailment > 1000:
                curtailment_str = f"{total_curtailment/1000:.1f} GWh"
            else:
                curtailment_str = f"{total_curtailment:,.0f} MWh"

            if total_generation > 1000000:
                generation_str = f"{total_generation/1000000:.2f} TWh"
            elif total_generation > 1000:
                generation_str = f"{total_generation/1000:.1f} GWh"
            else:
                generation_str = f"{total_generation:,.0f} MWh"

            time_range_str = self._get_time_range_display()

            stats_html = f"""
            <h3>Summary Statistics</h3>
            <table style='width:100%; font-size:13px; color:#f8f8f2;'>
            <tr><td colspan='2'><b>Period: {time_range_str}</b></td></tr>
            <tr><td colspan='2'><b>{filter_text}</b></td></tr>
            <tr><td colspan='2'><hr></td></tr>
            <tr><td><b>Curtailment Rate:</b></td><td>{avg_rate:.1f}%</td></tr>
            <tr><td><b>Total Curtailed:</b></td><td>{curtailment_str}</td></tr>
            <tr><td><b>Total Generation:</b></td><td>{generation_str}</td></tr>
            <tr><td><b>Max Curtailment:</b></td><td>{max_curtailment:.0f} MW</td></tr>
            </table>
            """

            return pn.pane.HTML(stats_html, width=320)

        except Exception as e:
            return pn.pane.Markdown(f"Error: {str(e)}")

    def create_region_table(self, time_range):
        """Create regional comparison table"""
        try:
            start_dt, end_dt = self._get_effective_date_range()

            summary = self.query_manager.query_region_summary(start_dt, end_dt)

            if summary.empty:
                return pn.pane.Markdown("No regional data available")

            # Format for display
            display_df = summary[[
                'region',
                'solar_curtailment_mwh',
                'wind_curtailment_mwh',
                'total_curtailment_mwh',
                'total_curtailment_rate_pct'
            ]].copy()

            display_df.columns = ['Region', 'Solar (MWh)', 'Wind (MWh)', 'Total (MWh)', 'Rate %']
            display_df['Solar (MWh)'] = display_df['Solar (MWh)'].round(0)
            display_df['Wind (MWh)'] = display_df['Wind (MWh)'].round(0)
            display_df['Total (MWh)'] = display_df['Total (MWh)'].round(0)
            display_df['Rate %'] = display_df['Rate %'].round(1)

            return pn.widgets.Tabulator(
                display_df,
                show_index=False,
                height=200,
                width=600
            )

        except Exception as e:
            return pn.pane.Markdown(f"Error: {str(e)}")

    def create_fuel_table(self, region, time_range):
        """Create fuel type comparison table"""
        try:
            start_dt, end_dt = self._get_effective_date_range()

            region_filter = None if region == 'All' else region
            summary = self.query_manager.query_fuel_summary(start_dt, end_dt, region_filter)

            if summary.empty:
                return pn.pane.Markdown("No fuel data available")

            # Format for display
            display_df = summary[[
                'fuel',
                'curtailment_mwh',
                'generation_mwh',
                'potential_mwh',
                'curtailment_rate_pct',
                'max_curtailment_mw'
            ]].copy()

            display_df.columns = ['Fuel', 'Curtailed (MWh)', 'Generated (MWh)', 'Potential (MWh)', 'Rate %', 'Max (MW)']
            display_df['Curtailed (MWh)'] = display_df['Curtailed (MWh)'].round(0)
            display_df['Generated (MWh)'] = display_df['Generated (MWh)'].round(0)
            display_df['Potential (MWh)'] = display_df['Potential (MWh)'].round(0)
            display_df['Rate %'] = display_df['Rate %'].round(1)
            display_df['Max (MW)'] = display_df['Max (MW)'].round(0)

            return pn.widgets.Tabulator(
                display_df,
                show_index=False,
                height=150,
                width=800
            )

        except Exception as e:
            return pn.pane.Markdown(f"Error: {str(e)}")

    def create_duid_lollipop(self, time_range, region, curtailment_type, top_n=20):
        """Create horizontal bar chart showing top N curtailed DUIDs with economic/grid split"""
        try:
            start_dt, end_dt = self._get_effective_date_range()
            time_range_str = self._get_time_range_display()
            region_filter = None if region == 'All' else region

            if curtailment_type == 'Both':
                # Query data split by type for side-by-side bars
                type_data = self.query_manager.query_top_duids_by_type(
                    start_dt, end_dt, top_n=top_n, region=region_filter
                )

                if type_data.empty:
                    region_msg = f" in {region}" if region and region != 'All' else ""
                    return pn.pane.Markdown(f"No DUID curtailment data available{region_msg} for selected period")

                # Convert to GWh if large values
                max_val = type_data['curtailment_mwh'].max()
                if max_val > 10000:
                    type_data['Curtailment'] = type_data['curtailment_mwh'] / 1000
                    unit = 'GWh'
                else:
                    type_data['Curtailment'] = type_data['curtailment_mwh']
                    unit = 'MWh'

                # Build title
                region_suffix = f" - {region}" if region and region != 'All' else ""
                n_duids = type_data['duid'].nunique()
                title = f'Top {n_duids} Curtailed DUIDs{region_suffix} - {time_range_str}'

                # Create grouped bar chart
                # Sort DUIDs by total curtailment (descending)
                duid_order = type_data.groupby('duid')['Curtailment'].sum().sort_values(ascending=True).index.tolist()

                bars = type_data.hvplot.barh(
                    x='duid',
                    y='Curtailment',
                    by='curtailment_type',
                    color=['#ff5555', '#50fa7b'],  # Red for Economic, Green for Grid
                    alpha=0.8,
                    height=max(500, n_duids * 32),
                    width=950,
                    title=title,
                    xlabel=f'Total Curtailment ({unit})',
                    ylabel='',
                    legend='top_right',
                    stacked=False
                ).opts(
                    bgcolor='#282a36',
                    show_grid=True,
                    gridstyle={'grid_line_color': '#44475a', 'grid_line_alpha': 0.5},
                    invert_yaxis=False
                )

                # Add legend description
                legend_html = pn.pane.HTML(
                    """<div style='font-size: 11px; color: #aaa; margin-bottom: 10px;'>
                    <span style='color: #ff5555;'>■ Economic</span> = Curtailed when price &lt; $0 (voluntary)
                    &nbsp;|&nbsp;
                    <span style='color: #50fa7b;'>■ Grid</span> = Curtailed when price ≥ $0 (constraint-based)
                    </div>"""
                )

                return pn.Column(legend_html, bars)

            else:
                # Query filtered by specific type
                curt_type_filter = curtailment_type.lower()  # 'economic' or 'grid'
                top_duids = self.query_manager.query_top_duids(
                    start_dt, end_dt, top_n=top_n, region=region_filter, curtailment_type=curt_type_filter
                )

                if top_duids.empty:
                    region_msg = f" in {region}" if region and region != 'All' else ""
                    return pn.pane.Markdown(f"No {curtailment_type} curtailment data available{region_msg} for selected period")

                # Prepare data
                plot_df = top_duids.copy()
                plot_df = plot_df.sort_values('curtailment_mwh', ascending=False).reset_index(drop=True)

                # Convert to GWh if large values
                max_val = plot_df['curtailment_mwh'].max()
                if max_val > 10000:
                    plot_df['Curtailment'] = plot_df['curtailment_mwh'] / 1000
                    unit = 'GWh'
                else:
                    plot_df['Curtailment'] = plot_df['curtailment_mwh']
                    unit = 'MWh'

                # Rename columns for better display
                plot_df['Rate %'] = plot_df['curtailment_rate_pct'].round(1)
                plot_df['Max MW'] = plot_df['max_curtailment_mw'].round(0)

                # Build title
                region_suffix = f" - {region}" if region and region != 'All' else ""
                type_suffix = f" ({curtailment_type})"
                title = f'Top {len(plot_df)} Curtailed DUIDs{region_suffix}{type_suffix} - {time_range_str}'

                # Color based on type
                bar_color = '#ff5555' if curtailment_type == 'Economic' else '#50fa7b'

                # Create horizontal bar chart
                bars = plot_df.hvplot.bar(
                    x='duid',
                    y='Curtailment',
                    color=bar_color,
                    alpha=0.8,
                    height=max(450, len(plot_df) * 24),
                    width=950,
                    title=title,
                    ylabel=f'Total Curtailment ({unit})',
                    xlabel='',
                    invert=True,
                    hover_cols=['Rate %', 'Max MW']
                ).opts(
                    xrotation=0,
                    bgcolor='#282a36',
                    show_grid=True,
                    gridstyle={'grid_line_color': '#44475a', 'grid_line_alpha': 0.5}
                )

                return bars

        except Exception as e:
            import traceback
            logger.error(f"Error creating DUID bar chart: {e}\n{traceback.format_exc()}")
            return pn.pane.Markdown(f"Error creating DUID chart: {str(e)}")

    def create_duid_table(self, time_range, region, curtailment_type, top_n=20):
        """Create table showing top curtailed DUIDs with economic/grid breakdown"""
        try:
            start_dt, end_dt = self._get_effective_date_range()
            region_filter = None if region == 'All' else region

            if curtailment_type == 'Both':
                # Get data split by type
                type_data = self.query_manager.query_top_duids_by_type(
                    start_dt, end_dt, top_n=top_n, region=region_filter
                )

                if type_data.empty:
                    return pn.pane.Markdown("No DUID curtailment data available")

                # Pivot to get economic and grid columns
                pivot = type_data.pivot_table(
                    index='duid',
                    columns='curtailment_type',
                    values='curtailment_mwh',
                    fill_value=0
                ).reset_index()

                # Ensure both columns exist
                if 'Economic' not in pivot.columns:
                    pivot['Economic'] = 0
                if 'Grid' not in pivot.columns:
                    pivot['Grid'] = 0

                pivot['Total'] = pivot['Economic'] + pivot['Grid']
                pivot = pivot.sort_values('Total', ascending=False)

                # Format for display
                display_df = pivot[['duid', 'Economic', 'Grid', 'Total']].copy()
                display_df.columns = ['DUID', 'Economic (MWh)', 'Grid (MWh)', 'Total (MWh)']
                display_df['Economic (MWh)'] = display_df['Economic (MWh)'].round(0)
                display_df['Grid (MWh)'] = display_df['Grid (MWh)'].round(0)
                display_df['Total (MWh)'] = display_df['Total (MWh)'].round(0)

                # Calculate percentages
                display_df['Economic %'] = (display_df['Economic (MWh)'] / display_df['Total (MWh)'] * 100).round(1)
                display_df['Grid %'] = (display_df['Grid (MWh)'] / display_df['Total (MWh)'] * 100).round(1)

                return pn.widgets.Tabulator(
                    display_df,
                    show_index=False,
                    height=400,
                    width=900,
                    frozen_columns=['DUID']
                )

            else:
                # Filter by specific type
                curt_type_filter = curtailment_type.lower()
                top_duids = self.query_manager.query_top_duids(
                    start_dt, end_dt, top_n=top_n, region=region_filter, curtailment_type=curt_type_filter
                )

                if top_duids.empty:
                    return pn.pane.Markdown(f"No {curtailment_type} curtailment data available")

                # Format for display
                display_df = top_duids[[
                    'duid',
                    'curtailment_mwh',
                    'generation_mwh',
                    'curtailment_rate_pct',
                    'max_curtailment_mw',
                    'curtailment_intervals'
                ]].copy()

                display_df.columns = ['DUID', 'Curtailed (MWh)', 'Generated (MWh)', 'Rate %', 'Max (MW)', 'Intervals']
                display_df['Curtailed (MWh)'] = display_df['Curtailed (MWh)'].round(0)
                display_df['Generated (MWh)'] = display_df['Generated (MWh)'].round(0)
                display_df['Rate %'] = display_df['Rate %'].round(1)
                display_df['Max (MW)'] = display_df['Max (MW)'].round(0)

                return pn.widgets.Tabulator(
                    display_df,
                    show_index=False,
                    height=400,
                    width=800,
                    frozen_columns=['DUID']
                )

        except Exception as e:
            import traceback
            logger.error(f"Error creating DUID table: {e}\n{traceback.format_exc()}")
            return pn.pane.Markdown(f"Error: {str(e)}")

    def create_regional_content(self):
        """Create the regional analysis content"""
        return pn.Column(
            pn.bind(
                self.create_plot,
                self.mode_selector.param.value,
                self.region_selector.param.value,
                self.fuel_selector.param.value,
                self.aggregation_selector.param.value,
                self.param.time_range
            ),
            pn.pane.Markdown("### Regional Comparison"),
            pn.bind(
                self.create_region_table,
                self.param.time_range
            ),
            pn.pane.Markdown("### Fuel Type Comparison"),
            pn.bind(
                self.create_fuel_table,
                self.region_selector.param.value,
                self.param.time_range
            )
        )

    def create_duid_content(self):
        """Create the DUID analysis content"""
        # Create curtailment type selector section
        type_selector_section = pn.Row(
            pn.pane.Markdown("**Curtailment Type:**", margin=(5, 10, 5, 0)),
            self.curtailment_type_selector,
            margin=(0, 0, 15, 0)
        )

        return pn.Column(
            type_selector_section,
            pn.bind(
                self.create_duid_lollipop,
                self.param.time_range,
                self.region_selector.param.value,
                self.curtailment_type_selector.param.value,
                20  # top 20 DUIDs
            ),
            pn.pane.Markdown("### Top Curtailed DUIDs - Details"),
            pn.bind(
                self.create_duid_table,
                self.param.time_range,
                self.region_selector.param.value,
                self.curtailment_type_selector.param.value,
                20
            )
        )

    def create_tab(self):
        """Create the curtailment tab content with subtabs"""
        # Get data info
        stats = self.query_manager.get_statistics()
        coverage = stats.get('data_coverage', {})
        earliest = coverage.get('earliest', 'Unknown')
        latest = coverage.get('latest', 'Unknown')
        records = coverage.get('total_records', 0)

        # Time range selector with label
        time_range_section = pn.Column(
            pn.pane.HTML("<div style='color: #aaa; font-size: 11px; margin-bottom: 4px;'>Days</div>"),
            self.time_range_widget,
            width=350,
            margin=(10, 0)
        )

        # Custom date pickers
        date_pickers = pn.Row(
            self.start_date_picker,
            self.end_date_picker,
            width=320
        )

        # Create sidebar controls
        controls = pn.Column(
            pn.pane.Markdown("## Curtailment Controls"),
            self.mode_selector,
            self.region_selector,
            self.fuel_selector,
            "---",
            time_range_section,
            date_pickers,
            pn.pane.Markdown("*Custom dates override preset selection*", styles={'font-size': '10px', 'color': '#888'}),
            "---",
            self.aggregation_selector,
            pn.pane.Markdown("---"),
            pn.bind(
                self.create_stats,
                self.mode_selector.param.value,
                self.region_selector.param.value,
                self.fuel_selector.param.value,
                self.param.time_range
            ),
            pn.pane.Markdown("---"),
            pn.pane.Markdown(
                f"""
                ### Data Info
                **Source**: AEMO UIGF
                **Records**: {records:,}
                **From**: {earliest}
                **To**: {latest}

                ### Method
                Curtailment = UIGF - Cleared
                (UIGF = Unconstrained Intermittent Generation Forecast)
                """,
                width=320
            ),
            width=350
        )

        # Create subtabs for Regional and DUID analysis
        subtabs = pn.Tabs(
            ('Regional', self.create_regional_content()),
            ('By DUID', self.create_duid_content()),
            tabs_location='above'
        )

        # Return row layout with sidebar and subtabs
        return pn.Row(controls, subtabs)


def create_curtailment_tab():
    """Factory function to create curtailment tab"""
    tab = CurtailmentTab()
    return tab.create_tab()
