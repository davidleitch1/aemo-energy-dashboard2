#!/usr/bin/env python3
"""
Curtailment Tab for AEMO Energy Dashboard
DuckDB-based curtailment analysis with efficient querying
"""

import panel as pn
import pandas as pd
import hvplot.pandas
import pickle
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Optional

from .curtailment_query_manager import CurtailmentQueryManager
from ..shared.flexoki_theme import (
    FLEXOKI_PAPER,
    FLEXOKI_BLACK,
    FLEXOKI_BASE,
    FLEXOKI_ACCENT,
)

# NEM Regions
NEM_REGIONS = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']

# Tabulator CSS for Flexoki theme
TABULATOR_CSS = f"""
/* Tabulator Flexoki light theme */
.tabulator {{
    background-color: {FLEXOKI_PAPER};
    border: 1px solid {FLEXOKI_BASE[150]};
}}

.tabulator-header {{
    background-color: {FLEXOKI_BASE[50]};
    border-bottom: 1px solid {FLEXOKI_BASE[150]};
    color: {FLEXOKI_BLACK};
}}

.tabulator-row {{
    background-color: {FLEXOKI_PAPER};
    border-bottom: 1px solid {FLEXOKI_BASE[100]};
    color: {FLEXOKI_BLACK};
}}

.tabulator-row:nth-child(even) {{
    background-color: {FLEXOKI_BASE[50]};
}}

.tabulator-row:hover {{
    background-color: {FLEXOKI_BASE[100]};
}}

.tabulator-cell {{
    color: {FLEXOKI_BLACK};
}}
"""


def set_flexoki_backgrounds(plot, element):
    """Hook function to set Flexoki theme backgrounds on hvplot charts."""
    try:
        p = plot.state
        # Set plot area background
        p.background_fill_color = FLEXOKI_PAPER
        p.border_fill_color = FLEXOKI_PAPER

        # Style legend if present
        if hasattr(p, 'legend') and p.legend:
            for legend in p.legend:
                legend.background_fill_color = FLEXOKI_PAPER
                legend.border_line_color = FLEXOKI_BASE[150]
                legend.border_line_width = 1
                legend.label_text_color = FLEXOKI_BLACK
    except Exception:
        pass  # Silently handle any styling errors


class CurtailmentTab:
    """Curtailment analysis tab for dashboard"""

    def __init__(self):
        """Initialize curtailment tab with query manager and metadata"""
        self.query_manager = CurtailmentQueryManager()

        # Load generator metadata
        self.gen_info = self._load_gen_info()

        # Build lookup tables
        self.duid_to_station = {}
        self.duid_to_fuel = {}
        self.duid_to_region = {}
        self.station_to_duids = {}
        self.wind_solar_stations = set()

        self._build_lookups()

        # Get list of curtailed units and their stations
        self.curtailed_stations = self._get_curtailed_stations()

        # Create widgets
        self._create_widgets()

    def _load_gen_info(self):
        """Load generator information from pickle file"""
        from ..shared.config import config
        gen_info_path = config.gen_info_file

        if gen_info_path.exists():
            try:
                with open(gen_info_path, 'rb') as f:
                    gen_info = pickle.load(f)
                return gen_info
            except Exception as e:
                print(f"Error loading gen_info: {e}")
                return {}
        else:
            return {}

    def _build_lookups(self):
        """Build lookup tables from gen_info DataFrame"""
        import pandas as pd

        # Check if gen_info is a DataFrame or dict
        if isinstance(self.gen_info, pd.DataFrame):
            # gen_info is a DataFrame - iterate through rows
            for idx, row in self.gen_info.iterrows():
                duid = row['DUID']
                fuel = row['Fuel']

                # Only process wind and solar units for curtailment
                is_wind_solar = False
                if 'Wind' in fuel or 'wind' in fuel:
                    fuel = 'Wind'
                    is_wind_solar = True
                elif 'Solar' in fuel or 'solar' in fuel or 'PV' in fuel:
                    fuel = 'Solar'
                    is_wind_solar = True

                if not is_wind_solar:
                    continue

                self.duid_to_fuel[duid] = fuel

                # Station mapping
                station = row['Site Name']
                if station and station != duid:
                    self.duid_to_station[duid] = station
                    if station not in self.station_to_duids:
                        self.station_to_duids[station] = []
                    self.station_to_duids[station].append(duid)
                    self.wind_solar_stations.add(station)
                else:
                    self.duid_to_station[duid] = duid

                # Region mapping
                region = row['Region']
                if region in NEM_REGIONS:
                    self.duid_to_region[duid] = region
                else:
                    # Try to infer from DUID patterns if region not found
                    if any(x in duid for x in ['NSW', 'BW', 'LD', 'MP']):
                        self.duid_to_region[duid] = 'NSW1'
                    elif any(x in duid for x in ['QLD', 'BRA', 'DAR']):
                        self.duid_to_region[duid] = 'QLD1'
                    elif any(x in duid for x in ['SA1', 'SA2', 'LK', 'TOR']):
                        self.duid_to_region[duid] = 'SA1'
                    elif any(x in duid for x in ['TAS', 'BAS', 'WOO']):
                        self.duid_to_region[duid] = 'TAS1'
                    elif any(x in duid for x in ['VIC', 'MUR', 'YAL']):
                        self.duid_to_region[duid] = 'VIC1'
                    else:
                        self.duid_to_region[duid] = region
        else:
            # gen_info is a dict - use old logic
            for duid, info in self.gen_info.items():
                fuel = info.get('Fuel', 'Unknown')

                is_wind_solar = False
                if 'Wind' in fuel or 'wind' in fuel:
                    fuel = 'Wind'
                    is_wind_solar = True
                elif 'Solar' in fuel or 'solar' in fuel or 'PV' in fuel:
                    fuel = 'Solar'
                    is_wind_solar = True

                if not is_wind_solar:
                    continue

                self.duid_to_fuel[duid] = fuel
                station = info.get('Site Name', duid)
                if station and station != duid:
                    self.duid_to_station[duid] = station
                    if station not in self.station_to_duids:
                        self.station_to_duids[station] = []
                    self.station_to_duids[station].append(duid)
                    self.wind_solar_stations.add(station)
                else:
                    self.duid_to_station[duid] = duid

                region = info.get('Region', 'Unknown')
                if region in NEM_REGIONS:
                    self.duid_to_region[duid] = region
                else:
                    if any(x in duid for x in ['NSW', 'BW', 'LD', 'MP']):
                        self.duid_to_region[duid] = 'NSW1'
                    elif any(x in duid for x in ['QLD', 'BRA', 'DAR']):
                        self.duid_to_region[duid] = 'QLD1'
                    elif any(x in duid for x in ['SA1', 'SA2', 'LK', 'TOR']):
                        self.duid_to_region[duid] = 'SA1'
                    elif any(x in duid for x in ['TAS', 'BAS', 'WOO']):
                        self.duid_to_region[duid] = 'TAS1'
                    elif any(x in duid for x in ['VIC', 'MUR', 'YAL']):
                        self.duid_to_region[duid] = 'VIC1'
                    else:
                        self.duid_to_region[duid] = region

    def _get_curtailed_stations(self):
        """Get list of stations that have been curtailed recently"""
        try:
            # Query last 30 days to find curtailed units
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)

            top_units = self.query_manager.query_top_curtailed_units(
                start_date, end_date, limit=500
            )

            curtailed_stations = set()
            if not top_units.empty:
                for duid in top_units['duid'].unique():
                    if duid in self.duid_to_station:
                        station = self.duid_to_station[duid]
                        curtailed_stations.add(station)

                return sorted(list(curtailed_stations))

        except Exception as e:
            print(f"Error getting curtailed stations: {e}")

        # Fallback: return all wind/solar stations
        return sorted(list(self.wind_solar_stations))

    def _create_widgets(self):
        """Create all widgets"""
        # View mode selector
        self.mode_selector = pn.widgets.RadioButtonGroup(
            name='View Mode',
            value='By Region',
            options=['By Region', 'By Fuel Type', 'By Station']
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
            options=['All', 'Wind', 'Solar'],
            visible=False
        )

        # Station selector
        station_options = ['All'] + self.curtailed_stations if self.curtailed_stations else ['All']
        self.station_selector = pn.widgets.Select(
            name='Station',
            value='All',
            options=station_options,
            visible=False
        )

        # Date range - limited to available data
        self.date_range_slider = pn.widgets.DateRangeSlider(
            name='Date Range',
            start=date(2024, 9, 1),
            end=datetime.now().date(),
            value=(datetime.now().date() - timedelta(days=7), datetime.now().date())
        )

        # Aggregation
        self.aggregation_selector = pn.widgets.Select(
            name='Aggregation',
            value='hourly',
            options=['5min', 'hourly', 'daily']
        )

        # Wire up mode selector
        self.mode_selector.param.watch(self._update_selectors, 'value')

    def _update_selectors(self, event):
        """Update visibility of selectors based on mode"""
        if self.mode_selector.value == 'By Region':
            self.region_selector.visible = True
            self.fuel_selector.visible = False
            self.station_selector.visible = False
        elif self.mode_selector.value == 'By Fuel Type':
            self.region_selector.visible = True
            self.fuel_selector.visible = True
            self.station_selector.visible = False
        else:  # By Station
            self.region_selector.visible = True
            self.fuel_selector.visible = False
            self.station_selector.visible = True

    def create_plot(self, mode, region, fuel, station, date_range, aggregation):
        """Create curtailment visualization"""
        try:
            # Parse dates
            start, end = date_range
            start_dt = datetime.combine(start, datetime.min.time())
            end_dt = datetime.combine(end, datetime.max.time())

            # Determine filters
            region_filter = None if region == 'All' else region
            fuel_filter = None if fuel == 'All' else fuel
            duid_filter = None

            # If station selected, get its DUIDs
            if mode == 'By Station' and station != 'All':
                if station in self.station_to_duids:
                    duids = self.station_to_duids[station]
                    duid_filter = duids[0] if duids else None
                else:
                    duid_filter = station

            # Query data
            data = self.query_manager.query_curtailment_data(
                start_date=start_dt,
                end_date=end_dt,
                region=region_filter,
                fuel=fuel_filter,
                duid=duid_filter,
                resolution=aggregation
            )

            if data.empty:
                return pn.pane.Markdown("No data available for selected filters")

            # Create title
            title_parts = ["Curtailment Analysis"]
            if mode == 'By Region' and region != 'All':
                title_parts.append(f"Region: {region}")
            elif mode == 'By Fuel Type':
                if fuel != 'All':
                    title_parts.append(f"{fuel} Generation")
                if region != 'All':
                    title_parts.append(f"in {region}")
            elif mode == 'By Station' and station != 'All':
                title_parts.append(f"Station: {station}")

            title = " - ".join(title_parts)

            # Create plot using Flexoki theme colors
            plot = data.hvplot.area(
                x='timestamp',
                y='scada',
                label='Actual Generation',
                color=FLEXOKI_ACCENT['green'],  # Flexoki green
                alpha=0.7,
                height=450,
                width=900,
                title=title,
                ylabel='Power (MW)',
                legend='top_left'
            ).opts(
                bgcolor=FLEXOKI_PAPER,
                show_grid=True,
                hooks=[set_flexoki_backgrounds]
            )

            # Add curtailment
            if 'curtailment' in data.columns:
                plot = plot * data.hvplot.area(
                    x='timestamp',
                    y='curtailment',
                    label='Curtailment',
                    color=FLEXOKI_ACCENT['red'],  # Flexoki red
                    alpha=0.4
                )

            # Add lines
            if 'availgen' in data.columns:
                plot = plot * data.hvplot.line(
                    x='timestamp',
                    y='availgen',
                    label='Available Generation',
                    color=FLEXOKI_ACCENT['cyan'],  # Flexoki cyan
                    line_dash='dashed',
                    line_width=2
                )

            if 'dispatchcap' in data.columns:
                plot = plot * data.hvplot.line(
                    x='timestamp',
                    y='dispatchcap',
                    label='Dispatch Cap',
                    color=FLEXOKI_ACCENT['orange'],  # Flexoki orange
                    line_width=2
                )

            return plot

        except Exception as e:
            return pn.pane.Markdown(f"Error: {str(e)}")

    def create_stats(self, mode, region, fuel, station, date_range):
        """Create statistics panel"""
        try:
            # Parse dates
            start, end = date_range
            start_dt = datetime.combine(start, datetime.min.time())
            end_dt = datetime.combine(end, datetime.max.time())

            # Get summary
            summary = self.query_manager.query_region_summary(start_dt, end_dt)

            if summary.empty:
                return pn.pane.Markdown("No statistics available")

            # Filter by region if needed
            if region != 'All':
                summary = summary[summary['region'] == region]

            if summary.empty:
                return pn.pane.Markdown("No data for selected filters")

            # Calculate stats
            total_curtailment_mwh = summary['total_curtailment_mwh'].sum()
            avg_curtailment_rate = summary['curtailment_rate_pct'].mean()
            max_curtailment_mw = summary['max_curtailment_mw'].max() if 'max_curtailment_mw' in summary else 0
            network_events = summary['network_events'].sum() if 'network_events' in summary else 0
            economic_events = summary['economic_events'].sum() if 'economic_events' in summary else 0

            # Build filter description
            filter_desc = []
            if mode == 'By Region' and region != 'All':
                filter_desc.append(f"Region: {region}")
            elif mode == 'By Fuel Type':
                if fuel != 'All':
                    filter_desc.append(f"Fuel: {fuel}")
                if region != 'All':
                    filter_desc.append(f"Region: {region}")
            elif mode == 'By Station' and station != 'All':
                filter_desc.append(f"Station: {station}")

            filter_text = ", ".join(filter_desc) if filter_desc else "All Data"

            stats_html = f"""
            <h3 style='color:{FLEXOKI_BLACK};'>Summary Statistics</h3>
            <table style='width:100%; font-size:13px; color:{FLEXOKI_BLACK}; background-color:{FLEXOKI_PAPER};'>
            <tr><td colspan='2'><b>Period: {start} to {end}</b></td></tr>
            <tr><td colspan='2'><b>{filter_text}</b></td></tr>
            <tr><td colspan='2'><hr style='border-color:{FLEXOKI_BASE[150]};'></td></tr>
            <tr><td><b>Curtailment Rate:</b></td><td>{avg_curtailment_rate:.1f}%</td></tr>
            <tr><td><b>Total Curtailed:</b></td><td>{total_curtailment_mwh:,.0f} MWh</td></tr>
            <tr><td><b>Max Curtailment:</b></td><td>{max_curtailment_mw:.1f} MW</td></tr>
            <tr><td colspan='2'><hr style='border-color:{FLEXOKI_BASE[150]};'></td></tr>
            <tr><td><b>Network Curtailment:</b></td><td>{network_events:,}</td></tr>
            <tr><td><b>Economic Curtailment:</b></td><td>{economic_events:,}</td></tr>
            </table>
            """

            return pn.pane.HTML(stats_html, width=320)

        except Exception as e:
            return pn.pane.Markdown(f"Error: {str(e)}")

    def create_region_table(self, date_range):
        """Create regional comparison table"""
        try:
            start, end = date_range
            start_dt = datetime.combine(start, datetime.min.time())
            end_dt = datetime.combine(end, datetime.max.time())

            summary = self.query_manager.query_region_summary(start_dt, end_dt)

            if summary.empty:
                return pn.pane.Markdown("No regional data available")

            display_df = summary[['region', 'unit_count', 'curtailment_rate_pct', 'total_curtailment_mwh', 'actual_generation_mwh']].copy()
            display_df.columns = ['Region', 'Units', 'Curtailment %', 'Curtailed (MWh)', 'Actual Output (MWh)']
            display_df['Curtailment %'] = display_df['Curtailment %'].round(1)
            display_df['Curtailed (MWh)'] = display_df['Curtailed (MWh)'].round(0)
            display_df['Actual Output (MWh)'] = display_df['Actual Output (MWh)'].round(0)

            return pn.widgets.Tabulator(
                display_df,
                show_index=False,
                height=200,
                width=650,
                stylesheets=[TABULATOR_CSS]
            )

        except Exception as e:
            return pn.pane.Markdown(f"Error: {str(e)}")

    def create_top_units_table(self, date_range, region):
        """Create top curtailed units table filtered by region"""
        try:
            start, end = date_range
            start_dt = datetime.combine(start, datetime.min.time())
            end_dt = datetime.combine(end, datetime.max.time())

            region_filter = None if region == 'All' else region
            top_units = self.query_manager.query_top_curtailed_units(
                start_dt, end_dt, limit=10, region=region_filter
            )

            if top_units.empty:
                return pn.pane.Markdown("No curtailed units found")

            # Add station names (handle both 'duid' and 'DUID' column names)
            display_df = top_units.copy()

            # Check which column name is present
            duid_col = 'duid' if 'duid' in display_df.columns else 'DUID'
            region_col = 'region' if 'region' in display_df.columns else 'Region'
            fuel_col = 'fuel' if 'fuel' in display_df.columns else 'Fuel'

            display_df['station'] = display_df[duid_col].map(lambda x: self.duid_to_station.get(x, x))

            display_df = display_df[[duid_col, 'station', region_col, fuel_col, 'curtailment_rate_pct', 'total_curtailment_mwh']]
            display_df.columns = ['DUID', 'Station', 'Region', 'Fuel', 'Rate %', 'Total MWh']
            display_df['Rate %'] = display_df['Rate %'].round(1)
            display_df['Total MWh'] = display_df['Total MWh'].round(0)

            return pn.widgets.Tabulator(
                display_df,
                show_index=False,
                height=300,
                width=800,
                stylesheets=[TABULATOR_CSS]
            )

        except Exception as e:
            return pn.pane.Markdown(f"Error: {str(e)}")

    def create_tab(self):
        """Create the curtailment tab content"""
        # Create sidebar controls
        controls = pn.Column(
            pn.pane.Markdown("## Curtailment Controls"),
            self.mode_selector,
            self.region_selector,
            self.fuel_selector,
            self.station_selector,
            self.date_range_slider,
            self.aggregation_selector,
            pn.pane.Markdown("---"),
            pn.bind(
                self.create_stats,
                self.mode_selector.param.value,
                self.region_selector.param.value,
                self.fuel_selector.param.value,
                self.station_selector.param.value,
                self.date_range_slider.param.value
            ),
            pn.pane.Markdown("---"),
            pn.pane.Markdown(
                f"""
                ### Data Info
                **Generators**: {len(self.gen_info)}
                **Curtailed Units**: {len(self.curtailed_stations)}
                **Data Period**: Sept 2024 - Present

                ### Performance
                **Backend**: DuckDB
                **Memory**: < 200MB
                """,
                width=320
            ),
            width=350
        )

        # Create main content
        main_content = pn.Column(
            pn.bind(
                self.create_plot,
                self.mode_selector.param.value,
                self.region_selector.param.value,
                self.fuel_selector.param.value,
                self.station_selector.param.value,
                self.date_range_slider.param.value,
                self.aggregation_selector.param.value
            ),
            pn.pane.Markdown("### Regional Comparison"),
            pn.bind(
                self.create_region_table,
                self.date_range_slider.param.value
            ),
            pn.pane.Markdown("### Top Curtailed Units"),
            pn.bind(
                self.create_top_units_table,
                self.date_range_slider.param.value,
                self.region_selector.param.value
            )
        )

        # Return row layout with sidebar and main content
        return pn.Row(controls, main_content)


def create_curtailment_tab():
    """Factory function to create curtailment tab"""
    tab = CurtailmentTab()
    return tab.create_tab()