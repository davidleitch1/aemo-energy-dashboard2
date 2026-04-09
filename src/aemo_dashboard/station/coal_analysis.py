"""
Coal Station Analysis - Revenue and capacity utilization analysis for coal stations.

Provides comparison between latest 12 months and previous 12 months for:
- Total revenue by station
- Capacity utilization by station
"""

import os
import pandas as pd
import numpy as np
import duckdb
import pickle
import panel as pn
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from scipy.ndimage import uniform_filter1d

from ..shared.logging_config import get_logger
from ..shared.config import config
from ..shared.flexoki_theme import (
    FLEXOKI_PAPER, FLEXOKI_BLACK, FLEXOKI_BASE, FLEXOKI_ACCENT
)

DUCKDB_PATH = os.getenv('AEMO_DUCKDB_PATH')

# Target stations for evolution analysis
EVOLUTION_STATIONS = ['Bayswater', 'Tarong', 'Loy Yang B']

logger = get_logger(__name__)


class CoalAnalysis:
    """Analysis engine for coal station performance"""

    def __init__(self):
        """Initialize coal analysis with data connections"""
        self.conn = duckdb.connect(':memory:')
        self.gen_info = None
        self.coal_info = None
        self.station_capacity = {}
        self.duid_to_region = {}
        self.duid_to_station = {}
        self.coal_duids = []
        self.interval_hours = 0.5  # Default for 30-minute data

        self._load_gen_info()
        self._create_views()

    def _load_gen_info(self):
        """Load generator info and extract coal station data"""
        try:
            gen_info_path = config.gen_info_file
            with open(gen_info_path, 'rb') as f:
                self.gen_info = pickle.load(f)

            # Filter to coal stations
            self.coal_info = self.gen_info[
                self.gen_info['Fuel'].str.contains('Coal', case=False, na=False)
            ][['DUID', 'Site Name', 'Region', 'Capacity(MW)']].copy()

            # Aggregate station capacity (sum of all units)
            self.station_capacity = self.coal_info.groupby('Site Name')['Capacity(MW)'].sum().to_dict()

            # Create mappings
            self.coal_duids = self.coal_info['DUID'].tolist()
            self.duid_to_region = dict(zip(self.coal_info['DUID'], self.coal_info['Region']))
            self.duid_to_station = dict(zip(self.coal_info['DUID'], self.coal_info['Site Name']))

            logger.info(f"Loaded {len(self.coal_duids)} coal DUIDs from {len(self.station_capacity)} stations")

        except Exception as e:
            logger.error(f"Error loading gen_info: {e}")
            raise

    def _create_views(self):
        """Create DuckDB views for SCADA and price data"""
        try:
            if DUCKDB_PATH:
                # Attach external DuckDB and create views from its tables
                self.conn.execute(f"ATTACH '{DUCKDB_PATH}' AS prod (READ_ONLY)")
                self.conn.execute("""
                    CREATE OR REPLACE VIEW scada AS
                    SELECT settlementdate, duid, scadavalue
                    FROM prod.scada30
                """)
                self.conn.execute("""
                    CREATE OR REPLACE VIEW prices AS
                    SELECT settlementdate, regionid as region, rrp as price
                    FROM prod.prices30
                """)
                logger.info("Created SCADA and prices views from external DuckDB")
            else:
                # Fallback: read from parquet files
                scada_path = config.scada30_file
                prices_path = config.spot_hist_file
                self.conn.execute(f"""
                    CREATE OR REPLACE VIEW scada AS
                    SELECT settlementdate, duid, scadavalue
                    FROM read_parquet('{scada_path}')
                """)
                self.conn.execute(f"""
                    CREATE OR REPLACE VIEW prices AS
                    SELECT settlementdate, regionid as region, rrp as price
                    FROM read_parquet('{prices_path}')
                """)
                logger.info("Created SCADA and prices views from parquet files")

            # Store the interval duration for energy calculations (30 min = 0.5 hours)
            self.interval_hours = 0.5

        except Exception as e:
            logger.error(f"Error creating views: {e}")
            raise

    def calculate_station_metrics(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> pd.DataFrame:
        """
        Calculate revenue and capacity factor for all coal stations.

        Args:
            start_date: Start of analysis period
            end_date: End of analysis period

        Returns:
            DataFrame with station metrics
        """
        try:
            # Query SCADA data for coal DUIDs
            duid_list = ','.join([f"'{d}'" for d in self.coal_duids])
            scada_query = f"""
                SELECT settlementdate, duid, scadavalue
                FROM scada
                WHERE settlementdate >= '{start_date.strftime("%Y-%m-%d")}'
                  AND settlementdate < '{end_date.strftime("%Y-%m-%d")}'
                  AND duid IN ({duid_list})
            """
            scada_df = self.conn.execute(scada_query).fetchdf()

            if scada_df.empty:
                logger.warning(f"No SCADA data for period {start_date} to {end_date}")
                return pd.DataFrame()

            # Add region and station mappings
            scada_df['region'] = scada_df['duid'].map(self.duid_to_region)
            scada_df['station'] = scada_df['duid'].map(self.duid_to_station)

            # Query prices
            prices_query = f"""
                SELECT settlementdate, region, price
                FROM prices
                WHERE settlementdate >= '{start_date.strftime("%Y-%m-%d")}'
                  AND settlementdate < '{end_date.strftime("%Y-%m-%d")}'
            """
            prices_df = self.conn.execute(prices_query).fetchdf()

            # Merge SCADA with prices
            merged = scada_df.merge(prices_df, on=['settlementdate', 'region'], how='left')

            # Calculate revenue and generation per interval (30 min = 0.5 hour)
            merged['revenue'] = merged['scadavalue'] * self.interval_hours * merged['price']
            merged['generation_mwh'] = merged['scadavalue'] * self.interval_hours

            # Aggregate by station
            station_summary = merged.groupby('station').agg({
                'revenue': 'sum',
                'generation_mwh': 'sum'
            }).reset_index()

            # Add station capacity
            station_summary['capacity_mw'] = station_summary['station'].map(self.station_capacity)

            # Calculate capacity factor
            hours_in_period = (end_date - start_date).total_seconds() / 3600
            station_summary['capacity_factor'] = (
                station_summary['generation_mwh'] /
                (station_summary['capacity_mw'] * hours_in_period)
            ) * 100

            # Convert revenue to millions
            station_summary['revenue_millions'] = station_summary['revenue'] / 1_000_000

            logger.info(f"Calculated metrics for {len(station_summary)} stations")
            return station_summary

        except Exception as e:
            logger.error(f"Error calculating station metrics: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def get_comparison_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Get comparison data for latest 12 months vs previous 12 months.

        Returns:
            Tuple of (latest_df, previous_df)
        """
        now = datetime.now()
        latest_start = now - timedelta(days=365)
        prev_start = latest_start - timedelta(days=365)

        latest_df = self.calculate_station_metrics(latest_start, now)
        prev_df = self.calculate_station_metrics(prev_start, latest_start)

        return latest_df, prev_df

    def _match_station(self, site_name: str, target_stations: List[str]) -> bool:
        """Check if a site name matches any target station, excluding unwanted variants"""
        site_lower = site_name.lower()
        for station in target_stations:
            station_lower = station.lower()
            if station_lower in site_lower:
                # Special case: exclude "Tarong North" when looking for "Tarong"
                if station_lower == 'tarong' and 'tarong north' in site_lower:
                    continue
                return True
        return False

    def get_daily_capacity_factor(self, stations: List[str]) -> pd.DataFrame:
        """
        Get daily capacity factor time series for specified stations.

        Args:
            stations: List of station names to include

        Returns:
            DataFrame with date, station, and capacity_factor columns
        """
        try:
            # Get DUIDs for these stations (with precise matching)
            station_duids = self.coal_info[
                self.coal_info['Site Name'].apply(lambda x: self._match_station(x, stations))
            ]['DUID'].tolist()

            if not station_duids:
                logger.warning(f"No DUIDs found for stations: {stations}")
                return pd.DataFrame()

            duid_list = ','.join([f"'{d}'" for d in station_duids])

            # Query daily aggregated SCADA data
            query = f"""
                SELECT
                    DATE_TRUNC('day', settlementdate) as date,
                    duid,
                    SUM(scadavalue) * {self.interval_hours} as generation_mwh,
                    COUNT(*) as intervals
                FROM scada
                WHERE duid IN ({duid_list})
                GROUP BY DATE_TRUNC('day', settlementdate), duid
                ORDER BY date
            """
            df = self.conn.execute(query).fetchdf()

            if df.empty:
                return pd.DataFrame()

            # Map DUID to station
            df['station'] = df['duid'].map(self.duid_to_station)

            # Aggregate by station and date
            daily = df.groupby(['date', 'station']).agg({
                'generation_mwh': 'sum',
                'intervals': 'sum'
            }).reset_index()

            # Calculate capacity factor
            # Expected intervals per day for 30-min data = 48
            for station in daily['station'].unique():
                capacity = self.station_capacity.get(station, 0)
                if capacity > 0:
                    mask = daily['station'] == station
                    # Daily capacity = capacity_mw * 24 hours
                    daily.loc[mask, 'capacity_factor'] = (
                        daily.loc[mask, 'generation_mwh'] / (capacity * 24)
                    ) * 100

            return daily[['date', 'station', 'capacity_factor']].dropna()

        except Exception as e:
            logger.error(f"Error getting daily capacity factor: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def get_hourly_pattern(self, stations: List[str], start_date: datetime,
                           end_date: datetime) -> pd.DataFrame:
        """
        Get average hourly generation pattern for specified stations and period.

        Args:
            stations: List of station names
            start_date: Start of period
            end_date: End of period

        Returns:
            DataFrame with hour, station, and capacity_factor columns
        """
        try:
            # Get DUIDs for these stations (with precise matching)
            station_duids = self.coal_info[
                self.coal_info['Site Name'].apply(lambda x: self._match_station(x, stations))
            ]['DUID'].tolist()

            if not station_duids:
                return pd.DataFrame()

            duid_list = ','.join([f"'{d}'" for d in station_duids])

            # Query hourly aggregated data
            query = f"""
                SELECT
                    EXTRACT(HOUR FROM settlementdate) as hour,
                    duid,
                    AVG(scadavalue) as avg_mw
                FROM scada
                WHERE duid IN ({duid_list})
                  AND settlementdate >= '{start_date.strftime("%Y-%m-%d")}'
                  AND settlementdate < '{end_date.strftime("%Y-%m-%d")}'
                GROUP BY EXTRACT(HOUR FROM settlementdate), duid
                ORDER BY hour
            """
            df = self.conn.execute(query).fetchdf()

            if df.empty:
                return pd.DataFrame()

            # Map DUID to station
            df['station'] = df['duid'].map(self.duid_to_station)

            # Aggregate by station and hour
            hourly = df.groupby(['hour', 'station']).agg({
                'avg_mw': 'sum'
            }).reset_index()

            # Convert to capacity factor for comparability
            for station in hourly['station'].unique():
                capacity = self.station_capacity.get(station, 0)
                if capacity > 0:
                    mask = hourly['station'] == station
                    hourly.loc[mask, 'capacity_factor'] = (
                        hourly.loc[mask, 'avg_mw'] / capacity
                    ) * 100

            return hourly[['hour', 'station', 'capacity_factor']].dropna()

        except Exception as e:
            logger.error(f"Error getting hourly pattern: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()


class CoalAnalysisUI:
    """UI components for coal analysis subtab"""

    def __init__(self):
        self.engine = CoalAnalysis()
        self.latest_data = None
        self.prev_data = None

    def _load_data(self):
        """Load comparison data (always refresh)"""
        self.latest_data = None
        self.prev_data = None
        self.latest_data, self.prev_data = self.engine.get_comparison_data()

    def _create_grouped_hbar_figure(self, merged, value_col_latest, value_col_prev,
                                      title, xlabel, xlim=None, color_latest=None):
        """Create a Plotly grouped horizontal bar chart for comparison data"""
        if color_latest is None:
            color_latest = FLEXOKI_ACCENT['green']

        stations = merged['station'].tolist()

        fig = go.Figure()

        # Previous 12m bars (drawn first so latest is on top in legend)
        fig.add_trace(go.Bar(
            y=stations,
            x=merged[value_col_prev],
            orientation='h',
            name='Previous 12m',
            marker_color=FLEXOKI_BASE[300],
        ))

        # Latest 12m bars
        fig.add_trace(go.Bar(
            y=stations,
            x=merged[value_col_latest],
            orientation='h',
            name='Latest 12m',
            marker_color=color_latest,
        ))

        n_stations = len(stations)
        fig_height = max(300, n_stations * 35 + 120)

        layout_kwargs = dict(
            barmode='group',
            title=dict(text=title, font=dict(size=13, color=FLEXOKI_BLACK)),
            xaxis=dict(
                title=dict(text=xlabel, font=dict(size=11, color=FLEXOKI_BLACK)),
                tickfont=dict(size=10, color=FLEXOKI_BLACK),
                showgrid=False,
                zeroline=False,
            ),
            yaxis=dict(
                tickfont=dict(size=10, color=FLEXOKI_BLACK),
                showgrid=False,
            ),
            paper_bgcolor=FLEXOKI_PAPER,
            plot_bgcolor=FLEXOKI_PAPER,
            font=dict(color=FLEXOKI_BLACK),
            height=fig_height,
            autosize=True,
            margin=dict(l=10, r=20, t=40, b=60),
            legend=dict(
                orientation='h',
                yanchor='top',
                y=-0.12,
                xanchor='center',
                x=0.5,
                font=dict(size=10, color=FLEXOKI_BLACK),
                bgcolor='rgba(0,0,0,0)',
            ),
        )

        if xlim:
            layout_kwargs['xaxis']['range'] = list(xlim)

        fig.update_layout(**layout_kwargs)

        # Attribution annotation
        fig.add_annotation(
            text='Design: ITK, Data: AEMO',
            xref='paper', yref='paper',
            x=1.0, y=-0.18,
            showarrow=False,
            font=dict(size=9, color=FLEXOKI_BASE[600]),
        )

        return fig

    def create_revenue_plot(self):
        """Create grouped horizontal bar chart for revenue comparison"""
        try:
            self._load_data()

            if self.latest_data.empty:
                return pn.pane.Markdown("No data available for coal station revenue analysis")

            # Merge for comparison
            merged = self.latest_data[['station', 'revenue_millions']].merge(
                self.prev_data[['station', 'revenue_millions']],
                on='station',
                suffixes=('_latest', '_prev'),
                how='outer'
            ).fillna(0)

            # Sort by latest revenue (ascending so highest at top in plot)
            merged = merged.sort_values('revenue_millions_latest', ascending=True).reset_index(drop=True)

            fig = self._create_grouped_hbar_figure(
                merged,
                value_col_latest='revenue_millions_latest',
                value_col_prev='revenue_millions_prev',
                title='Coal Station Revenue Comparison',
                xlabel='Revenue ($M)',
                color_latest=FLEXOKI_ACCENT['green'],
            )

            return pn.pane.Plotly(fig, sizing_mode='stretch_width')

        except Exception as e:
            logger.error(f"Error creating revenue plot: {e}")
            import traceback
            traceback.print_exc()
            return pn.pane.Markdown(f"Error creating revenue plot: {e}")

    def create_utilization_plot(self):
        """Create grouped horizontal bar chart for capacity utilization comparison"""
        try:
            self._load_data()

            if self.latest_data.empty:
                return pn.pane.Markdown("No data available for coal station utilization analysis")

            # Merge for comparison
            merged = self.latest_data[['station', 'capacity_factor']].merge(
                self.prev_data[['station', 'capacity_factor']],
                on='station',
                suffixes=('_latest', '_prev'),
                how='outer'
            ).fillna(0)

            # Sort by latest capacity factor (ascending so highest at top in plot)
            merged = merged.sort_values('capacity_factor_latest', ascending=True).reset_index(drop=True)

            fig = self._create_grouped_hbar_figure(
                merged,
                value_col_latest='capacity_factor_latest',
                value_col_prev='capacity_factor_prev',
                title='Coal Station Capacity Utilization Comparison',
                xlabel='Capacity Utilization (%)',
                xlim=(0, 100),
                color_latest=FLEXOKI_ACCENT['blue'],
            )

            return pn.pane.Plotly(fig, sizing_mode='stretch_width')

        except Exception as e:
            logger.error(f"Error creating utilization plot: {e}")
            import traceback
            traceback.print_exc()
            return pn.pane.Markdown(f"Error creating utilization plot: {e}")

    def create_content(self):
        """Create the full Coal analysis content"""
        try:
            # Force data refresh
            self.latest_data = None
            self.prev_data = None

            # Create both plots
            revenue_plot = self.create_revenue_plot()
            utilization_plot = self.create_utilization_plot()

            # Combine into a column layout
            content = pn.Column(
                pn.pane.Markdown("## Coal Station Analysis", styles={'margin': '10px 0'}),
                pn.pane.Markdown(
                    "*Comparing latest 12 months vs previous 12 months*",
                    styles={'color': '#aaa', 'font-size': '12px', 'margin-bottom': '20px'}
                ),
                pn.pane.Markdown("### Revenue by Station"),
                revenue_plot,
                pn.Spacer(height=30),
                pn.pane.Markdown("### Capacity Utilization by Station"),
                utilization_plot,
                sizing_mode='stretch_width'
            )

            return content

        except Exception as e:
            logger.error(f"Error creating coal content: {e}")
            return pn.pane.Markdown(f"Error creating coal analysis: {e}")


def create_coal_tab():
    """Factory function to create coal analysis tab content — refreshes on each view"""
    def _build():
        try:
            ui = CoalAnalysisUI()
            return ui.create_content()
        except Exception as e:
            logger.error(f"Error creating coal tab: {e}")
            return pn.pane.Markdown(f"Error creating coal analysis tab: {e}")
    return pn.panel(_build, loading_indicator=True)


class CoalEvolutionUI:
    """UI components for coal evolution analysis subtab"""

    # Station colors — Flexoki accent palette
    STATION_COLORS = {
        'Bayswater': FLEXOKI_ACCENT['red'],     # #AF3029
        'Tarong': FLEXOKI_ACCENT['blue'],        # #205EA6
        'Loy Yang B': FLEXOKI_ACCENT['green'],   # #66800B
    }

    def __init__(self):
        self.engine = CoalAnalysis()
        self.daily_cf_data = None
        self.hourly_latest = None
        self.hourly_historical = None

    def _load_data(self):
        """Load all data for evolution analysis (always refresh)"""
        self.daily_cf_data = None
        self.hourly_latest = None
        self.hourly_historical = None

        # Get daily capacity factor for all time
        self.daily_cf_data = self.engine.get_daily_capacity_factor(EVOLUTION_STATIONS)

        # Get hourly patterns for latest 12 months
        now = datetime.now()
        latest_start = now - timedelta(days=365)
        self.hourly_latest = self.engine.get_hourly_pattern(
            EVOLUTION_STATIONS, latest_start, now
        )

        # Get hourly patterns for 5 years ago (12 month period)
        historical_end = now - timedelta(days=5*365)
        historical_start = historical_end - timedelta(days=365)
        self.hourly_historical = self.engine.get_hourly_pattern(
            EVOLUTION_STATIONS, historical_start, historical_end
        )

    def _get_station_color(self, station_name: str) -> str:
        """Get color for a station, handling partial name matches"""
        for key, color in self.STATION_COLORS.items():
            if key.lower() in station_name.lower():
                return color
        return FLEXOKI_BASE[400]  # Default gray

    def _get_station_short_name(self, station_name: str) -> str:
        """Get short display name for station"""
        for key in self.STATION_COLORS.keys():
            if key.lower() in station_name.lower():
                return key
        return station_name

    def create_utilization_trend_plot(self):
        """Create capacity utilization trend chart with 90-day MA smoothing"""
        try:
            self._load_data()

            if self.daily_cf_data.empty:
                return pn.pane.Markdown("No data available for capacity utilization trend")

            fig = go.Figure()

            # Plot each station
            for station in self.daily_cf_data['station'].unique():
                station_data = self.daily_cf_data[
                    self.daily_cf_data['station'] == station
                ].sort_values('date').copy()

                if len(station_data) < 90:
                    continue

                # Calculate 90-day moving average
                station_data['ma_90'] = station_data['capacity_factor'].rolling(
                    window=90, min_periods=45
                ).mean()

                # Apply additional smoothing using uniform filter
                valid_data = station_data.dropna(subset=['ma_90'])
                if len(valid_data) > 30:
                    smoothed = uniform_filter1d(valid_data['ma_90'].values, size=30)

                    color = self._get_station_color(station)
                    short_name = self._get_station_short_name(station)

                    fig.add_trace(go.Scatter(
                        x=valid_data['date'],
                        y=smoothed,
                        mode='lines',
                        name=short_name,
                        line=dict(color=color, width=2),
                    ))

            fig.update_layout(
                title=dict(
                    text='Coal Station Capacity Utilization Over Time (90-day smoothed)',
                    font=dict(size=13, color=FLEXOKI_BLACK),
                ),
                xaxis=dict(
                    tickfont=dict(size=10, color=FLEXOKI_BLACK),
                    showgrid=False,
                    zeroline=False,
                ),
                yaxis=dict(
                    title=dict(text='Capacity Utilization (%)', font=dict(size=11, color=FLEXOKI_BLACK)),
                    range=[0, 100],
                    tickfont=dict(size=10, color=FLEXOKI_BLACK),
                    gridcolor=FLEXOKI_BASE[100],
                    gridwidth=0.5,
                    zeroline=False,
                ),
                paper_bgcolor=FLEXOKI_PAPER,
                plot_bgcolor=FLEXOKI_PAPER,
                font=dict(color=FLEXOKI_BLACK),
                height=400,
                autosize=True,
                margin=dict(l=10, r=20, t=40, b=50),
                legend=dict(
                    yanchor='top',
                    y=0.99,
                    xanchor='right',
                    x=0.99,
                    font=dict(size=10, color=FLEXOKI_BLACK),
                    bgcolor='rgba(0,0,0,0)',
                ),
            )

            # Attribution annotation
            fig.add_annotation(
                text='Design: ITK, Data: AEMO',
                xref='paper', yref='paper',
                x=1.0, y=-0.10,
                showarrow=False,
                font=dict(size=9, color=FLEXOKI_BASE[600]),
            )

            return pn.pane.Plotly(fig, sizing_mode='stretch_width')

        except Exception as e:
            logger.error(f"Error creating utilization trend plot: {e}")
            import traceback
            traceback.print_exc()
            return pn.pane.Markdown(f"Error creating utilization trend plot: {e}")

    def create_time_of_day_plot(self):
        """Create time of day pattern comparison chart"""
        try:
            self._load_data()

            if self.hourly_latest.empty:
                return pn.pane.Markdown("No data available for time of day pattern")

            # Create subplot for each station
            stations = self.hourly_latest['station'].unique()
            n_stations = len(stations)

            fig = make_subplots(
                rows=1, cols=n_stations,
                shared_yaxes=True,
                subplot_titles=[self._get_station_short_name(s) for s in stations],
                horizontal_spacing=0.05,
            )

            for idx, station in enumerate(stations):
                col = idx + 1
                color = self._get_station_color(station)

                # Latest data
                latest = self.hourly_latest[self.hourly_latest['station'] == station]
                if not latest.empty:
                    fig.add_trace(
                        go.Scatter(
                            x=latest['hour'],
                            y=latest['capacity_factor'],
                            mode='lines',
                            name='Latest 12m',
                            line=dict(color=color, width=2),
                            showlegend=(idx == n_stations - 1),
                        ),
                        row=1, col=col,
                    )

                # Historical data (5 years ago)
                if self.hourly_historical is not None and not self.hourly_historical.empty:
                    historical = self.hourly_historical[
                        self.hourly_historical['station'] == station
                    ]
                    if not historical.empty:
                        fig.add_trace(
                            go.Scatter(
                                x=historical['hour'],
                                y=historical['capacity_factor'],
                                mode='lines',
                                name='5 years ago',
                                line=dict(color=color, width=2, dash='dash'),
                                opacity=0.6,
                                showlegend=(idx == n_stations - 1),
                            ),
                            row=1, col=col,
                        )

                # Per-subplot axis styling
                fig.update_xaxes(
                    range=[0, 23],
                    tickvals=[0, 6, 12, 18, 23],
                    title_text='Hour of Day' if idx == n_stations // 2 else '',
                    tickfont=dict(size=9, color=FLEXOKI_BLACK),
                    showgrid=False,
                    zeroline=False,
                    row=1, col=col,
                )
                fig.update_yaxes(
                    range=[0, 100],
                    gridcolor=FLEXOKI_BASE[100],
                    gridwidth=0.5,
                    zeroline=False,
                    tickfont=dict(size=9, color=FLEXOKI_BLACK),
                    row=1, col=col,
                )

            # First y-axis label
            fig.update_yaxes(
                title_text='Capacity Factor (%)',
                title_font=dict(size=10, color=FLEXOKI_BLACK),
                row=1, col=1,
            )

            fig.update_layout(
                title=dict(
                    text='Time of Day Dispatch Pattern',
                    font=dict(size=13, color=FLEXOKI_BLACK),
                ),
                paper_bgcolor=FLEXOKI_PAPER,
                plot_bgcolor=FLEXOKI_PAPER,
                font=dict(color=FLEXOKI_BLACK),
                height=350,
                autosize=True,
                margin=dict(l=10, r=20, t=50, b=60),
                legend=dict(
                    orientation='h',
                    yanchor='top',
                    y=-0.18,
                    xanchor='center',
                    x=0.5,
                    font=dict(size=10, color=FLEXOKI_BLACK),
                    bgcolor='rgba(0,0,0,0)',
                ),
            )

            # Attribution annotation
            fig.add_annotation(
                text='Design: ITK, Data: AEMO',
                xref='paper', yref='paper',
                x=1.0, y=-0.25,
                showarrow=False,
                font=dict(size=9, color=FLEXOKI_BASE[600]),
            )

            return pn.pane.Plotly(fig, sizing_mode='stretch_width')

        except Exception as e:
            logger.error(f"Error creating time of day plot: {e}")
            import traceback
            traceback.print_exc()
            return pn.pane.Markdown(f"Error creating time of day plot: {e}")

    def create_content(self):
        """Create the full Coal Evolution content"""
        try:
            # Force data refresh
            self.daily_cf_data = None
            self.hourly_latest = None
            self.hourly_historical = None

            trend_plot = self.create_utilization_trend_plot()
            tod_plot = self.create_time_of_day_plot()

            content = pn.Column(
                pn.pane.Markdown("## Coal Station Evolution", styles={'margin': '10px 0'}),
                pn.pane.Markdown(
                    f"*Tracking {', '.join(EVOLUTION_STATIONS)}*",
                    styles={'color': '#aaa', 'font-size': '12px', 'margin-bottom': '20px'}
                ),
                pn.pane.Markdown("### Long-term Capacity Utilization"),
                trend_plot,
                pn.Spacer(height=20),
                pn.pane.Markdown("### Time of Day Dispatch Pattern"),
                pn.pane.Markdown(
                    "*Comparing latest 12 months (solid) vs 5 years ago (dashed)*",
                    styles={'color': '#aaa', 'font-size': '11px', 'margin-bottom': '10px'}
                ),
                tod_plot,
                sizing_mode='stretch_width'
            )

            return content

        except Exception as e:
            logger.error(f"Error creating coal evolution content: {e}")
            return pn.pane.Markdown(f"Error creating coal evolution analysis: {e}")


def create_coal_evolution_tab():
    """Factory function to create coal evolution tab content — refreshes on each view"""
    def _build():
        try:
            ui = CoalEvolutionUI()
            return ui.create_content()
        except Exception as e:
            logger.error(f"Error creating coal evolution tab: {e}")
            return pn.pane.Markdown(f"Error creating coal evolution tab: {e}")
    return pn.panel(_build, loading_indicator=True)
