"""
Coal Station Analysis - Revenue and capacity utilization analysis for coal stations.

Provides comparison between latest 12 months and previous 12 months for:
- Total revenue by station
- Capacity utilization by station
"""

import pandas as pd
import numpy as np
import duckdb
import pickle
import panel as pn
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend for Panel
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from scipy.ndimage import uniform_filter1d

from ..shared.logging_config import get_logger
from ..shared.config import config

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
            # Use 30-minute SCADA data for longer historical coverage (back to 2020)
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

            # Store the interval duration for energy calculations (30 min = 0.5 hours)
            self.interval_hours = 0.5

            logger.info("Created SCADA (30min) and prices views")

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
            DataFrame with hour, station, and avg_generation columns
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
        """Load comparison data if not already loaded"""
        if self.latest_data is None:
            self.latest_data, self.prev_data = self.engine.get_comparison_data()

    def _create_grouped_hbar_figure(self, merged, value_col_latest, value_col_prev,
                                      title, xlabel, xlim=None, color_latest='#2ca02c'):
        """Create a matplotlib grouped horizontal bar chart for comparison data"""
        n_stations = len(merged)
        fig_height = max(4, n_stations * 0.35)

        # Create figure with dark background
        fig, ax = plt.subplots(figsize=(8, fig_height))
        fig.patch.set_facecolor('#282a36')
        ax.set_facecolor('#282a36')

        # Y positions for stations
        y_positions = range(n_stations)
        bar_height = 0.35

        # Plot bars for latest period
        bars_latest = ax.barh(
            [y + bar_height/2 for y in y_positions],
            merged[value_col_latest],
            height=bar_height,
            color=color_latest,
            label='Latest 12m'
        )

        # Plot bars for previous period (gray)
        bars_prev = ax.barh(
            [y - bar_height/2 for y in y_positions],
            merged[value_col_prev],
            height=bar_height,
            color='#7f7f7f',
            label='Previous 12m'
        )

        # Set y-axis labels (station names)
        ax.set_yticks(list(y_positions))
        ax.set_yticklabels(merged['station'].tolist(), fontsize=9)

        # Set x-axis limits
        if xlim:
            ax.set_xlim(xlim)
        else:
            ax.set_xlim(left=0)

        # Set y-axis limits with padding
        ax.set_ylim(-0.5, n_stations - 0.5)

        # Remove spines
        for spine in ax.spines.values():
            spine.set_visible(False)

        # Remove ticks
        ax.tick_params(axis='both', length=0)

        # Style labels for dark theme
        ax.tick_params(axis='x', colors='#f8f8f2', labelsize=9)
        ax.tick_params(axis='y', colors='#f8f8f2')
        ax.xaxis.label.set_color('#f8f8f2')

        # Title and labels
        ax.set_title(title, color='#f8f8f2', fontsize=12, pad=8)
        ax.set_xlabel(xlabel, color='#f8f8f2', fontsize=10)

        # Legend - place below the chart
        ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.08),
                 ncol=2, facecolor='#282a36', edgecolor='none',
                 labelcolor='#f8f8f2', fontsize=9)

        plt.tight_layout()
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

            # Create matplotlib figure
            fig = self._create_grouped_hbar_figure(
                merged,
                value_col_latest='revenue_millions_latest',
                value_col_prev='revenue_millions_prev',
                title='Coal Station Revenue Comparison',
                xlabel='Revenue ($M)',
                color_latest='#2ca02c'  # Green
            )

            # Add attribution
            attribution = pn.pane.HTML(
                "<div style='text-align: right; font-size: 9pt; color: #6272a4; margin-top: 5px;'>"
                "Design: ITK, Data: AEMO"
                "</div>"
            )

            return pn.Column(pn.pane.Matplotlib(fig, tight=True), attribution)

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

            # Create matplotlib figure with xlim 0-100%
            fig = self._create_grouped_hbar_figure(
                merged,
                value_col_latest='capacity_factor_latest',
                value_col_prev='capacity_factor_prev',
                title='Coal Station Capacity Utilization Comparison',
                xlabel='Capacity Utilization (%)',
                xlim=(0, 100),
                color_latest='#1f77b4'  # Blue
            )

            # Add attribution
            attribution = pn.pane.HTML(
                "<div style='text-align: right; font-size: 9pt; color: #6272a4; margin-top: 5px;'>"
                "Design: ITK, Data: AEMO"
                "</div>"
            )

            return pn.Column(pn.pane.Matplotlib(fig, tight=True), attribution)

        except Exception as e:
            logger.error(f"Error creating utilization plot: {e}")
            import traceback
            traceback.print_exc()
            return pn.pane.Markdown(f"Error creating utilization plot: {e}")

    def create_content(self):
        """Create the full Coal analysis content"""
        try:
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
    """Factory function to create coal analysis tab content"""
    try:
        ui = CoalAnalysisUI()
        return ui.create_content()
    except Exception as e:
        logger.error(f"Error creating coal tab: {e}")
        return pn.pane.Markdown(f"Error creating coal analysis tab: {e}")


class CoalEvolutionUI:
    """UI components for coal evolution analysis subtab"""

    # Station colors for consistency
    STATION_COLORS = {
        'Bayswater': '#e41a1c',      # Red
        'Tarong': '#377eb8',          # Blue
        'Loy Yang B': '#4daf4a',      # Green
    }

    def __init__(self):
        self.engine = CoalAnalysis()
        self.daily_cf_data = None
        self.hourly_latest = None
        self.hourly_historical = None

    def _load_data(self):
        """Load all data for evolution analysis"""
        if self.daily_cf_data is None:
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
        return '#999999'  # Default gray

    def _get_station_short_name(self, station_name: str) -> str:
        """Get short display name for station"""
        for key in self.STATION_COLORS.keys():
            if key.lower() in station_name.lower():
                return key
        return station_name

    def create_utilization_trend_plot(self):
        """Create capacity utilization trend chart with 90-day MA and LOESS smoothing"""
        try:
            self._load_data()

            if self.daily_cf_data.empty:
                return pn.pane.Markdown("No data available for capacity utilization trend")

            fig, ax = plt.subplots(figsize=(10, 5))
            fig.patch.set_facecolor('#282a36')
            ax.set_facecolor('#282a36')

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

                # Apply additional smoothing using uniform filter (similar to LOESS effect)
                valid_data = station_data.dropna(subset=['ma_90'])
                if len(valid_data) > 30:
                    # Apply light smoothing
                    smoothed = uniform_filter1d(valid_data['ma_90'].values, size=30)

                    color = self._get_station_color(station)
                    short_name = self._get_station_short_name(station)

                    ax.plot(
                        valid_data['date'].values,
                        smoothed,
                        color=color,
                        linewidth=2,
                        label=short_name
                    )

            # Style the plot
            ax.set_ylim(0, 100)
            ax.set_ylabel('Capacity Utilization (%)', color='#f8f8f2', fontsize=10)
            ax.set_xlabel('')
            ax.set_title('Coal Station Capacity Utilization Over Time (90-day smoothed)',
                        color='#f8f8f2', fontsize=12, pad=10)

            # Remove spines
            for spine in ax.spines.values():
                spine.set_visible(False)

            ax.tick_params(axis='both', length=0, colors='#f8f8f2', labelsize=9)
            ax.yaxis.grid(True, color='#44475a', alpha=0.3, linestyle='-')

            # Legend
            ax.legend(loc='upper right', facecolor='#282a36', edgecolor='none',
                     labelcolor='#f8f8f2', fontsize=9)

            plt.tight_layout()

            # Attribution
            attribution = pn.pane.HTML(
                "<div style='text-align: right; font-size: 9pt; color: #6272a4; margin-top: 5px;'>"
                "Design: ITK, Data: AEMO"
                "</div>"
            )

            return pn.Column(pn.pane.Matplotlib(fig, tight=True), attribution)

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

            fig, axes = plt.subplots(1, n_stations, figsize=(10, 4), sharey=True)
            fig.patch.set_facecolor('#282a36')

            if n_stations == 1:
                axes = [axes]

            for idx, station in enumerate(stations):
                ax = axes[idx]
                ax.set_facecolor('#282a36')

                color = self._get_station_color(station)
                short_name = self._get_station_short_name(station)

                # Latest data
                latest = self.hourly_latest[self.hourly_latest['station'] == station]
                if not latest.empty:
                    ax.plot(
                        latest['hour'].values,
                        latest['capacity_factor'].values,
                        color=color,
                        linewidth=2,
                        label='Latest 12m'
                    )

                # Historical data (5 years ago)
                if not self.hourly_historical.empty:
                    historical = self.hourly_historical[
                        self.hourly_historical['station'] == station
                    ]
                    if not historical.empty:
                        ax.plot(
                            historical['hour'].values,
                            historical['capacity_factor'].values,
                            color=color,
                            linewidth=2,
                            linestyle='--',
                            alpha=0.6,
                            label='5 years ago'
                        )

                ax.set_title(short_name, color='#f8f8f2', fontsize=11)
                ax.set_xlabel('Hour of Day', color='#f8f8f2', fontsize=9)
                if idx == 0:
                    ax.set_ylabel('Capacity Factor (%)', color='#f8f8f2', fontsize=9)

                ax.set_xlim(0, 23)
                ax.set_ylim(0, 100)
                ax.set_xticks([0, 6, 12, 18, 23])

                # Remove spines
                for spine in ax.spines.values():
                    spine.set_visible(False)

                ax.tick_params(axis='both', length=0, colors='#f8f8f2', labelsize=8)
                ax.yaxis.grid(True, color='#44475a', alpha=0.3, linestyle='-')

                if idx == n_stations - 1:
                    ax.legend(loc='upper right', facecolor='#282a36', edgecolor='none',
                             labelcolor='#f8f8f2', fontsize=8)

            fig.suptitle('Time of Day Dispatch Pattern', color='#f8f8f2', fontsize=12, y=1.02)
            plt.tight_layout()

            # Attribution
            attribution = pn.pane.HTML(
                "<div style='text-align: right; font-size: 9pt; color: #6272a4; margin-top: 5px;'>"
                "Design: ITK, Data: AEMO"
                "</div>"
            )

            return pn.Column(pn.pane.Matplotlib(fig, tight=True), attribution)

        except Exception as e:
            logger.error(f"Error creating time of day plot: {e}")
            import traceback
            traceback.print_exc()
            return pn.pane.Markdown(f"Error creating time of day plot: {e}")

    def create_content(self):
        """Create the full Coal Evolution content"""
        try:
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
    """Factory function to create coal evolution tab content"""
    try:
        ui = CoalEvolutionUI()
        return ui.create_content()
    except Exception as e:
        logger.error(f"Error creating coal evolution tab: {e}")
        return pn.pane.Markdown(f"Error creating coal evolution tab: {e}")
