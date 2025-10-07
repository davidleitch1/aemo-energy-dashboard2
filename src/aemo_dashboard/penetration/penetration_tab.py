"""
Penetration tab implementation for the AEMO Energy Dashboard - Version 2.
Fixed to properly handle 30-minute data without unnecessary conversions.
"""
import pandas as pd
import numpy as np
import panel as pn
import hvplot.pandas
import holoviews as hv
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple
from pathlib import Path
import os

from aemo_dashboard.shared.logging_config import get_logger
from aemo_dashboard.shared.smoothing import apply_ewm_smoothing
from aemo_dashboard.generation.generation_query_manager import GenerationQueryManager
from aemo_dashboard.shared.config import Config

logger = get_logger(__name__)

class PenetrationTab:
    """Renewable energy penetration analysis tab."""
    
    def __init__(self):
        """Initialize the penetration tab."""
        self.query_manager = GenerationQueryManager()
        self.config = Config()
        
        # Interactive widgets
        self.region_select = pn.widgets.Select(
            name='Region',
            value='NEM',
            options=['NEM', 'NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1'],
            width=150
        )
        
        self.fuel_select = pn.widgets.Select(
            name='Fuel Type',
            value='VRE',
            options=['VRE', 'Solar', 'Wind', 'Rooftop'],
            width=150
        )
        
        # Bind update method to widgets
        self.region_select.param.watch(self._update_charts, 'value')
        self.fuel_select.param.watch(self._update_charts, 'value')
        
        # Chart panes - use object=None to ensure clean initialization
        self.vre_production_pane = pn.pane.HoloViews(
            object=None,
            sizing_mode='stretch_width',
            height=500,
            linked_axes=False  # Prevent axis linking between updates
        )
        
        self.vre_by_fuel_pane = pn.pane.HoloViews(
            object=None,
            sizing_mode='stretch_width',
            height=500,
            linked_axes=False
        )
        
        self.thermal_vs_renewables_pane = pn.pane.HoloViews(
            object=None,
            sizing_mode='stretch_width',
            height=500,
            linked_axes=False
        )
        
        # Initialize charts
        self._update_charts()
        
    def _update_charts(self, event=None):
        """Update all charts based on current selections."""
        try:
            # Clear existing plots to prevent series accumulation
            self.vre_production_pane.object = None
            self.vre_by_fuel_pane.object = None
            self.thermal_vs_renewables_pane.object = None
            
            # Update VRE production chart
            self.vre_production_pane.object = self._create_vre_production_chart()
            # Update VRE by fuel chart
            self.vre_by_fuel_pane.object = self._create_vre_by_fuel_chart()
            # Update thermal vs renewables chart
            self.thermal_vs_renewables_pane.object = self._create_thermal_vs_renewables_chart()
        except Exception as e:
            logger.error(f"Error updating penetration charts: {e}")
            # Create error plots instead of Markdown
            empty_df = pd.DataFrame({'x': [0], 'y': [0]})
            error_plot = empty_df.hvplot.line(x='x', y='y', label='Error').opts(
                width=700, height=400, bgcolor='#2B2B3B', 
                title=f'Error loading data: {str(e)}'
            )
            self.vre_production_pane.object = error_plot
            self.vre_by_fuel_pane.object = error_plot
            # Thermal chart is wider
            error_plot_wide = empty_df.hvplot.line(x='x', y='y', label='Error').opts(
                width=1440, height=400, bgcolor='#2B2B3B', 
                title=f'Error loading data: {str(e)}'
            )
            self.thermal_vs_renewables_pane.object = error_plot_wide
    
    def _load_rooftop_30min(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """
        Load rooftop data directly from parquet file without conversion to 5-minute.
        """
        # Get rooftop file path
        rooftop_file = self.config.rooftop_solar_file
        
        if not rooftop_file or not Path(rooftop_file).exists():
            logger.warning(f"Rooftop file not found: {rooftop_file}")
            return pd.DataFrame()
        
        # Load parquet file directly
        df = pd.read_parquet(rooftop_file)
        
        # Filter date range
        df['settlementdate'] = pd.to_datetime(df['settlementdate'])
        df = df[(df['settlementdate'] >= start_date) & (df['settlementdate'] <= end_date)]
        
        # Check format - should have regionid column for long format
        if 'regionid' in df.columns:
            # Long format - pivot to wide
            df_wide = df.pivot(
                index='settlementdate',
                columns='regionid', 
                values='power'
            ).reset_index()
            return df_wide
        else:
            # Already in wide format
            return df
    
    def _get_generation_data(self, years: List[int], months_only_first_year: int = None) -> pd.DataFrame:
        """
        Get generation data including rooftop for specified years.
        All data at 30-minute resolution.
        
        Args:
            years: List of years to load
            months_only_first_year: If specified, only load last N months of first year
        """
        all_data = []
        
        for i, year in enumerate(years):
            if i == 0 and months_only_first_year is not None:
                # For first year, only load last N months
                start_date = datetime(year, 12 - months_only_first_year + 1, 1)
            else:
                start_date = datetime(year, 1, 1)
            end_date = datetime(year, 12, 31, 23, 59, 59)
            
            # Get generation data at 30-minute resolution
            data = self.query_manager.query_generation_by_fuel(
                start_date=start_date,
                end_date=end_date,
                region=self.region_select.value,
                resolution='30min'
            )
            
            if not data.empty:
                logger.info(f"Year {year}: Retrieved {len(data)} rows of generation data")
                all_data.append(data)
                
            # Load rooftop data at 30-minute resolution
            try:
                rooftop_data = self._load_rooftop_30min(start_date, end_date)
                
                if not rooftop_data.empty:
                    # Convert to long format matching generation data
                    if self.region_select.value == 'NEM':
                        # Sum all regions
                        region_cols = [col for col in rooftop_data.columns if col != 'settlementdate']
                        rooftop_long = pd.DataFrame({
                            'settlementdate': rooftop_data['settlementdate'],
                            'fuel_type': 'Rooftop',
                            'total_generation_mw': rooftop_data[region_cols].sum(axis=1)
                        })
                    else:
                        # Get specific region
                        if self.region_select.value in rooftop_data.columns:
                            rooftop_long = pd.DataFrame({
                                'settlementdate': rooftop_data['settlementdate'],
                                'fuel_type': 'Rooftop',
                                'total_generation_mw': rooftop_data[self.region_select.value]
                            })
                        else:
                            logger.warning(f"Region {self.region_select.value} not found in rooftop data")
                            continue
                    
                    logger.info(f"Year {year}: Retrieved {len(rooftop_long)} rows of rooftop data")
                    all_data.append(rooftop_long)
                    
            except Exception as e:
                logger.warning(f"Could not load rooftop data for year {year}: {e}")
        
        if not all_data:
            return pd.DataFrame()
            
        return pd.concat(all_data, ignore_index=True)
    
    def _create_vre_production_chart(self):
        """Create the VRE production annualised chart."""
        # Get current year and two previous years
        current_year = datetime.now().year
        years_to_display = [current_year - 2, current_year - 1, current_year]
        
        # Also load 2 months from the year before to calculate proper rolling average
        earliest_year = min(years_to_display)
        years_to_load = [earliest_year - 1] + years_to_display
        
        # Get generation data including buffer year (only last 2 months for smoothing)
        logger.info(f"Fetching generation data for years: {years_to_load} (displaying {years_to_display})")
        df = self._get_generation_data(years_to_load, months_only_first_year=2)
        
        if df.empty:
            logger.warning("No generation data returned")
            # Return empty plot instead of Markdown
            import hvplot.pandas
            empty_df = pd.DataFrame({'x': [0], 'y': [0]})
            return empty_df.hvplot.line(x='x', y='y', label='No data available').opts(
                width=700, height=400, bgcolor='#2B2B3B', 
                title=f'{self.fuel_select.value} production annualised over last 30 days - {self.region_select.value} - No data available'
            )
        
        logger.info(f"Retrieved {len(df)} total rows of data")
        
        # Filter for selected fuel types
        if self.fuel_select.value == 'VRE':
            fuel_filter = ['Wind', 'Solar', 'Rooftop']
            # For VRE, sum all types together
            df_filtered = df[df['fuel_type'].isin(fuel_filter)].copy()
        else:
            # For individual fuel, filter to just that type
            fuel_filter = [self.fuel_select.value]
            df_filtered = df[df['fuel_type'] == self.fuel_select.value].copy()
        
        if df_filtered.empty:
            # Return empty plot instead of Markdown
            empty_df = pd.DataFrame({'x': [0], 'y': [0]})
            return empty_df.hvplot.line(x='x', y='y', label='No data').opts(
                width=700, height=400, bgcolor='#2B2B3B', 
                title=f'{self.fuel_select.value} production annualised over last 30 days - {self.region_select.value} - No data'
            )
        
        # Ensure datetime
        df_filtered['settlementdate'] = pd.to_datetime(df_filtered['settlementdate'])
        df_filtered['year'] = df_filtered['settlementdate'].dt.year
        df_filtered['dayofyear'] = df_filtered['settlementdate'].dt.dayofyear
        
        # Sum across fuel types for each timestamp (all data is 30-minute)
        hourly_sum = df_filtered.groupby(['settlementdate', 'year', 'dayofyear'])['total_generation_mw'].sum().reset_index()
        
        # Sort by timestamp
        hourly_sum = hourly_sum.sort_values('settlementdate')
        
        # Apply 30-day rolling average on 30-minute data (30 days * 48 periods = 1440)
        hourly_sum['mw_rolling_30d'] = hourly_sum['total_generation_mw'].rolling(
            window=1440, center=False, min_periods=720
        ).mean()
        
        # Now get daily samples (take noon values for each day)
        hourly_sum['date'] = hourly_sum['settlementdate'].dt.date
        hourly_sum['hour'] = hourly_sum['settlementdate'].dt.hour
        
        # Take the 12:00 reading for each day (or closest available)
        # Group and get the data without the grouping columns in the lambda
        grouped = hourly_sum.groupby(['year', 'dayofyear'])
        daily_data = grouped.apply(
            lambda x: x.iloc[x['hour'].sub(12).abs().argmin()] if len(x) > 0 else None
        ).reset_index(drop=True)
        
        logger.info(f"Daily data shape after 30-day rolling average: {daily_data.shape}")
        
        # Prepare data for plotting
        colors = {
            years_to_display[0]: '#5DADE2',  # Light blue for oldest year
            years_to_display[1]: '#F39C12',  # Orange for middle year
            years_to_display[2]: '#58D68D'   # Green for current year
        }
        
        plots = []
        for year in years_to_display:
            year_data = daily_data[daily_data['year'] == year].copy()
            
            if not year_data.empty:
                # Sort by day of year
                year_data = year_data.sort_values('dayofyear')
                
                logger.info(f"Year {year}: {len(year_data)} days of data")
                
                # Use the pre-calculated 30-day rolling average
                # Fill any NaN values at the beginning with actual values
                year_data['mw_smoothed'] = year_data['mw_rolling_30d'].fillna(year_data['total_generation_mw'])
                
                # Annualise: MW * 24 hours * 365 days / 1,000,000 = TWh
                year_data['twh_annualised'] = year_data['mw_smoothed'] * 24 * 365 / 1_000_000
                
                logger.info(f"Year {year} TWh range: {year_data['twh_annualised'].min():.2f} - {year_data['twh_annualised'].max():.2f}")
                
                # Create line plot
                plot = year_data.hvplot.line(
                    x='dayofyear',
                    y='twh_annualised',
                    label=str(year),
                    color=colors[year],
                    line_width=2
                )
                plots.append(plot)
            else:
                logger.warning(f"No data for year {year}")
        
        if not plots:
            # Return empty plot instead of Markdown
            empty_df = pd.DataFrame({'x': [0], 'y': [0]})
            return empty_df.hvplot.line(x='x', y='y', label='No data').opts(
                width=700, height=400, bgcolor='#2B2B3B', 
                title=f'{self.fuel_select.value} production annualised over last 30 days - {self.region_select.value} - No data for criteria'
            )
        
        # Combine plots
        combined_plot = plots[0]
        for plot in plots[1:]:
            combined_plot = combined_plot * plot
        
        # Style the plot to match screenshot
        final_plot = combined_plot.opts(
            width=700,
            height=400,
            bgcolor='#2B2B3B',
            title=f'{self.fuel_select.value} production annualised over last 30 days - {self.region_select.value}',
            xlabel='day of year',
            ylabel='TWh',
            fontsize={'title': 14, 'labels': 12, 'xticks': 10, 'yticks': 10},
            show_grid=False,
            toolbar='above',
            legend_position='bottom_left',
            framewise=True,
            yformatter='%.0f',
            # Ensure Y-axis auto-scales to data
            ylim=(None, None)
        )
        
        return final_plot
    
    def _create_vre_by_fuel_chart(self):
        """Create the VRE production by fuel type chart (2018-present) using Plotly."""
        import plotly.graph_objects as go

        # Get data from 2018 onwards
        start_year = 2018
        current_year = datetime.now().year
        years = list(range(start_year, current_year + 1))

        logger.info(f"Creating VRE by fuel chart for years: {years}")

        # Get generation data
        df = self._get_generation_data(years, months_only_first_year=None)

        if df.empty:
            # Return empty plot
            fig = go.Figure()
            fig.add_annotation(
                text="No data available",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=14, color='#f8f8f2')
            )
            fig.update_layout(
                width=700, height=400,
                paper_bgcolor='#2B2B3B', plot_bgcolor='#2B2B3B',
                title='VRE production by fuel rolling 30 day avg - No data available'
            )
            return pn.pane.Plotly(fig, sizing_mode='fixed', width=700, height=400)

        # Filter for VRE fuels only and NEM-wide data
        df_vre = df[df['fuel_type'].isin(['Wind', 'Solar', 'Rooftop'])].copy()

        if df_vre.empty:
            # Return empty plot
            fig = go.Figure()
            fig.add_annotation(
                text="No VRE data available",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=14, color='#f8f8f2')
            )
            fig.update_layout(
                width=700, height=400,
                paper_bgcolor='#2B2B3B', plot_bgcolor='#2B2B3B',
                title='VRE production by fuel rolling 30 day avg - No VRE data available'
            )
            return pn.pane.Plotly(fig, sizing_mode='fixed', width=700, height=400)

        # Ensure datetime
        df_vre['settlementdate'] = pd.to_datetime(df_vre['settlementdate'])
        df_vre = df_vre.sort_values('settlementdate')

        # Group by fuel type and timestamp
        fuel_data = df_vre.groupby(['settlementdate', 'fuel_type'])['total_generation_mw'].sum().reset_index()

        # Create Plotly figure
        fig = go.Figure()

        # Colors for each fuel type
        colors = {
            'Rooftop': '#5DADE2',  # Light blue
            'Solar': '#F39C12',    # Orange
            'Wind': '#58D68D'      # Green
        }

        # Add line for each fuel type
        for fuel in ['Rooftop', 'Solar', 'Wind']:
            fuel_df = fuel_data[fuel_data['fuel_type'] == fuel].copy()

            if not fuel_df.empty:
                # Apply 30-day rolling average (1440 periods)
                fuel_df['mw_rolling_30d'] = fuel_df['total_generation_mw'].rolling(
                    window=1440, center=False, min_periods=720
                ).mean()

                # Annualise
                fuel_df['twh_annualised'] = fuel_df['mw_rolling_30d'] * 24 * 365 / 1_000_000

                # Add trace
                fig.add_trace(go.Scatter(
                    x=fuel_df['settlementdate'],
                    y=fuel_df['twh_annualised'],
                    name=f'nem_{fuel.lower()}',
                    mode='lines',
                    line=dict(color=colors[fuel], width=2),
                    hovertemplate='<b>%{fullData.name}</b><br>%{y:.0f} TWh<extra></extra>'
                ))

        # Apply layout styling with Dracula theme
        fig.update_layout(
            title=dict(
                text=f'VRE production by fuel rolling 30 day avg - {self.region_select.value}<br><sub>© ITK</sub>',
                font=dict(size=14, color='#f8f8f2')
            ),
            xaxis_title='date',
            yaxis_title='TWh annualised',
            width=700,
            height=400,
            paper_bgcolor='#2B2B3B',
            plot_bgcolor='#2B2B3B',
            font=dict(color='#f8f8f2', size=12),
            xaxis=dict(
                gridcolor='#44475a',
                showgrid=False,
                linecolor='#6272a4'
            ),
            yaxis=dict(
                gridcolor='#44475a',
                showgrid=False,
                linecolor='#6272a4',
                tickformat='.0f'
            ),
            legend=dict(
                bgcolor='rgba(43, 43, 59, 0.8)',
                bordercolor='#6272a4',
                borderwidth=1,
                orientation='v',
                yanchor='top',
                y=0.98,
                xanchor='left',
                x=0.02
            ),
            hovermode='x unified',
            margin=dict(l=60, r=20, t=60, b=40)
        )

        return pn.pane.Plotly(fig, sizing_mode='fixed', width=700, height=400)
    
    def _create_thermal_vs_renewables_chart(self):
        """Create the thermal vs renewables chart with 180-day moving average."""
        # Get data from 2018 onwards (to match the screenshot)
        start_year = 2018
        current_year = datetime.now().year
        years = list(range(start_year, current_year + 1))
        
        logger.info(f"Creating thermal vs renewables chart for years: {years}")
        
        # Get generation data for selected region
        df = self._get_generation_data(years, months_only_first_year=None)
        
        if df.empty:
            # Return empty plot instead of Markdown
            empty_df = pd.DataFrame({'x': [0], 'y': [0]})
            return empty_df.hvplot.line(x='x', y='y', label='No data').opts(
                width=1440, height=400, bgcolor='#2B2B3B', 
                title='Thermal v Renewables 180 day annualised - No data available'
            )
        
        # Define fuel type categories
        renewable_fuels = ['Wind', 'Solar', 'Rooftop', 'Water']  # Water = Hydro
        thermal_fuels = ['Coal', 'CCGT', 'OCGT', 'Gas other']  # All gas types
        
        # Filter and categorize
        df['category'] = df['fuel_type'].apply(
            lambda x: 'renewable' if x in renewable_fuels else ('thermal' if x in thermal_fuels else 'other')
        )
        
        # Keep only renewable and thermal
        df_filtered = df[df['category'].isin(['renewable', 'thermal'])].copy()
        
        if df_filtered.empty:
            # Return empty plot instead of Markdown
            empty_df = pd.DataFrame({'x': [0], 'y': [0]})
            return empty_df.hvplot.line(x='x', y='y', label='No data').opts(
                width=1440, height=400, bgcolor='#2B2B3B', 
                title='Thermal v Renewables 180 day annualised - No thermal/renewable data'
            )
        
        # Ensure datetime and sort
        df_filtered['settlementdate'] = pd.to_datetime(df_filtered['settlementdate'])
        df_filtered = df_filtered.sort_values('settlementdate')
        
        # Group by timestamp and category
        category_data = df_filtered.groupby(['settlementdate', 'category'])['total_generation_mw'].sum().reset_index()
        
        # Pivot to get renewable and thermal columns
        pivot_data = category_data.pivot(index='settlementdate', columns='category', values='total_generation_mw').fillna(0)
        
        # Apply 180-day rolling average (180 days * 48 periods = 8640)
        # Or use EWM with equivalent span
        window_days = 180
        window_periods = window_days * 48
        
        # Using rolling average to match the screenshot
        pivot_data['renewable_ma'] = pivot_data['renewable'].rolling(
            window=window_periods, center=False, min_periods=window_periods//2
        ).mean()
        
        pivot_data['thermal_ma'] = pivot_data['thermal'].rolling(
            window=window_periods, center=False, min_periods=window_periods//2
        ).mean()
        
        # Annualise
        pivot_data['renewable_twh'] = pivot_data['renewable_ma'] * 24 * 365 / 1_000_000
        pivot_data['thermal_twh'] = pivot_data['thermal_ma'] * 24 * 365 / 1_000_000
        
        # Reset index for plotting
        plot_data = pivot_data.reset_index()
        
        # Create plots
        colors = {
            'renewable': '#5DADE2',  # Light blue
            'coal+gas': '#F39C12'    # Orange
        }
        
        # Renewable plot
        renewable_plot = plot_data.hvplot.line(
            x='settlementdate',
            y='renewable_twh',
            label='renewable',
            color=colors['renewable'],
            line_width=2
        )
        
        # Thermal plot
        thermal_plot = plot_data.hvplot.line(
            x='settlementdate',
            y='thermal_twh',
            label='coal+gas',
            color=colors['coal+gas'],
            line_width=2
        )
        
        # Combine plots
        combined_plot = renewable_plot * thermal_plot
        
        # Style the plot
        final_plot = combined_plot.opts(
            width=1440,
            height=400,
            bgcolor='#2B2B3B',
            title=f'Thermal v Renewables 180 day annualised - {self.region_select.value}',
            xlabel='',
            ylabel='TWh',
            fontsize={'title': 14, 'labels': 12, 'xticks': 10, 'yticks': 10},
            show_grid=False,
            toolbar='above',
            legend_position='top_right',
            framewise=True,
            yformatter='%.0f'
            # ylim removed to allow auto-scaling
        )
        
        return final_plot
    
    def create_layout(self) -> pn.Column:
        """Create the full penetration tab layout."""
        # Source attribution
        source_text_vre = pn.pane.HTML(
            '<div style="text-align: right; color: #888; font-size: 10px; margin-right: 10px; margin-top: -10px;">© ITK</div>',
            height=15
        )
        source_text_fuel = pn.pane.HTML(
            '<div style="text-align: right; color: #888; font-size: 10px; margin-right: 10px; margin-top: -10px;">© ITK</div>',
            height=15
        )
        source_text_thermal = pn.pane.HTML(
            '<div style="text-align: right; color: #888; font-size: 10px; margin-right: 10px; margin-top: -10px;">© ITK</div>',
            height=15
        )
        
        # Create controls row
        controls_row = pn.Row(
            self.region_select,
            self.fuel_select,
            width_policy='min',
            margin=(10, 0)
        )
        
        # Create top row with two charts side by side
        # Each chart in its own column to ensure complete independence
        vre_chart_column = pn.Column(
            self.vre_production_pane,
            source_text_vre,
            width=720,
            margin=(0, 10, 0, 0)
        )
        
        fuel_chart_column = pn.Column(
            self.vre_by_fuel_pane,
            source_text_fuel,
            width=720,
            margin=(0, 0, 0, 10)
        )
        
        top_row = pn.Row(
            vre_chart_column,
            fuel_chart_column,
            sizing_mode='fixed',
            align='start'
        )
        
        # Create bottom row with thermal vs renewables chart
        bottom_row = pn.Column(
            self.thermal_vs_renewables_pane,
            source_text_thermal,
            width=1460,  # Full width for bottom chart
            margin=(20, 0, 0, 0)
        )
        
        # Main layout
        layout = pn.Column(
            pn.pane.Markdown("## Renewable Energy Penetration Analysis", 
                           styles={'color': '#008B8B'}),
            controls_row,
            top_row,
            bottom_row,
            sizing_mode='fixed',
            width=1480
        )
        
        return layout