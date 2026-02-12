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
        
        # Chart panes
        self.vre_production_pane = pn.pane.HoloViews(
            sizing_mode='stretch_width',
            height=500
        )
        
        # Initialize charts
        self._update_charts()
        
    def _update_charts(self, event=None):
        """Update all charts based on current selections."""
        try:
            # Update VRE production chart
            self.vre_production_pane.object = self._create_vre_production_chart()
        except Exception as e:
            logger.error(f"Error updating penetration charts: {e}")
            self.vre_production_pane.object = pn.pane.Markdown(
                f"Error loading data: {str(e)}"
            )
    
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
    
    def _get_generation_data(self, years: List[int]) -> pd.DataFrame:
        """
        Get generation data including rooftop for specified years.
        All data at 30-minute resolution.
        """
        all_data = []
        
        for year in years:
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
        years = [current_year - 2, current_year - 1, current_year]
        
        # Get generation data
        logger.info(f"Fetching generation data for years: {years}")
        df = self._get_generation_data(years)
        
        if df.empty:
            logger.warning("No generation data returned")
            return pn.pane.Markdown(
                '## No data available',
                styles={'text-align': 'center', 'padding': '100px'}
            )
        
        logger.info(f"Retrieved {len(df)} total rows of data")
        
        # Filter for selected fuel types
        if self.fuel_select.value == 'VRE':
            fuel_filter = ['Wind', 'Solar', 'Rooftop']
        else:
            fuel_filter = [self.fuel_select.value]
        
        df_filtered = df[df['fuel_type'].isin(fuel_filter)].copy()
        
        if df_filtered.empty:
            return pn.pane.Markdown(
                f'## No data available for {self.fuel_select.value}',
                styles={'text-align': 'center', 'padding': '100px'}
            )
        
        # Ensure datetime
        df_filtered['settlementdate'] = pd.to_datetime(df_filtered['settlementdate'])
        df_filtered['year'] = df_filtered['settlementdate'].dt.year
        df_filtered['dayofyear'] = df_filtered['settlementdate'].dt.dayofyear
        
        # Sum across fuel types for each timestamp (all data is 30-minute)
        hourly_sum = df_filtered.groupby(['settlementdate', 'year', 'dayofyear'])['total_generation_mw'].sum().reset_index()
        
        # Calculate daily average (48 half-hour periods per day)
        daily_avg = hourly_sum.groupby(['year', 'dayofyear'])['total_generation_mw'].mean().reset_index()
        
        logger.info(f"Daily average data shape: {daily_avg.shape}")
        
        # Prepare data for plotting
        colors = {
            years[0]: '#5DADE2',  # Light blue for 2023
            years[1]: '#F39C12',  # Orange for 2024
            years[2]: '#58D68D'   # Green for 2025
        }
        
        plots = []
        for year in years:
            year_data = daily_avg[daily_avg['year'] == year].copy()
            
            if not year_data.empty:
                # Sort by day of year
                year_data = year_data.sort_values('dayofyear')
                
                logger.info(f"Year {year}: {len(year_data)} days of data")
                
                # Apply 30-day moving average (not EWM for now to match reference)
                year_data['mw_smoothed'] = year_data['total_generation_mw'].rolling(
                    window=30, center=True, min_periods=15
                ).mean()
                
                # Fill any NaN values at edges
                year_data['mw_smoothed'] = year_data['mw_smoothed'].fillna(year_data['total_generation_mw'])
                
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
            return pn.pane.Markdown(
                '## No data available for selected criteria',
                styles={'text-align': 'center', 'padding': '100px'}
            )
        
        # Combine plots
        combined_plot = plots[0]
        for plot in plots[1:]:
            combined_plot = combined_plot * plot
        
        # Style the plot to match screenshot
        final_plot = combined_plot.opts(
            width=900,
            height=500,
            bgcolor='#2B2B3B',
            title='VRE production annualised over last 30 days',
            xlabel='day of year',
            ylabel='TWh',
            fontsize={'title': 18, 'labels': 12, 'xticks': 10, 'yticks': 10},
            show_grid=False,
            toolbar='above',
            legend_position='bottom_left',
            framewise=True,
            yformatter='%.0f',
            ylim=(45, 95)
        )
        
        return final_plot
    
    def create_layout(self) -> pn.Column:
        """Create the full penetration tab layout."""
        # Control panel
        controls = pn.Row(
            self.region_select,
            self.fuel_select,
            width_policy='min'
        )
        
        # Source attribution
        source_text = pn.pane.HTML(
            '<div style="text-align: right; color: #888; font-size: 12px; margin-right: 20px;">© ITK</div>',
            width_policy='max',
            height=20
        )
        
        # Main layout
        layout = pn.Column(
            pn.pane.Markdown("## Renewable Energy Penetration Analysis", 
                           styles={'color': '#008B8B'}),
            controls,
            self.vre_production_pane,
            source_text,
            sizing_mode='stretch_width'
        )
        
        return layout