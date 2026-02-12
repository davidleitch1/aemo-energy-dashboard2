"""
Penetration tab implementation for the AEMO Energy Dashboard.
Shows renewable energy penetration metrics and trends.
"""
import pandas as pd
import numpy as np
import panel as pn
import hvplot.pandas
import holoviews as hv
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple

from aemo_dashboard.shared.logging_config import get_logger
from aemo_dashboard.shared.smoothing import apply_ewm_smoothing
from aemo_dashboard.generation.generation_query_manager import GenerationQueryManager
from aemo_dashboard.shared.rooftop_adapter import load_rooftop_data

logger = get_logger(__name__)

class PenetrationTab:
    """Renewable energy penetration analysis tab."""
    
    def __init__(self):
        """Initialize the penetration tab."""
        self.query_manager = GenerationQueryManager()
        
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
    
    def _get_generation_data(self, years: List[int]) -> pd.DataFrame:
        """
        Get generation data for specified years.
        
        Parameters
        ----------
        years : List[int]
            List of years to fetch data for
            
        Returns
        -------
        pd.DataFrame
            Generation data with columns: settlementdate, fuel_type, total_generation_mw
        """
        all_data = []
        
        for year in years:
            start_date = datetime(year, 1, 1)
            end_date = datetime(year, 12, 31, 23, 59, 59)
            
            # Use 30-minute resolution for full year data
            data = self.query_manager.query_generation_by_fuel(
                start_date=start_date,
                end_date=end_date,
                region=self.region_select.value,
                resolution='30min'
            )
            
            if not data.empty:
                logger.info(f"Year {year}: Retrieved {len(data)} rows")
                logger.info(f"Year {year} columns: {data.columns.tolist()}")
                logger.info(f"Year {year} fuel types: {data['fuel_type'].unique()}")
                all_data.append(data)
            else:
                logger.warning(f"No data for year {year}")
                
            # Also load rooftop data for this year
            try:
                rooftop_data = load_rooftop_data(
                    start_date=start_date,
                    end_date=end_date,
                    resolution='30min'  # Match generation data resolution
                )
                
                if not rooftop_data.empty:
                    # Rooftop data comes in wide format with regions as columns
                    # Convert to long format matching generation data structure
                    
                    # If NEM is selected, sum all regions
                    if self.region_select.value == 'NEM':
                        # Sum across all region columns (excluding settlementdate)
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
        """
        Create the VRE production annualised chart.
        
        Returns
        -------
        hvplot.Plot
            The VRE production chart
        """
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
        
        logger.info(f"Retrieved {len(df)} rows of generation data")
        
        # Filter for selected fuel types
        if self.fuel_select.value == 'VRE':
            fuel_filter = ['Wind', 'Solar', 'Rooftop']
        else:
            fuel_filter = [self.fuel_select.value]
        
        df_filtered = df[df['fuel_type'].isin(fuel_filter)].copy()
        
        # Group by date and sum across fuel types
        df_filtered['date'] = pd.to_datetime(df_filtered['settlementdate'])
        df_filtered['year'] = df_filtered['date'].dt.year
        df_filtered['dayofyear'] = df_filtered['date'].dt.dayofyear
        
        # Calculate daily totals
        # First, ensure settlementdate is datetime
        df_filtered['settlementdate'] = pd.to_datetime(df_filtered['settlementdate'])
        
        # Resample all data to 30-minute intervals to ensure consistency
        # Group by fuel type first, then resample
        resampled_data = []
        for fuel in df_filtered['fuel_type'].unique():
            fuel_data = df_filtered[df_filtered['fuel_type'] == fuel].copy()
            fuel_data = fuel_data.set_index('settlementdate')
            # Resample to 30min and take mean (for 5-min data this averages, for 30-min it's unchanged)
            fuel_resampled = fuel_data.groupby('fuel_type')['total_generation_mw'].resample('30min').mean().reset_index()
            fuel_resampled['year'] = fuel_resampled['settlementdate'].dt.year
            fuel_resampled['dayofyear'] = fuel_resampled['settlementdate'].dt.dayofyear
            resampled_data.append(fuel_resampled)
        
        df_resampled = pd.concat(resampled_data, ignore_index=True)
        
        # Now aggregate: sum across fuel types for each timestamp
        timestamp_sum = df_resampled.groupby(['settlementdate', 'year', 'dayofyear'])['total_generation_mw'].sum().reset_index()
        
        # Then calculate daily average (48 half-hour periods per day)
        daily_avg = timestamp_sum.groupby(['year', 'dayofyear'])['total_generation_mw'].mean().reset_index()
        
        logger.info(f"Daily average data shape: {daily_avg.shape}")
        logger.info(f"Sample daily_avg data:\n{daily_avg.head()}")
        
        # Prepare data for plotting
        plot_data = []
        colors = {
            years[0]: '#5DADE2',  # Light blue for 2023
            years[1]: '#F39C12',  # Orange for 2024
            years[2]: '#58D68D'   # Green for 2025
        }
        
        plots = []
        for year in years:
            year_data = daily_avg[daily_avg['year'] == year].copy()
            
            if not year_data.empty:
                logger.info(f"Year {year}: {len(year_data)} days of data")
                
                # Apply EWM smoothing to daily MW values FIRST
                year_data['mw_smoothed'] = apply_ewm_smoothing(
                    year_data['total_generation_mw'],
                    span=30
                )
                
                # Then annualise the smoothed values
                # MW * 24 hours * 365 days / 1,000,000 = TWh
                year_data['twh_annualised'] = year_data['mw_smoothed'] * 24 * 365 / 1_000_000
                
                logger.info(f"Year {year} MW range: {year_data['total_generation_mw'].min():.2f} - {year_data['total_generation_mw'].max():.2f}")
                logger.info(f"Year {year} smoothed MW range: {year_data['mw_smoothed'].min():.2f} - {year_data['mw_smoothed'].max():.2f}")
                logger.info(f"Year {year} annualised TWh range: {year_data['twh_annualised'].min():.2f} - {year_data['twh_annualised'].max():.2f}")
                
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
            bgcolor='#2B2B3B',  # Dark background like screenshot
            title=f'VRE production annualised over last 30 days',
            xlabel='day of year',
            ylabel='TWh',
            fontsize={'title': 18, 'labels': 12, 'xticks': 10, 'yticks': 10},
            show_grid=False,
            toolbar='above',
            legend_position='bottom_left',
            framewise=True,
            yformatter='%.0f',
            ylim=(45, 95)  # Match screenshot scale
        )
        
        # Add source attribution
        # Note: hvplot doesn't support text annotations easily, 
        # so we'll add it as part of the layout
        
        return final_plot
    
    def create_layout(self) -> pn.Column:
        """
        Create the full penetration tab layout.
        
        Returns
        -------
        pn.Column
            The complete tab layout
        """
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