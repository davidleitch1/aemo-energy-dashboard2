"""
Optimized Penetration tab implementation with improved performance.
Key optimizations:
1. Daily pre-aggregation for multi-year data
2. Efficient rolling averages on daily data (30 vs 1440 window)
3. Optional LOESS smoothing for better visual quality
4. Caching of processed data
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
from functools import lru_cache

from aemo_dashboard.shared.logging_config import get_logger
from aemo_dashboard.shared.smoothing import apply_ewm_smoothing
from aemo_dashboard.generation.generation_query_manager import GenerationQueryManager
from aemo_dashboard.shared.config import Config

logger = get_logger(__name__)

# Optional imports for advanced smoothing
try:
    from statsmodels.nonparametric.smoothers_lowess import lowess
    HAS_LOESS = True
except ImportError:
    HAS_LOESS = False
    logger.warning("statsmodels not available, LOESS smoothing disabled")

class OptimizedPenetrationTab:
    """Optimized renewable energy penetration analysis tab."""
    
    def __init__(self):
        """Initialize the penetration tab."""
        self.query_manager = GenerationQueryManager()
        self.config = Config()
        
        # Cache for processed data
        self._data_cache = {}
        self._cache_timestamp = {}
        self.cache_ttl = 300  # 5 minutes
        
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
        
        # Add smoothing method selector
        smoothing_options = ['Rolling Average', 'EWM']
        if HAS_LOESS:
            smoothing_options.append('LOESS')
            
        self.smoothing_select = pn.widgets.Select(
            name='Smoothing',
            value='Rolling Average',
            options=smoothing_options,
            width=150
        )
        
        # Bind update method to widgets
        self.region_select.param.watch(self._update_charts, 'value')
        self.fuel_select.param.watch(self._update_charts, 'value')
        self.smoothing_select.param.watch(self._update_charts, 'value')
        
        # Chart panes
        self.vre_production_pane = pn.pane.HoloViews(
            object=None,
            sizing_mode='stretch_width',
            height=500,
            linked_axes=False
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
    
    def _get_cache_key(self, chart_type: str) -> str:
        """Generate cache key based on current selections"""
        return f"{chart_type}_{self.region_select.value}_{self.fuel_select.value}_{self.smoothing_select.value}"
    
    def _is_cache_valid(self, key: str) -> bool:
        """Check if cached data is still valid"""
        if key not in self._cache_timestamp:
            return False
        age = datetime.now() - self._cache_timestamp[key]
        return age.total_seconds() < self.cache_ttl
    
    def _update_charts(self, event=None):
        """Update all charts based on current selections."""
        try:
            # Clear existing plots
            self.vre_production_pane.object = None
            self.vre_by_fuel_pane.object = None
            self.thermal_vs_renewables_pane.object = None
            
            # Update charts with caching
            self.vre_production_pane.object = self._create_vre_production_chart()
            self.vre_by_fuel_pane.object = self._create_vre_by_fuel_chart()
            self.thermal_vs_renewables_pane.object = self._create_thermal_vs_renewables_chart()
            
        except Exception as e:
            logger.error(f"Error updating penetration charts: {e}")
            self._show_error_plots(str(e))
    
    def _show_error_plots(self, error_msg: str):
        """Show error plots instead of empty panes"""
        empty_df = pd.DataFrame({'x': [0], 'y': [0]})
        error_plot = empty_df.hvplot.line(x='x', y='y', label='Error').opts(
            width=700, height=400, bgcolor='#2B2B3B', 
            title=f'Error loading data: {error_msg}'
        )
        self.vre_production_pane.object = error_plot
        self.vre_by_fuel_pane.object = error_plot
        
        # Thermal chart is wider
        error_plot_wide = empty_df.hvplot.line(x='x', y='y', label='Error').opts(
            width=1440, height=400, bgcolor='#2B2B3B', 
            title=f'Error loading data: {error_msg}'
        )
        self.thermal_vs_renewables_pane.object = error_plot_wide
    
    def _get_generation_data_daily(self, years: List[int], months_only_first_year: int = None) -> pd.DataFrame:
        """
        Get generation data with daily aggregation for performance.
        Uses 30-min data but aggregates to daily immediately.
        """
        cache_key = f"gen_data_{years}_{months_only_first_year}_{self.region_select.value}"
        
        # Check cache
        if self._is_cache_valid(cache_key):
            logger.info(f"Using cached data for {cache_key}")
            return self._data_cache[cache_key]
        
        all_data = []
        
        for i, year in enumerate(years):
            if i == 0 and months_only_first_year is not None:
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
                # Convert to daily immediately for performance
                data['settlementdate'] = pd.to_datetime(data['settlementdate'])
                data['date'] = data['settlementdate'].dt.date
                
                # Daily aggregation by fuel type
                daily = data.groupby(['date', 'fuel_type'])['total_generation_mw'].mean().reset_index()
                daily.columns = ['settlementdate', 'fuel_type', 'total_generation_mw']
                
                logger.info(f"Year {year}: Aggregated to {len(daily)} daily records")
                all_data.append(daily)
                
            # Load rooftop data
            try:
                rooftop_data = self._load_rooftop_daily(start_date, end_date)
                if not rooftop_data.empty:
                    all_data.append(rooftop_data)
            except Exception as e:
                logger.warning(f"Could not load rooftop data for year {year}: {e}")
        
        if not all_data:
            return pd.DataFrame()
        
        result = pd.concat(all_data, ignore_index=True)
        
        # Cache the result
        self._data_cache[cache_key] = result
        self._cache_timestamp[cache_key] = datetime.now()
        
        return result
    
    def _load_rooftop_daily(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Load rooftop data and aggregate to daily"""
        rooftop_file = self.config.rooftop_solar_file
        
        if not rooftop_file or not Path(rooftop_file).exists():
            return pd.DataFrame()
        
        # Load parquet file
        df = pd.read_parquet(rooftop_file)
        
        # Filter date range
        df['settlementdate'] = pd.to_datetime(df['settlementdate'])
        df = df[(df['settlementdate'] >= start_date) & (df['settlementdate'] <= end_date)]
        
        if df.empty:
            return pd.DataFrame()
        
        # Convert to daily
        df['date'] = df['settlementdate'].dt.date
        
        # Handle different formats
        if 'regionid' in df.columns:
            # Long format
            if self.region_select.value == 'NEM':
                daily = df.groupby('date')['power'].mean().reset_index()
                daily['fuel_type'] = 'Rooftop'
                daily.columns = ['settlementdate', 'total_generation_mw', 'fuel_type']
            else:
                daily = df[df['regionid'] == self.region_select.value].groupby('date')['power'].mean().reset_index()
                daily['fuel_type'] = 'Rooftop'
                daily.columns = ['settlementdate', 'total_generation_mw', 'fuel_type']
        else:
            # Wide format
            if self.region_select.value == 'NEM':
                region_cols = [col for col in df.columns if col not in ['settlementdate', 'date']]
                daily = pd.DataFrame({
                    'settlementdate': df.groupby('date')[region_cols].mean().index,
                    'total_generation_mw': df.groupby('date')[region_cols].sum(axis=1).mean(),
                    'fuel_type': 'Rooftop'
                })
            else:
                if self.region_select.value in df.columns:
                    daily = pd.DataFrame({
                        'settlementdate': df.groupby('date')[self.region_select.value].mean().index,
                        'total_generation_mw': df.groupby('date')[self.region_select.value].mean(),
                        'fuel_type': 'Rooftop'
                    })
                else:
                    return pd.DataFrame()
        
        return daily[['settlementdate', 'fuel_type', 'total_generation_mw']]
    
    def _apply_smoothing(self, data: pd.DataFrame, value_col: str, window: int = 30) -> pd.Series:
        """Apply selected smoothing method"""
        if self.smoothing_select.value == 'Rolling Average':
            return data[value_col].rolling(window=window, center=False, min_periods=window//2).mean()
        
        elif self.smoothing_select.value == 'EWM':
            return apply_ewm_smoothing(data[value_col], span=window, min_periods=window//2)
        
        elif self.smoothing_select.value == 'LOESS' and HAS_LOESS:
            # Convert date to numeric for LOESS
            if 'settlementdate' in data.columns:
                date_numeric = pd.to_datetime(data['settlementdate']).astype(np.int64) / 1e9
            else:
                date_numeric = np.arange(len(data))
            
            # Apply LOESS with reasonable frac for daily data
            frac = min(0.1, max(0.02, 30 / len(data)))  # Adaptive fraction
            smoothed = lowess(
                data[value_col],
                date_numeric,
                frac=frac,
                it=0,  # No iterations for speed
                return_sorted=False
            )
            return pd.Series(smoothed, index=data.index)
        
        else:
            return data[value_col]
    
    def _create_vre_production_chart(self):
        """Create the VRE production annualised chart with daily data."""
        cache_key = self._get_cache_key('vre_production')
        if self._is_cache_valid(cache_key):
            return self._data_cache[cache_key]
        
        # Get current year and two previous years
        current_year = datetime.now().year
        years_to_display = [current_year - 2, current_year - 1, current_year]
        
        # Load data with buffer for smoothing
        earliest_year = min(years_to_display)
        years_to_load = [earliest_year - 1] + years_to_display
        
        logger.info(f"Loading daily data for VRE production chart")
        df = self._get_generation_data_daily(years_to_load, months_only_first_year=2)
        
        if df.empty:
            return self._create_empty_plot('VRE production')
        
        # Filter for selected fuel types
        if self.fuel_select.value == 'VRE':
            fuel_filter = ['Wind', 'Solar', 'Rooftop']
            df_filtered = df[df['fuel_type'].isin(fuel_filter)].copy()
        else:
            df_filtered = df[df['fuel_type'] == self.fuel_select.value].copy()
        
        if df_filtered.empty:
            return self._create_empty_plot(f'{self.fuel_select.value} production')
        
        # Ensure datetime and aggregate by date
        df_filtered['settlementdate'] = pd.to_datetime(df_filtered['settlementdate'])
        df_filtered['year'] = df_filtered['settlementdate'].dt.year
        df_filtered['dayofyear'] = df_filtered['settlementdate'].dt.dayofyear
        
        # Sum across fuel types for each date
        daily_sum = df_filtered.groupby(['settlementdate', 'year', 'dayofyear'])['total_generation_mw'].sum().reset_index()
        daily_sum = daily_sum.sort_values('settlementdate')
        
        # Apply smoothing (30-day window on daily data)
        daily_sum['mw_smoothed'] = self._apply_smoothing(daily_sum, 'total_generation_mw', window=30)
        
        # Annualise
        daily_sum['twh_annualised'] = daily_sum['mw_smoothed'] * 24 * 365 / 1_000_000
        
        # Create plots for each year
        plots = []
        colors = {
            years_to_display[0]: '#5DADE2',  # Light blue
            years_to_display[1]: '#F39C12',  # Orange
            years_to_display[2]: '#58D68D'   # Green
        }
        
        for year in years_to_display:
            year_data = daily_sum[daily_sum['year'] == year].copy()
            
            if not year_data.empty:
                plot = year_data.hvplot.line(
                    x='dayofyear',
                    y='twh_annualised',
                    label=str(year),
                    color=colors[year],
                    line_width=2
                )
                plots.append(plot)
        
        if not plots:
            return self._create_empty_plot('VRE production')
        
        # Combine and style
        combined_plot = plots[0]
        for plot in plots[1:]:
            combined_plot = combined_plot * plot
        
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
            ylim=(None, None)
        )
        
        # Cache the result
        self._data_cache[cache_key] = final_plot
        self._cache_timestamp[cache_key] = datetime.now()
        
        return final_plot
    
    def _create_vre_by_fuel_chart(self):
        """Create the VRE production by fuel type chart with daily data."""
        cache_key = self._get_cache_key('vre_by_fuel')
        if self._is_cache_valid(cache_key):
            return self._data_cache[cache_key]
        
        # Get data from 2018 onwards
        start_year = 2018
        current_year = datetime.now().year
        years = list(range(start_year, current_year + 1))
        
        logger.info(f"Loading daily data for VRE by fuel chart")
        df = self._get_generation_data_daily(years)
        
        if df.empty:
            return self._create_empty_plot('VRE by fuel')
        
        # Filter for VRE fuels
        df_vre = df[df['fuel_type'].isin(['Wind', 'Solar', 'Rooftop'])].copy()
        
        if df_vre.empty:
            return self._create_empty_plot('VRE by fuel')
        
        # Ensure datetime
        df_vre['settlementdate'] = pd.to_datetime(df_vre['settlementdate'])
        df_vre = df_vre.sort_values('settlementdate')
        
        # Create plots for each fuel type
        plots = []
        colors = {
            'Rooftop': '#5DADE2',  # Light blue
            'Solar': '#F39C12',    # Orange  
            'Wind': '#58D68D'      # Green
        }
        
        for fuel in ['Rooftop', 'Solar', 'Wind']:
            fuel_df = df_vre[df_vre['fuel_type'] == fuel].copy()
            
            if not fuel_df.empty:
                # Apply smoothing
                fuel_df['mw_smoothed'] = self._apply_smoothing(fuel_df, 'total_generation_mw', window=30)
                
                # Annualise
                fuel_df['twh_annualised'] = fuel_df['mw_smoothed'] * 24 * 365 / 1_000_000
                
                # Create line plot
                plot = fuel_df.hvplot.line(
                    x='settlementdate',
                    y='twh_annualised',
                    label=f'nem_{fuel.lower()}',
                    color=colors[fuel],
                    line_width=2
                )
                plots.append(plot)
        
        if not plots:
            return self._create_empty_plot('VRE by fuel')
        
        # Combine and style
        combined_plot = plots[0]
        for plot in plots[1:]:
            combined_plot = combined_plot * plot
        
        final_plot = combined_plot.opts(
            width=700,
            height=400,
            bgcolor='#2B2B3B',
            title=f'VRE production by fuel rolling 30 day avg - {self.region_select.value}',
            xlabel='date',
            ylabel='TWh annualised',
            fontsize={'title': 14, 'labels': 12, 'xticks': 10, 'yticks': 10},
            show_grid=False,
            toolbar='above',
            legend_position='top_left',
            framewise=True,
            yformatter='%.0f'
        )
        
        # Cache the result
        self._data_cache[cache_key] = final_plot
        self._cache_timestamp[cache_key] = datetime.now()
        
        return final_plot
    
    def _create_thermal_vs_renewables_chart(self):
        """Create thermal vs renewables chart with 180-day smoothing on daily data."""
        cache_key = self._get_cache_key('thermal_renewables')
        if self._is_cache_valid(cache_key):
            return self._data_cache[cache_key]
        
        # Get data from 2018 onwards
        start_year = 2018
        current_year = datetime.now().year
        years = list(range(start_year, current_year + 1))
        
        logger.info(f"Loading daily data for thermal vs renewables chart")
        df = self._get_generation_data_daily(years)
        
        if df.empty:
            return self._create_empty_plot('Thermal vs Renewables', wide=True)
        
        # Define fuel categories
        renewable_fuels = ['Wind', 'Solar', 'Rooftop', 'Water']
        thermal_fuels = ['Coal', 'CCGT', 'OCGT', 'Gas other']
        
        # Categorize
        df['category'] = df['fuel_type'].apply(
            lambda x: 'renewable' if x in renewable_fuels else ('thermal' if x in thermal_fuels else 'other')
        )
        
        # Filter and aggregate by date and category
        df_filtered = df[df['category'].isin(['renewable', 'thermal'])].copy()
        
        if df_filtered.empty:
            return self._create_empty_plot('Thermal vs Renewables', wide=True)
        
        # Ensure datetime
        df_filtered['settlementdate'] = pd.to_datetime(df_filtered['settlementdate'])
        category_data = df_filtered.groupby(['settlementdate', 'category'])['total_generation_mw'].sum().reset_index()
        
        # Pivot
        pivot_data = category_data.pivot(index='settlementdate', columns='category', values='total_generation_mw').fillna(0)
        
        # Apply 180-day smoothing (on daily data, so window=180)
        if self.smoothing_select.value == 'LOESS' and HAS_LOESS:
            # Special handling for LOESS on long series
            date_numeric = pivot_data.index.astype(np.int64) / 1e9
            frac = min(0.15, max(0.05, 180 / len(pivot_data)))
            
            pivot_data['renewable_smoothed'] = lowess(
                pivot_data['renewable'], date_numeric, frac=frac, it=0, return_sorted=False
            )
            pivot_data['thermal_smoothed'] = lowess(
                pivot_data['thermal'], date_numeric, frac=frac, it=0, return_sorted=False
            )
        else:
            # Use rolling average or EWM
            pivot_data['renewable_smoothed'] = self._apply_smoothing(
                pivot_data.reset_index(), 'renewable', window=180
            ).values
            pivot_data['thermal_smoothed'] = self._apply_smoothing(
                pivot_data.reset_index(), 'thermal', window=180
            ).values
        
        # Annualise
        pivot_data['renewable_twh'] = pivot_data['renewable_smoothed'] * 24 * 365 / 1_000_000
        pivot_data['thermal_twh'] = pivot_data['thermal_smoothed'] * 24 * 365 / 1_000_000
        
        # Reset index for plotting
        plot_data = pivot_data.reset_index()
        
        # Create plots
        colors = {
            'renewable': '#5DADE2',  # Light blue
            'coal+gas': '#F39C12'    # Orange
        }
        
        renewable_plot = plot_data.hvplot.line(
            x='settlementdate',
            y='renewable_twh',
            label='renewable',
            color=colors['renewable'],
            line_width=2
        )
        
        thermal_plot = plot_data.hvplot.line(
            x='settlementdate',
            y='thermal_twh',
            label='coal+gas',
            color=colors['coal+gas'],
            line_width=2
        )
        
        # Combine and style
        combined_plot = renewable_plot * thermal_plot
        
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
        )
        
        # Cache the result
        self._data_cache[cache_key] = final_plot
        self._cache_timestamp[cache_key] = datetime.now()
        
        return final_plot
    
    def _create_empty_plot(self, title: str, wide: bool = False):
        """Create an empty plot with appropriate dimensions"""
        empty_df = pd.DataFrame({'x': [0], 'y': [0]})
        width = 1440 if wide else 700
        return empty_df.hvplot.line(x='x', y='y', label='No data').opts(
            width=width, height=400, bgcolor='#2B2B3B', 
            title=f'{title} - No data available'
        )
    
    def create_layout(self) -> pn.Column:
        """Create the full penetration tab layout."""
        # Source attribution
        source_text = pn.pane.HTML(
            '<div style="text-align: right; color: #888; font-size: 10px; margin-right: 10px; margin-top: -10px;">© ITK</div>',
            height=15
        )
        
        # Create controls row
        controls_row = pn.Row(
            self.region_select,
            self.fuel_select,
            self.smoothing_select,
            width_policy='min',
            margin=(10, 0)
        )
        
        # Create top row with two charts
        vre_chart_column = pn.Column(
            self.vre_production_pane,
            source_text.clone(),
            width=720,
            margin=(0, 10, 0, 0)
        )
        
        fuel_chart_column = pn.Column(
            self.vre_by_fuel_pane,
            source_text.clone(),
            width=720,
            margin=(0, 0, 0, 10)
        )
        
        top_row = pn.Row(
            vre_chart_column,
            fuel_chart_column,
            sizing_mode='fixed',
            align='start'
        )
        
        # Create bottom row
        bottom_row = pn.Column(
            self.thermal_vs_renewables_pane,
            source_text.clone(),
            width=1460,
            margin=(20, 0, 0, 0)
        )
        
        # Main layout
        layout = pn.Column(
            pn.pane.Markdown("## Renewable Energy Penetration Analysis (Optimized)", 
                           styles={'color': '#008B8B'}),
            controls_row,
            top_row,
            bottom_row,
            sizing_mode='fixed',
            width=1480
        )
        
        return layout