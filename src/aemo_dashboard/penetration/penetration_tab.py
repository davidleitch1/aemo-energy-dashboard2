"""
Correctly optimized Penetration tab implementation.
Maintains exact calculation order while adding performance improvements.
"""
import pandas as pd
import numpy as np
import panel as pn
import hvplot.pandas
import holoviews as hv
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple, Any
from pathlib import Path
import os
import time

from aemo_dashboard.shared.logging_config import get_logger
from aemo_dashboard.shared.smoothing import apply_ewm_smoothing
from aemo_dashboard.shared.fuel_categories import (
    RENEWABLE_FUELS,
    THERMAL_FUELS,
    EXCLUDED_FROM_GENERATION
)
from aemo_dashboard.generation.generation_query_manager import GenerationQueryManager
from aemo_dashboard.shared.config import Config
from aemo_dashboard.shared.flexoki_theme import (
    FLEXOKI_PAPER,
    FLEXOKI_BLACK,
    FLEXOKI_BASE,
    FLEXOKI_ACCENT,
)

logger = get_logger(__name__)

# Optional LOESS import
try:
    from statsmodels.nonparametric.smoothers_lowess import lowess
    HAS_LOESS = True
except ImportError:
    HAS_LOESS = False
    logger.warning("statsmodels not available, LOESS smoothing disabled")

class PenetrationTab:
    """Optimized renewable energy penetration analysis tab."""
    
    def __init__(self):
        """Initialize the penetration tab with caching."""
        self.query_manager = GenerationQueryManager()
        self.config = Config()
        
        # Cache for expensive calculations
        self._cache = {}
        self._cache_timestamps = {}
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
        smoothing_options = ['Moving Average']
        if HAS_LOESS:
            smoothing_options.append('LOESS (No Lag)')
        # Add Exponential Weighted Moving Average options
        smoothing_options.extend([
            'EWM (14 days, minimal lag)',
            'EWM (30 days, balanced)',
            'EWM (60 days, smoother)'
        ])
            
        self.smoothing_select = pn.widgets.Select(
            name='Smoothing',
            value='Moving Average',
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
    
    def _get_cache_key(self, operation: str, **kwargs) -> str:
        """Generate cache key from operation and parameters."""
        parts = [operation]
        for k, v in sorted(kwargs.items()):
            if isinstance(v, list):
                v = tuple(v)
            parts.append(f"{k}={v}")
        return "|".join(parts)
    
    def _get_cached_or_compute(self, cache_key: str, compute_func: callable) -> Any:
        """Get from cache or compute and cache result."""
        # Check if cached and still valid
        if cache_key in self._cache and cache_key in self._cache_timestamps:
            age = datetime.now() - self._cache_timestamps[cache_key]
            if age.total_seconds() < self.cache_ttl:
                logger.debug(f"Cache hit for {cache_key}")
                return self._cache[cache_key]
        
        # Compute and cache
        logger.debug(f"Cache miss for {cache_key}, computing...")
        start_time = time.time()
        result = compute_func()
        compute_time = time.time() - start_time
        logger.info(f"Computed {cache_key} in {compute_time:.2f}s")
        
        self._cache[cache_key] = result
        self._cache_timestamps[cache_key] = datetime.now()
        return result
    
    def _update_charts(self, event=None):
        """Update all charts based on current selections."""
        try:
            # Clear existing plots
            self.vre_production_pane.object = None
            self.vre_by_fuel_pane.object = None
            self.thermal_vs_renewables_pane.object = None
            
            # Update charts
            self.vre_production_pane.object = self._create_vre_production_chart()
            self.vre_by_fuel_pane.object = self._create_vre_by_fuel_chart()
            self.thermal_vs_renewables_pane.object = self._create_thermal_vs_renewables_chart()
            
        except Exception as e:
            logger.error(f"Error updating penetration charts: {e}")
            self._show_error_plots(str(e))
    
    def _show_error_plots(self, error_msg: str):
        """Show error plots instead of empty panes."""
        empty_df = pd.DataFrame({'x': [0], 'y': [0]})
        error_plot = empty_df.hvplot.line(x='x', y='y', label='Error').opts(
            width=700, height=400, bgcolor=FLEXOKI_PAPER,
            title=f'Error loading data: {error_msg}',
            hooks=[self._get_legend_style_hook()]
        )
        self.vre_production_pane.object = error_plot
        self.vre_by_fuel_pane.object = error_plot

        error_plot_wide = empty_df.hvplot.line(x='x', y='y', label='Error').opts(
            width=1440, height=400, bgcolor=FLEXOKI_PAPER,
            title=f'Error loading data: {error_msg}',
            hooks=[self._get_legend_style_hook()]
        )
        self.thermal_vs_renewables_pane.object = error_plot_wide
    
    def _load_rooftop_30min(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Load rooftop data at 30-minute resolution."""
        rooftop_file = self.config.rooftop_solar_file
        
        if not rooftop_file or not Path(rooftop_file).exists():
            logger.warning(f"Rooftop file not found: {rooftop_file}")
            return pd.DataFrame()
        
        # Cache key for rooftop data
        cache_key = self._get_cache_key(
            'rooftop',
            start=start_date.isoformat(),
            end=end_date.isoformat()
        )
        
        def load_rooftop():
            df = pd.read_parquet(rooftop_file)
            df['settlementdate'] = pd.to_datetime(df['settlementdate'])
            df = df[(df['settlementdate'] >= start_date) & (df['settlementdate'] <= end_date)]
            
            if 'regionid' in df.columns:
                df_wide = df.pivot(
                    index='settlementdate',
                    columns='regionid', 
                    values='power'
                ).reset_index()
                return df_wide
            else:
                return df
        
        return self._get_cached_or_compute(cache_key, load_rooftop)
    
    def _get_generation_data(self, years: List[int], months_only_first_year: int = None) -> pd.DataFrame:
        """Get generation data including rooftop for specified years."""
        cache_key = self._get_cache_key(
            'generation',
            years=years,
            months_first=months_only_first_year,
            region=self.region_select.value
        )
        
        def load_generation():
            all_data = []
            
            for i, year in enumerate(years):
                if i == 0 and months_only_first_year is not None:
                    start_date = datetime(year, 12 - months_only_first_year + 1, 1)
                else:
                    start_date = datetime(year, 1, 1)
                end_date = datetime(year, 12, 31, 23, 59, 59)
                
                # Get generation data
                data = self.query_manager.query_generation_by_fuel(
                    start_date=start_date,
                    end_date=end_date,
                    region=self.region_select.value,
                    resolution='30min'
                )
                
                if not data.empty:
                    logger.info(f"Year {year}: Retrieved {len(data)} rows of generation data")
                    all_data.append(data)
                
                # Load rooftop data
                try:
                    rooftop_data = self._load_rooftop_30min(start_date, end_date)
                    
                    if not rooftop_data.empty:
                        # Convert to long format
                        if self.region_select.value == 'NEM':
                            region_cols = [col for col in rooftop_data.columns if col != 'settlementdate']
                            rooftop_long = pd.DataFrame({
                                'settlementdate': rooftop_data['settlementdate'],
                                'fuel_type': 'Rooftop',
                                'total_generation_mw': rooftop_data[region_cols].sum(axis=1)
                            })
                        else:
                            if self.region_select.value in rooftop_data.columns:
                                rooftop_long = pd.DataFrame({
                                    'settlementdate': rooftop_data['settlementdate'],
                                    'fuel_type': 'Rooftop',
                                    'total_generation_mw': rooftop_data[self.region_select.value]
                                })
                            else:
                                continue
                        
                        logger.info(f"Year {year}: Retrieved {len(rooftop_long)} rows of rooftop data")
                        all_data.append(rooftop_long)
                        
                except Exception as e:
                    logger.warning(f"Could not load rooftop data for year {year}: {e}")
            
            if not all_data:
                return pd.DataFrame()
                
            return pd.concat(all_data, ignore_index=True)
        
        return self._get_cached_or_compute(cache_key, load_generation)
    
    def _apply_smoothing_30day(self, df: pd.DataFrame, value_col: str = 'total_generation_mw') -> pd.DataFrame:
        """Apply 30-day smoothing based on selected method."""
        if self.smoothing_select.value == 'Moving Average':
            # Note: This method is different from others - it applies 30-day rolling average 
            # to 30-minute data FIRST, then samples at noon. This means the noon value 
            # represents the 30-day average around that point, not just noon generation.
            df['mw_rolling_30d'] = df[value_col].rolling(
                window=1440, center=False, min_periods=720
            ).mean()
            
            # Sample daily at noon (after smoothing)
            df['date'] = df['settlementdate'].dt.date
            df['hour'] = df['settlementdate'].dt.hour
            df['dayofyear'] = df['settlementdate'].dt.dayofyear
            df['year'] = df['settlementdate'].dt.year
            
            grouped = df.groupby(['year', 'dayofyear'])
            daily_data = grouped.apply(
                lambda x: x.iloc[x['hour'].sub(12).abs().argmin()] if len(x) > 0 else None
            ).reset_index(drop=True)
            
            daily_data['mw_smoothed'] = daily_data['mw_rolling_30d'].fillna(daily_data[value_col])
            return daily_data
            
        elif self.smoothing_select.value == 'LOESS (No Lag)' and HAS_LOESS:
            # First resample to daily averages (not noon values)
            df['settlementdate'] = pd.to_datetime(df['settlementdate'])
            df_daily = df.set_index('settlementdate').resample('D').agg({
                value_col: 'mean',
                # Keep other columns for reference
            }).reset_index()
            
            # Apply LOESS on daily averaged data
            date_numeric = df_daily['settlementdate'].astype(np.int64) / 1e9
            
            # Fraction for ~14 days (better for year-over-year comparison)
            # Since each year is shown separately, we want finer detail
            frac = min(0.05, max(0.01, 14 / len(df_daily)))
            
            smoothed = lowess(
                df_daily[value_col],
                date_numeric,
                frac=frac,
                it=0,
                return_sorted=False
            )
            
            df_daily['mw_smoothed'] = smoothed
            
            # Add back year/dayofyear columns for consistency
            df_daily['year'] = df_daily['settlementdate'].dt.year
            df_daily['dayofyear'] = df_daily['settlementdate'].dt.dayofyear
            
            return df_daily
            
        elif self.smoothing_select.value.startswith('EWM'):
            # Exponentially Weighted Moving Average
            # First resample to daily averages for efficiency
            df['settlementdate'] = pd.to_datetime(df['settlementdate'])
            df_daily = df.set_index('settlementdate').resample('D').agg({
                value_col: 'mean',
            }).reset_index()
            
            # Sort by date to ensure proper time series
            df_daily = df_daily.sort_values('settlementdate').reset_index(drop=True)
            
            # Extract span parameter from option
            if '14 days' in self.smoothing_select.value:
                span = 14
            elif '30 days' in self.smoothing_select.value:
                span = 30
            elif '60 days' in self.smoothing_select.value:
                span = 60
            else:
                span = 30  # Default
            
            # Apply EWM smoothing
            df_daily['mw_smoothed'] = df_daily[value_col].ewm(
                span=span, 
                adjust=False  # Use recursive calculation (more stable)
            ).mean()
            
            # Add year/dayofyear columns for consistency
            df_daily['year'] = df_daily['settlementdate'].dt.year
            df_daily['dayofyear'] = df_daily['settlementdate'].dt.dayofyear
            
            return df_daily
        
        else:
            # Fallback: return unsmoothed daily averages
            df['settlementdate'] = pd.to_datetime(df['settlementdate'])
            df_daily = df.set_index('settlementdate').resample('D').agg({
                value_col: 'mean',
            }).reset_index()
            
            df_daily['mw_smoothed'] = df_daily[value_col]
            
            # Add year/dayofyear columns for consistency
            df_daily['year'] = df_daily['settlementdate'].dt.year
            df_daily['dayofyear'] = df_daily['settlementdate'].dt.dayofyear
            
            return df_daily
    
    def _create_vre_production_chart(self):
        """Create the VRE production annualised chart."""
        # Cache key for the plot itself
        plot_cache_key = self._get_cache_key(
            'plot_vre_production',
            region=self.region_select.value,
            fuel=self.fuel_select.value,
            smoothing=self.smoothing_select.value
        )
        
        # Try to get cached plot
        if plot_cache_key in self._cache and plot_cache_key in self._cache_timestamps:
            age = datetime.now() - self._cache_timestamps[plot_cache_key]
            if age.total_seconds() < self.cache_ttl:
                logger.info("Using cached VRE production plot")
                return self._cache[plot_cache_key]
        
        # Get current year and two previous years
        current_year = datetime.now().year
        years_to_display = [current_year - 2, current_year - 1, current_year]
        
        # Load data with buffer
        earliest_year = min(years_to_display)
        years_to_load = [earliest_year - 1] + years_to_display
        
        logger.info(f"Fetching generation data for VRE production chart")
        df = self._get_generation_data(years_to_load, months_only_first_year=2)
        
        if df.empty:
            empty_df = pd.DataFrame({'x': [0], 'y': [0]})
            return empty_df.hvplot.line(x='x', y='y', label='No data').opts(
                width=700, height=400, bgcolor=FLEXOKI_PAPER,
                title=f'{self.fuel_select.value} production - No data available',
                hooks=[self._get_legend_style_hook()]
            )
        
        # Filter for selected fuel types
        if self.fuel_select.value == 'VRE':
            fuel_filter = ['Wind', 'Solar', 'Rooftop']
            df_filtered = df[df['fuel_type'].isin(fuel_filter)].copy()
        else:
            df_filtered = df[df['fuel_type'] == self.fuel_select.value].copy()
        
        if df_filtered.empty:
            empty_df = pd.DataFrame({'x': [0], 'y': [0]})
            return empty_df.hvplot.line(x='x', y='y', label='No data').opts(
                width=700, height=400, bgcolor=FLEXOKI_PAPER,
                title=f'{self.fuel_select.value} production - No data',
                hooks=[self._get_legend_style_hook()]
            )
        
        # Ensure datetime
        df_filtered['settlementdate'] = pd.to_datetime(df_filtered['settlementdate'])
        
        # Sum across fuel types
        hourly_sum = df_filtered.groupby('settlementdate')['total_generation_mw'].sum().reset_index()
        hourly_sum = hourly_sum.sort_values('settlementdate')
        
        # Apply smoothing and get daily data
        daily_data = self._apply_smoothing_30day(hourly_sum)
        
        # Prepare plots
        colors = {
            years_to_display[0]: '#5DADE2',
            years_to_display[1]: '#F39C12',
            years_to_display[2]: '#58D68D'
        }
        
        plots = []
        for year in years_to_display:
            year_data = daily_data[daily_data['year'] == year].copy()
            
            if not year_data.empty:
                year_data = year_data.sort_values('dayofyear')
                
                # Annualise
                year_data['twh_annualised'] = year_data['mw_smoothed'] * 24 * 365 / 1_000_000
                
                plot = year_data.hvplot.line(
                    x='dayofyear',
                    y='twh_annualised',
                    label=str(year),
                    color=colors[year],
                    line_width=2
                )
                plots.append(plot)
        
        if not plots:
            empty_df = pd.DataFrame({'x': [0], 'y': [0]})
            return empty_df.hvplot.line(x='x', y='y', label='No data').opts(
                width=700, height=400, bgcolor=FLEXOKI_PAPER,
                title=f'{self.fuel_select.value} production - No data',
                hooks=[self._get_legend_style_hook()]
            )
        
        # Combine plots
        combined_plot = plots[0]
        for plot in plots[1:]:
            combined_plot = combined_plot * plot
        
        # Style
        final_plot = combined_plot.opts(
            width=700,
            height=400,
            bgcolor=FLEXOKI_PAPER,
            title=f'{self.region_select.value} {self.fuel_select.value} production annualised',
            xlabel='day of year',
            ylabel='TWh',
            fontsize={'title': 14, 'labels': 12, 'xticks': 10, 'yticks': 10},
            show_grid=False,
            toolbar='above',
            legend_position='bottom_left',
            framewise=True,
            yformatter='%.0f',
            ylim=(None, None),
            hooks=[self._get_attribution_hook(), self._get_smoothing_text_hook(), self._get_legend_style_hook()]
        )

        # Cache the plot
        self._cache[plot_cache_key] = final_plot
        self._cache_timestamps[plot_cache_key] = datetime.now()

        return final_plot

    def _create_vre_by_fuel_chart(self):
        """Create the VRE production by fuel type chart."""
        # Cache key for the plot
        plot_cache_key = self._get_cache_key(
            'plot_vre_by_fuel',
            region=self.region_select.value,
            smoothing=self.smoothing_select.value
        )
        
        # Try to get cached plot
        if plot_cache_key in self._cache and plot_cache_key in self._cache_timestamps:
            age = datetime.now() - self._cache_timestamps[plot_cache_key]
            if age.total_seconds() < self.cache_ttl:
                logger.info("Using cached VRE by fuel plot")
                return self._cache[plot_cache_key]
        
        # Get data from 2018 onwards
        start_year = 2018
        current_year = datetime.now().year
        years = list(range(start_year, current_year + 1))
        
        logger.info(f"Creating VRE by fuel chart")
        df = self._get_generation_data(years)
        
        if df.empty:
            empty_df = pd.DataFrame({'x': [0], 'y': [0]})
            return empty_df.hvplot.line(x='x', y='y', label='No data').opts(
                width=700, height=400, bgcolor=FLEXOKI_PAPER,
                title='VRE production by fuel - No data available',
                hooks=[self._get_legend_style_hook()]
            )
        
        # Filter for VRE fuels
        df_vre = df[df['fuel_type'].isin(['Wind', 'Solar', 'Rooftop'])].copy()
        
        if df_vre.empty:
            empty_df = pd.DataFrame({'x': [0], 'y': [0]})
            return empty_df.hvplot.line(x='x', y='y', label='No data').opts(
                width=700, height=400, bgcolor=FLEXOKI_PAPER,
                title='VRE production by fuel - No VRE data',
                hooks=[self._get_legend_style_hook()]
            )
        
        # Ensure datetime
        df_vre['settlementdate'] = pd.to_datetime(df_vre['settlementdate'])
        df_vre = df_vre.sort_values('settlementdate')
        
        # Group by fuel type and timestamp
        fuel_data = df_vre.groupby(['settlementdate', 'fuel_type'])['total_generation_mw'].sum().reset_index()
        
        # Apply smoothing for each fuel type
        plots = []
        colors = {
            'Rooftop': '#5DADE2',
            'Solar': '#F39C12',
            'Wind': '#58D68D'
        }
        
        for fuel in ['Rooftop', 'Solar', 'Wind']:
            fuel_df = fuel_data[fuel_data['fuel_type'] == fuel].copy()
            
            if not fuel_df.empty:
                if self.smoothing_select.value == 'Moving Average':
                    # Original method: 30-day rolling average
                    fuel_df['mw_rolling_30d'] = fuel_df['total_generation_mw'].rolling(
                        window=1440, center=False, min_periods=720
                    ).mean()
                    fuel_df['twh_annualised'] = fuel_df['mw_rolling_30d'] * 24 * 365 / 1_000_000
                    
                elif self.smoothing_select.value == 'LOESS (No Lag)' and HAS_LOESS:
                    # Resample to daily first for efficiency
                    daily_fuel = fuel_df.set_index('settlementdate').resample('D')['total_generation_mw'].mean()
                    
                    date_numeric = daily_fuel.index.astype(np.int64) / 1e9
                    # Keep 30-day window for long-term trend analysis (7+ years of data)
                    frac = min(0.05, max(0.01, 30 / len(daily_fuel)))
                    
                    smoothed = lowess(
                        daily_fuel.values,
                        date_numeric,
                        frac=frac,
                        it=0,
                        return_sorted=False
                    )
                    
                    # Create dataframe with smoothed values
                    fuel_df = pd.DataFrame({
                        'settlementdate': daily_fuel.index,
                        'twh_annualised': smoothed * 24 * 365 / 1_000_000
                    })
                    
                elif self.smoothing_select.value.startswith('EWM'):
                    # Resample to daily for efficiency
                    daily_fuel = fuel_df.set_index('settlementdate').resample('D')['total_generation_mw'].mean()
                    
                    # Extract span parameter
                    if '14 days' in self.smoothing_select.value:
                        span = 14
                    elif '30 days' in self.smoothing_select.value:
                        span = 30
                    elif '60 days' in self.smoothing_select.value:
                        span = 60
                    else:
                        span = 30
                    
                    # Apply EWM smoothing
                    smoothed = daily_fuel.ewm(span=span, adjust=False).mean()
                    
                    # Create dataframe with smoothed values
                    fuel_df = pd.DataFrame({
                        'settlementdate': daily_fuel.index,
                        'twh_annualised': smoothed * 24 * 365 / 1_000_000
                    })
                        
                else:
                    # No smoothing
                    fuel_df['twh_annualised'] = fuel_df['total_generation_mw'] * 24 * 365 / 1_000_000
                
                # Create plot
                plot = fuel_df.hvplot.line(
                    x='settlementdate',
                    y='twh_annualised',
                    label=f'nem_{fuel.lower()}',
                    color=colors[fuel],
                    line_width=2
                )
                plots.append(plot)
        
        if not plots:
            empty_df = pd.DataFrame({'x': [0], 'y': [0]})
            return empty_df.hvplot.line(x='x', y='y', label='No data').opts(
                width=700, height=400, bgcolor=FLEXOKI_PAPER,
                title='VRE production by fuel - No data to plot',
                hooks=[self._get_legend_style_hook()]
            )
        
        # Combine plots
        combined_plot = plots[0]
        for plot in plots[1:]:
            combined_plot = combined_plot * plot
        
        # Style
        final_plot = combined_plot.opts(
            width=700,
            height=400,
            bgcolor=FLEXOKI_PAPER,
            title=f'{self.region_select.value} VRE production by fuel',
            xlabel='date',
            ylabel='TWh annualised',
            fontsize={'title': 14, 'labels': 12, 'xticks': 10, 'yticks': 10},
            show_grid=False,
            toolbar='above',
            legend_position='top_left',
            framewise=True,
            yformatter='%.0f',
            hooks=[self._get_attribution_hook(), self._get_smoothing_text_hook(), self._get_legend_style_hook()]
        )

        # Cache the plot
        self._cache[plot_cache_key] = final_plot
        self._cache_timestamps[plot_cache_key] = datetime.now()

        return final_plot

    def _create_thermal_vs_renewables_chart(self):
        """Create the thermal vs renewables chart with 180-day moving average."""
        # Cache key for the plot
        plot_cache_key = self._get_cache_key(
            'plot_thermal_renewables',
            region=self.region_select.value,
            smoothing=self.smoothing_select.value
        )
        
        # Try to get cached plot
        if plot_cache_key in self._cache and plot_cache_key in self._cache_timestamps:
            age = datetime.now() - self._cache_timestamps[plot_cache_key]
            if age.total_seconds() < self.cache_ttl:
                logger.info("Using cached thermal vs renewables plot")
                return self._cache[plot_cache_key]
        
        # Get data from 2018 onwards
        start_year = 2018
        current_year = datetime.now().year
        years = list(range(start_year, current_year + 1))
        
        logger.info(f"Creating thermal vs renewables chart")
        df = self._get_generation_data(years)
        
        if df.empty:
            empty_df = pd.DataFrame({'x': [0], 'y': [0]})
            return empty_df.hvplot.line(x='x', y='y', label='No data').opts(
                width=1440, height=400, bgcolor=FLEXOKI_PAPER,
                title='Thermal v Renewables - No data available',
                hooks=[self._get_legend_style_hook()]
            )
        
        # Define fuel categories - imported from fuel_categories module
        # Note: Using centralized RENEWABLE_FUELS and THERMAL_FUELS
        renewable_fuels = [f for f in RENEWABLE_FUELS if f in ['Wind', 'Solar', 'Rooftop', 'Rooftop Solar', 'Water', 'Hydro']]
        thermal_fuels = [f for f in THERMAL_FUELS if f in ['Coal', 'CCGT', 'OCGT', 'Gas other']]
        
        # Categorize
        df['category'] = df['fuel_type'].apply(
            lambda x: 'renewable' if x in renewable_fuels else ('thermal' if x in thermal_fuels else 'other')
        )
        
        # Filter
        df_filtered = df[df['category'].isin(['renewable', 'thermal'])].copy()
        
        if df_filtered.empty:
            empty_df = pd.DataFrame({'x': [0], 'y': [0]})
            return empty_df.hvplot.line(x='x', y='y', label='No data').opts(
                width=1440, height=400, bgcolor=FLEXOKI_PAPER,
                title='Thermal v Renewables - No thermal/renewable data',
                hooks=[self._get_legend_style_hook()]
            )
        
        # Ensure datetime
        df_filtered['settlementdate'] = pd.to_datetime(df_filtered['settlementdate'])
        df_filtered = df_filtered.sort_values('settlementdate')
        
        # Group and pivot
        category_data = df_filtered.groupby(['settlementdate', 'category'])['total_generation_mw'].sum().reset_index()
        pivot_data = category_data.pivot(index='settlementdate', columns='category', values='total_generation_mw').fillna(0)
        
        if self.smoothing_select.value == 'Moving Average':
            # Original method: 180-day rolling average on 30-minute data
            window_periods = 180 * 48
            
            pivot_data['renewable_ma'] = pivot_data['renewable'].rolling(
                window=window_periods, center=False, min_periods=window_periods//2
            ).mean()
            
            pivot_data['thermal_ma'] = pivot_data['thermal'].rolling(
                window=window_periods, center=False, min_periods=window_periods//2
            ).mean()
            
        elif self.smoothing_select.value == 'LOESS (No Lag)' and HAS_LOESS:
            # Resample to daily for efficiency
            daily_pivot = pivot_data.resample('D').mean()
            
            date_numeric = daily_pivot.index.astype(np.int64) / 1e9
            # Keep 180-day window for long-term transition analysis
            frac = min(0.15, max(0.05, 180 / len(daily_pivot)))
            
            renewable_smoothed = lowess(
                daily_pivot['renewable'], date_numeric, frac=frac, it=0, return_sorted=False
            )
            thermal_smoothed = lowess(
                daily_pivot['thermal'], date_numeric, frac=frac, it=0, return_sorted=False
            )
            
            # Create new dataframe with daily smoothed values
            pivot_data = pd.DataFrame({
                'renewable_ma': renewable_smoothed,
                'thermal_ma': thermal_smoothed
            }, index=daily_pivot.index)
            
        elif self.smoothing_select.value.startswith('EWM'):
            # Resample to daily for efficiency
            daily_pivot = pivot_data.resample('D').mean()
            
            # Extract span parameter
            if '14 days' in self.smoothing_select.value:
                span = 14
            elif '30 days' in self.smoothing_select.value:
                span = 30
            elif '60 days' in self.smoothing_select.value:
                span = 60
            else:
                span = 30
            
            # Apply EWM to both series
            pivot_data = pd.DataFrame({
                'renewable_ma': daily_pivot['renewable'].ewm(span=span, adjust=False).mean(),
                'thermal_ma': daily_pivot['thermal'].ewm(span=span, adjust=False).mean()
            }, index=daily_pivot.index)
            
        else:
            # No smoothing
            pivot_data['renewable_ma'] = pivot_data['renewable']
            pivot_data['thermal_ma'] = pivot_data['thermal']
        
        # Annualise
        pivot_data['renewable_twh'] = pivot_data['renewable_ma'] * 24 * 365 / 1_000_000
        pivot_data['thermal_twh'] = pivot_data['thermal_ma'] * 24 * 365 / 1_000_000
        
        # Reset index for plotting
        plot_data = pivot_data.reset_index()
        
        # Create plots
        colors = {
            'renewable': '#5DADE2',
            'coal+gas': '#F39C12'
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
            bgcolor=FLEXOKI_PAPER,
            title=f'{self.region_select.value} Thermal v Renewables annualised',
            xlabel='',
            ylabel='TWh',
            fontsize={'title': 14, 'labels': 12, 'xticks': 10, 'yticks': 10},
            show_grid=False,
            toolbar='above',
            legend_position='top_right',
            framewise=True,
            yformatter='%.0f',
            hooks=[self._get_attribution_hook(), self._get_smoothing_text_hook(), self._get_legend_style_hook()]
        )

        # Cache the plot
        self._cache[plot_cache_key] = final_plot
        self._cache_timestamps[plot_cache_key] = datetime.now()

        return final_plot

    def _get_attribution_hook(self, align='right', offset=-5):
        """Get a reusable attribution hook for hvplot charts
        
        Args:
            align: Alignment of attribution text ('left', 'center', 'right')
            offset: Vertical offset for the attribution text
            
        Returns:
            Hook function that adds attribution to plot
        """
        def add_attribution(plot, element):
            """Add attribution text to the plot after rendering"""
            try:
                from bokeh.models import Title
                # Get the plot figure
                p = plot.state
                # Add attribution as a subtitle below the plot
                attribution = Title(text='Design: ITK, Data: AEMO', 
                                  text_font_size='9pt',
                                  text_color='#6272a4',
                                  align=align,
                                  offset=offset)
                p.add_layout(attribution, 'below')
            except Exception as e:
                logger.debug(f"Could not add attribution: {e}")
        
        return add_attribution
    
    def _get_smoothing_text_hook(self):
        """Return a hook function that adds smoothing method text to the plot."""
        smoothing_text = self.smoothing_select.value

        def add_smoothing_text(plot, element):
            """Add smoothing method text below the plot."""
            try:
                import bokeh.models as bm

                # Create text annotation for smoothing method
                smoothing_label = bm.Label(
                    text=f'Smoothing: {smoothing_text}',
                    x=10, y=10,
                    x_units='screen', y_units='screen',
                    text_font_size='10pt',
                    text_color='#888888',
                    background_fill_color=None,
                    border_line_color=None
                )

                plot.state.add_layout(smoothing_label)
            except Exception as e:
                logger.debug(f"Could not add smoothing text: {e}")

        return add_smoothing_text

    def _get_legend_style_hook(self):
        """Return a hook function that styles the legend and plot backgrounds to match Flexoki theme."""
        def style_legend(plot, element):
            """Style the legend and plot backgrounds to match FLEXOKI_PAPER."""
            try:
                p = plot.state
                # Explicitly set plot area and outer frame backgrounds
                p.background_fill_color = FLEXOKI_PAPER
                p.border_fill_color = FLEXOKI_PAPER

                # Style legend if present
                if p.legend:
                    for legend in p.legend:
                        legend.background_fill_color = FLEXOKI_PAPER
                        legend.border_line_color = FLEXOKI_BASE[150]
                        legend.border_line_width = 1
                        legend.label_text_color = FLEXOKI_BLACK
            except Exception as e:
                logger.debug(f"Could not style legend: {e}")

        return style_legend
    
    def create_layout(self) -> pn.Column:
        """Create the full penetration tab layout."""
        # Create controls row
        controls_row = pn.Row(
            self.region_select,
            self.fuel_select,
            self.smoothing_select,
            width_policy='min',
            margin=(10, 0)
        )
        
        # Create top row
        vre_chart_column = pn.Column(
            self.vre_production_pane,
            width=720,
            margin=(0, 10, 0, 0)
        )
        
        fuel_chart_column = pn.Column(
            self.vre_by_fuel_pane,
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
            width=1460,
            margin=(20, 0, 0, 0)
        )
        
        # Main layout
        layout = pn.Column(
            pn.pane.Markdown("## Renewable Energy Trends Analysis", 
                           styles={'color': '#008B8B'}),
            controls_row,
            top_row,
            bottom_row,
            sizing_mode='fixed',
            width=1480
        )
        
        return layout